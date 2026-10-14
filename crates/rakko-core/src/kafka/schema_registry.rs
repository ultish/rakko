//! Thin Schema Registry client with a local schema-id cache.
//!
//! Decode paths look up the cache only (sync). Background tasks call
//! [`fetch_schema_by_id`] and then [`SchemaRegistry::insert_cached`].

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use apache_avro::types::Value as AvroValue;
use apache_avro::Schema;
use serde::Deserialize;

use crate::error::{AppError, AppResult};

/// Upper bound on distinct referenced schemas fetched for one schema id — guards
/// against a pathological or cyclic reference graph hammering the registry.
const MAX_REFERENCED_SCHEMAS: usize = 64;

/// A parsed writer schema plus the named schemas it references (Confluent "schema
/// references": `references: [{name, subject, version}]`). The root schema contains
/// `Schema::Ref`s into `schemata`, so decoding must hand both to apache-avro.
#[derive(Debug, Clone)]
pub struct CachedSchema {
    schema: Schema,
    schemata: Vec<Schema>,
}

impl CachedSchema {
    /// A self-contained schema with no references.
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            schemata: Vec::new(),
        }
    }

    /// Parse `schema_json` resolving named types from the `references` schemas.
    /// With no references this is plain `Schema::parse_str`.
    pub fn parse(schema_json: &str, references: &[String]) -> AppResult<Self> {
        if references.is_empty() {
            let schema = Schema::parse_str(schema_json)
                .map_err(|err| AppError::SchemaRegistry(format!("failed to parse Avro schema: {err}")))?;
            return Ok(Self::new(schema));
        }
        let (schema, schemata) = Schema::parse_str_with_list(schema_json, references).map_err(|err| {
            AppError::SchemaRegistry(format!(
                "failed to parse Avro schema with {} reference(s): {err}",
                references.len()
            ))
        })?;
        Ok(Self { schema, schemata })
    }

    /// Decode one Avro datum (the bytes after the 5-byte Confluent header).
    pub fn decode_datum(&self, payload: &[u8]) -> Option<AvroValue> {
        let mut cursor = payload;
        if self.schemata.is_empty() {
            return apache_avro::from_avro_datum(&self.schema, &mut cursor, None).ok();
        }
        // apache-avro registers names in a single pass, so every schema must come
        // after the ones it references: dependencies first (see
        // `dependencies_first`), the root last.
        let all: Vec<&Schema> = self
            .schemata
            .iter()
            .chain(std::iter::once(&self.schema))
            .collect();
        apache_avro::from_avro_datum_schemata(&self.schema, all, &mut cursor, None).ok()
    }
}

impl CachedSchema {
    /// A JSON value shaped like a decoded message of this schema, with *every* field
    /// present (union branches merged, arrays holding one element). Autocomplete walks
    /// it like a sample message, so it offers all fields the schema allows — including
    /// optional ones no recent message happened to carry.
    pub fn skeleton(&self) -> serde_json::Value {
        let mut named = HashMap::new();
        for schema in self.schemata.iter().chain(std::iter::once(&self.schema)) {
            collect_named(schema, &mut named);
        }
        skeleton_of(&self.schema, &named, 0)
    }
}

fn collect_named<'a>(schema: &'a Schema, out: &mut HashMap<String, &'a Schema>) {
    match schema {
        Schema::Record(record) => {
            out.insert(record.name.fullname(None), schema);
            for field in &record.fields {
                collect_named(&field.schema, out);
            }
        }
        Schema::Array(array) => collect_named(&array.items, out),
        Schema::Map(map) => collect_named(&map.types, out),
        Schema::Union(union) => union.variants().iter().for_each(|v| collect_named(v, out)),
        _ => {}
    }
}

/// Recursive types (a linked list, a tree) stop expanding at this depth.
const SKELETON_MAX_DEPTH: usize = 12;

fn skeleton_of(schema: &Schema, named: &HashMap<String, &Schema>, depth: usize) -> serde_json::Value {
    use serde_json::Value;
    if depth > SKELETON_MAX_DEPTH {
        return Value::Null;
    }
    match schema {
        Schema::Record(record) => Value::Object(
            record
                .fields
                .iter()
                .map(|f| (f.name.clone(), skeleton_of(&f.schema, named, depth + 1)))
                .collect(),
        ),
        Schema::Array(array) => Value::Array(vec![skeleton_of(&array.items, named, depth + 1)]),
        // Map keys are data, not schema: nothing to suggest.
        Schema::Map(_) => Value::Object(Default::default()),
        Schema::Union(union) => union
            .variants()
            .iter()
            .map(|v| skeleton_of(v, named, depth + 1))
            .fold(Value::Null, merge_skeletons),
        Schema::Ref { name } => named
            .get(&name.fullname(None))
            .map(|s| skeleton_of(s, named, depth + 1))
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn merge_skeletons(a: serde_json::Value, b: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match (a, b) {
        (Value::Object(mut a), Value::Object(b)) => {
            for (k, v) in b {
                let merged = match a.remove(&k) {
                    Some(existing) => merge_skeletons(existing, v),
                    None => v,
                };
                a.insert(k, merged);
            }
            Value::Object(a)
        }
        (Value::Array(a), Value::Array(b)) => {
            let merged = a.into_iter().chain(b).fold(Value::Null, merge_skeletons);
            Value::Array(vec![merged])
        }
        (Value::Null, other) | (other, Value::Null) => other,
        (a, _) => a,
    }
}

/// In-memory schema-id cache for Confluent wire-format Avro decode.
pub struct SchemaRegistry {
    base_url: String,
    cache: HashMap<u32, CachedSchema>,
}

#[derive(Debug, Deserialize)]
struct SchemaReference {
    subject: String,
    version: i64,
}

#[derive(Debug, Deserialize)]
struct SchemaResponse {
    schema: String,
    /// Absent for Avro (the registry's default); `PROTOBUF` / `JSON` otherwise.
    #[serde(default, rename = "schemaType")]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<SchemaReference>,
}

impl SchemaRegistry {
    /// Creates a registry cache pointed at `url` (e.g. `"http://localhost:8081"`).
    /// Trailing slashes are stripped. No network I/O happens here.
    pub fn new(url: &str) -> AppResult<Self> {
        let base_url = url.trim().trim_end_matches('/').to_string();
        if base_url.is_empty() {
            return Err(AppError::SchemaRegistry(
                "schema registry URL must not be empty".into(),
            ));
        }
        Ok(Self {
            base_url,
            cache: HashMap::new(),
        })
    }

    /// Configured registry base URL (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns a previously cached schema, if any. Used by sync decode paths.
    pub fn cached_schema(&self, id: u32) -> Option<&CachedSchema> {
        self.cache.get(&id)
    }

    /// Inserts (or replaces) a self-contained schema in the local cache.
    pub fn insert_schema(&mut self, id: u32, schema: Schema) {
        self.cache.insert(id, CachedSchema::new(schema));
    }

    /// Inserts (or replaces) a schema that may carry resolved references.
    pub fn insert_cached(&mut self, id: u32, schema: CachedSchema) {
        self.cache.insert(id, schema);
    }

    /// Number of schemas currently held in the local cache.
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }
}

/// First retry delay after a transient fetch failure; doubles per attempt.
const RETRY_BASE: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(60);

/// Which schema ids are being fetched, which can never succeed, and which failed
/// transiently and are waiting out a backoff. Pure state (the caller supplies `now`)
/// so every frontend shares the same retry policy and tests don't need a clock.
#[derive(Debug, Default)]
pub struct SchemaFetchTracker {
    inflight: HashSet<u32>,
    permanent: HashSet<u32>,
    /// id → (failed attempts so far, earliest next attempt).
    backoff: HashMap<u32, (u32, Instant)>,
}

impl SchemaFetchTracker {
    /// Whether a fetch for `id` should start now.
    pub fn should_fetch(&self, id: u32, now: Instant) -> bool {
        !self.inflight.contains(&id)
            && !self.permanent.contains(&id)
            && self.backoff.get(&id).is_none_or(|(_, not_before)| now >= *not_before)
    }

    pub fn mark_started(&mut self, id: u32) {
        self.inflight.insert(id);
    }

    pub fn mark_loaded(&mut self, id: u32) {
        self.inflight.remove(&id);
        self.backoff.remove(&id);
    }

    pub fn mark_failed(&mut self, id: u32, transient: bool, now: Instant) {
        self.inflight.remove(&id);
        if !transient {
            self.backoff.remove(&id);
            self.permanent.insert(id);
            return;
        }
        let attempts = self.backoff.get(&id).map_or(0, |(n, _)| *n) + 1;
        let delay = RETRY_BASE
            .saturating_mul(1u32.checked_shl(attempts - 1).unwrap_or(u32::MAX))
            .min(RETRY_MAX);
        self.backoff.insert(id, (attempts, now + delay));
    }

    /// Ids that failed transiently and whose backoff has elapsed.
    pub fn due_retries(&self, now: Instant) -> Vec<u32> {
        let mut due: Vec<u32> = self
            .backoff
            .iter()
            .filter(|(id, (_, not_before))| now >= *not_before && !self.inflight.contains(id))
            .map(|(id, _)| *id)
            .collect();
        due.sort_unstable();
        due
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RefKey {
    subject: String,
    version: i64,
}

impl From<&SchemaReference> for RefKey {
    fn from(r: &SchemaReference) -> Self {
        Self {
            subject: r.subject.clone(),
            version: r.version,
        }
    }
}

/// Schema JSON strings of every (transitively) referenced schema, ordered so each
/// comes after the schemas it references. Cycles can't be valid Avro; a repeat visit
/// is simply skipped.
fn dependencies_first(
    roots: &[RefKey],
    nodes: &HashMap<RefKey, (String, Vec<RefKey>)>,
) -> Vec<String> {
    let mut out = Vec::new();
    let mut visited: HashSet<&RefKey> = HashSet::new();
    // (node, children already pushed) — post-order without recursion.
    let mut stack: Vec<(&RefKey, bool)> = roots.iter().rev().map(|k| (k, false)).collect();
    while let Some((key, expanded)) = stack.pop() {
        let Some((schema, children)) = nodes.get(key) else { continue };
        if expanded {
            out.push(schema.clone());
        } else if visited.insert(key) {
            stack.push((key, true));
            stack.extend(children.iter().rev().map(|k| (k, false)));
        }
    }
    out
}

async fn get_schema_response(
    client: &reqwest::Client,
    url: reqwest::Url,
    what: &str,
) -> AppResult<SchemaResponse> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|err| AppError::SchemaRegistryTransient(format!("request for {what} failed: {err}")))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let message = format!("{what}: HTTP {status}: {body}");
        let transient = status.is_server_error()
            || status == reqwest::StatusCode::REQUEST_TIMEOUT
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS;
        return Err(if transient {
            AppError::SchemaRegistryTransient(message)
        } else {
            AppError::SchemaRegistry(message)
        });
    }
    response
        .json()
        .await
        .map_err(|err| AppError::SchemaRegistry(format!("{what}: invalid JSON response: {err}")))
}

fn registry_url(base_url: &str, segments: &[&str]) -> AppResult<reqwest::Url> {
    let mut url = reqwest::Url::parse(base_url.trim().trim_end_matches('/'))
        .map_err(|err| AppError::SchemaRegistry(format!("invalid registry URL '{base_url}': {err}")))?;
    url.path_segments_mut()
        .map_err(|_| AppError::SchemaRegistry(format!("registry URL '{base_url}' cannot be a base")))?
        .pop_if_empty()
        .extend(segments);
    Ok(url)
}

/// [`fetch_schema_by_id`] with a fresh HTTP client — for callers that don't keep one.
pub async fn fetch_schema(base_url: &str, id: u32) -> AppResult<CachedSchema> {
    let client = reqwest::Client::builder()
        .build()
        .map_err(|err| AppError::SchemaRegistry(format!("http client: {err}")))?;
    fetch_schema_by_id(&client, base_url, id).await
}

/// `GET {base_url}/schemas/ids/{id}`, then — for schemas that use Confluent schema
/// references — `GET /subjects/{subject}/versions/{version}` for each (transitive)
/// reference, and parse the lot together.
pub async fn fetch_schema_by_id(
    client: &reqwest::Client,
    base_url: &str,
    id: u32,
) -> AppResult<CachedSchema> {
    let id_str = id.to_string();
    let root = get_schema_response(
        client,
        registry_url(base_url, &["schemas", "ids", &id_str])?,
        &format!("schema id {id}"),
    )
    .await?;

    if let Some(kind) = root.schema_type.as_deref() {
        if !kind.eq_ignore_ascii_case("AVRO") {
            return Err(AppError::SchemaRegistry(format!(
                "schema id {id} is {kind}; only Avro is decoded"
            )));
        }
    }

    let root_refs: Vec<RefKey> = root.references.iter().map(RefKey::from).collect();
    let mut pending: Vec<SchemaReference> = root.references;
    let mut nodes: HashMap<RefKey, (String, Vec<RefKey>)> = HashMap::new();
    while let Some(reference) = pending.pop() {
        let key = RefKey::from(&reference);
        if nodes.contains_key(&key) {
            continue;
        }
        if nodes.len() >= MAX_REFERENCED_SCHEMAS {
            return Err(AppError::SchemaRegistry(format!(
                "schema id {id}: more than {MAX_REFERENCED_SCHEMAS} referenced schemas"
            )));
        }
        let version = reference.version.to_string();
        let fetched = get_schema_response(
            client,
            registry_url(base_url, &["subjects", &reference.subject, "versions", &version])?,
            &format!(
                "schema id {id}: reference {}/{}",
                reference.subject, reference.version
            ),
        )
        .await?;
        let children = fetched.references.iter().map(RefKey::from).collect();
        pending.extend(fetched.references);
        nodes.insert(key, (fetched.schema, children));
    }
    let reference_schemas = dependencies_first(&root_refs, &nodes);

    CachedSchema::parse(&root.schema, &reference_schemas)
        .map_err(|err| AppError::SchemaRegistry(format!("schema id {id}: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_SCHEMA: &str = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }"#;

    pub(super) const ADDRESS_SCHEMA: &str = r#"{
        "type": "record", "name": "Address", "namespace": "com.example",
        "fields": [{"name": "city", "type": "string"}]
    }"#;

    pub(super) const PERSON_SCHEMA: &str = r#"{
        "type": "record", "name": "Person", "namespace": "com.example",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "home", "type": "com.example.Address"},
            {"name": "past", "type": {"type": "array", "items": "com.example.Address"}}
        ]
    }"#;

    pub(super) fn encode_person() -> Vec<u8> {
        // name="al", home.city="x", past=[{city:"yz"}]
        let mut out = vec![];
        out.extend([4, b'a', b'l']);
        out.extend([2, b'x']);
        out.extend([2, 4, b'y', b'z', 0]);
        out
    }

    #[test]
    fn schema_with_reference_fails_to_parse_without_it() {
        assert!(CachedSchema::parse(PERSON_SCHEMA, &[]).is_err());
    }

    #[test]
    fn schema_with_reference_decodes_when_reference_supplied() {
        let cached = CachedSchema::parse(PERSON_SCHEMA, &[ADDRESS_SCHEMA.to_string()]).unwrap();
        let value: serde_json::Value = cached.decode_datum(&encode_person()).unwrap().try_into().unwrap();
        assert_eq!(
            value,
            serde_json::json!({"name": "al", "home": {"city": "x"}, "past": [{"city": "yz"}]})
        );
    }

    #[test]
    fn tracker_backs_off_transient_failures_and_doubles() {
        let t0 = Instant::now();
        let mut t = SchemaFetchTracker::default();
        assert!(t.should_fetch(1, t0));
        t.mark_started(1);
        assert!(!t.should_fetch(1, t0));

        t.mark_failed(1, true, t0);
        assert!(!t.should_fetch(1, t0));
        assert!(t.due_retries(t0).is_empty());
        assert!(t.should_fetch(1, t0 + Duration::from_secs(1)));
        assert_eq!(t.due_retries(t0 + Duration::from_secs(1)), vec![1]);

        t.mark_started(1);
        t.mark_failed(1, true, t0 + Duration::from_secs(1));
        assert!(!t.should_fetch(1, t0 + Duration::from_secs(2)));
        assert!(t.should_fetch(1, t0 + Duration::from_secs(3)));
    }

    #[test]
    fn tracker_caps_backoff_and_never_overflows() {
        let t0 = Instant::now();
        let mut t = SchemaFetchTracker::default();
        for _ in 0..100 {
            t.mark_started(1);
            t.mark_failed(1, true, t0);
        }
        assert!(!t.should_fetch(1, t0 + Duration::from_secs(59)));
        assert!(t.should_fetch(1, t0 + Duration::from_secs(60)));
    }

    #[test]
    fn tracker_permanent_failure_is_never_retried_and_success_resets() {
        let t0 = Instant::now();
        let mut t = SchemaFetchTracker::default();
        t.mark_started(1);
        t.mark_failed(1, false, t0);
        assert!(!t.should_fetch(1, t0 + Duration::from_secs(3600)));
        assert!(t.due_retries(t0 + Duration::from_secs(3600)).is_empty());

        t.mark_started(2);
        t.mark_failed(2, true, t0);
        t.mark_started(2);
        t.mark_loaded(2);
        assert!(t.should_fetch(2, t0));
        assert!(t.due_retries(t0 + Duration::from_secs(3600)).is_empty());
    }

    #[test]
    fn dependencies_first_orders_transitive_references_before_dependents() {
        let k = |n: &str| RefKey { subject: n.into(), version: 1 };
        let mut nodes = HashMap::new();
        nodes.insert(k("a"), ("A".to_string(), vec![k("b"), k("c")]));
        nodes.insert(k("b"), ("B".to_string(), vec![k("c")]));
        nodes.insert(k("c"), ("C".to_string(), vec![]));
        assert_eq!(dependencies_first(&[k("a")], &nodes), vec!["C", "B", "A"]);
        // Shared dependency appears once even when reachable from two roots.
        assert_eq!(dependencies_first(&[k("b"), k("a")], &nodes), vec!["C", "B", "A"]);
    }

    #[test]
    fn chained_references_decode_regardless_of_fetch_order() {
        let street = r#"{"type":"record","name":"Street","namespace":"com.example","fields":[{"name":"s","type":"string"}]}"#;
        let address = r#"{"type":"record","name":"Address","namespace":"com.example","fields":[{"name":"street","type":"com.example.Street"}]}"#;
        let person = r#"{"type":"record","name":"Person","namespace":"com.example","fields":[{"name":"home","type":"com.example.Address"}]}"#;
        let k = |n: &str| RefKey { subject: n.into(), version: 1 };
        let mut nodes = HashMap::new();
        nodes.insert(k("address"), (address.to_string(), vec![k("street")]));
        nodes.insert(k("street"), (street.to_string(), vec![]));
        let refs = dependencies_first(&[k("address")], &nodes);
        let cached = CachedSchema::parse(person, &refs).unwrap();
        let v: serde_json::Value = cached.decode_datum(&[2, b'z']).unwrap().try_into().unwrap();
        assert_eq!(v, serde_json::json!({"home": {"street": {"s": "z"}}}));
    }

    #[test]
    fn registry_url_percent_encodes_subject_segments() {
        let url = registry_url("http://sr:8081/", &["subjects", "a/b c", "versions", "1"]).unwrap();
        assert_eq!(url.as_str(), "http://sr:8081/subjects/a%2Fb%20c/versions/1");
    }

    #[test]
    fn registry_url_keeps_a_base_path_prefix() {
        let url = registry_url("http://sr:8081/registry/", &["schemas", "ids", "7"]).unwrap();
        assert_eq!(url.as_str(), "http://sr:8081/registry/schemas/ids/7");
    }

    #[test]
    fn new_rejects_empty_url() {
        assert!(SchemaRegistry::new("").is_err());
        assert!(SchemaRegistry::new("   ").is_err());
    }

    #[test]
    fn new_strips_trailing_slash() {
        let sr = SchemaRegistry::new("http://localhost:8081/").unwrap();
        assert_eq!(sr.base_url(), "http://localhost:8081");
    }

    #[test]
    fn cache_hit_via_insert_and_lookup() {
        let mut sr = SchemaRegistry::new("http://localhost:8081").unwrap();
        assert!(sr.cached_schema(42).is_none());

        let schema = Schema::parse_str(SAMPLE_SCHEMA).unwrap();
        sr.insert_schema(42, schema);
        assert!(sr.cached_schema(42).is_some());
        assert_eq!(sr.cache_len(), 1);
        assert!(sr.cached_schema(99).is_none());
    }

    #[test]
    fn skeleton_has_every_field_including_union_and_nested_ones() {
        let schema = r#"{
            "type": "record", "name": "Order", "namespace": "t",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "note", "type": ["null", "string"], "default": null},
                {"name": "customer", "type": ["null", {
                    "type": "record", "name": "Customer",
                    "fields": [{"name": "tier", "type": "string"}, {"name": "vip", "type": ["null", "boolean"], "default": null}]
                }], "default": null},
                {"name": "items", "type": {"type": "array", "items": {
                    "type": "record", "name": "Item", "fields": [{"name": "sku", "type": "string"}]
                }}},
                {"name": "backup", "type": ["null", "Customer"], "default": null},
                {"name": "labels", "type": {"type": "map", "values": "string"}}
            ]
        }"#;
        let skeleton = CachedSchema::parse(schema, &[]).unwrap().skeleton();
        let keys = |v: &serde_json::Value, path: &[&str]| -> Vec<String> {
            let path: Vec<String> = path.iter().map(|s| s.to_string()).collect();
            crate::query_complete::candidate_children(v, &path).into_iter().collect()
        };
        assert_eq!(keys(&skeleton, &[]), ["backup", "customer", "id", "items", "labels", "note"]);
        assert_eq!(keys(&skeleton, &["customer"]), ["tier", "vip"]);
        assert_eq!(keys(&skeleton, &["items"]), ["sku"], "arrays don't consume a path segment");
        assert_eq!(keys(&skeleton, &["backup"]), ["tier", "vip"], "named reuse resolves");
        assert!(keys(&skeleton, &["labels"]).is_empty(), "map keys are data");
    }

    #[test]
    fn skeleton_of_a_recursive_type_terminates() {
        let schema = r#"{"type":"record","name":"Node","fields":[
            {"name":"value","type":"int"},
            {"name":"next","type":["null","Node"],"default":null}]}"#;
        let skeleton = CachedSchema::parse(schema, &[]).unwrap().skeleton();
        assert_eq!(skeleton["value"], serde_json::Value::Null);
        assert!(skeleton["next"]["next"]["next"].is_object());
    }
}

/// Docker-compose-gated: `docker compose up -d` then `cargo test -- --ignored`.
/// See `kafka::integration_support` for the shared setup rationale.
#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::kafka::integration_support::{unique_name, SCHEMA_REGISTRY_URL};

    #[derive(serde::Serialize)]
    struct RegisterRequest<'a> {
        schema: &'a str,
    }

    #[derive(serde::Deserialize)]
    struct RegisterResponse {
        id: u32,
    }

    #[tokio::test]
    #[ignore = "requires `docker compose up -d` (localhost:18081)"]
    async fn fetch_schema_by_id_round_trips_a_registered_schema() {
        let client = reqwest::Client::new();
        let subject = unique_name("subject");
        let schema_str =
            r#"{"type":"record","name":"IntegrationTestRecord","fields":[{"name":"id","type":"long"}]}"#;

        let register_url = format!("{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions");
        let response = client
            .post(&register_url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(&RegisterRequest { schema: schema_str })
            .send()
            .await
            .expect("register-schema request should reach the local registry");
        assert!(
            response.status().is_success(),
            "schema registration failed: {}",
            response.status()
        );
        let registered: RegisterResponse = response.json().await.expect("register response body");

        let fetched = fetch_schema_by_id(&client, SCHEMA_REGISTRY_URL, registered.id)
            .await
            .expect("fetch_schema_by_id should succeed against the live registry");

        let expected = Schema::parse_str(schema_str).expect("parse expected schema");
        assert_eq!(fetched.schema.canonical_form(), expected.canonical_form());
    }

    #[derive(serde::Serialize)]
    struct RegisterWithRefs<'a> {
        schema: &'a str,
        references: Vec<serde_json::Value>,
    }

    async fn register(client: &reqwest::Client, subject: &str, body: &impl serde::Serialize) -> u32 {
        let response = client
            .post(format!("{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"))
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(body)
            .send()
            .await
            .expect("register request");
        assert!(response.status().is_success(), "register {subject}: {}", response.status());
        response.json::<RegisterResponse>().await.expect("register body").id
    }

    #[tokio::test]
    #[ignore = "requires `docker compose up -d` (localhost:18081)"]
    async fn fetch_schema_by_id_resolves_schema_references() {
        let client = reqwest::Client::new();
        let address_subject = unique_name("address");
        let person_subject = unique_name("person");
        register(&client, &address_subject, &RegisterRequest { schema: tests::ADDRESS_SCHEMA }).await;
        let person_id = register(
            &client,
            &person_subject,
            &RegisterWithRefs {
                schema: tests::PERSON_SCHEMA,
                references: vec![serde_json::json!({
                    "name": "com.example.Address",
                    "subject": address_subject,
                    "version": 1
                })],
            },
        )
        .await;

        let fetched = fetch_schema_by_id(&client, SCHEMA_REGISTRY_URL, person_id)
            .await
            .expect("schema with references should fetch and parse");
        let value: serde_json::Value = fetched
            .decode_datum(&tests::encode_person())
            .expect("decode with resolved references")
            .try_into()
            .unwrap();
        assert_eq!(value["home"]["city"], "x");
    }
}
