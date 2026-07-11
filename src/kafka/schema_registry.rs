//! Thin Schema Registry client with a local schema-id cache.
//!
//! Decode paths look up the cache only (sync). Background tasks call
//! [`fetch_schema_by_id`] and then [`SchemaRegistry::insert_schema`].

use std::collections::HashMap;

use apache_avro::Schema;
use serde::Deserialize;

use crate::error::{AppError, AppResult};

/// In-memory schema-id cache for Confluent wire-format Avro decode.
pub struct SchemaRegistry {
    base_url: String,
    cache: HashMap<u32, Schema>,
}

#[derive(Debug, Deserialize)]
struct SchemaIdResponse {
    schema: String,
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
    pub fn cached_schema(&self, id: u32) -> Option<&Schema> {
        self.cache.get(&id)
    }

    /// Inserts (or replaces) a schema in the local cache.
    pub fn insert_schema(&mut self, id: u32, schema: Schema) {
        self.cache.insert(id, schema);
    }

    /// Number of schemas currently held in the local cache.
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }
}

/// `GET {base_url}/schemas/ids/{id}` → parse Avro schema string from the response body.
pub async fn fetch_schema_by_id(
    client: &reqwest::Client,
    base_url: &str,
    id: u32,
) -> AppResult<Schema> {
    let base = base_url.trim().trim_end_matches('/');
    let url = format!("{base}/schemas/ids/{id}");
    let response = client.get(&url).send().await.map_err(|err| {
        AppError::SchemaRegistry(format!("request for schema id {id} failed: {err}"))
    })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::SchemaRegistry(format!(
            "schema id {id}: HTTP {status}: {body}"
        )));
    }

    let body: SchemaIdResponse = response.json().await.map_err(|err| {
        AppError::SchemaRegistry(format!("schema id {id}: invalid JSON response: {err}"))
    })?;

    Schema::parse_str(&body.schema).map_err(|err| {
        AppError::SchemaRegistry(format!("schema id {id}: failed to parse Avro schema: {err}"))
    })
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
    #[ignore = "requires `docker compose up -d` (localhost:8081)"]
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
        assert_eq!(fetched.canonical_form(), expected.canonical_form());
    }
}
