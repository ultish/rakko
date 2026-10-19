//! Everything that isn't browsing: consumer groups, brokers, producing, replay,
//! JSONL export/import and the live tail.
//!
//! rakko-web has no login of its own by design (an authenticating proxy sits in front);
//! every handler here can write.
//! Destructive calls therefore insist on an explicit `confirm` that names the target, so
//! a stray request can't do damage by accident — this is not a security boundary.

use std::convert::Infallible;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderValue};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use futures::stream::{self, Stream};
use rakko_core::export::{message_to_jsonl_line, parse_jsonl_line};
use rakko_core::kafka::admin::{create_topic as admin_create_topic, delete_records_before, delete_topic as admin_delete_topic, fetch_topic_configs, fetch_broker_configs, list_brokers};
use rakko_core::kafka::schema_admin::{self, SchemaReference};
use rakko_core::kafka::consumer::{fetch_message, run_tail, BrowseEvent};
use rakko_core::kafka::group_offsets::{describe_group, list_groups, topic_consumers, reset_group_offsets, OffsetResetTarget};
use rakko_core::kafka::producer::RawProducer;
use rakko_core::kafka::scan::{ScanRange, TopicScan};
use rakko_core::message_filter::MessageFilter;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};

use crate::dto::MessageSummary;
use crate::error::{ApiError, ApiResult};
use crate::scan::{evaluate, summarize_loaded};
use crate::state::AppState;

type Shared = State<Arc<AppState>>;

/// Largest single produce request (a JSON body carrying base64), a little over Kafka's
/// common 20 MiB message limit once base64 inflates it.
const PRODUCE_BODY_LIMIT: usize = 32 * 1024 * 1024;
/// Largest import upload.
const IMPORT_BODY_LIMIT: usize = 2 * 1024 * 1024 * 1024;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/p/{profile}/groups", get(groups))
        .route("/p/{profile}/groups/{group}", get(group))
        .route("/p/{profile}/topics/{topic}/consumers", get(topic_consumer_groups))
        .route("/p/{profile}/groups/{group}/reset", post(reset_group))
        .route("/p/{profile}/groups/{group}", axum::routing::delete(delete_group))
        .route("/p/{profile}/topics/{topic}/clear", post(clear_topic))
        .route("/p/{profile}/groups/{group}/lag-history", get(lag_history))
        .route("/p/{profile}/brokers", get(brokers))
        .route("/p/{profile}/acls", get(acls))
        .route("/p/{profile}/brokers/{id}/configs", get(broker_configs))
        .route(
            "/p/{profile}/topics/{topic}/produce",
            post(produce).layer(DefaultBodyLimit::max(PRODUCE_BODY_LIMIT)),
        )
        .route("/p/{profile}/topics/{topic}/messages/{partition}/{offset}/replay", post(replay))
        .route("/p/{profile}/topics/{topic}/export", get(export))
        .route(
            "/p/{profile}/topics/{topic}/import",
            post(import).layer(DefaultBodyLimit::max(IMPORT_BODY_LIMIT)),
        )
        .route("/p/{profile}/topics/{topic}/tail", get(tail))
        .route("/p/{profile}/topics/{topic}/messages/{partition}/{offset}/download", get(download))
        .route("/p/{profile}/topics/{topic}/messages/{partition}/{offset}/tombstone", post(tombstone))
        .route("/p/{profile}/topics/{topic}/delete-records", post(delete_records))
        .route("/p/{profile}/topics", post(create_topic))
        .route("/p/{profile}/topics/{topic}", axum::routing::delete(delete_topic))
        .route("/p/{profile}/topics/{topic}/configs", get(topic_configs).put(topic_configs_alter))
        .route("/p/{profile}/topics/{topic}/configs/all", get(topic_configs_all))
        .route("/p/{profile}/schemas/{subject}/template", get(schema_template))
        .route("/p/{profile}/topics/{topic}/avro-subjects", get(avro_subjects))
        .route("/p/{profile}/topics/{topic}/partition-details", get(partition_detail_list))
        .route("/p/{profile}/schema-ids/{id}/subjects", get(schema_id_subjects))
        .route("/p/{profile}/schemas", get(schema_subjects))
        .route(
            "/p/{profile}/schemas/{subject}",
            get(schema_versions).post(schema_register).delete(schema_delete_subject),
        )
        .route("/p/{profile}/schemas/{subject}/check", post(schema_check))
        .route("/p/{profile}/schema-config", get(schema_config_global).put(schema_config_set_global))
        .route("/p/{profile}/schemas/{subject}/config", get(schema_config).put(schema_config_set))
        .route(
            "/p/{profile}/schemas/{subject}/versions/{version}",
            get(schema_version).delete(schema_delete_version),
        )
}

fn profile(state: &AppState, name: &str) -> Result<Arc<crate::runtime::ProfileRuntime>, ApiError> {
    state.profile(name).ok_or_else(|| ApiError::not_found(format!("unknown profile '{name}'")))
}

// ---------------------------------------------------------------- groups

#[derive(Serialize)]
struct GroupInfo {
    name: String,
    state: String,
    members: usize,
    protocol: String,
}

async fn groups(State(state): Shared, Path(p): Path<String>) -> ApiResult<Vec<GroupInfo>> {
    let rt = profile(&state, &p)?;
    let mut groups = list_groups(&rt.profile).await?;
    groups.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(
        groups
            .into_iter()
            .map(|g| GroupInfo { name: g.name, state: g.state, members: g.member_count, protocol: g.protocol })
            .collect(),
    ))
}

#[derive(Serialize)]
struct TopicConsumerInfo {
    name: String,
    state: String,
    total_lag: i64,
}

/// Consumer groups with committed offsets on the topic. Kafka has no topic-to-groups index,
/// so every group is described (a few at a time) and filtered; groups that fail to describe
/// are skipped rather than failing the whole panel.
async fn topic_consumer_groups(State(state): Shared, Path((p, topic)): Path<(String, String)>) -> ApiResult<Vec<TopicConsumerInfo>> {
    use futures::StreamExt;
    let rt = profile(&state, &p)?;
    let names = list_groups(&rt.profile).await?;
    let details: Vec<_> = stream::iter(names)
        .map(|g| {
            let rt = rt.clone();
            async move { describe_group(&rt.profile, &g.name).await.ok() }
        })
        .buffer_unordered(8)
        .filter_map(|d| async move { d })
        .collect()
        .await;
    Ok(Json(
        topic_consumers(&details, &topic)
            .into_iter()
            .map(|c| TopicConsumerInfo { name: c.group, state: c.state, total_lag: c.total_lag })
            .collect(),
    ))
}

#[derive(Serialize)]
struct GroupDetailInfo {
    name: String,
    state: String,
    total_lag: i64,
    members: Vec<Value>,
    lags: Vec<Value>,
}

async fn group(State(state): Shared, Path((p, g)): Path<(String, String)>) -> ApiResult<GroupDetailInfo> {
    let rt = profile(&state, &p)?;
    let d = describe_group(&rt.profile, &g).await?;
    Ok(Json(GroupDetailInfo {
        total_lag: d.total_lag(),
        members: d
            .members
            .iter()
            .map(|m| json!({ "id": m.id, "client_id": m.client_id, "host": m.client_host }))
            .collect(),
        lags: d
            .lags
            .iter()
            .map(|l| {
                json!({
                    "topic": l.topic, "partition": l.partition, "committed": l.committed_offset,
                    "low": l.low_watermark, "high": l.high_watermark, "lag": l.lag,
                })
            })
            .collect(),
        name: d.name,
        state: d.state,
    }))
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum ResetTarget {
    Earliest,
    Latest,
    Offset { value: i64 },
    Timestamp { value: i64 },
}

#[derive(Deserialize)]
struct PartitionRef {
    topic: String,
    partition: i32,
}

#[derive(Deserialize)]
struct ResetBody {
    target: ResetTarget,
    /// Omitted = every partition the group has a committed offset for.
    partitions: Option<Vec<PartitionRef>>,
    /// Must equal the group name.
    confirm: String,
}

async fn reset_group(
    State(state): Shared,
    Path((p, g)): Path<(String, String)>,
    Json(body): Json<ResetBody>,
) -> ApiResult<Value> {
    if body.confirm != g {
        return Err(ApiError::bad_request("confirm must equal the group name"));
    }
    let rt = profile(&state, &p)?;
    let partitions: Vec<(String, i32)> = match body.partitions {
        Some(list) => list.into_iter().map(|r| (r.topic, r.partition)).collect(),
        None => describe_group(&rt.profile, &g).await?.lags.into_iter().map(|l| (l.topic, l.partition)).collect(),
    };
    let target = match body.target {
        ResetTarget::Earliest => OffsetResetTarget::Earliest,
        ResetTarget::Latest => OffsetResetTarget::Latest,
        ResetTarget::Offset { value } => OffsetResetTarget::Absolute(value),
        ResetTarget::Timestamp { value } => OffsetResetTarget::Timestamp(value),
    };
    reset_group_offsets(&rt.profile, &g, target, &partitions).await?;
    tracing::info!("reset offsets of group {g} on profile {p} ({} partition(s))", partitions.len());
    Ok(Json(json!({ "reset": partitions.len() })))
}

// ---------------------------------------------------------------- brokers

async fn brokers(State(state): Shared, Path(p): Path<String>) -> ApiResult<Value> {
    let rt = profile(&state, &p)?;
    let (brokers, health) = list_brokers(&rt.profile).await?;
    Ok(Json(json!({
        "under_replicated": health.under_replicated,
        "offline": health.offline,
        "brokers": brokers.iter().map(|b| json!({
            "id": b.id, "host": b.host, "port": b.port,
            "leader_partitions": b.leader_partitions, "replica_partitions": b.replica_partitions,
        })).collect::<Vec<_>>(),
    })))
}

async fn broker_configs(State(state): Shared, Path((p, id)): Path<(String, i32)>) -> ApiResult<Vec<Value>> {
    let rt = profile(&state, &p)?;
    let mut entries = fetch_broker_configs(&rt.profile, id).await?;
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(entries.into_iter().map(|e| json!({ "name": e.name, "value": e.value })).collect()))
}

// ---------------------------------------------------------------- produce / replay

/// A key or value: typed text, or arbitrary bytes as base64. Absent = null (tombstone).
#[derive(Deserialize, Default)]
struct Field {
    text: Option<String>,
    base64: Option<String>,
    /// Avro: JSON encoded with the latest schema of `subject`, in Confluent wire format.
    avro: Option<AvroField>,
}

#[derive(Deserialize)]
struct AvroField {
    subject: String,
    json: String,
}

impl Field {
    fn bytes(self, what: &str) -> Result<Option<Vec<u8>>, ApiError> {
        match (self.text, self.base64) {
            _ if self.avro.is_some() => Err(ApiError::bad_request(format!("{what}: avro fields are encoded by the server"))),
            (Some(_), Some(_)) => Err(ApiError::bad_request(format!("{what}: give text or base64, not both"))),
            (Some(t), None) => Ok(Some(t.into_bytes())),
            (None, Some(b)) => B64
                .decode(b.trim())
                .map(Some)
                .map_err(|e| ApiError::bad_request(format!("{what}: invalid base64: {e}"))),
            (None, None) => Ok(None),
        }
    }
}

#[derive(Deserialize)]
struct HeaderPair {
    key: String,
    #[serde(default)]
    value: String,
}

#[derive(Deserialize)]
struct ProduceBody {
    key: Option<Field>,
    value: Option<Field>,
    #[serde(default)]
    headers: Vec<HeaderPair>,
}

/// Bytes for a produce field; Avro fields are encoded against their subject's latest schema.
async fn resolve_field(rt: &crate::runtime::ProfileRuntime, field: Option<Field>, what: &str) -> Result<Option<Vec<u8>>, ApiError> {
    let Some(mut field) = field else { return Ok(None) };
    let Some(avro) = field.avro.take() else { return field.bytes(what) };
    let base = registry_url(rt)?;
    let latest = schema_admin::get_version(&base, &avro.subject, "latest")
        .await
        .map_err(|e| ApiError::bad_request(format!("{what}: schema for '{}': {e}", avro.subject)))?;
    let id = u32::try_from(latest.id).map_err(|_| ApiError::bad_request("schema id out of range"))?;
    let schema = rakko_core::kafka::schema_registry::fetch_schema(&base, id).await?;
    let json: Value = serde_json::from_str(&avro.json).map_err(|e| ApiError::bad_request(format!("{what}: not valid JSON: {e}")))?;
    let datum = schema.encode_json(json).map_err(|e| ApiError::bad_request(format!("{what}: {e}")))?;
    let mut wire = Vec::with_capacity(datum.len() + 5);
    wire.push(0);
    wire.extend_from_slice(&id.to_be_bytes());
    wire.extend(datum);
    Ok(Some(wire))
}

async fn produce(
    State(state): Shared,
    Path((p, topic)): Path<(String, String)>,
    Json(body): Json<ProduceBody>,
) -> ApiResult<Value> {
    let rt = profile(&state, &p)?;
    let key = resolve_field(&rt, body.key, "key").await?;
    let value = resolve_field(&rt, body.value, "value").await?;
    if key.is_none() && value.is_none() {
        return Err(ApiError::bad_request("a message needs a key or a value"));
    }
    let headers: Vec<(String, Vec<u8>)> =
        body.headers.into_iter().filter(|h| !h.key.is_empty()).map(|h| (h.key, h.value.into_bytes())).collect();
    let (partition, offset) = RawProducer::new(&rt.profile)?.send(&topic, key.as_deref(), value.as_deref(), &headers).await?;
    Ok(Json(json!({ "partition": partition, "offset": offset })))
}

/// Re-sends a stored message's exact bytes to the same topic.
async fn replay(
    State(state): Shared,
    Path((p, topic, partition, offset)): Path<(String, String, i32, i64)>,
) -> ApiResult<Value> {
    let rt = profile(&state, &p)?;
    let m = fetch_message(&rt.profile, &topic, partition, offset)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("{topic}/{partition}@{offset} no longer exists")))?;
    let (np, no) = RawProducer::new(&rt.profile)?.send(&topic, m.key.as_deref(), m.value.as_deref(), &m.headers).await?;
    Ok(Json(json!({ "partition": np, "offset": no })))
}

// ---------------------------------------------------------------- export / import

#[derive(Deserialize)]
struct ExportQuery {
    text: Option<String>,
    query: Option<String>,
    /// Stop after this many messages.
    limit: Option<u64>,
    // Spelled out (not `#[serde(flatten)]`): query strings can't flatten numbers.
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    partition: Option<i32>,
    from_offset: Option<i64>,
    to_offset: Option<i64>,
}

impl ExportQuery {
    fn range(&self) -> ScanRange {
        ScanRange {
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            partition: self.partition,
            from_offset: self.from_offset,
            to_offset: self.to_offset,
        }
    }
}

/// Streams matching messages as JSONL (raw base64 bytes — re-importable byte for byte).
/// Walks the whole topic newest-first, exactly like a filtered browse.
async fn export(
    State(state): Shared,
    Path((p, topic)): Path<(String, String)>,
    Query(q): Query<ExportQuery>,
) -> Result<Response, ApiError> {
    let rt = profile(&state, &p)?;
    let filter = MessageFilter::parse(q.text.as_deref(), q.query.as_deref()).map_err(ApiError::bad_request)?;
    let limit = q.limit.unwrap_or(u64::MAX);
    let window = state.scan_window;
    let range = q.range();
    let q_range = range.clone();
    range.validate().map_err(|e| ApiError::bad_request(e.to_string()))?;

    let (profile_cfg, t) = (rt.profile.clone(), topic.clone());
    let mut scan = tokio::task::spawn_blocking(move || TopicScan::open_range(&profile_cfg, &t, window, &range))
        .await
        .map_err(|e| ApiError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;

    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let mut sent = 0u64;
        let mut closed = false;
        while !closed && sent < limit && !scan.done() {
            let outcome = scan.scan_next(&mut |message| {
                if closed || sent >= limit {
                    return;
                }
                if !q_range.contains_timestamp(message.timestamp_millis) {
                    return;
                }
                let (matched, _) = evaluate(&rt, &handle, &filter, &message);
                if !matched {
                    return;
                }
                match message_to_jsonl_line(&message, None) {
                    Ok(mut line) => {
                        line.push('\n');
                        if tx.blocking_send(Ok(Bytes::from(line))).is_err() {
                            closed = true;
                        }
                        sent += 1;
                    }
                    Err(err) => tracing::warn!("export: skipping unserialisable message: {err}"),
                }
            });
            if let Err(err) = outcome {
                // Headers are long gone; end the body with an error so the download fails
                // visibly instead of looking complete.
                let _ = tx.blocking_send(Err(std::io::Error::other(err.to_string())));
                return;
            }
        }
    });

    let body = Body::from_stream(stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|item| (item, rx)) }));
    let filename = format!("{topic}.jsonl").replace(['"', '/', '\\'], "_");
    let mut response = body.into_response();
    let h = response.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/x-ndjson"));
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\"")) {
        h.insert(header::CONTENT_DISPOSITION, v);
    }
    Ok(response)
}

/// Produces every line of an uploaded JSONL file (as written by export) to `topic`,
/// byte for byte. Bad lines are reported, not fatal.
async fn import(
    State(state): Shared,
    Path((p, topic)): Path<(String, String)>,
    body: Body,
) -> ApiResult<Value> {
    use futures::StreamExt;
    let rt = profile(&state, &p)?;
    let producer = RawProducer::new(&rt.profile)?;
    let mut stream = body.into_data_stream();
    let (mut sent, mut failed) = (0u64, 0u64);
    let mut errors: Vec<String> = Vec::new();
    let mut line_no = 0u64;
    let mut buf: Vec<u8> = Vec::new();

    async fn send_line(
        producer: &RawProducer,
        topic: &str,
        raw: &[u8],
        line_no: u64,
        sent: &mut u64,
        failed: &mut u64,
        errors: &mut Vec<String>,
    ) {
        let text = String::from_utf8_lossy(raw);
        if text.trim().is_empty() {
            return;
        }
        let result = match parse_jsonl_line(&text) {
            Ok(m) => producer.send(topic, m.key.as_deref(), m.value.as_deref(), &m.headers).await.map(|_| ()),
            Err(e) => Err(e),
        };
        match result {
            Ok(()) => *sent += 1,
            Err(e) => {
                *failed += 1;
                if errors.len() < 20 {
                    errors.push(format!("line {line_no}: {e}"));
                }
            }
        }
    }

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| ApiError::bad_request(format!("upload interrupted: {e}")))?;
        buf.extend_from_slice(&chunk);
        while let Some(pos) = buf.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = buf.drain(..=pos).collect();
            line_no += 1;
            send_line(&producer, &topic, &line, line_no, &mut sent, &mut failed, &mut errors).await;
        }
    }
    if !buf.is_empty() {
        line_no += 1;
        send_line(&producer, &topic, &buf, line_no, &mut sent, &mut failed, &mut errors).await;
    }
    Ok(Json(json!({ "sent": sent, "failed": failed, "errors": errors })))
}

// ---------------------------------------------------------------- live tail

#[derive(Deserialize)]
struct TailQuery {
    text: Option<String>,
    query: Option<String>,
}

enum TailItem {
    Message(Box<MessageSummary>),
    /// The browser fell behind; this many matching messages were skipped.
    Dropped(u64),
    Failed(String),
}

/// Stops the consumer thread when the SSE stream is dropped (client went away).
struct StopOnDrop(watch::Sender<bool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

/// Server-sent events of new messages (from the log end) that match the filter.
/// Events: `messages` (an array of summaries), `dropped` (`{count}`), `error` (`{message}`).
async fn tail(
    State(state): Shared,
    Path((p, topic)): Path<(String, String)>,
    Query(q): Query<TailQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let rt = profile(&state, &p)?;
    let filter = MessageFilter::parse(q.text.as_deref(), q.query.as_deref()).map_err(ApiError::bad_request)?;

    let (tx, rx) = mpsc::channel::<TailItem>(20_000);
    let (stop_tx, stop_rx) = watch::channel(false);
    let handle = tokio::runtime::Handle::current();
    let profile_cfg = rt.profile.clone();
    let mut dropped = 0u64;
    tokio::spawn(run_tail(
        profile_cfg,
        topic,
        move |event| match event {
            BrowseEvent::Message { message, .. } => {
                let (matched, _) = evaluate(&rt, &handle, &filter, &message);
                if !matched {
                    return true;
                }
                if dropped > 0 && tx.try_send(TailItem::Dropped(dropped)).is_ok() {
                    dropped = 0;
                }
                let summary = summarize_loaded(&rt, &handle, &message, crate::dto::ROW_PREVIEW_CHARS);
                match tx.try_send(TailItem::Message(Box::new(summary))) {
                    Ok(()) => true,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        dropped += 1;
                        true
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => false,
                }
            }
            BrowseEvent::Failed(msg) => {
                let _ = tx.try_send(TailItem::Failed(msg));
                false
            }
            BrowseEvent::SeekPage { .. } => true,
        },
        stop_rx,
    ));

    // Messages leave in batches (one SSE event per drain of the queue): at tens of thousands
    // of messages a second the per-event overhead, not Kafka, is what falls behind.
    let events = stream::unfold((rx, StopOnDrop(stop_tx), None::<TailItem>), |(mut rx, guard, pending)| async move {
        let first = match pending {
            Some(item) => item,
            None => rx.recv().await?,
        };
        let mut batch = Vec::new();
        let mut next = None;
        let event = match first {
            TailItem::Message(m) => {
                batch.push(m);
                while batch.len() < 500 {
                    match rx.try_recv() {
                        Ok(TailItem::Message(m)) => batch.push(m),
                        Ok(item) => {
                            next = Some(item);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                Event::default().event("messages").json_data(&batch)
            }
            TailItem::Dropped(n) => Event::default().event("dropped").json_data(json!({ "count": n })),
            TailItem::Failed(m) => Event::default().event("error").json_data(json!({ "message": m })),
        };
        let event = event.unwrap_or_else(|_| Event::default().event("error").data("serialisation failed"));
        Some((Ok(event), (rx, guard, next)))
    });
    Ok(Sse::new(events).keep_alive(KeepAlive::default()))
}

// ---------------------------------------------------------------- download

#[derive(Deserialize)]
struct DownloadQuery {
    /// `key`, `value`, or `message` (the whole record as one re-importable JSONL line).
    part: String,
    /// `raw` (the exact wire bytes) or `decoded` (e.g. Avro rendered as JSON).
    #[serde(default = "default_raw")]
    format: String,
}

fn default_raw() -> String {
    "raw".into()
}

fn attachment(body: Vec<u8>, content_type: &'static str, filename: &str) -> Response {
    let mut response = Body::from(body).into_response();
    let h = response.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    let safe = filename.replace(['"', '/', '\\'], "_");
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename=\"{safe}\"")) {
        h.insert(header::CONTENT_DISPOSITION, v);
    }
    response
}

/// The full, unabridged content of a message (no chunking, no preview caps).
async fn download(
    State(state): Shared,
    Path((p, topic, partition, offset)): Path<(String, String, i32, i64)>,
    Query(q): Query<DownloadQuery>,
) -> Result<Response, ApiError> {
    use crate::details::Part;
    let rt = profile(&state, &p)?;
    let entry = state
        .details
        .get(&rt, &topic, partition, offset)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("{topic}/{partition}@{offset} no longer exists")))?;
    let stem = format!("{topic}-{partition}-{offset}");
    if q.part == "message" {
        let mut line = message_to_jsonl_line(&entry.message, None)?;
        line.push('\n');
        return Ok(attachment(line.into_bytes(), "application/x-ndjson", &format!("{stem}.jsonl")));
    }
    let part = match q.part.as_str() {
        "key" => Part::Key,
        "value" => Part::Value,
        other => return Err(ApiError::bad_request(format!("part must be key, value or message, got '{other}'"))),
    };
    let raw = match part {
        Part::Key => entry.message.key.clone(),
        Part::Value => entry.message.value.clone(),
    };
    let Some(raw) = raw else {
        return Err(ApiError::not_found(format!("the {} is null", q.part)));
    };
    match q.format.as_str() {
        "raw" => Ok(attachment(raw, "application/octet-stream", &format!("{stem}.{}.bin", q.part))),
        "decoded" => {
            let rendered = entry
                .render(&rt, part)
                .await?
                .ok_or_else(|| ApiError::not_found(format!("the {} is null", q.part)))?;
            let (ct, ext) = if rendered.format == "raw" { ("text/plain; charset=utf-8", "txt") } else { ("application/json", "json") };
            Ok(attachment(rendered.text.clone().into_bytes(), ct, &format!("{stem}.{}.{ext}", q.part)))
        }
        other => Err(ApiError::bad_request(format!("format must be raw or decoded, got '{other}'"))),
    }
}

// ---------------------------------------------------------------- delete

/// Kafka can't delete a record in place. On a compacted topic a null-value record with
/// the same key makes compaction drop the key; this sends exactly that.
async fn tombstone(
    State(state): Shared,
    Path((p, topic, partition, offset)): Path<(String, String, i32, i64)>,
) -> ApiResult<Value> {
    let rt = profile(&state, &p)?;
    let m = fetch_message(&rt.profile, &topic, partition, offset)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("{topic}/{partition}@{offset} no longer exists")))?;
    let key = m.key.ok_or_else(|| ApiError::bad_request("this message has no key, so there is nothing to tombstone"))?;
    let (np, no) = RawProducer::new(&rt.profile)?.send(&topic, Some(&key), None, &[]).await?;
    Ok(Json(json!({ "partition": np, "offset": no })))
}

#[derive(Deserialize)]
struct DeleteRecordsBody {
    partition: i32,
    /// Everything below this offset is deleted permanently.
    before_offset: i64,
    /// Must equal the topic name.
    confirm: String,
}

async fn delete_records(
    State(state): Shared,
    Path((p, topic)): Path<(String, String)>,
    Json(body): Json<DeleteRecordsBody>,
) -> ApiResult<Value> {
    if body.confirm != topic {
        return Err(ApiError::bad_request("confirm must equal the topic name"));
    }
    if body.before_offset < 0 {
        return Err(ApiError::bad_request("before_offset must be >= 0"));
    }
    let rt = profile(&state, &p)?;
    let low = delete_records_before(&rt.profile, &topic, body.partition, body.before_offset).await?;
    tracing::info!("deleted records below {}/{}@{} on profile {p}", topic, body.partition, body.before_offset);
    state.details.clear();
    Ok(Json(json!({ "low": low })))
}

// ---------------------------------------------------------------- schema registry

fn registry_url(rt: &crate::runtime::ProfileRuntime) -> Result<String, ApiError> {
    rt.profile
        .schema_registry_url
        .clone()
        .ok_or_else(|| ApiError::bad_request(format!("profile '{}' has no schema registry configured", rt.name)))
}

async fn schema_subjects(State(state): Shared, Path(p): Path<String>) -> ApiResult<Vec<String>> {
    let rt = profile(&state, &p)?;
    Ok(Json(schema_admin::list_subjects(&registry_url(&rt)?).await?))
}

async fn schema_versions(State(state): Shared, Path((p, subject)): Path<(String, String)>) -> ApiResult<Vec<i64>> {
    let rt = profile(&state, &p)?;
    Ok(Json(schema_admin::list_versions(&registry_url(&rt)?, &subject).await?))
}

async fn schema_version(
    State(state): Shared,
    Path((p, subject, version)): Path<(String, String, String)>,
) -> ApiResult<schema_admin::SubjectVersion> {
    let rt = profile(&state, &p)?;
    Ok(Json(schema_admin::get_version(&registry_url(&rt)?, &subject, &version).await?))
}

#[derive(Deserialize)]
struct SchemaBody {
    schema: String,
    #[serde(default = "default_schema_type")]
    schema_type: String,
    #[serde(default)]
    references: Vec<SchemaReference>,
}

fn default_schema_type() -> String {
    "AVRO".into()
}

async fn schema_check(
    State(state): Shared,
    Path((p, subject)): Path<(String, String)>,
    Json(b): Json<SchemaBody>,
) -> ApiResult<Value> {
    let rt = profile(&state, &p)?;
    let ok = schema_admin::check_compatibility(&registry_url(&rt)?, &subject, &b.schema_type, &b.schema, &b.references).await?;
    Ok(Json(json!({ "compatible": ok })))
}

async fn schema_register(
    State(state): Shared,
    Path((p, subject)): Path<(String, String)>,
    Json(b): Json<SchemaBody>,
) -> ApiResult<Value> {
    let rt = profile(&state, &p)?;
    let id = schema_admin::register(&registry_url(&rt)?, &subject, &b.schema_type, &b.schema, &b.references).await?;
    tracing::info!("registered schema id {id} under subject {subject} on profile {p}");
    Ok(Json(json!({ "id": id })))
}

#[derive(Deserialize)]
struct DeleteSchemaQuery {
    #[serde(default)]
    permanent: bool,
    /// Must equal the subject name.
    confirm: String,
}

async fn schema_delete_version(
    State(state): Shared,
    Path((p, subject, version)): Path<(String, String, String)>,
    Query(q): Query<DeleteSchemaQuery>,
) -> ApiResult<Value> {
    if q.confirm != subject {
        return Err(ApiError::bad_request("confirm must equal the subject name"));
    }
    let rt = profile(&state, &p)?;
    schema_admin::delete_version(&registry_url(&rt)?, &subject, &version, q.permanent).await?;
    tracing::info!("deleted {subject} v{version} (permanent={}) on profile {p}", q.permanent);
    Ok(Json(json!({ "deleted": true })))
}

async fn schema_delete_subject(
    State(state): Shared,
    Path((p, subject)): Path<(String, String)>,
    Query(q): Query<DeleteSchemaQuery>,
) -> ApiResult<Value> {
    if q.confirm != subject {
        return Err(ApiError::bad_request("confirm must equal the subject name"));
    }
    let rt = profile(&state, &p)?;
    schema_admin::delete_subject(&registry_url(&rt)?, &subject, q.permanent).await?;
    tracing::info!("deleted subject {subject} (permanent={}) on profile {p}", q.permanent);
    Ok(Json(json!({ "deleted": true })))
}

#[derive(Deserialize)]
struct CompatBody {
    level: String,
}

const COMPAT_LEVELS: [&str; 7] =
    ["BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE", "NONE"];

async fn compat_get(state: &AppState, p: &str, subject: Option<&str>) -> ApiResult<Value> {
    let rt = profile(state, p)?;
    let level = schema_admin::get_compatibility(&registry_url(&rt)?, subject).await?;
    Ok(Json(json!({ "level": level })))
}

async fn compat_set(state: &AppState, p: &str, subject: Option<&str>, level: &str) -> ApiResult<Value> {
    let level = level.to_ascii_uppercase();
    if !COMPAT_LEVELS.contains(&level.as_str()) {
        return Err(ApiError::bad_request(format!("level must be one of {}", COMPAT_LEVELS.join(", "))));
    }
    let rt = profile(state, p)?;
    schema_admin::set_compatibility(&registry_url(&rt)?, subject, &level).await?;
    Ok(Json(json!({ "level": level })))
}

async fn schema_config_global(State(state): Shared, Path(p): Path<String>) -> ApiResult<Value> {
    compat_get(&state, &p, None).await
}

async fn schema_config_set_global(State(state): Shared, Path(p): Path<String>, Json(b): Json<CompatBody>) -> ApiResult<Value> {
    compat_set(&state, &p, None, &b.level).await
}

async fn schema_config(State(state): Shared, Path((p, subject)): Path<(String, String)>) -> ApiResult<Value> {
    compat_get(&state, &p, Some(&subject)).await
}

async fn schema_config_set(
    State(state): Shared,
    Path((p, subject)): Path<(String, String)>,
    Json(b): Json<CompatBody>,
) -> ApiResult<Value> {
    compat_set(&state, &p, Some(&subject), &b.level).await
}

// ---------------------------------------------------------------- topic management

#[derive(Deserialize)]
struct CreateTopicBody {
    name: String,
    partitions: i32,
    replication_factor: i32,
    /// Per-topic overrides, e.g. `{"retention.ms": "86400000", "cleanup.policy": "compact"}`.
    #[serde(default)]
    configs: std::collections::BTreeMap<String, String>,
}

async fn create_topic(State(state): Shared, Path(p): Path<String>, Json(b): Json<CreateTopicBody>) -> ApiResult<Value> {
    let rt = profile(&state, &p)?;
    if b.partitions > 10_000 {
        return Err(ApiError::bad_request("partitions must be at most 10000"));
    }
    if b.configs.keys().any(|k| k.trim().is_empty()) {
        return Err(ApiError::bad_request("config names must not be empty"));
    }
    let configs = b.configs.into_iter().collect();
    admin_create_topic(&rt.profile, &b.name, b.partitions, b.replication_factor, configs)
        .await
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    tracing::info!("created topic {} on profile {p}", b.name);
    Ok(Json(json!({ "name": b.name })))
}

#[derive(Deserialize)]
struct ConfirmQuery {
    /// Must equal the topic name.
    confirm: String,
}

async fn delete_topic(
    State(state): Shared,
    Path((p, topic)): Path<(String, String)>,
    Query(q): Query<ConfirmQuery>,
) -> ApiResult<Value> {
    if q.confirm != topic {
        return Err(ApiError::bad_request("confirm must equal the topic name"));
    }
    let rt = profile(&state, &p)?;
    admin_delete_topic(&rt.profile, &topic).await.map_err(|e| ApiError::bad_request(e.to_string()))?;
    tracing::info!("deleted topic {topic} on profile {p}");
    state.details.clear();
    state.samples.sweep();
    Ok(Json(json!({ "deleted": true })))
}

async fn topic_configs(State(state): Shared, Path((p, topic)): Path<(String, String)>) -> ApiResult<Vec<Value>> {
    let rt = profile(&state, &p)?;
    let entries = fetch_topic_configs(&rt.profile, &topic).await?;
    Ok(Json(entries.into_iter().map(|e| json!({ "name": e.name, "value": e.value })).collect()))
}

async fn topic_configs_all(State(state): Shared, Path((p, topic)): Path<(String, String)>) -> ApiResult<Vec<Value>> {
    let rt = profile(&state, &p)?;
    let entries = rakko_core::kafka::admin::fetch_topic_configs_all(&rt.profile, &topic).await?;
    Ok(Json(
        entries
            .into_iter()
            .map(|e| {
                json!({ "name": e.name, "value": e.value, "overridden": e.overridden, "read_only": e.read_only, "sensitive": e.sensitive, "source": e.source })
            })
            .collect(),
    ))
}

#[derive(Deserialize)]
struct AlterConfigsBody {
    #[serde(default)]
    set: std::collections::BTreeMap<String, String>,
    /// Names whose topic override is removed (back to the broker default).
    #[serde(default)]
    unset: Vec<String>,
}

async fn topic_configs_alter(
    State(state): Shared,
    Path((p, topic)): Path<(String, String)>,
    Json(b): Json<AlterConfigsBody>,
) -> ApiResult<Value> {
    let rt = profile(&state, &p)?;
    let (n_set, n_unset) = (b.set.len(), b.unset.len());
    rakko_core::kafka::admin::alter_topic_configs(&rt.profile, &topic, b.set.into_iter().collect(), b.unset)
        .await
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    tracing::info!("changed {n_set} and reset {n_unset} config(s) of topic {topic} on profile {p}");
    Ok(Json(json!({ "ok": true })))
}

/// Latest schema of a subject plus an editable JSON template of it (for the producer).
async fn schema_template(State(state): Shared, Path((p, subject)): Path<(String, String)>) -> ApiResult<Value> {
    let rt = profile(&state, &p)?;
    let base = registry_url(&rt)?;
    let latest = schema_admin::get_version(&base, &subject, "latest").await?;
    if !latest.schema_type.eq_ignore_ascii_case("avro") {
        return Err(ApiError::bad_request(format!("subject '{subject}' is {}, only Avro can be produced here", latest.schema_type)));
    }
    let id = u32::try_from(latest.id).map_err(|_| ApiError::bad_request("schema id out of range"))?;
    let schema = rakko_core::kafka::schema_registry::fetch_schema(&base, id).await?;
    Ok(Json(json!({ "subject": subject, "version": latest.version, "id": id, "template": schema.template() })))
}

async fn subjects_of(base: &str, ids: &[u32]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for id in ids {
        // An id the registry no longer knows just contributes nothing.
        if let Ok(subjects) = schema_admin::subjects_for_id(base, *id).await {
            for s in subjects {
                if !out.contains(&s) {
                    out.push(s);
                }
            }
        }
    }
    out
}

/// Subjects this topic's messages are encoded with, for the Avro producer. Works for any
/// naming strategy: it follows the schema ids of recent messages back to their subjects
/// (TopicNameStrategy `<topic>-value`, RecordNameStrategy `com.acme.Order`, …).
async fn avro_subjects(State(state): Shared, Path((p, topic)): Path<(String, String)>) -> ApiResult<Value> {
    let rt = profile(&state, &p)?;
    let base = registry_url(&rt)?;
    let samples = state.samples.get(&rt, &topic).await?;
    let (key, value) = (subjects_of(&base, &samples.key_schema_ids).await, subjects_of(&base, &samples.value_schema_ids).await);
    Ok(Json(json!({ "key": key, "value": value })))
}

async fn schema_id_subjects(State(state): Shared, Path((p, id)): Path<(String, u32)>) -> ApiResult<Vec<String>> {
    let rt = profile(&state, &p)?;
    Ok(Json(subjects_of(&registry_url(&rt)?, &[id]).await))
}

async fn partition_detail_list(State(state): Shared, Path((p, topic)): Path<(String, String)>) -> ApiResult<Vec<Value>> {
    let rt = profile(&state, &p)?;
    let parts = rakko_core::kafka::admin::partition_details(&rt.profile, &topic).await?;
    Ok(Json(
        parts
            .into_iter()
            .map(|d| {
                json!({
                    "partition": d.partition, "leader": d.leader, "replicas": d.replicas, "isr": d.isr,
                    "low": d.low, "high": d.high, "messages": (d.high - d.low).max(0),
                })
            })
            .collect(),
    ))
}

/// Lag samples for a group (see `lag.rs`). The first view also takes a sample straight away
/// so the chart isn't empty until the next tick.
async fn lag_history(State(state): Shared, Path((p, g)): Path<(String, String)>) -> ApiResult<Value> {
    profile(&state, &p)?;
    let mut samples = state.lag.view(&p, &g);
    if samples.is_empty() {
        if let Some(s) = crate::lag::sample(&state, &p, &g).await {
            state.lag.record(&p, &g, s.clone());
            samples.push(s);
        }
    }
    Ok(Json(json!({ "interval_s": crate::lag::SAMPLE_EVERY.as_secs(), "samples": samples })))
}

async fn acls(State(state): Shared, Path(p): Path<String>) -> ApiResult<Vec<Value>> {
    let rt = profile(&state, &p)?;
    let entries = rakko_core::kafka::admin::list_acls(&rt.profile).await?;
    Ok(Json(
        entries
            .into_iter()
            .map(|a| {
                json!({
                    "resource_type": a.resource_type, "resource_name": a.resource_name, "pattern": a.pattern,
                    "principal": a.principal, "host": a.host, "operation": a.operation, "permission": a.permission,
                })
            })
            .collect(),
    ))
}

async fn delete_group(
    State(state): Shared,
    Path((p, group)): Path<(String, String)>,
    Query(q): Query<ConfirmQuery>,
) -> ApiResult<Value> {
    if q.confirm != group {
        return Err(ApiError::bad_request("confirm must equal the group name"));
    }
    let rt = profile(&state, &p)?;
    rakko_core::kafka::admin::delete_group(&rt.profile, &group).await.map_err(|e| {
        let text = e.to_string();
        // Kafka refuses with NonEmptyGroup while consumers are still connected.
        if text.contains("NonEmptyGroup") || text.contains("non-empty") {
            ApiError::bad_request(format!("The group still has active members. Stop its consumers first, then delete it. ({text})"))
        } else {
            ApiError::bad_request(text)
        }
    })?;
    tracing::info!("deleted consumer group {group} on profile {p}");
    Ok(Json(json!({ "deleted": true })))
}

#[derive(Deserialize)]
struct ClearBody {
    /// Must equal the topic name.
    confirm: String,
}

async fn clear_topic(State(state): Shared, Path((p, topic)): Path<(String, String)>, Json(b): Json<ClearBody>) -> ApiResult<Value> {
    if b.confirm != topic {
        return Err(ApiError::bad_request("confirm must equal the topic name"));
    }
    let rt = profile(&state, &p)?;
    let removed = rakko_core::kafka::admin::clear_topic(&rt.profile, &topic).await.map_err(|e| ApiError::bad_request(e.to_string()))?;
    tracing::info!("cleared topic {topic} ({removed} offsets) on profile {p}");
    state.details.clear();
    state.samples.sweep();
    Ok(Json(json!({ "removed": removed })))
}
