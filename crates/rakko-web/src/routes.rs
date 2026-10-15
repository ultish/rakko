use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use rakko_core::kafka::admin::list_topics;
use rakko_core::kafka::consumer::partition_watermarks;
use rakko_core::kafka::scan::ScanRange;
use rakko_core::message_filter::MessageFilter;
use rakko_core::query_complete::suggest;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::details::{slice_text, Part};
use crate::error::{ApiError, ApiResult};
use crate::runtime::ProfileRuntime;
use crate::scan::{MatchesPage, ScanOptions, ScanSession};
use crate::state::AppState;

type Shared = State<Arc<AppState>>;

pub const SESSION_HEADER: &str = "x-rakko-session";

pub fn api_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/profiles", get(profiles))
        .route("/p/{profile}/topics", get(topics))
        .route("/p/{profile}/topic-counts", get(topic_counts))
        .route("/p/{profile}/topics/{topic}/partitions", get(partitions))
        .route("/p/{profile}/topics/{topic}/size", get(size_estimate))
        .route("/p/{profile}/topics/{topic}/scans", post(create_scan))
        .route("/p/{profile}/topics/{topic}/complete", post(complete))
        .route("/p/{profile}/topics/{topic}/messages/{partition}/{offset}", get(message_detail))
        .route("/scans/{id}/matches", get(scan_matches))
        .route("/scans/{id}", delete(delete_scan))
        .merge(crate::ops::router())
}

/// The caller's session. Scans belong to the session that created them, so two people
/// looking at the same topic never see or disturb each other's filter.
fn session(headers: &HeaderMap) -> Result<String, ApiError> {
    headers
        .get(SESSION_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|v| (8..=128).contains(&v.len()) && v.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'))
        .map(str::to_string)
        .ok_or_else(|| ApiError::bad_request(format!("missing or invalid {SESSION_HEADER} header")))
}

fn profile(state: &AppState, name: &str) -> Result<Arc<ProfileRuntime>, ApiError> {
    state.profile(name).ok_or_else(|| ApiError::not_found(format!("unknown profile '{name}'")))
}

#[derive(Serialize)]
struct ProfileInfo {
    name: String,
    has_schema_registry: bool,
}

async fn profiles(State(state): Shared) -> Json<Vec<ProfileInfo>> {
    Json(
        state
            .profiles
            .values()
            .map(|p| ProfileInfo { name: p.name.clone(), has_schema_registry: p.has_registry() })
            .collect(),
    )
}

#[derive(Serialize)]
struct TopicInfo {
    name: String,
    partitions: usize,
    replication_factor: i32,
    compression: String,
    messages: i64,
}

async fn topics(State(state): Shared, Path(profile_name): Path<String>) -> ApiResult<Vec<TopicInfo>> {
    let rt = profile(&state, &profile_name)?;
    let topics = list_topics(&rt.profile).await?;
    Ok(Json(
        topics
            .into_iter()
            .map(|t| TopicInfo {
                name: t.name,
                partitions: t.partition_count,
                replication_factor: t.replication_factor,
                compression: t.compression_type,
                messages: t.total_message_count,
            })
            .collect(),
    ))
}

/// Just the stored-offset count per topic: the light call the topic list polls for live numbers.
async fn topic_counts(State(state): Shared, Path(profile_name): Path<String>) -> ApiResult<std::collections::HashMap<String, i64>> {
    let rt = profile(&state, &profile_name)?;
    Ok(Json(rakko_core::kafka::admin::topic_message_counts(&rt.profile).await?.into_iter().collect()))
}

#[derive(Serialize)]
struct PartitionInfo {
    partition: i32,
    low: i64,
    high: i64,
    messages: i64,
}

async fn partitions(State(state): Shared, Path((profile_name, topic)): Path<(String, String)>) -> ApiResult<Vec<PartitionInfo>> {
    let rt = profile(&state, &profile_name)?;
    let marks = partition_watermarks(&rt.profile, &topic).await?;
    Ok(Json(
        marks
            .into_iter()
            .map(|m| PartitionInfo { partition: m.partition, low: m.low, high: m.high, messages: m.message_count() })
            .collect(),
    ))
}

#[derive(Serialize)]
struct PartitionSize {
    partition: i32,
    messages: i64,
    bytes: i64,
}

/// Messages sampled per window (start, middle, end of each partition).
const SIZE_SAMPLE: usize = 30;

/// Kafka has no cheap "how big is this topic" call (log-dir sizes are not reachable through
/// the client library), so estimate: average the payload size of a few sampled messages per
/// partition and multiply by the number of stored offsets.
async fn size_estimate(State(state): Shared, Path((profile_name, topic)): Path<(String, String)>) -> ApiResult<Value> {
    use rakko_core::kafka::consumer::{read_seek_page, SeekPageRequest};
    let rt = profile(&state, &profile_name)?;
    let marks = partition_watermarks(&rt.profile, &topic).await?;
    let per_partition = futures::future::join_all(marks.iter().map(|m| {
        let (profile, topic) = (rt.profile.clone(), topic.clone());
        let m = *m;
        async move {
            let count = m.message_count();
            if count == 0 {
                return PartitionSize { partition: m.partition, messages: 0, bytes: 0 };
            }
            let mid = m.low + count / 2;
            let requests = [
                SeekPageRequest::Forward { from_offset: m.low, page_size: SIZE_SAMPLE },
                SeekPageRequest::Forward { from_offset: mid, page_size: SIZE_SAMPLE },
                SeekPageRequest::Latest { page_size: SIZE_SAMPLE },
            ];
            let (mut total, mut n) = (0u64, 0u64);
            let mut seen = std::collections::HashSet::new();
            for r in requests {
                if let Ok((messages, _)) = read_seek_page(&profile, &topic, m.partition, r).await {
                    for msg in messages.iter().filter(|x| seen.insert(x.offset)) {
                        let headers: usize = msg.headers.iter().map(|(k, v)| k.len() + v.len()).sum();
                        total += (msg.key.as_ref().map_or(0, Vec::len) + msg.value.as_ref().map_or(0, Vec::len) + headers) as u64;
                        n += 1;
                    }
                }
            }
            let bytes = if n == 0 { 0 } else { (total as f64 / n as f64 * count as f64) as i64 };
            PartitionSize { partition: m.partition, messages: count, bytes }
        }
    }))
    .await;
    let bytes: i64 = per_partition.iter().map(|p| p.bytes).sum();
    Ok(Json(json!({ "bytes": bytes, "partitions": per_partition })))
}

#[derive(Deserialize, Default)]
struct ScanRequest {
    /// Case-insensitive substring over key and value.
    text: Option<String>,
    /// Structured query, e.g. `value.customer.tier = "gold" AND value.qty > 2`.
    query: Option<String>,
    /// Limit the scan to a time window or one partition's offset window.
    #[serde(default)]
    range: ScanRange,
    /// Read the topic as a table: only the latest record per key, tombstoned keys removed.
    #[serde(default)]
    table: bool,
    /// Order the results (default: newest first, streamed). Anything else scans the whole topic first.
    sort: Option<crate::scan::Sort>,
}

async fn create_scan(
    State(state): Shared,
    headers: HeaderMap,
    Path((profile_name, topic)): Path<(String, String)>,
    Json(body): Json<ScanRequest>,
) -> ApiResult<Value> {
    let owner = session(&headers)?;
    let rt = profile(&state, &profile_name)?;
    let filter = MessageFilter::parse(body.text.as_deref(), body.query.as_deref()).map_err(ApiError::bad_request)?;
    body.range.validate().map_err(|e| ApiError::bad_request(e.to_string()))?;
    let scan = ScanSession::open(owner, rt, topic, filter, body.range, ScanOptions { window: state.scan_window, table: body.table, sort: body.sort }).await?;
    let (id, total) = (scan.id.clone(), scan.total_offsets());
    state
        .scans
        .insert(scan)
        .map_err(|msg| ApiError::new(axum::http::StatusCode::TOO_MANY_REQUESTS, msg))?;
    Ok(Json(json!({ "scan_id": id, "total_offsets": total })))
}

#[derive(Deserialize)]
struct MatchesQuery {
    #[serde(default)]
    from: usize,
    #[serde(default = "default_limit")]
    limit: usize,
    /// How long to wait for matches while the scan advances.
    #[serde(default = "default_wait_ms")]
    wait_ms: u64,
}

fn default_limit() -> usize {
    50
}

fn default_wait_ms() -> u64 {
    2500
}

async fn scan_matches(
    State(state): Shared,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<MatchesQuery>,
) -> ApiResult<MatchesPage> {
    let owner = session(&headers)?;
    let scan = state.scans.get(&id, &owner).ok_or_else(|| ApiError::not_found("no such scan (it may have expired)"))?;
    let limit = q.limit.clamp(1, 500);
    let wait = Duration::from_millis(q.wait_ms.min(15_000));
    Ok(Json(scan.matches(q.from, limit, wait).await))
}

async fn delete_scan(State(state): Shared, headers: HeaderMap, Path(id): Path<String>) -> Result<(), ApiError> {
    let owner = session(&headers)?;
    state.scans.remove(&id, &owner).then_some(()).ok_or_else(|| ApiError::not_found("no such scan"))
}

#[derive(Deserialize)]
struct CompleteRequest {
    input: String,
}

async fn complete(
    State(state): Shared,
    Path((profile_name, topic)): Path<(String, String)>,
    Json(body): Json<CompleteRequest>,
) -> ApiResult<Value> {
    let rt = profile(&state, &profile_name)?;
    // Root words need no samples; don't touch Kafka for them.
    let samples = match state.samples.get(&rt, &topic).await {
        Ok(s) => Some(s),
        Err(err) => {
            tracing::warn!("autocomplete samples for {topic} unavailable: {err}");
            None
        }
    };
    let suggestions = suggest(&body.input, |root| samples.as_ref().map(|s| s.for_root(root)).unwrap_or_default());
    Ok(Json(match suggestions {
        Some(s) => json!({ "prefix": s.prefix, "candidates": s.candidates }),
        None => json!({ "prefix": "", "candidates": [] }),
    }))
}

#[derive(Deserialize)]
struct DetailQuery {
    part: String,
    #[serde(default)]
    start: usize,
    #[serde(default = "default_chunk")]
    max: usize,
}

const MAX_CHUNK: usize = 4 * 1024 * 1024;

fn default_chunk() -> usize {
    256 * 1024
}

#[derive(Serialize)]
struct DetailResponse {
    partition: i32,
    offset: i64,
    timestamp_ms: Option<i64>,
    headers: Vec<(String, String)>,
    part: String,
    /// False for a null key/value; the fields below are then empty.
    present: bool,
    format: String,
    /// Raw payload size in bytes.
    size: usize,
    text: String,
    /// Byte range of `text` within the full decoded text, and that text's length —
    /// request `start=end` for the next chunk until `end == total`.
    start: usize,
    end: usize,
    total: usize,
}

async fn message_detail(
    State(state): Shared,
    Path((profile_name, topic, partition, offset)): Path<(String, String, i32, i64)>,
    Query(q): Query<DetailQuery>,
) -> ApiResult<DetailResponse> {
    let rt = profile(&state, &profile_name)?;
    let part = match q.part.as_str() {
        "key" => Part::Key,
        "value" => Part::Value,
        other => return Err(ApiError::bad_request(format!("part must be 'key' or 'value', got '{other}'"))),
    };
    let entry = state
        .details
        .get(&rt, &topic, partition, offset)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("{topic}/{partition}@{offset} no longer exists")))?;
    let m = &entry.message;
    let raw_len = match part {
        Part::Key => m.key.as_ref(),
        Part::Value => m.value.as_ref(),
    }
    .map(Vec::len);
    let rendered = entry.render(&rt, part).await?;
    let (present, format, text, start, end, total) = match &rendered {
        Some(r) => {
            let (s, e, piece) = slice_text(&r.text, q.start, q.max.clamp(1, MAX_CHUNK));
            (true, r.format.clone(), piece.to_string(), s, e, r.text.len())
        }
        None => (false, "—".to_string(), String::new(), 0, 0, 0),
    };
    Ok(Json(DetailResponse {
        partition: m.partition,
        offset: m.offset,
        timestamp_ms: m.timestamp_millis,
        headers: m.headers.iter().map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned())).collect(),
        part: q.part,
        present,
        format,
        size: raw_len.unwrap_or(0),
        text,
        start,
        end,
        total,
    }))
}
