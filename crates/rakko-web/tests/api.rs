//! End-to-end API tests against the docker-compose stack (`docker compose up -d`):
//! `cargo test --workspace -- --ignored`. They drive the real router, so routing,
//! sessions, scans, autocomplete and detail are exercised together.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use rakko_core::config::{AuthMode, Config, Profile};
use rakko_web::state::AppState;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde_json::{json, Value};
use tower::ServiceExt;

const BOOTSTRAP: &str = "localhost:19092";
const REGISTRY: &str = "http://localhost:18081";
const SESSION: &str = "session-aaaaaaaa";
const OTHER_SESSION: &str = "session-bbbbbbbb";

fn app() -> Router {
    let profile = Profile {
        name: "local".into(),
        bootstrap_servers: BOOTSTRAP.into(),
        tls_enabled: false,
        auth: AuthMode::None,
        schema_registry_url: Some(REGISTRY.into()),
        message_max_bytes: None,
        extra_producer_config: HashMap::new(),
    };
    let state = AppState::new(Config { profiles: vec![profile], ..Default::default() }, 40);
    rakko_web::build_router(state, None)
}

fn unique(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("rakko-web-it-{prefix}-{}-{nanos}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed))
}

async fn call(app: &Router, method: Method, uri: &str, session: Option<&str>, body: Option<Value>) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(s) = session {
        req = req.header("x-rakko-session", s);
    }
    let body = match body {
        Some(v) => {
            req = req.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let resp = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn create_topic(name: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> =
        ClientConfig::new().set("bootstrap.servers", BOOTSTRAP).create().unwrap();
    admin
        .create_topics(&[NewTopic::new(name, partitions, TopicReplication::Fixed(1))], &AdminOptions::new())
        .await
        .unwrap()[0]
        .as_ref()
        .expect("topic created");
}

async fn produce(topic: &str, records: Vec<(String, Vec<u8>)>) {
    let producer: FutureProducer = ClientConfig::new().set("bootstrap.servers", BOOTSTRAP).create().unwrap();
    let sends: Vec<_> = records
        .into_iter()
        .map(|(key, value)| {
            let (producer, topic) = (producer.clone(), topic.to_string());
            tokio::spawn(async move {
                producer
                    .send(FutureRecord::to(&topic).key(&key).payload(&value), Duration::from_secs(30))
                    .await
                    .map(|_| ())
                    .map_err(|(e, _)| e)
            })
        })
        .collect();
    for s in sends {
        s.await.unwrap().expect("produce");
    }
}

/// Pulls every match of a scan, page by page, until it reports done.
async fn drain(app: &Router, scan_id: &str, session: &str) -> (Vec<Value>, Value) {
    let mut all: Vec<Value> = Vec::new();
    for _ in 0..200 {
        let uri = format!("/api/scans/{scan_id}/matches?from={}&limit=25&wait_ms=5000", all.len());
        let (status, page) = call(app, Method::GET, &uri, Some(session), None).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        all.extend(page["matches"].as_array().unwrap().iter().cloned());
        if page["done"].as_bool().unwrap() && all.len() >= page["found"].as_u64().unwrap() as usize {
            return (all, page);
        }
    }
    panic!("scan never finished");
}

#[tokio::test]
#[ignore = "requires `docker compose up -d`"]
async fn json_topic_scan_filter_complete_and_detail() {
    let app = app();
    let topic = unique("json");
    create_topic(&topic, 3).await;
    let records = (0..200)
        .map(|i| {
            (
                format!("k{i}"),
                json!({"n": i, "tier": if i % 2 == 0 { "gold" } else { "silver" }, "who": {"name": format!("user{i}")}})
                    .to_string()
                    .into_bytes(),
            )
        })
        .collect();
    produce(&topic, records).await;

    // Topics and partitions are listed.
    let (status, partitions) = call(&app, Method::GET, &format!("/api/p/local/topics/{topic}/partitions"), None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(partitions.as_array().unwrap().iter().map(|p| p["messages"].as_i64().unwrap()).sum::<i64>(), 200);

    // A structured query finds exactly the matching messages across all partitions.
    let (status, created) = call(
        &app,
        Method::POST,
        &format!("/api/p/local/topics/{topic}/scans"),
        Some(SESSION),
        Some(json!({"query": "value.n >= 150"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert_eq!(created["total_offsets"], 200);
    let scan_id = created["scan_id"].as_str().unwrap().to_string();
    let (matches, last) = drain(&app, &scan_id, SESSION).await;
    assert_eq!(matches.len(), 50);
    assert_eq!(last["scanned_offsets"], 200);
    let found: HashSet<i64> = matches
        .iter()
        .map(|m| serde_json::from_str::<Value>(&m["value"]["preview"].as_str().unwrap().replace('…', "")).unwrap()["n"].as_i64().unwrap())
        .collect();
    assert_eq!(found, (150..200).collect::<HashSet<_>>());

    // Another session can't read or cancel it.
    let uri = format!("/api/scans/{scan_id}/matches");
    assert_eq!(call(&app, Method::GET, &uri, Some(OTHER_SESSION), None).await.0, StatusCode::NOT_FOUND);
    assert_eq!(call(&app, Method::DELETE, &format!("/api/scans/{scan_id}"), Some(OTHER_SESSION), None).await.0, StatusCode::NOT_FOUND);
    assert_eq!(call(&app, Method::GET, &uri, None, None).await.0, StatusCode::BAD_REQUEST);

    // Text filter and an empty filter (= browse the whole topic) both work.
    let (_, t) = call(&app, Method::POST, &format!("/api/p/local/topics/{topic}/scans"), Some(SESSION), Some(json!({"text": "USER7"}))).await;
    let (text_matches, _) = drain(&app, t["scan_id"].as_str().unwrap(), SESSION).await;
    assert_eq!(text_matches.len(), 11, "user7, user70..user79");
    let (_, all) = call(&app, Method::POST, &format!("/api/p/local/topics/{topic}/scans"), Some(SESSION), Some(json!({}))).await;
    let (everything, _) = drain(&app, all["scan_id"].as_str().unwrap(), SESSION).await;
    assert_eq!(everything.len(), 200);

    // A bad query is a 400 before any scanning happens.
    let (status, err) = call(&app, Method::POST, &format!("/api/p/local/topics/{topic}/scans"), Some(SESSION), Some(json!({"query": "value.a ===="}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(err["error"].as_str().unwrap().len() > 3);

    // Autocomplete offers real field names, including nested ones.
    let complete = |input: &str| {
        let (app, uri) = (app.clone(), format!("/api/p/local/topics/{topic}/complete"));
        let input = input.to_string();
        async move { call(&app, Method::POST, &uri, None, Some(json!({"input": input}))).await.1 }
    };
    assert_eq!(complete("value.").await["candidates"], json!(["n", "tier", "who"]));
    assert_eq!(complete("value.t").await["candidates"], json!(["tier"]));
    assert_eq!(complete("value.who.").await["candidates"], json!(["name"]));
    assert_eq!(complete("val").await["candidates"], json!(["value"]));

    // Detail: fetched on demand and served in chunks.
    let first = &matches[0];
    let (p, o) = (first["partition"].as_i64().unwrap(), first["offset"].as_i64().unwrap());
    let base = format!("/api/p/local/topics/{topic}/messages/{p}/{o}");
    let (status, detail) = call(&app, Method::GET, &format!("{base}?part=value&max=20"), None, None).await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["format"], "json");
    assert_eq!(detail["end"], 20);
    assert!(detail["total"].as_u64().unwrap() > 20);
    let (_, rest) = call(&app, Method::GET, &format!("{base}?part=value&start=20&max=100000"), None, None).await;
    let whole = format!("{}{}", detail["text"].as_str().unwrap(), rest["text"].as_str().unwrap());
    assert_eq!(whole.len(), detail["total"].as_u64().unwrap() as usize);
    assert!(serde_json::from_str::<Value>(&whole).is_ok(), "chunks reassemble into the full JSON: {whole}");
    let (_, key) = call(&app, Method::GET, &format!("{base}?part=key"), None, None).await;
    assert!(key["text"].as_str().unwrap().starts_with('k'));
    assert_eq!(call(&app, Method::GET, &format!("{base}?part=nope"), None, None).await.0, StatusCode::BAD_REQUEST);
    let missing = format!("/api/p/local/topics/{topic}/messages/{p}/99999?part=value");
    let (status, body) = call(&app, Method::GET, &missing, None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

fn zigzag_len(s: &str) -> u8 {
    assert!(s.len() < 64);
    (s.len() * 2) as u8
}

#[tokio::test]
#[ignore = "requires `docker compose up -d`"]
async fn avro_with_schema_references_scans_without_a_preloaded_schema() {
    let app = app();
    let client = reqwest::Client::new();
    let register = |subject: String, body: Value| {
        let client = client.clone();
        async move {
            let r = client
                .post(format!("{REGISTRY}/subjects/{subject}/versions"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .json(&body)
                .send()
                .await
                .unwrap();
            assert!(r.status().is_success());
            r.json::<Value>().await.unwrap()["id"].as_u64().unwrap() as u32
        }
    };
    let address = r#"{"type":"record","name":"Address","namespace":"com.example","fields":[{"name":"city","type":"string"}]}"#;
    let person = r#"{"type":"record","name":"Person","namespace":"com.example","fields":[{"name":"name","type":"string"},{"name":"home","type":"com.example.Address"}]}"#;
    let address_subject = unique("address");
    register(address_subject.clone(), json!({"schema": address})).await;
    // RecordNameStrategy-style subject (not `<topic>-value`): decode must not care.
    let person_id = register(
        format!("com.example.Person-{}", unique("p")),
        json!({"schema": person, "references": [{"name": "com.example.Address", "subject": address_subject, "version": 1}]}),
    )
    .await;

    let topic = unique("avro");
    create_topic(&topic, 2).await;
    let records = (0..60)
        .map(|i| {
            let (name, city) = (format!("p{i}"), format!("city{}", i % 3));
            let mut v = vec![0u8];
            v.extend(person_id.to_be_bytes());
            v.push(zigzag_len(&name));
            v.extend(name.as_bytes());
            v.push(zigzag_len(&city));
            v.extend(city.as_bytes());
            (format!("k{i}"), v)
        })
        .collect();
    produce(&topic, records).await;

    let (status, created) = call(
        &app,
        Method::POST,
        &format!("/api/p/local/topics/{topic}/scans"),
        Some(SESSION),
        Some(json!({"query": "value.home.city = \"city1\""})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let (matches, last) = drain(&app, created["scan_id"].as_str().unwrap(), SESSION).await;
    assert_eq!(matches.len(), 20, "every third of 60");
    assert_eq!(last["skipped_undecodable"], 0);
    let format = matches[0]["value"]["format"].as_str().unwrap();
    assert_eq!(format, format!("avro:{person_id}"), "schema (and its reference) was fetched mid-scan");
    assert!(matches[0]["value"]["preview"].as_str().unwrap().contains("\"city\":\"city1\""));

    // Autocomplete reaches into the referenced record's fields.
    let (_, c) = call(&app, Method::POST, &format!("/api/p/local/topics/{topic}/complete"), None, Some(json!({"input": "value.home."}))).await;
    assert_eq!(c["candidates"], json!(["city"]));

    // Detail renders pretty JSON from the Avro payload.
    let (p, o) = (matches[0]["partition"].as_i64().unwrap(), matches[0]["offset"].as_i64().unwrap());
    let (status, d) = call(&app, Method::GET, &format!("/api/p/local/topics/{topic}/messages/{p}/{o}?part=value"), None, None).await;
    assert_eq!(status, StatusCode::OK, "{d}");
    assert_eq!(d["format"], format!("avro:{person_id}"));
    assert!(d["text"].as_str().unwrap().contains("\"city\": \"city1\""), "{}", d["text"]);
}

async fn call_raw(app: &Router, method: Method, uri: &str, content_type: &str, body: Vec<u8>) -> (StatusCode, Vec<u8>) {
    let req = Request::builder().method(method).uri(uri).header("content-type", content_type).body(Body::from(body)).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    (status, resp.into_body().collect().await.unwrap().to_bytes().to_vec())
}

/// Reads server-sent events until `want` `message` events arrived (or time runs out).
async fn read_sse_messages(resp: axum::response::Response, want: usize) -> Vec<Value> {
    use futures::StreamExt;
    let mut stream = resp.into_body().into_data_stream();
    let (mut text, mut out) = (String::new(), Vec::new());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while out.len() < want {
        let Ok(Some(Ok(chunk))) = tokio::time::timeout_at(deadline, stream.next()).await else { break };
        text.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(end) = text.find("\n\n") {
            let frame: String = text.drain(..end + 2).collect();
            if frame.contains("event: message") {
                let data = frame.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
                out.push(serde_json::from_str(data).unwrap());
            }
        }
    }
    out
}

#[tokio::test]
#[ignore = "requires `docker compose up -d`"]
async fn produce_replay_export_import_groups_brokers_and_tail() {
    let app = app();
    let topic = unique("ops");
    let copy = unique("ops-copy");
    create_topic(&topic, 1).await;
    create_topic(&copy, 1).await;
    let base = format!("/api/p/local/topics/{topic}");

    // Produce: text key + text value + header, and a base64 binary value.
    let (st, body) = call(
        &app,
        Method::POST,
        &format!("{base}/produce"),
        None,
        Some(json!({"key": {"text": "k1"}, "value": {"text": "{\"n\":1}"}, "headers": [{"key": "h", "value": "v"}]})),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let first_offset = body["offset"].as_i64().unwrap();
    let binary = [0u8, 255, 1, 2, 254];
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(binary);
    let (st, _) = call(&app, Method::POST, &format!("{base}/produce"), None, Some(json!({"value": {"base64": b64}}))).await;
    assert_eq!(st, StatusCode::OK);
    let (st, body) = call(&app, Method::POST, &format!("{base}/produce"), None, Some(json!({}))).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "an empty message is rejected: {body}");

    // Replay re-sends the exact bytes and headers as a new message.
    let (st, body) = call(&app, Method::POST, &format!("{base}/messages/0/{first_offset}/replay"), None, None).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["offset"].as_i64().unwrap(), 2);

    // Export is raw base64 JSONL; the binary value survives byte for byte.
    let (st, bytes) = call_raw(&app, Method::GET, &format!("{base}/export"), "application/json", vec![]).await;
    assert_eq!(st, StatusCode::OK);
    let exported = String::from_utf8(bytes.clone()).unwrap();
    let lines: Vec<Value> = exported.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    assert_eq!(lines.len(), 3);
    assert!(lines.iter().any(|l| l["value_b64"] == b64.as_str()));

    // Filtered export only carries matches.
    let (_, bytes) = call_raw(&app, Method::GET, &format!("{base}/export?query=value.n%20%3D%201"), "application/json", vec![]).await;
    assert_eq!(String::from_utf8(bytes).unwrap().lines().count(), 2, "original + its replay");

    // Import into another topic reproduces the bytes.
    let (st, bytes) = call_raw(&app, Method::POST, &format!("/api/p/local/topics/{copy}/import"), "application/x-ndjson", exported.into_bytes()).await;
    assert_eq!(st, StatusCode::OK);
    let report: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!((report["sent"].as_u64(), report["failed"].as_u64()), (Some(3), Some(0)), "{report}");
    let (_, bytes) = call_raw(&app, Method::GET, &format!("/api/p/local/topics/{copy}/export"), "application/json", vec![]).await;
    let copied: Vec<Value> = String::from_utf8(bytes).unwrap().lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    let values = |v: &[Value]| {
        let mut x: Vec<_> = v.iter().map(|l| (l["key_b64"].clone(), l["value_b64"].clone(), l["headers"].clone())).collect();
        x.sort_by_key(|t| t.1.to_string());
        x
    };
    assert_eq!(values(&lines), values(&copied));

    // Bad import lines are reported, not fatal.
    let (_, bytes) =
        call_raw(&app, Method::POST, &format!("/api/p/local/topics/{copy}/import"), "application/x-ndjson", b"not json\n".to_vec()).await;
    let report: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(report["failed"], 1);
    assert!(report["errors"][0].as_str().unwrap().starts_with("line 1"));

    // Brokers.
    let (st, brokers) = call(&app, Method::GET, "/api/p/local/brokers", None, None).await;
    assert_eq!(st, StatusCode::OK);
    assert!(!brokers["brokers"].as_array().unwrap().is_empty());

    // Group: commit via a real consumer, see lag, reset needs the typed confirmation.
    let group = unique("grp");
    {
        use rdkafka::consumer::{CommitMode, Consumer, BaseConsumer};
        use rdkafka::TopicPartitionList;
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", BOOTSTRAP)
            .set("group.id", &group)
            .set("enable.auto.commit", "false")
            .create()
            .unwrap();
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&topic, 0, rdkafka::Offset::Offset(2)).unwrap();
        consumer.assign(&tpl).unwrap();
        consumer.commit(&tpl, CommitMode::Sync).unwrap();
    }
    let (_, g) = call(&app, Method::GET, &format!("/api/p/local/groups/{group}"), None, None).await;
    assert_eq!(g["total_lag"], 1, "{g}");
    let reset = |confirm: &str| json!({"target": {"kind": "latest"}, "confirm": confirm});
    let (st, _) = call(&app, Method::POST, &format!("/api/p/local/groups/{group}/reset"), None, Some(reset("wrong"))).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let (st, body) = call(&app, Method::POST, &format!("/api/p/local/groups/{group}/reset"), None, Some(reset(&group))).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let (_, g) = call(&app, Method::GET, &format!("/api/p/local/groups/{group}"), None, None).await;
    assert_eq!(g["total_lag"], 0, "{g}");

    // Live tail: only messages produced after connecting, filtered server-side.
    let req = Request::builder().uri(format!("{base}/tail?query=value.n%20%3E%2010")).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let reader = tokio::spawn(read_sse_messages(resp, 1));
    tokio::time::sleep(Duration::from_secs(3)).await;
    for n in [5, 50] {
        let body = json!({"value": {"text": format!("{{\"n\":{n}}}")}});
        call(&app, Method::POST, &format!("{base}/produce"), None, Some(body)).await;
    }
    let got = reader.await.unwrap();
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0]["value"]["preview"], "{\"n\":50}");
}

#[tokio::test]
#[ignore = "requires `docker compose up -d`"]
async fn delete_records_tombstone_download_and_schema_management() {
    let app = app();
    let topic = unique("del");
    create_topic(&topic, 1).await;
    let base = format!("/api/p/local/topics/{topic}");
    produce(&topic, (0..5).map(|i| (format!("k{i}"), format!("{{\"i\":{i}}}").into_bytes())).collect()).await;

    // Download: raw bytes, decoded text, and the whole record as importable JSONL.
    let (st, bytes) = call_raw(&app, Method::GET, &format!("{base}/messages/0/3/download?part=value"), "x/y", vec![]).await;
    assert_eq!((st, bytes.as_slice()), (StatusCode::OK, &br#"{"i":3}"#[..]));
    let (_, bytes) = call_raw(&app, Method::GET, &format!("{base}/messages/0/3/download?part=key&format=decoded"), "x/y", vec![]).await;
    assert_eq!(bytes, b"k3");
    let (_, bytes) = call_raw(&app, Method::GET, &format!("{base}/messages/0/3/download?part=message"), "x/y", vec![]).await;
    let line = rakko_core::export::parse_jsonl_line(std::str::from_utf8(&bytes).unwrap()).unwrap();
    assert_eq!((line.offset, line.key.as_deref(), line.value.as_deref()), (3, Some(&b"k3"[..]), Some(&br#"{"i":3}"#[..])));
    let (st, _) = call_raw(&app, Method::GET, &format!("{base}/messages/0/3/download?part=nope"), "x/y", vec![]).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // Tombstone: same key, null value, appended.
    let (st, body) = call(&app, Method::POST, &format!("{base}/messages/0/3/tombstone"), None, None).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["offset"], 5);
    let (st, _) = call_raw(&app, Method::GET, &format!("{base}/messages/0/5/download?part=value"), "x/y", vec![]).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "a null value has nothing to download");

    // Delete records needs the topic name typed, then moves the start forward.
    let del = |confirm: &str| json!({"partition": 0, "before_offset": 3, "confirm": confirm});
    let (st, _) = call(&app, Method::POST, &format!("{base}/delete-records"), None, Some(del("nope"))).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let (st, body) = call(&app, Method::POST, &format!("{base}/delete-records"), None, Some(del(&topic))).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["low"], 3);
    let (_, parts) = call(&app, Method::GET, &format!("{base}/partitions"), None, None).await;
    assert_eq!((parts[0]["low"].as_i64(), parts[0]["messages"].as_i64()), (Some(3), Some(3)));

    // Schemas: register, list, read, compatibility, evolve, delete.
    let subject = unique("subj");
    let v1 = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}"#;
    let v2 = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"string"},{"name":"b","type":"int","default":0}]}"#;
    let bad = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#;
    let s = format!("/api/p/local/schemas/{subject}");
    let (st, body) = call(&app, Method::POST, &s, None, Some(json!({"schema": v1}))).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let (_, subjects) = call(&app, Method::GET, "/api/p/local/schemas", None, None).await;
    assert!(subjects.as_array().unwrap().iter().any(|x| *x == subject.as_str()));
    let (_, ok) = call(&app, Method::POST, &format!("{s}/check"), None, Some(json!({"schema": v2}))).await;
    assert_eq!(ok["compatible"], true);
    let (_, no) = call(&app, Method::POST, &format!("{s}/check"), None, Some(json!({"schema": bad}))).await;
    assert_eq!(no["compatible"], false);
    let (st, _) = call(&app, Method::POST, &s, None, Some(json!({"schema": "not avro"}))).await;
    assert_ne!(st, StatusCode::OK, "an invalid schema is rejected by the registry");
    call(&app, Method::POST, &s, None, Some(json!({"schema": v2}))).await;
    let (_, versions) = call(&app, Method::GET, &s, None, None).await;
    assert_eq!(versions, json!([1, 2]));
    let (_, latest) = call(&app, Method::GET, &format!("{s}/versions/latest"), None, None).await;
    assert_eq!(latest["version"], 2);
    assert!(latest["schema"].as_str().unwrap().contains("\"b\""));
    let (st, _) = call(&app, Method::PUT, &format!("{s}/config"), None, Some(json!({"level": "FULL"}))).await;
    assert_eq!(st, StatusCode::OK);
    let (_, cfg) = call(&app, Method::GET, &format!("{s}/config"), None, None).await;
    assert_eq!(cfg["level"], "FULL");
    let (st, _) = call(&app, Method::DELETE, &format!("{s}/versions/1?confirm=wrong"), None, None).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let (st, body) = call(&app, Method::DELETE, &format!("{s}/versions/1?confirm={subject}"), None, None).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let (_, versions) = call(&app, Method::GET, &s, None, None).await;
    assert_eq!(versions, json!([2]));
    let (st, body) = call(&app, Method::DELETE, &format!("{s}?confirm={subject}&permanent=true"), None, None).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let (_, subjects) = call(&app, Method::GET, "/api/p/local/schemas", None, None).await;
    assert!(!subjects.as_array().unwrap().iter().any(|x| *x == subject.as_str()));
}

#[tokio::test]
#[ignore = "requires `docker compose up -d`"]
async fn topic_create_configs_and_delete() {
    let app = app();
    let name = unique("mgmt");
    let body = json!({"name": name, "partitions": 2, "replication_factor": 1, "configs": {"retention.ms": "86400000"}});
    let (st, b) = call(&app, Method::POST, "/api/p/local/topics", None, Some(body.clone())).await;
    assert_eq!(st, StatusCode::OK, "{b}");
    let (st, _) = call(&app, Method::POST, "/api/p/local/topics", None, Some(body)).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "creating it twice fails");
    let bad = json!({"name": "no spaces!", "partitions": 1, "replication_factor": 1});
    let (st, _) = call(&app, Method::POST, "/api/p/local/topics", None, Some(bad)).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    let (_, topics) = call(&app, Method::GET, "/api/p/local/topics", None, None).await;
    let ours = topics.as_array().unwrap().iter().find(|t| t["name"] == name.as_str()).expect("listed");
    assert_eq!(ours["partitions"], 2);
    let (_, cfg) = call(&app, Method::GET, &format!("/api/p/local/topics/{name}/configs"), None, None).await;
    assert!(cfg.as_array().unwrap().iter().any(|c| c["name"] == "retention.ms" && c["value"] == "86400000"), "{cfg}");

    let (st, _) = call(&app, Method::DELETE, &format!("/api/p/local/topics/{name}?confirm=nope"), None, None).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let (st, b) = call(&app, Method::DELETE, &format!("/api/p/local/topics/{name}?confirm={name}"), None, None).await;
    assert_eq!(st, StatusCode::OK, "{b}");
    let (st, _) = call(&app, Method::DELETE, "/api/p/local/topics/__consumer_offsets?confirm=__consumer_offsets", None, None).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "internal topics are protected");
    for _ in 0..20 {
        let (_, topics) = call(&app, Method::GET, "/api/p/local/topics", None, None).await;
        if !topics.as_array().unwrap().iter().any(|t| t["name"] == name.as_str()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("topic still listed after delete");
}

#[tokio::test]
#[ignore = "requires `docker compose up -d`"]
async fn autocomplete_comes_from_the_registered_schema_even_on_an_empty_topic() {
    let app = app();
    let topic = unique("schema-ac");
    create_topic(&topic, 1).await;
    let schema = r#"{"type":"record","name":"Evt","fields":[
        {"name":"id","type":"string"},
        {"name":"rare","type":["null",{"type":"record","name":"Rare","fields":[{"name":"deep","type":"int"}]}],"default":null}]}"#;
    let (st, body) =
        call(&app, Method::POST, &format!("/api/p/local/schemas/{topic}-value"), None, Some(json!({"schema": schema}))).await;
    assert_eq!(st, StatusCode::OK, "{body}");

    let complete = |input: &str| {
        let (app, uri, input) = (app.clone(), format!("/api/p/local/topics/{topic}/complete"), input.to_string());
        async move { call(&app, Method::POST, &uri, None, Some(json!({ "input": input }))).await.1 }
    };
    assert_eq!(complete("value.").await["candidates"], json!(["id", "rare"]));
    assert_eq!(complete("value.rare.").await["candidates"], json!(["deep"]));
    assert_eq!(complete("key.").await["candidates"], json!([]), "no key schema, no key suggestions");
}
