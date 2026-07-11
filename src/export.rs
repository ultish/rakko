//! JSONL export / import for Kafka messages.
//!
//! Source of truth is base64-encoded raw key/value/header bytes so re-import is
//! byte-identical to what was consumed. An optional `decoded_value` field is
//! written for human inspection only and ignored when reconstructing messages.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::raw_message::RawMessage;

/// Expand leading `~` / `~/…` to the user's home directory (like a shell).
/// Other paths are returned unchanged (relative to the process cwd).
pub fn expand_user_path(path: &str) -> AppResult<PathBuf> {
    let path = path.trim();
    if path.is_empty() {
        return Err(AppError::Other("path is empty".into()));
    }
    if path == "~" {
        return dirs::home_dir()
            .ok_or_else(|| AppError::Other("could not determine home directory".into()));
    }
    if let Some(rest) = path.strip_prefix("~/") {
        let home = dirs::home_dir()
            .ok_or_else(|| AppError::Other("could not determine home directory".into()))?;
        return Ok(home.join(rest));
    }
    // Also support "~\" on Windows-ish input; keep simple for macOS/Linux primary.
    if let Some(rest) = path.strip_prefix("~\\") {
        let home = dirs::home_dir()
            .ok_or_else(|| AppError::Other("could not determine home directory".into()))?;
        return Ok(home.join(rest));
    }
    Ok(PathBuf::from(path))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct JsonlHeader {
    key: String,
    value_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct JsonlLine {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_millis: Option<i64>,
    #[serde(default)]
    key_b64: Option<String>,
    #[serde(default)]
    value_b64: Option<String>,
    #[serde(default)]
    headers: Vec<JsonlHeader>,
    /// Optional decoded view for humans; never used to reconstruct `RawMessage`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    decoded_value: Option<String>,
}

/// Serialize one message as a single JSONL line (no trailing newline).
pub fn message_to_jsonl_line(msg: &RawMessage, decoded: Option<&str>) -> AppResult<String> {
    let line = JsonlLine {
        topic: msg.topic.clone(),
        partition: msg.partition,
        offset: msg.offset,
        timestamp_millis: msg.timestamp_millis,
        key_b64: msg.key.as_ref().map(|k| B64.encode(k)),
        value_b64: msg.value.as_ref().map(|v| B64.encode(v)),
        headers: msg
            .headers
            .iter()
            .map(|(key, value)| JsonlHeader {
                key: key.clone(),
                value_b64: B64.encode(value),
            })
            .collect(),
        decoded_value: decoded.map(str::to_string),
    };
    serde_json::to_string(&line)
        .map_err(|err| AppError::Other(format!("failed to serialize JSONL line: {err}")))
}

/// Parse one JSONL line back into a [`RawMessage`] (base64 fields are the source of truth).
pub fn parse_jsonl_line(line: &str) -> AppResult<RawMessage> {
    let line = line.trim();
    if line.is_empty() {
        return Err(AppError::Other("empty JSONL line".into()));
    }
    let parsed: JsonlLine = serde_json::from_str(line)
        .map_err(|err| AppError::Other(format!("invalid JSONL line: {err}")))?;

    let key = match parsed.key_b64 {
        Some(s) => Some(decode_b64_field("key_b64", &s)?),
        None => None,
    };
    let value = match parsed.value_b64 {
        Some(s) => Some(decode_b64_field("value_b64", &s)?),
        None => None,
    };
    let mut headers = Vec::with_capacity(parsed.headers.len());
    for h in parsed.headers {
        headers.push((h.key, decode_b64_field("headers.value_b64", &h.value_b64)?));
    }

    Ok(RawMessage {
        topic: parsed.topic,
        partition: parsed.partition,
        offset: parsed.offset,
        timestamp_millis: parsed.timestamp_millis,
        key,
        value,
        headers,
    })
}

fn decode_b64_field(field: &str, s: &str) -> AppResult<Vec<u8>> {
    B64.decode(s)
        .map_err(|err| AppError::Other(format!("invalid base64 in {field}: {err}")))
}

/// Write all messages to `path` as JSONL (one object per line). No decoded field.
/// Creates parent directories when missing. Callers should pass an already-expanded path
/// (see [`expand_user_path`]) so `~` is resolved.
pub fn write_jsonl_messages(path: impl AsRef<Path>, messages: &[RawMessage]) -> AppResult<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|err| {
                AppError::Other(format!(
                    "failed to create directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
    }
    let mut file = File::create(path).map_err(|err| {
        AppError::Other(format!("failed to create {}: {err}", path.display()))
    })?;
    for msg in messages {
        let line = message_to_jsonl_line(msg, None)?;
        writeln!(file, "{line}")?;
    }
    Ok(())
}

/// Read an entire JSONL file into memory. Prefer [`JsonlReader`] for large imports.
#[cfg(test)]
pub fn read_jsonl_messages(path: impl AsRef<Path>) -> AppResult<Vec<RawMessage>> {
    let mut reader = JsonlReader::open(path)?;
    let mut out = Vec::new();
    while let Some(msg) = reader.next_message()? {
        out.push(msg);
    }
    Ok(out)
}

/// Streaming JSONL reader for import paths that should not buffer a whole file.
pub struct JsonlReader {
    lines: std::io::Lines<BufReader<File>>,
}

impl JsonlReader {
    pub fn open(path: impl AsRef<Path>) -> AppResult<Self> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|err| {
            AppError::Other(format!("failed to open {}: {err}", path.display()))
        })?;
        Ok(Self {
            lines: BufReader::new(file).lines(),
        })
    }

    /// Returns the next non-empty line as a [`RawMessage`], or `Ok(None)` at EOF.
    pub fn next_message(&mut self) -> AppResult<Option<RawMessage>> {
        loop {
            match self.lines.next() {
                None => return Ok(None),
                Some(Err(err)) => return Err(AppError::Io(err)),
                Some(Ok(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    return Ok(Some(parse_jsonl_line(&line)?));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn sample_message() -> RawMessage {
        RawMessage {
            topic: "orders".into(),
            partition: 2,
            offset: 1001,
            timestamp_millis: Some(1_700_000_000_000),
            key: Some(vec![0x00, 0xff, 0x01]),
            value: Some(br#"{"id":1}"#.to_vec()),
            headers: vec![
                ("content-type".into(), b"application/json".to_vec()),
                ("trace".into(), vec![0xde, 0xad, 0xbe, 0xef]),
            ],
        }
    }

    #[test]
    fn round_trip_line_preserves_raw_bytes() {
        let original = sample_message();
        let line = message_to_jsonl_line(&original, Some(r#"{"id":1}"#)).unwrap();
        assert!(line.contains("decoded_value"));
        assert!(line.contains("orders"));

        let restored = parse_jsonl_line(&line).unwrap();
        assert_eq!(restored.topic, original.topic);
        assert_eq!(restored.partition, original.partition);
        assert_eq!(restored.offset, original.offset);
        assert_eq!(restored.timestamp_millis, original.timestamp_millis);
        assert_eq!(restored.key, original.key);
        assert_eq!(restored.value, original.value);
        assert_eq!(restored.headers, original.headers);
    }

    #[test]
    fn base64_byte_identity_for_key_value_headers() {
        let original = sample_message();
        let line = message_to_jsonl_line(&original, None).unwrap();
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();

        let key_b64 = v["key_b64"].as_str().unwrap();
        let value_b64 = v["value_b64"].as_str().unwrap();
        assert_eq!(
            B64.decode(key_b64).unwrap(),
            original.key.as_ref().unwrap().as_slice()
        );
        assert_eq!(
            B64.decode(value_b64).unwrap(),
            original.value.as_ref().unwrap().as_slice()
        );

        let headers = v["headers"].as_array().unwrap();
        assert_eq!(headers.len(), 2);
        for (i, (k, bytes)) in original.headers.iter().enumerate() {
            assert_eq!(headers[i]["key"], *k);
            let hb = headers[i]["value_b64"].as_str().unwrap();
            assert_eq!(B64.decode(hb).unwrap(), *bytes);
        }
    }

    #[test]
    fn null_key_and_value_round_trip() {
        let msg = RawMessage {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp_millis: None,
            key: None,
            value: None,
            headers: vec![],
        };
        let line = message_to_jsonl_line(&msg, None).unwrap();
        let restored = parse_jsonl_line(&line).unwrap();
        assert!(restored.key.is_none());
        assert!(restored.value.is_none());
        assert!(restored.timestamp_millis.is_none());
        assert!(restored.headers.is_empty());
    }

    #[test]
    fn empty_key_bytes_round_trip() {
        let msg = RawMessage {
            topic: "t".into(),
            partition: 0,
            offset: 5,
            timestamp_millis: Some(1),
            key: Some(vec![]),
            value: Some(vec![]),
            headers: vec![("h".into(), vec![])],
        };
        let restored = parse_jsonl_line(&message_to_jsonl_line(&msg, None).unwrap()).unwrap();
        assert_eq!(restored.key, Some(vec![]));
        assert_eq!(restored.value, Some(vec![]));
        assert_eq!(restored.headers, vec![("h".into(), vec![])]);
    }

    #[test]
    fn decoded_value_is_optional_and_ignored_on_parse() {
        let msg = sample_message();
        let with = message_to_jsonl_line(&msg, Some("pretty")).unwrap();
        let without = message_to_jsonl_line(&msg, None).unwrap();
        assert!(with.contains("decoded_value"));
        assert!(!without.contains("decoded_value"));

        // Reconstructing never consults decoded_value.
        let a = parse_jsonl_line(&with).unwrap();
        let b = parse_jsonl_line(&without).unwrap();
        assert_eq!(a.key, b.key);
        assert_eq!(a.value, b.value);
    }

    #[test]
    fn expand_user_path_resolves_tilde() {
        let home = dirs::home_dir().expect("home");
        assert_eq!(expand_user_path("~").unwrap(), home);
        assert_eq!(
            expand_user_path("~/Developer/tmp/msg.jsonl").unwrap(),
            home.join("Developer/tmp/msg.jsonl")
        );
        assert_eq!(
            expand_user_path("/abs/path.jsonl").unwrap(),
            PathBuf::from("/abs/path.jsonl")
        );
        assert_eq!(
            expand_user_path("relative/out.jsonl").unwrap(),
            PathBuf::from("relative/out.jsonl")
        );
    }

    #[test]
    fn write_creates_missing_parent_dirs() {
        let dir = std::env::temp_dir().join(format!(
            "rakko-export-nested-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("sub").join("out.jsonl");
        write_jsonl_messages(&path, &[sample_message()]).unwrap();
        assert!(path.is_file());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_round_trip() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "rakko-export-test-{}-{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let messages = vec![
            sample_message(),
            RawMessage {
                topic: "other".into(),
                partition: 1,
                offset: 2,
                timestamp_millis: None,
                key: None,
                value: Some(vec![1, 2, 3, 4]),
                headers: vec![],
            },
        ];

        write_jsonl_messages(&path, &messages).unwrap();
        let loaded = read_jsonl_messages(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].key, messages[0].key);
        assert_eq!(loaded[0].value, messages[0].value);
        assert_eq!(loaded[0].headers, messages[0].headers);
        assert_eq!(loaded[1].value, messages[1].value);
        assert_eq!(loaded[1].topic, "other");
    }

    #[test]
    fn jsonl_reader_skips_blank_lines() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "rakko-export-blank-{}-{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let msg = sample_message();
        let line = message_to_jsonl_line(&msg, None).unwrap();
        {
            let mut f = File::create(&path).unwrap();
            writeln!(f, "\n{line}\n\n").unwrap();
        }

        let mut reader = JsonlReader::open(&path).unwrap();
        let first = reader.next_message().unwrap().unwrap();
        assert_eq!(first.offset, msg.offset);
        assert!(reader.next_message().unwrap().is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn parse_rejects_invalid_base64() {
        let line = r#"{"topic":"t","partition":0,"offset":0,"timestamp_millis":null,"key_b64":"!!!","value_b64":null,"headers":[]}"#;
        let err = parse_jsonl_line(line).unwrap_err();
        assert!(err.to_string().contains("base64"));
    }

    #[test]
    fn parse_rejects_empty_line() {
        assert!(parse_jsonl_line("   ").is_err());
    }
}
