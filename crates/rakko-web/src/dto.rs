//! JSON shapes the API returns. Lists carry capped previews only; full payloads are
//! fetched lazily (see `details`).

use rakko_core::kafka::schema_registry::SchemaRegistry;
use rakko_core::preview::bytes_display;
use rakko_core::raw_message::RawMessage;
use serde::Serialize;

/// Preview text per list row — the client shows one line, so this is generous.
pub const ROW_PREVIEW_CHARS: usize = 400;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Payload {
    /// `json`, `raw`, `avro:<id>` (`avro:<id>?` when the schema isn't loaded), or `—`.
    pub format: String,
    pub preview: String,
    /// Byte length; `None` for a null key/value (a tombstone).
    pub size: Option<usize>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MessageSummary {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Payload,
    pub value: Payload,
    pub header_count: usize,
}

fn payload(bytes: Option<&[u8]>, registry: Option<&SchemaRegistry>, max_chars: usize) -> Payload {
    let (format, preview) = bytes_display(bytes, registry);
    let preview = if preview.chars().count() > max_chars {
        let cut: String = preview.chars().take(max_chars).collect();
        format!("{cut}…")
    } else {
        preview
    };
    Payload { format, preview, size: bytes.map(<[u8]>::len) }
}

pub fn summarize(message: &RawMessage, registry: Option<&SchemaRegistry>, max_chars: usize) -> MessageSummary {
    MessageSummary {
        partition: message.partition,
        offset: message.offset,
        timestamp_ms: message.timestamp_millis,
        key: payload(message.key.as_deref(), registry, max_chars),
        value: payload(message.value.as_deref(), registry, max_chars),
        header_count: message.headers.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(key: Option<&[u8]>, value: Option<&[u8]>) -> RawMessage {
        RawMessage {
            topic: "t".into(),
            partition: 2,
            offset: 7,
            timestamp_millis: Some(1_700_000_000_000),
            key: key.map(<[u8]>::to_vec),
            value: value.map(<[u8]>::to_vec),
            headers: vec![("h".into(), vec![1])],
        }
    }

    #[test]
    fn summary_carries_coordinates_sizes_and_labels() {
        let s = summarize(&msg(Some(b"k1"), Some(br#"{"a":1}"#)), None, ROW_PREVIEW_CHARS);
        assert_eq!((s.partition, s.offset, s.header_count), (2, 7, 1));
        assert_eq!(s.key, Payload { format: "raw".into(), preview: "k1".into(), size: Some(2) });
        assert_eq!(s.value.format, "json");
        assert_eq!(s.value.size, Some(7));
    }

    #[test]
    fn null_value_is_a_tombstone_with_no_size() {
        let s = summarize(&msg(Some(b"k"), None), None, ROW_PREVIEW_CHARS);
        assert_eq!(s.value.size, None);
        assert_eq!(s.value.preview, "<null>");
    }

    #[test]
    fn long_previews_are_capped() {
        let big = format!("\"{}\"", "x".repeat(5_000));
        let s = summarize(&msg(None, Some(big.as_bytes())), None, ROW_PREVIEW_CHARS);
        assert!(s.value.preview.chars().count() <= ROW_PREVIEW_CHARS + 1);
        assert!(s.value.preview.ends_with('…'));
        assert_eq!(s.value.size, Some(5_002));
    }
}
