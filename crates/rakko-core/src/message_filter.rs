//! The message predicate shared by every frontend: a case-insensitive substring filter
//! and the structured query filter, ANDed. Pure; Avro needs its schema already cached.

use crate::kafka::schema_registry::SchemaRegistry;
use crate::query_filter::{self, QueryFilter};
use crate::raw_message::RawMessage;
use crate::serde_detect;

/// Case-insensitive substring match against key, raw value, and the serde_detect
/// decoded view (JSON/Avro text/hex). Never mutates the raw message.
pub fn text_matches(message: &RawMessage, needle_lowercase: &str, registry: Option<&SchemaRegistry>) -> bool {
    // Key: raw UTF-8 view and decoded view (Avro/JSON/text/hex) — formats can differ from value.
    if let Some(key) = message.key.as_deref() {
        if String::from_utf8_lossy(key).to_lowercase().contains(needle_lowercase) {
            return true;
        }
        if serde_detect::decode_value(key, registry).as_str().to_lowercase().contains(needle_lowercase) {
            return true;
        }
    }
    let Some(value) = message.value.as_deref() else {
        return false;
    };
    if String::from_utf8_lossy(value).to_lowercase().contains(needle_lowercase) {
        return true;
    }
    // Decoded value view (JSON / Avro-as-JSON when schema is cached / text / hex).
    serde_detect::decode_value(value, registry).as_str().to_lowercase().contains(needle_lowercase)
}

/// Structured-query match: decodes key/value to `serde_json::Value` (same Avro-cache
/// boundary as the substring filter) and evaluates the parsed query against them.
pub fn query_matches(message: &RawMessage, query: &QueryFilter, registry: Option<&SchemaRegistry>) -> bool {
    let key = message.key.as_deref().and_then(|k| serde_detect::decode_json_value(k, registry));
    let value = message.value.as_deref().and_then(|v| serde_detect::decode_json_value(v, registry));
    query.matches(key.as_ref(), value.as_ref())
}

/// The substring filter and the structured query, both optional; a message must satisfy
/// every one that is set.
#[derive(Debug, Clone, Default)]
pub struct MessageFilter {
    needle_lowercase: Option<String>,
    query: Option<QueryFilter>,
}

impl MessageFilter {
    /// Blank (or whitespace-only) inputs mean "no such filter". The query is parsed here,
    /// so a syntax error is reported before any scanning starts.
    pub fn parse(text: Option<&str>, query: Option<&str>) -> Result<Self, String> {
        let needle_lowercase = text.map(str::trim).filter(|t| !t.is_empty()).map(str::to_lowercase);
        let query = match query.map(str::trim).filter(|q| !q.is_empty()) {
            Some(q) => Some(query_filter::parse(q)?),
            None => None,
        };
        Ok(Self { needle_lowercase, query })
    }

    pub fn is_empty(&self) -> bool {
        self.needle_lowercase.is_none() && self.query.is_none()
    }

    pub fn matches(&self, message: &RawMessage, registry: Option<&SchemaRegistry>) -> bool {
        self.needle_lowercase.as_deref().is_none_or(|n| text_matches(message, n, registry))
            && self.query.as_ref().is_none_or(|q| query_matches(message, q, registry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(key: Option<&str>, value: Option<&str>) -> RawMessage {
        RawMessage {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp_millis: None,
            key: key.map(|k| k.as_bytes().to_vec()),
            value: value.map(|v| v.as_bytes().to_vec()),
            headers: vec![],
        }
    }

    #[test]
    fn empty_filter_matches_everything() {
        let f = MessageFilter::parse(Some("  "), Some("")).unwrap();
        assert!(f.is_empty());
        assert!(f.matches(&msg(None, None), None));
    }

    #[test]
    fn text_is_case_insensitive_over_key_and_value() {
        let f = MessageFilter::parse(Some("HeLLo"), None).unwrap();
        assert!(f.matches(&msg(Some("k"), Some("say hello")), None));
        assert!(f.matches(&msg(Some("HELLO"), None), None));
        assert!(!f.matches(&msg(Some("k"), Some("bye")), None));
        assert!(!f.matches(&msg(None, None), None));
    }

    #[test]
    fn query_walks_json_fields() {
        let f = MessageFilter::parse(None, Some(r#"value.owner = "jx""#)).unwrap();
        assert!(f.matches(&msg(None, Some(r#"{"owner":"jx"}"#)), None));
        assert!(!f.matches(&msg(None, Some(r#"{"owner":"other"}"#)), None));
    }

    #[test]
    fn text_and_query_must_both_hold() {
        let f = MessageFilter::parse(Some("gold"), Some("value.qty > 2")).unwrap();
        assert!(f.matches(&msg(None, Some(r#"{"tier":"gold","qty":3}"#)), None));
        assert!(!f.matches(&msg(None, Some(r#"{"tier":"gold","qty":1}"#)), None));
        assert!(!f.matches(&msg(None, Some(r#"{"tier":"tin","qty":3}"#)), None));
    }

    #[test]
    fn a_bad_query_is_rejected_up_front() {
        assert!(MessageFilter::parse(None, Some("value.a ==== 1")).is_err());
    }
}
