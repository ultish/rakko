//! Single-message detail: the full decoded key or value, served in ranges.
//!
//! A big message is fetched from Kafka once, decoded once, and kept briefly so the
//! client can page through it (and read the key after the value) without repeating
//! either. The cache is small and byte-budgeted.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use rakko_core::error::{AppError, AppResult};
use rakko_core::kafka::consumer::fetch_message;
use rakko_core::raw_message::RawMessage;
use rakko_core::serde_detect::{avro_schema_id, decode_value, DecodedValue};

use crate::runtime::ProfileRuntime;

const MAX_ENTRIES: usize = 8;
/// Approximate ceiling on cached bytes (raw payload × 3 to cover the rendered text).
const BUDGET_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Part {
    Key,
    Value,
}

pub struct Rendered {
    pub format: String,
    pub text: String,
}

pub struct DetailEntry {
    pub message: Arc<RawMessage>,
    rendered: Mutex<HashMap<Part, Arc<Rendered>>>,
}

type Key = (String, String, i32, i64);

#[derive(Default)]
pub struct DetailCache {
    entries: Mutex<VecDeque<(Key, Arc<DetailEntry>)>>,
    inflight: Mutex<HashMap<Key, Arc<tokio::sync::Mutex<()>>>>,
}

fn weight(m: &RawMessage) -> usize {
    (m.key.as_ref().map_or(0, Vec::len) + m.value.as_ref().map_or(0, Vec::len)) * 3
}

impl DetailCache {
    /// Forgets everything (after records were deleted, cached copies would be stale).
    pub fn clear(&self) {
        self.entries.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    fn lookup(&self, key: &Key) -> Option<Arc<DetailEntry>> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.iter().find(|(k, _)| k == key).map(|(_, e)| e.clone())
    }

    fn insert(&self, key: Key, entry: Arc<DetailEntry>) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.push_back((key, entry));
        let mut total: usize = entries.iter().map(|(_, e)| weight(&e.message)).sum();
        // Always keep the newest entry, even if it alone is over budget.
        while entries.len() > 1 && (entries.len() > MAX_ENTRIES || total > BUDGET_BYTES) {
            if let Some((_, old)) = entries.pop_front() {
                total -= weight(&old.message);
            }
        }
    }

    /// The message at `partition`/`offset`, or `None` if it no longer exists.
    pub async fn get(
        &self,
        rt: &Arc<ProfileRuntime>,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> AppResult<Option<Arc<DetailEntry>>> {
        let key: Key = (rt.name.clone(), topic.to_string(), partition, offset);
        if let Some(hit) = self.lookup(&key) {
            return Ok(Some(hit));
        }
        let gate = {
            let mut inflight = self.inflight.lock().unwrap_or_else(|e| e.into_inner());
            inflight.entry(key.clone()).or_default().clone()
        };
        let result = async {
            let _one_fetch = gate.lock().await;
            if let Some(hit) = self.lookup(&key) {
                return Ok(Some(hit));
            }
            let Some(message) = fetch_message(&rt.profile, topic, partition, offset).await? else {
                return Ok(None);
            };
            let entry = Arc::new(DetailEntry { message: Arc::new(message), rendered: Mutex::new(HashMap::new()) });
            self.insert(key.clone(), entry.clone());
            Ok(Some(entry))
        }
        .await;
        self.inflight.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
        result
    }
}

impl DetailEntry {
    /// The decoded text for `part` (rendered once, then cached). `None` for a null key/value.
    pub async fn render(self: &Arc<Self>, rt: &Arc<ProfileRuntime>, part: Part) -> AppResult<Option<Arc<Rendered>>> {
        let bytes = match part {
            Part::Key => self.message.key.as_ref(),
            Part::Value => self.message.value.as_ref(),
        };
        let Some(bytes) = bytes else { return Ok(None) };
        if let Some(done) = self.rendered.lock().unwrap_or_else(|e| e.into_inner()).get(&part) {
            return Ok(Some(done.clone()));
        }
        if let Some(id) = avro_schema_id(bytes) {
            rt.ensure_schema(id).await;
        }
        let (entry, rt2) = (self.clone(), rt.clone());
        let rendered = tokio::task::spawn_blocking(move || {
            let bytes = match part {
                Part::Key => entry.message.key.as_deref(),
                Part::Value => entry.message.value.as_deref(),
            }
            .unwrap_or_default();
            rt2.with_registry(|reg| render_bytes(bytes, reg))
        })
        .await
        .map_err(|e| AppError::Other(format!("render panicked: {e}")))?;
        let rendered = Arc::new(rendered);
        self.rendered.lock().unwrap_or_else(|e| e.into_inner()).insert(part, rendered.clone());
        Ok(Some(rendered))
    }
}

pub fn render_bytes(bytes: &[u8], registry: Option<&rakko_core::kafka::schema_registry::SchemaRegistry>) -> Rendered {
    use rakko_core::serde_detect::{detect_format, DetectedFormat};
    let format = match detect_format(bytes) {
        DetectedFormat::Avro { schema_id } => {
            let cached = registry.is_some_and(|r| r.cached_schema(schema_id).is_some());
            if cached { format!("avro:{schema_id}") } else { format!("avro:{schema_id}?") }
        }
        DetectedFormat::Json => "json".into(),
        DetectedFormat::Raw => "raw".into(),
    };
    let text = match decode_value(bytes, registry) {
        DecodedValue::Avro(s) | DecodedValue::Json(s) => match serde_json::from_str::<serde_json::Value>(&s) {
            Ok(v) => serde_json::to_string_pretty(&v).unwrap_or(s),
            Err(_) => s,
        },
        DecodedValue::RawText(s) | DecodedValue::RawHex(s) => s,
    };
    Rendered { format, text }
}

/// `text[start..start+max]`, nudged to UTF-8 boundaries. Returns the actual `(start, end)`.
pub fn slice_text(text: &str, start: usize, max: usize) -> (usize, usize, &str) {
    let mut s = start.min(text.len());
    while !text.is_char_boundary(s) {
        s -= 1;
    }
    let mut e = s.saturating_add(max).min(text.len());
    while !text.is_char_boundary(e) {
        e -= 1;
    }
    (s, e, &text[s..e])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slices_never_split_a_character() {
        let text = "aé€b"; // 1 + 2 + 3 + 1 bytes
        let (s, e, piece) = slice_text(text, 0, 2);
        assert_eq!((s, e, piece), (0, 1, "a"), "end backs up off the middle of é");
        let (s, e, piece) = slice_text(text, 2, 10);
        assert_eq!((s, e, piece), (1, 7, "é€b"), "start backs up onto é's boundary");
    }

    #[test]
    fn slice_past_the_end_is_empty() {
        assert_eq!(slice_text("abc", 99, 10), (3, 3, ""));
    }

    #[test]
    fn json_is_pretty_printed_and_raw_text_is_untouched() {
        let r = render_bytes(br#"{"a":[1,2]}"#, None);
        assert_eq!(r.format, "json");
        assert!(r.text.contains("\n  \"a\": ["), "{}", r.text);
        let r = render_bytes(b"plain text", None);
        assert_eq!((r.format.as_str(), r.text.as_str()), ("raw", "plain text"));
    }

    #[test]
    fn binary_renders_as_hex() {
        let r = render_bytes(&[0xff, 0x00, 0x10], None);
        assert_eq!(r.format, "raw");
        assert_eq!(r.text, "ff0010");
    }
}
