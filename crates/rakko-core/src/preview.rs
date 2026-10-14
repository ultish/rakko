//! Display previews of key/value bytes for list rows — shared by every frontend so a
//! 20 MiB Avro message costs the same bounded work in the TUI and on the web.

use crate::kafka::schema_registry::SchemaRegistry;
use crate::serde_detect;

/// Safety cap when building list-row text. Prevents multi-MB payloads from being held
/// in every visible row; frontends clip further to their own width.
pub const LIST_PREVIEW_SAFETY_CAP: usize = 2_048;

/// Above this many bytes, a payload gets its format/preview computed cheaply
/// instead of a full decode — `bytes_display` runs for every visible row on every
/// render (the message list redraws on the ~200ms banner-tick animation, not just
/// on data changes), so redoing a full decode of a multi-MB message here on every
/// frame makes browsing large-message topics grind to a halt (mouse/keyboard input
/// piles up behind each slow re-render), for work whose result is soft-capped to
/// `LIST_PREVIEW_SAFETY_CAP` characters anyway. Sized well above that cap for
/// UTF-8/JSON-syntax margin.
const LIST_PREVIEW_DECODE_BYTE_CAP: usize = 8 * 1024;

/// Format label + decoded preview for key or value bytes via `serde_detect`
/// (never mutates raw bytes). Key and value are detected independently — they
/// often differ (e.g. raw string key + Avro value).
///
/// Preview text is only soft-capped for memory; the message table truncates to
/// the live column width so Value grows with the terminal.
pub fn bytes_display(
    bytes: Option<&[u8]>,
    registry: Option<&SchemaRegistry>,
) -> (String, String) {
    let Some(bytes) = bytes else {
        return ("—".into(), "<null>".into());
    };
    if bytes.is_empty() {
        return ("raw".into(), "<empty>".into());
    }
    if bytes.len() > LIST_PREVIEW_DECODE_BYTE_CAP {
        // Confluent Avro magic prefix (0x00 + 4-byte schema id) is a cheap 5-byte
        // check — `detect_format` returns as soon as it sees this, so it's cheap
        // regardless of payload size. The binary decode itself is also cheap
        // regardless of size; what isn't is converting an untruncated multi-MB
        // field to `serde_json::Value` and JSON-escape-serializing it back to a
        // string. `avro_value_preview` truncates fields before that conversion so
        // it stays cheap while still showing real (if partial) content.
        if let serde_detect::DetectedFormat::Avro { schema_id } = serde_detect::detect_format(bytes) {
            let cached = registry.is_some_and(|sr| sr.cached_schema(schema_id).is_some());
            let label = if cached {
                format!("avro:{schema_id}")
            } else {
                format!("avro:{schema_id}?")
            };
            // `max_chars` here is a total content budget shared across the whole
            // record (see `truncate_avro_value_bounded`), not a per-field cap — a
            // single huge field and a record with many small fields both stay
            // within it. Fields are visited (and the budget spent) in schema
            // declaration order, not serde_json's key-sorted output order, so a
            // schema with identifying fields (id, type, ...) declared before a
            // large payload field shows those first rather than having them
            // crowded out by whichever field name sorts alphabetically first.
            // Budget is a fraction of the final display cap, not the cap itself:
            // JSON syntax (quotes/colons/commas/key names) adds overhead on top of
            // the raw content the budget counts, so passing the full cap can push
            // the serialized size just over it — leaving the *outer* soft-cap
            // below to trim the tail, which (since output is key-sorted) can cut
            // off a small field like `id` after a big one, undoing the ordering
            // guarantee above. This margin keeps normal-sized records comfortably
            // under the outer cap so it stays a no-op safety net, not a second
            // truncation pass.
            let preview =
                serde_detect::avro_value_preview(bytes, registry, LIST_PREVIEW_SAFETY_CAP * 3 / 4)
                    .unwrap_or_else(|| format!("<{} bytes — Enter to view>", bytes.len()));
            return (label, soft_cap_preview(&preview, LIST_PREVIEW_SAFETY_CAP));
        }
        let bounded = &bytes[..LIST_PREVIEW_DECODE_BYTE_CAP];
        let label = if serde_detect::looks_json_shaped(bounded) {
            "json"
        } else {
            "raw"
        };
        let preview = String::from_utf8_lossy(bounded);
        return (label.into(), soft_cap_preview(&preview, LIST_PREVIEW_SAFETY_CAP));
    }
    let format = serde_detect::detect_format(bytes);
    let label = match format {
        serde_detect::DetectedFormat::Avro { schema_id } => {
            let cached = registry.is_some_and(|sr| sr.cached_schema(schema_id).is_some());
            if cached {
                format!("avro:{schema_id}")
            } else {
                format!("avro:{schema_id}?")
            }
        }
        serde_detect::DetectedFormat::Json => "json".into(),
        serde_detect::DetectedFormat::Raw => "raw".into(),
    };
    let decoded = serde_detect::decode_value(bytes, registry);
    (label, soft_cap_preview(decoded.as_str(), LIST_PREVIEW_SAFETY_CAP))
}

fn soft_cap_preview(text: &str, max: usize) -> String {
    if text.chars().count() > max {
        let truncated: String = text.chars().take(max).collect();
        format!("{truncated}…")
    } else {
        text.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::bytes_display;

    #[test]
    fn small_json_is_labeled_and_previewed_exactly() {
        let (label, preview) = bytes_display(Some(br#"{"a":1}"#), None);
        assert_eq!(label, "json");
        assert_eq!(preview, r#"{"a":1}"#);
    }

    #[test]
    fn large_json_over_the_decode_cap_still_labels_as_json() {
        // Bigger than LIST_PREVIEW_DECODE_BYTE_CAP so it takes the bounded-prefix
        // path — the badge must come from the cheap shape probe, not a full parse
        // (a truncated document can never itself parse successfully).
        let body = "x".repeat(20_000);
        let big = format!(r#"{{"a":"{body}"}}"#);
        let (label, preview) = bytes_display(Some(big.as_bytes()), None);
        assert_eq!(label, "json");
        assert!(preview.starts_with("{\"a\":\"xxxx"));
        assert!(preview.len() < big.len(), "preview must not clone the whole payload");
    }

    #[test]
    fn large_non_json_over_the_decode_cap_labels_as_raw() {
        let big = "x".repeat(20_000);
        let (label, _) = bytes_display(Some(big.as_bytes()), None);
        assert_eq!(label, "raw");
    }

    #[test]
    fn large_avro_without_a_cached_schema_falls_back_to_a_placeholder() {
        // Confluent magic byte + schema id, then a payload big enough to trip the
        // bound. No registry is passed, so there's no cached schema to decode
        // against regardless of size — labeling still only reads the 5-byte magic
        // prefix, and the preview falls back to a placeholder rather than showing
        // raw undecoded bytes.
        let mut bytes = vec![0x00, 0, 0, 0, 7];
        bytes.extend(std::iter::repeat_n(b'x', 20_000));
        let (label, preview) = bytes_display(Some(&bytes), None);
        assert_eq!(label, "avro:7?");
        assert!(preview.contains("Enter to view"));
    }

    #[test]
    fn large_avro_with_a_cached_schema_shows_a_bounded_real_preview() {
        use apache_avro::types::Value as AvroValue;
        use apache_avro::Schema;
        use crate::kafka::schema_registry::SchemaRegistry;

        let schema = Schema::parse_str(
            r#"{"type":"record","name":"Big","fields":[
                {"name":"id","type":"int"},
                {"name":"blob","type":"string"}
            ]}"#,
        )
        .unwrap();
        let value = AvroValue::Record(vec![
            ("id".into(), AvroValue::Int(42)),
            ("blob".into(), AvroValue::String("y".repeat(2_000_000))),
        ]);
        let payload = apache_avro::to_avro_datum(&schema, value).unwrap();
        let mut bytes = vec![0x00, 0, 0, 0, 3];
        bytes.extend(payload);

        let mut registry = SchemaRegistry::new("http://localhost:8081").unwrap();
        registry.insert_schema(3, schema);

        let t0 = std::time::Instant::now();
        let (label, preview) = bytes_display(Some(&bytes), Some(&registry));
        // Regression guard for the actual bug: an untruncated decode+serialize of
        // this record took ~50ms in a debug build (~5s across a 100-row page,
        // redone on every ~200ms render) — generous ceiling so this doesn't flake
        // on a loaded CI box while still catching that regression.
        assert!(t0.elapsed().as_millis() < 50, "elapsed: {:?}", t0.elapsed());
        assert_eq!(label, "avro:3");
        assert!(preview.contains("\"id\":42"), "preview: {preview}");
        assert!(preview.len() < 4096, "preview should be bounded, got {} bytes", preview.len());
    }

    #[test]
    fn empty_and_null_bytes_are_unaffected_by_the_size_bound() {
        assert_eq!(bytes_display(None, None), ("—".into(), "<null>".into()));
        assert_eq!(bytes_display(Some(&[]), None), ("raw".into(), "<empty>".into()));
    }
}
