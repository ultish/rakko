//! Per-message encoding auto-detect and decode for display / filtering.
//!
//! Never mutates [`crate::raw_message::RawMessage`] — decoded views sit alongside the
//! raw bytes so replay and export remain byte-identical.

use apache_avro::Schema;

use crate::kafka::schema_registry::SchemaRegistry;

/// Result of a cheap magic-byte / UTF-8 sniff (no registry I/O).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetectedFormat {
    /// Confluent wire-format Avro: magic `0x00` + big-endian schema id + payload.
    Avro { schema_id: u32 },
    /// Valid UTF-8 JSON object or array.
    Json,
    /// Anything else (plain text or binary).
    Raw,
}

/// Human-readable view of a message value for UI / filter / optional export field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedValue {
    Avro(String),
    Json(String),
    RawHex(String),
    RawText(String),
}

impl DecodedValue {
    /// Borrow the display string regardless of variant.
    pub fn as_str(&self) -> &str {
        match self {
            DecodedValue::Avro(s)
            | DecodedValue::Json(s)
            | DecodedValue::RawHex(s)
            | DecodedValue::RawText(s) => s,
        }
    }
}

/// Sniff encoding from the value bytes alone (no registry access).
///
/// A Confluent Avro magic prefix is reported as [`DetectedFormat::Avro`] even when
/// the payload may later fail to decode — decode paths fall through on failure.
pub fn detect_format(value: &[u8]) -> DetectedFormat {
    if let Some(schema_id) = confluent_schema_id(value) {
        return DetectedFormat::Avro { schema_id };
    }
    if is_json_object_or_array(value) {
        return DetectedFormat::Json;
    }
    DetectedFormat::Raw
}

/// Decode `value` for display. Uses only schemas already present in `registry`'s
/// cache (sync). Unresolvable schema ids, decode errors, and missing registry all
/// fall through to JSON / text / hex — never panics.
pub fn decode_value(value: &[u8], registry: Option<&SchemaRegistry>) -> DecodedValue {
    if let Some(decoded) = try_decode_avro(value, registry) {
        return decoded;
    }
    decode_json_or_raw(value)
}

/// Decode using an explicitly provided schema (e.g. after a successful async fetch).
/// Falls through to JSON/raw on failure, same as [`decode_value`].
#[cfg(test)]
pub fn decode_value_with_schema(value: &[u8], schema: &Schema) -> DecodedValue {
    if confluent_schema_id(value).is_some() {
        if let Some(text) = avro_payload_to_json(&value[5..], schema) {
            return DecodedValue::Avro(text);
        }
    }
    decode_json_or_raw(value)
}

fn confluent_schema_id(value: &[u8]) -> Option<u32> {
    if value.len() >= 5 && value[0] == 0x00 {
        Some(u32::from_be_bytes([value[1], value[2], value[3], value[4]]))
    } else {
        None
    }
}

fn try_decode_avro(value: &[u8], registry: Option<&SchemaRegistry>) -> Option<DecodedValue> {
    let schema_id = confluent_schema_id(value)?;
    let registry = registry?;
    let schema = registry.cached_schema(schema_id)?;
    let text = avro_payload_to_json(&value[5..], schema)?;
    Some(DecodedValue::Avro(text))
}

fn avro_payload_to_json_value(payload: &[u8], schema: &Schema) -> Option<serde_json::Value> {
    let mut cursor = payload;
    let avro_value = apache_avro::from_avro_datum(schema, &mut cursor, None).ok()?;
    avro_value.try_into().ok()
}

fn avro_payload_to_json(payload: &[u8], schema: &Schema) -> Option<String> {
    avro_payload_to_json_value(payload, schema).map(|v| v.to_string())
}

/// Recursively caps string/bytes leaves to `max_chars` — for
/// [`avro_value_preview`], where converting an untruncated multi-MB field to
/// `serde_json::Value` and serializing it back to a string is the actual cost (not
/// the binary decode itself, which stays fast regardless of field size); truncating
/// first keeps that conversion+serialize proportional to the preview size instead
/// of the record size. Arrays/maps are also capped by element count so a huge
/// collection can't reintroduce the same cost via many small elements.
fn truncate_avro_value(value: apache_avro::types::Value, max_chars: usize) -> apache_avro::types::Value {
    use apache_avro::types::Value as AvroValue;
    const MAX_ELEMENTS: usize = 64;
    match value {
        AvroValue::String(s) => {
            if s.chars().count() > max_chars {
                AvroValue::String(s.chars().take(max_chars).collect())
            } else {
                AvroValue::String(s)
            }
        }
        AvroValue::Bytes(b) => AvroValue::Bytes(b.into_iter().take(max_chars).collect()),
        AvroValue::Fixed(size, b) => AvroValue::Fixed(size, b.into_iter().take(max_chars).collect()),
        AvroValue::Record(fields) => AvroValue::Record(
            fields
                .into_iter()
                .map(|(k, v)| (k, truncate_avro_value(v, max_chars)))
                .collect(),
        ),
        AvroValue::Array(items) => AvroValue::Array(
            items
                .into_iter()
                .take(MAX_ELEMENTS)
                .map(|v| truncate_avro_value(v, max_chars))
                .collect(),
        ),
        AvroValue::Map(map) => AvroValue::Map(
            map.into_iter()
                .take(MAX_ELEMENTS)
                .map(|(k, v)| (k, truncate_avro_value(v, max_chars)))
                .collect(),
        ),
        AvroValue::Union(idx, inner) => AvroValue::Union(idx, Box::new(truncate_avro_value(*inner, max_chars))),
        other => other,
    }
}

/// Bounded preview of an Avro record for contexts that can't afford a full
/// decode-and-serialize on every call — see
/// `ui::screens::topic_detail::bytes_display`'s list-row preview, which runs for
/// every visible row on every render. The binary decode itself is cheap regardless
/// of record size; what isn't is `serde_json`-serializing an untruncated multi-MB
/// string field, so fields are truncated *before* the JSON conversion rather than
/// after (bounding only the output writer doesn't help — the serializer still scans
/// the whole untruncated string first).
pub fn avro_value_preview(value: &[u8], registry: Option<&SchemaRegistry>, max_chars: usize) -> Option<String> {
    let schema_id = confluent_schema_id(value)?;
    let registry = registry?;
    let schema = registry.cached_schema(schema_id)?;
    let mut cursor = &value[5..];
    let avro_value = apache_avro::from_avro_datum(schema, &mut cursor, None).ok()?;
    let truncated = truncate_avro_value(avro_value, max_chars);
    let json_value: serde_json::Value = truncated.try_into().ok()?;
    Some(json_value.to_string())
}

/// Structured JSON view of `value`, for the advanced query filter
/// (`query_filter::QueryFilter`) to walk field paths directly rather than re-parsing
/// display text. Same decode boundary as [`decode_value`] — Avro needs a schema already
/// in `registry`'s cache, anything else needs to already be a JSON object/array — just
/// returns the parsed `serde_json::Value` instead of a display string.
pub fn decode_json_value(value: &[u8], registry: Option<&SchemaRegistry>) -> Option<serde_json::Value> {
    if let Some(schema_id) = confluent_schema_id(value) {
        let registry = registry?;
        let schema = registry.cached_schema(schema_id)?;
        return avro_payload_to_json_value(&value[5..], schema);
    }
    if is_json_object_or_array(value) {
        let s = std::str::from_utf8(value).ok()?;
        return serde_json::from_str(s).ok();
    }
    None
}

fn is_json_object_or_array(value: &[u8]) -> bool {
    let Ok(s) = std::str::from_utf8(value) else {
        return false;
    };
    let trimmed = s.trim_start();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return false;
    }
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => v.is_object() || v.is_array(),
        Err(_) => false,
    }
}

/// Cheap JSON-shape probe: UTF-8 and opens with `{`/`[`, but — unlike
/// [`detect_format`] — does **not** fully parse to confirm the document is well
/// formed. For a truncated prefix of a large message (see
/// `ui::screens::topic_detail::bytes_display`), a full parse always fails (the
/// document is incomplete by construction), so this is what a size-bounded preview
/// has to fall back to for its format badge; anywhere accuracy matters more than
/// bounded cost (filtering, export, the message inspector) should keep using
/// `detect_format` on the untruncated bytes instead.
pub fn looks_json_shaped(value: &[u8]) -> bool {
    let Ok(s) = std::str::from_utf8(value) else {
        return false;
    };
    let trimmed = s.trim_start();
    trimmed.starts_with('{') || trimmed.starts_with('[')
}

fn decode_json_or_raw(value: &[u8]) -> DecodedValue {
    if let Ok(s) = std::str::from_utf8(value) {
        if is_json_object_or_array(value) {
            return DecodedValue::Json(s.to_string());
        }
        return DecodedValue::RawText(s.to_string());
    }
    DecodedValue::RawHex(hex_encode(value))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

/// Build a Confluent wire-format Avro frame: magic + schema id + payload.
#[cfg(test)]
fn confluent_frame(schema_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(0x00);
    out.extend_from_slice(&schema_id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value as AvroValue;
    use crate::kafka::schema_registry::SchemaRegistry;

    #[test]
    fn looks_json_shaped_accepts_truncated_object_or_array_prefix() {
        // A prefix of a large document can never itself parse — the whole point of
        // the shape probe is to not require that.
        assert!(looks_json_shaped(br#"{"a":"xxxxxxxxxxxxxxxxxxxxxxxx"#));
        assert!(looks_json_shaped(br#"[1,2,3,4,5,6,7,8,9,1"#));
        assert!(!looks_json_shaped(b"plain text"));
        assert!(!looks_json_shaped(&[0xff, 0xfe, 0x00]));
    }

    #[test]
    fn avro_value_preview_truncates_large_fields_and_stays_cheap() {
        let schema = Schema::parse_str(
            r#"{
            "type":"record","name":"RakkoPerfTest",
            "fields":[
                {"name":"id","type":"int"},
                {"name":"kind","type":"string"},
                {"name":"blob","type":"string"}
            ]
        }"#,
        )
        .unwrap();
        let blob: String = "x".repeat(2_000_000);
        let value = AvroValue::Record(vec![
            ("id".into(), AvroValue::Int(1)),
            ("kind".into(), AvroValue::String("perf".into())),
            ("blob".into(), AvroValue::String(blob)),
        ]);
        let payload = apache_avro::to_avro_datum(&schema, value).unwrap();
        let mut registry = SchemaRegistry::new("http://localhost:8081").unwrap();
        registry.insert_schema(9, schema);
        let wire = confluent_frame(9, &payload);

        let t0 = std::time::Instant::now();
        let preview = avro_value_preview(&wire, Some(&registry), 2048).unwrap();
        // The whole point: bounding the field before conversion+serialize, not
        // after, keeps this proportional to the preview size, not the 2MB field —
        // generous ceiling (not a tight timing assertion) so this doesn't flake on
        // a loaded CI box while still catching a regression back to the ~50ms/record
        // (~5s+ for a 100-row page) unbounded-serialize cost.
        assert!(t0.elapsed().as_millis() < 50, "elapsed: {:?}", t0.elapsed());
        assert!(preview.contains("\"id\":1"));
        assert!(preview.contains("perf"));
        assert!(preview.len() < 4096, "preview should be bounded, got {} bytes", preview.len());
    }

    #[test]
    fn avro_value_preview_falls_through_without_cached_schema() {
        let wire = confluent_frame(9999, b"whatever");
        assert!(avro_value_preview(&wire, None, 2048).is_none());
    }

    const USER_SCHEMA: &str = r#"{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }"#;

    fn sample_avro_payload() -> (Schema, Vec<u8>) {
        let schema = Schema::parse_str(USER_SCHEMA).unwrap();
        let value = AvroValue::Record(vec![
            ("name".into(), AvroValue::String("ada".into())),
            ("age".into(), AvroValue::Int(36)),
        ]);
        let payload = apache_avro::to_avro_datum(&schema, value).unwrap();
        (schema, payload)
    }

    #[test]
    fn detect_normal_json_object() {
        let bytes = br#"{"hello":"world"}"#;
        assert_eq!(detect_format(bytes), DetectedFormat::Json);
        match decode_value(bytes, None) {
            DecodedValue::Json(s) => assert!(s.contains("hello")),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn detect_normal_json_array() {
        let bytes = br#"[1, 2, 3]"#;
        assert_eq!(detect_format(bytes), DetectedFormat::Json);
        assert!(matches!(decode_value(bytes, None), DecodedValue::Json(_)));
    }

    #[test]
    fn detect_plain_text_as_raw() {
        let bytes = b"hello plain text";
        assert_eq!(detect_format(bytes), DetectedFormat::Raw);
        match decode_value(bytes, None) {
            DecodedValue::RawText(s) => assert_eq!(s, "hello plain text"),
            other => panic!("expected RawText, got {other:?}"),
        }
    }

    #[test]
    fn detect_binary_as_raw_hex() {
        let bytes = [0xff, 0xfe, 0x01, 0x02];
        assert_eq!(detect_format(&bytes), DetectedFormat::Raw);
        match decode_value(&bytes, None) {
            DecodedValue::RawHex(s) => assert_eq!(s, "fffe0102"),
            other => panic!("expected RawHex, got {other:?}"),
        }
    }

    fn assert_raw_fallthrough(decoded: DecodedValue) {
        assert!(
            matches!(decoded, DecodedValue::RawHex(_) | DecodedValue::RawText(_)),
            "expected raw fallthrough (hex or text), got {decoded:?}"
        );
    }

    #[test]
    fn json_payload_starting_with_0x00_falls_through_on_failed_avro() {
        // Looks like Confluent Avro (magic + schema id) but is not decoded as Avro
        // without a resolvable schema. The full buffer is not a JSON object/array
        // either (leading NUL). Must not panic — fall through to raw text/hex.
        let mut bytes = vec![0x00, 0x00, 0x00, 0x00, 0x01];
        bytes.extend_from_slice(br#"{"a":1}"#);

        assert_eq!(
            detect_format(&bytes),
            DetectedFormat::Avro { schema_id: 1 }
        );

        // No registry → skip Avro → raw fallthrough.
        assert_raw_fallthrough(decode_value(&bytes, None));

        // Registry present but schema id unresolvable → same fallthrough, no panic.
        let sr = SchemaRegistry::new("http://localhost:8081").unwrap();
        assert_raw_fallthrough(decode_value(&bytes, Some(&sr)));
    }

    #[test]
    fn unresolvable_schema_id_falls_through_without_panic() {
        let (_, payload) = sample_avro_payload();
        let bytes = confluent_frame(9999, &payload);

        assert_eq!(
            detect_format(&bytes),
            DetectedFormat::Avro { schema_id: 9999 }
        );

        let sr = SchemaRegistry::new("http://localhost:8081").unwrap();
        // Cache is empty → unresolvable → fall through rather than panicking.
        assert_raw_fallthrough(decode_value(&bytes, Some(&sr)));
    }

    #[test]
    fn successful_avro_decode_with_cached_schema() {
        let (schema, payload) = sample_avro_payload();
        let schema_id = 7u32;
        let bytes = confluent_frame(schema_id, &payload);

        let mut sr = SchemaRegistry::new("http://localhost:8081").unwrap();
        sr.insert_schema(schema_id, schema);

        assert_eq!(
            detect_format(&bytes),
            DetectedFormat::Avro { schema_id }
        );

        match decode_value(&bytes, Some(&sr)) {
            DecodedValue::Avro(s) => {
                let v: serde_json::Value = serde_json::from_str(&s).unwrap();
                assert_eq!(v["name"], "ada");
                assert_eq!(v["age"], 36);
            }
            other => panic!("expected Avro, got {other:?}"),
        }
    }

    #[test]
    fn wrong_cached_schema_falls_through() {
        let (_, payload) = sample_avro_payload();
        let bytes = confluent_frame(1, &payload);

        // Boolean cannot decode a multi-byte record payload — forces a hard failure.
        let wrong = Schema::parse_str(r#"{"type":"boolean"}"#).unwrap();
        let mut sr = SchemaRegistry::new("http://localhost:8081").unwrap();
        sr.insert_schema(1, wrong);

        assert_raw_fallthrough(decode_value(&bytes, Some(&sr)));
    }

    #[test]
    fn decode_value_with_schema_helper() {
        let (schema, payload) = sample_avro_payload();
        let bytes = confluent_frame(3, &payload);
        match decode_value_with_schema(&bytes, &schema) {
            DecodedValue::Avro(s) => assert!(s.contains("ada")),
            other => panic!("expected Avro, got {other:?}"),
        }
    }

    #[test]
    fn empty_value_is_raw_text() {
        assert_eq!(detect_format(b""), DetectedFormat::Raw);
        assert_eq!(decode_value(b"", None), DecodedValue::RawText(String::new()));
    }

    #[test]
    fn json_scalar_is_raw_text_not_json() {
        // Only objects/arrays count as Json for display auto-detect.
        let bytes = b"42";
        assert_eq!(detect_format(bytes), DetectedFormat::Raw);
        assert_eq!(
            decode_value(bytes, None),
            DecodedValue::RawText("42".into())
        );
    }

    #[test]
    fn short_magic_prefix_is_not_avro() {
        // Needs 5 bytes for magic + schema id.
        let bytes = [0x00, 0x00, 0x01];
        assert_eq!(detect_format(&bytes), DetectedFormat::Raw);
    }

    #[test]
    fn decode_json_value_parses_plain_json_object() {
        let bytes = br#"{"name":"ada","age":36}"#;
        let value = decode_json_value(bytes, None).expect("should decode");
        assert_eq!(value["name"], "ada");
        assert_eq!(value["age"], 36);
    }

    #[test]
    fn decode_json_value_decodes_avro_with_cached_schema() {
        let (schema, payload) = sample_avro_payload();
        let schema_id = 11u32;
        let bytes = confluent_frame(schema_id, &payload);

        let mut sr = SchemaRegistry::new("http://localhost:8081").unwrap();
        sr.insert_schema(schema_id, schema);

        let value = decode_json_value(&bytes, Some(&sr)).expect("should decode");
        assert_eq!(value["name"], "ada");
        assert_eq!(value["age"], 36);
    }

    #[test]
    fn decode_json_value_none_for_raw_text_and_unresolvable_avro() {
        assert_eq!(decode_json_value(b"plain text", None), None);

        let (_, payload) = sample_avro_payload();
        let bytes = confluent_frame(9999, &payload);
        let sr = SchemaRegistry::new("http://localhost:8081").unwrap();
        assert_eq!(decode_json_value(&bytes, Some(&sr)), None);
    }
}
