/// Canonical byte-preserving representation of a Kafka record.
///
/// This is the one type threaded through browsing, replay, and export/import so that
/// "resend exactly what was consumed" never has to round-trip through a decoded value.
#[derive(Debug, Clone)]
pub struct RawMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// Kafka message timestamp in epoch millis, if present.
    pub timestamp_millis: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl RawMessage {
    pub fn key_len(&self) -> usize {
        self.key.as_ref().map_or(0, Vec::len)
    }

    pub fn value_len(&self) -> usize {
        self.value.as_ref().map_or(0, Vec::len)
    }
}
