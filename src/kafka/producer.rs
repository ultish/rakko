use std::time::Duration;

use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::config::Profile;
use crate::error::{AppError, AppResult};
use crate::kafka::client_config::producer_client_config;

const PRODUCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Builds an `OwnedHeaders` from `(key, value)` pairs. Pure helper — unit-tested without
/// a broker.
pub fn build_owned_headers(headers: &[(String, Vec<u8>)]) -> OwnedHeaders {
    let mut owned = OwnedHeaders::new_with_capacity(headers.len().max(1));
    for (key, value) in headers {
        owned = owned.insert(Header {
            key: key.as_str(),
            value: Some(value.as_slice()),
        });
    }
    owned
}

/// Sends a single message via rdkafka's `FutureProducer`.
///
/// `key`/`value` of `None` map to Kafka null key/payload. Headers are optional; an empty
/// slice means no headers are attached.
pub async fn produce(
    profile: &Profile,
    topic: &str,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Vec<u8>)>,
) -> AppResult<()> {
    let mut config = producer_client_config(profile);
    // Delivery timeout for the FutureProducer send future (not wall-clock of the whole
    // call — librdkafka also enforces this as message.timeout.ms).
    config.set("message.timeout.ms", "30000");

    let producer: FutureProducer = config
        .create()
        .map_err(|err| AppError::Other(format!("failed to create producer: {err}")))?;

    let mut record: FutureRecord<'_, Vec<u8>, Vec<u8>> = FutureRecord::to(topic);
    if let Some(ref k) = key {
        record = record.key(k);
    }
    if let Some(ref v) = value {
        record = record.payload(v);
    }
    if !headers.is_empty() {
        record = record.headers(build_owned_headers(&headers));
    }

    producer
        .send(record, PRODUCE_TIMEOUT)
        .await
        .map_err(|(err, _)| AppError::Kafka(err))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::message::Headers;

    #[test]
    fn build_owned_headers_preserves_keys_and_values() {
        let headers = vec![
            ("h1".to_string(), b"v1".to_vec()),
            ("h2".to_string(), b"v2".to_vec()),
        ];
        let owned = build_owned_headers(&headers);
        assert_eq!(owned.count(), 2);
        let first = owned.get(0);
        assert_eq!(first.key, "h1");
        assert_eq!(first.value, Some(&b"v1"[..]));
        let second = owned.get(1);
        assert_eq!(second.key, "h2");
        assert_eq!(second.value, Some(&b"v2"[..]));
    }

    #[test]
    fn build_owned_headers_empty_is_empty() {
        let owned = build_owned_headers(&[]);
        assert_eq!(owned.count(), 0);
    }
}
