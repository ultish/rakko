use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use rakko_core::error::AppError;
use serde_json::json;

/// An error that renders as `{"error": "..."}` with an HTTP status.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

/// Plain-language explanations for Kafka's terse error names. First match wins; the original
/// text stays attached so nothing is hidden.
const FRIENDLY: &[(&[&str], &str)] = &[
    (
        &["InvalidRecord", "Invalid record"],
        "The broker rejected the record. On a compacted topic every message needs a key (a null key is not allowed); otherwise check the topic's config.",
    ),
    (
        &["MessageSizeTooLarge", "Message size too large", "RecordTooLarge"],
        "The message is larger than the broker or topic allows (message.max.bytes / max.message.bytes).",
    ),
    (&["TopicAlreadyExists", "Topic already exists"], "A topic with that name already exists."),
    (
        &["UnknownTopicOrPartition", "Unknown topic or partition"],
        "That topic or partition doesn't exist (it may have been deleted).",
    ),
    (
        &["TopicAuthorizationFailed", "GroupAuthorizationFailed", "ClusterAuthorizationFailed", "AuthorizationFailed"],
        "Not authorised: the credentials rakko uses for this cluster lack the ACL for that operation.",
    ),
    (&["SecurityDisabled"], "ACLs aren't enabled on this cluster (no authorizer is configured)."),
    (
        &["InvalidReplicationFactor"],
        "The replication factor is larger than the number of brokers that are available.",
    ),
    (&["InvalidPartitions"], "That partition count isn't valid for this topic."),
    (
        &["AllBrokersDown", "all broker connections are down", "BrokerTransportFailure", "Failed to resolve"],
        "Can't reach the Kafka brokers. Check the bootstrap servers and the network path from this server.",
    ),
    (
        &["RequestTimedOut", "Timed out", "timed out"],
        "The broker didn't answer in time. It may be overloaded or unreachable; try again.",
    ),
    (
        &["NotCoordinator", "CoordinatorNotAvailable", "CoordinatorLoadInProgress"],
        "The consumer group's coordinator is moving or starting up. Try again in a few seconds.",
    ),
    (
        &["RebalanceInProgress", "UnknownMemberId", "IllegalGeneration"],
        "The group is rebalancing or has active members. Stop its consumers before changing offsets.",
    ),
    (&["UnsupportedVersion"], "This broker's version doesn't support that operation."),
    (&["PolicyViolation"], "The cluster's policy (create.topic.policy / alter.config.policy) refused this."),
    (&["NotEnoughReplicas"], "Not enough in-sync replicas are available to accept the write right now."),
];

fn friendly(message: String) -> String {
    for (needles, text) in FRIENDLY {
        if needles.iter().any(|n| message.contains(n)) {
            return format!("{text} (broker said: {message})");
        }
    }
    message
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self { status, message: friendly(message.into()) }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }
}

impl From<AppError> for ApiError {
    fn from(err: AppError) -> Self {
        // Kafka/registry trouble is the upstream's fault, not the caller's.
        Self::new(StatusCode::BAD_GATEWAY, err.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

pub type ApiResult<T> = Result<Json<T>, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_kafka_errors_get_an_explanation_and_keep_the_original() {
        let m = ApiError::bad_request("Message production error: InvalidRecord (Broker: Broker failed to validate record)").message;
        assert!(m.starts_with("The broker rejected the record"), "{m}");
        assert!(m.contains("Broker failed to validate record"), "{m}");
    }

    #[test]
    fn unknown_errors_pass_through_untouched() {
        assert_eq!(ApiError::bad_request("Invalid value abc for configuration retention.ms").message, "Invalid value abc for configuration retention.ms");
    }
}
