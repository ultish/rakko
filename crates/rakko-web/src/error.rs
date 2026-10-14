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

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self { status, message: message.into() }
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
