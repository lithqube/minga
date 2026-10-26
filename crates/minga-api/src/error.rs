//! API error type with Docker-compatible error responses.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::models::ErrorResponse;

/// Error type for the Docker Engine API.
///
/// Maps to Docker-compatible HTTP status codes and JSON error bodies:
/// `{"message": "..."}`.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),

    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    Conflict(String),

    #[error("{0}")]
    Internal(String),

    #[error("state error: {0}")]
    State(String),
}

impl ApiError {
    fn status(&self) -> StatusCode {
        match self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::Internal(_) | ApiError::State(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<minga_state::StateError> for ApiError {
    fn from(e: minga_state::StateError) -> Self {
        ApiError::State(e.to_string())
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        ApiError::BadRequest(format!("invalid JSON: {e}"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = ErrorResponse {
            message: self.to_string(),
        };
        (status, Json(body)).into_response()
    }
}

/// Convenience: `Result<T, ApiError>` for handlers.
pub type ApiResult<T> = Result<T, ApiError>;

/// Helper: create a not-found error.
pub fn not_found(kind: &str, id: &str) -> ApiError {
    ApiError::NotFound(format!("{kind} {id} not found"))
}

/// Helper: create a conflict error.
pub fn conflict(msg: impl Into<String>) -> ApiError {
    ApiError::Conflict(msg.into())
}

/// Helper: create an internal error.
pub fn internal(msg: impl Into<String>) -> ApiError {
    ApiError::Internal(msg.into())
}

/// A 200 OK response with no body (for DELETE endpoints).
pub fn no_content() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// A 200 OK response with `{"message": "..."}` body.
pub fn ok_message(msg: impl Into<String>) -> Json<serde_json::Value> {
    Json(json!({ "message": msg.into() }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn error_response_format() {
        let err = ApiError::NotFound("service abc not found".to_string());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["message"], "service abc not found");
    }

    #[tokio::test]
    async fn bad_request_status() {
        let err = ApiError::BadRequest("bad input".to_string());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn conflict_status() {
        let err = ApiError::Conflict("already exists".to_string());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn ok_message_json() {
        let msg = ok_message("removed");
        assert_eq!(msg.0["message"], "removed");
    }

    #[test]
    fn no_content_status() {
        assert_eq!(no_content(), StatusCode::NO_CONTENT);
    }

    #[test]
    fn not_found_helper() {
        let err = not_found("service", "abc123");
        assert!(err.to_string().contains("service"));
        assert!(err.to_string().contains("abc123"));
    }

    #[test]
    fn from_state_error() {
        let state_err = minga_state::StateError::Storage("test".to_string());
        let api_err: ApiError = state_err.into();
        assert!(matches!(api_err, ApiError::State(_)));
    }
}
