use axum::{http::StatusCode, response::IntoResponse};

pub(crate) async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "Service is healthy")
}
