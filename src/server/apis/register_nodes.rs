use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use tokio::sync::Mutex;

use crate::{
    meta::discovery::Node,
    server::{AppError, AppState},
};

pub(crate) async fn register_node(
    State(state): State<Arc<Mutex<AppState>>>,
    Json(node): Json<Node>,
) -> Result<impl IntoResponse, AppError> {
    use reqwest::StatusCode;
    let state = state.lock().await;
    state.discovery.register(node.clone()).await?;
    Ok((StatusCode::CREATED, Json(node)))
}
