use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use tokio::sync::Mutex;

use crate::meta::ClusterConfig;

use super::message::{NodeInfo, NodeStatus};
use super::Role;

struct AppState {
    nodes: Vec<NodeInfo>,
    cluster_config: ClusterConfig,
}

async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "Service is healthy")
}

async fn node_info(State(state): State<Arc<Mutex<AppState>>>) -> Json<NodeInfo> {
    let status = NodeInfo {
        name: "fixed-master-001".to_string(),
        role: Role::Master,
        ip: "127.0.0.1".to_string(),
        status: NodeStatus::Idle,
    };
    Json(status)
}

async fn list_nodes() -> Json<Vec<NodeInfo>> {
    let nodes = vec![
        NodeInfo {
            name: "fixed-master-001".to_string(),
            role: Role::Master,
            ip: "127.0.0.1".to_string(),6
            status: NodeStatus::Idle,
        },
        NodeInfo {
            name: "fixed-worker-001".to_string(),
            role: Role::Worker,
            ip: "127.0.0.2".to_string(),
            status: NodeStatus::Idle,
        },
    ];
    Json(nodes)
}

pub fn router(state: Arc<Mutex<AppState>>) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/status", get(node_info))
        .route("/nodes", get(list_nodes))
        .with_state(state)
}
