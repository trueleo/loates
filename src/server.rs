use std::{collections::HashMap, sync::Arc};

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{Response, StatusCode},
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use static_files::Resource;
use tokio::sync::Mutex;

use crate::{
    db::DatabaseConn,
    runner::{self, NodeStatus, RunnerCommand},
};

#[cfg(feature = "meta")]
use crate::meta::{
    discovery::{DiscoveryService, Node},
    message::NodeInfo,
};

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

pub struct AppState {
    #[cfg(feature = "meta")]
    pub discovery: Arc<dyn DiscoveryService>,
    #[cfg(feature = "meta")]
    pub node_info: NodeInfo,
    pub db: DatabaseConn,
    pub runner_state: Arc<Mutex<runner::RunnerState>>,
    pub runner_command: tokio::sync::mpsc::Sender<RunnerCommand>,
}

async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "Service is healthy")
}

async fn node_info(State(state): State<Arc<Mutex<AppState>>>) -> Json<serde_json::Value> {
    let app_state = state.lock().await;

    let mut info = if cfg!(feature = "meta") {
        serde_json::json!({
            "mode": "distributed",
        })
    } else {
        serde_json::json!({
            "mode": "single",
        })
    };

    let status = app_state.runner_state.lock().await.status;

    #[cfg(feature = "meta")]
    {
        let mut node_info = app_state.node_info.clone();
        node_info.status = status;
        info.as_object_mut()
            .unwrap()
            .insert("info".to_string(), serde_json::to_value(node_info).unwrap());
    }

    #[cfg(not(feature = "meta"))]
    {
        info.as_object_mut().unwrap().insert(
            "info".to_string(),
            serde_json::json!({
                "status": status,
            }),
        );
    }

    Json(info)
}

async fn index(static_files: State<Arc<HashMap<&'static str, Resource>>>) -> Html<&'static [u8]> {
    static_files.get("index.html").unwrap().data.into()
}

async fn serve_static(
    Path(path): Path<String>,
    static_files: State<Arc<HashMap<&'static str, Resource>>>,
) -> Result<impl IntoResponse, StatusCode> {
    if let Some(resource) = static_files.get(&*path) {
        let resp = Response::builder()
            .status(200)
            .header(axum::http::header::CONTENT_TYPE, resource.mime_type)
            .body(Body::from(Bytes::from_static(resource.data)))
            .unwrap();
        Ok(resp)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[axum::debug_handler]
#[cfg(feature = "meta")]
async fn list_nodes(
    State(state): State<Arc<Mutex<AppState>>>,
) -> Result<Json<Vec<NodeInfo>>, AppError> {
    use futures::{stream::FuturesUnordered, TryStreamExt};

    let app_state = state.lock().await;
    let snapshot = app_state.discovery.latest_snapshot().await;
    let nodes: FuturesUnordered<_> = snapshot
        .nodes
        .iter()
        .map(|node| async move {
            let status: NodeStatus = reqwest::get(node.endpoint().join("/status").unwrap())
                .await?
                .json()
                .await?;
            Ok(NodeInfo {
                name: node.name.clone(),
                role: node.role,
                endpoint: node.endpoint(),
                status,
            })
        })
        .collect();

    let nodes: Result<Vec<NodeInfo>, anyhow::Error> = nodes.try_collect().await;
    Ok(Json(nodes?))
}

#[cfg(feature = "meta")]
async fn register_node(
    State(state): State<Arc<Mutex<AppState>>>,
    Json(node): Json<Node>,
) -> Result<impl IntoResponse, AppError> {
    let state = state.lock().await;
    state.discovery.register(node.clone()).await?;
    Ok((StatusCode::CREATED, Json(node)))
}

pub fn router(state: Arc<Mutex<AppState>>) -> Router {
    let static_files = Arc::new(generate());
    let mut router = Router::new();

    router = router
        .route("/health", get(health_check))
        .route("/status", get(node_info));

    #[cfg(feature = "meta")]
    {
        use axum::routing::post;

        let meta_routes = Router::new()
            .route("/nodes", get(list_nodes))
            .route("/register", post(register_node));

        router = router.merge(meta_routes);
    }

    router
        .with_state(state)
        .route("/*path", get(serve_static))
        .fallback(index)
        .with_state(static_files)
        .layer(tower_http::cors::CorsLayer::very_permissive())
}

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("Failed to raise request: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Something went wrong: {0}")]
    AnyError(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AppError::Reqwest(err) => {
                (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
            }
            AppError::AnyError(err) => {
                (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
            }
        }
    }
}
