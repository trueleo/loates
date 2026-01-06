use std::{collections::HashMap, sync::Arc};

mod apis;

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{Response, StatusCode},
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use static_files::Resource;
use tokio::sync::Mutex;

use crate::{
    db::DatabaseConn,
    runner::{self, RunnerCommand},
};

#[cfg(feature = "meta")]
use crate::meta::{discovery::DiscoveryService, message::NodeInfo};

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

pub fn router(state: Arc<Mutex<AppState>>) -> Router {
    let static_files = Arc::new(generate());
    let mut router = Router::new();

    router = router
        .route("/health", get(apis::health_check))
        .route("/status", get(apis::node_info))
        .route("/updates/{run_id}/{scenario}", get(apis::run_updates));
        .route("/command/{run_id}", get(apis::run_command));
    // .route("/runs", get(apis::runs));

    #[cfg(feature = "meta")]
    {
        use axum::routing::post;

        let meta_routes = Router::new()
            .route("/nodes", get(apis::list_nodes))
            .route("/register", post(apis::register_node));

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
