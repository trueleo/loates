use axum::{extract::State, Json};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    meta::message::NodeInfo,
    runner::NodeStatus,
    server::{AppError, AppState},
};

#[cfg(feature = "meta")]
pub(crate) async fn list_nodes(
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
