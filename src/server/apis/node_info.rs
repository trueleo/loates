use std::sync::Arc;

use axum::{extract::State, Json};
use tokio::sync::Mutex;

use crate::server::AppState;

pub(crate) async fn node_info(
    State(state): State<Arc<Mutex<AppState>>>,
) -> Json<serde_json::Value> {
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
