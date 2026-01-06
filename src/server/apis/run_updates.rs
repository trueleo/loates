use std::sync::Arc;

use anyhow::anyhow;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::Utc;
use tokio::sync::Mutex;

use crate::server::{AppError, AppState};

pub(crate) async fn run_updates(
    Path(run_id): Path<String>,
    Path(scenario): Path<String>,
    Query(since): Query<chrono::DateTime<Utc>>,
    State(state): State<Arc<Mutex<AppState>>>,
) -> Result<Json<serde_json::Value>, AppError> {
    let requested_test_id = ulid::Ulid::from_string(&run_id)
        .map_err(|_err| anyhow!("Invalid run_id passed in the url parameter"))?;
    let (running, db) = {
        let app_state = state.lock().await;
        let running = app_state.runner_state.lock().await.status.clone();
        let db = app_state
            .db
            .db_conn
            .try_clone()
            .map_err(|err| anyhow::anyhow!(err));
        (running, db)
    };

    let db = db?;

    let (start_time, end_time) = extract_scenario_times(&db, &requested_test_id, &scenario)
        .await
        .unwrap_or((None, None));

    let result: serde_json::Value = serde_json::json!({
        "run_id": run_id,
        "runner_status": running.clone(),
        "scenario": {
            "id": scenario,
            "start_time": start_time,
            "end_time": end_time,
            "executors": []
        }
    });

    Ok(Json(result))
}

async fn extract_scenario_times(
    db: &duckdb::Connection,
    run_id: &ulid::Ulid,
    scenario_name: &str,
) -> Result<(Option<String>, Option<String>), anyhow::Error> {
    let mut start_time: Option<String> = None;
    let mut end_time: Option<String> = None;

    let start_message_json = db.query_row_and_then(
        "SELECT payload FROM messages WHERE run_id = ? AND scenario_name = ? AND message_type = 'ScenarioStarted'",
        [&run_id.to_string(), scenario_name],
        |row| -> Result<String, duckdb::Error> {
            let payload: String = row.get(0)?;
            Ok(payload)
        },
    )?;

    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&start_message_json) {
        if let Some(time) = json.get(" start_time").and_then(|v| v.as_str()) {
            start_time = Some(time.to_string());
        }
    }

    let end_message_json = db.query_row_and_then(
        "SELECT payload FROM messages WHERE run_id = ? AND scenario_name = ? AND message_type = 'ScenarioEnded'",
        [&run_id.to_string(), scenario_name],
        |row| -> Result<String, duckdb::Error> {
            let payload: String = row.get(0)?;
            Ok(payload)
        },
    )?;

    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&end_message_json) {
        if let Some(time) = json.get("end_time").and_then(|v| v.as_str()) {
            end_time = Some(time.to_string());
        }
    }

    Ok((start_time, end_time))
}

pub fn serialize_to_rfc3339_opts(t: &chrono::DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Millis, false)
}
