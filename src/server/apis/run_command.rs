use std::sync::Arc;

use anyhow::anyhow;
use axum::extract::{Path, Query, State};
use tokio::sync::Mutex;

use crate::{
    runner::{NodeStatus, RunnerCommand},
    server::{AppError, AppState},
};

pub(crate) async fn run_command(
    Path(run_id): Path<String>,
    Query(command): Query<RunnerCommand>,
    State(state): State<Arc<Mutex<AppState>>>,
) -> Result<(), AppError> {
    let requested_test_id = ulid::Ulid::from_string(&run_id)
        .map_err(|_err| anyhow!("Invalid run_id passed in the url parameter"))?;
    let app_state = state.lock().await;
    let mut runner_state = app_state.runner_state.lock().await;
    if runner_state.test_id == requested_test_id {
        let _ = app_state.runner_command.send(command);
        if command == RunnerCommand::Start {
            runner_state.status = NodeStatus::RunningTest(requested_test_id);
        } else {
            runner_state.status = NodeStatus::Stopping(requested_test_id)
        }
    }
    Ok(())
}
