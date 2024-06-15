use std::time::Duration;

use super::task_event::{metrics::MetricValue, MetricSetKey};

/// Output Message genenerated by this tracing layer
#[derive(Debug)]
pub enum Message {
    TaskTime {
        execution_id: usize,
        scenario_id: usize,
        duration: Duration,
    },
    ExecutorUpdate {
        id: usize,
        users: u64,
        max_users: u64,
        total_iteration: Option<u64>,
        duration: Duration,
        total_duration: Option<Duration>,
        stage: Option<usize>,
        stage_duration: Option<Duration>,
        stages: Option<usize>,
        metrics: Vec<(MetricSetKey, MetricValue)>,
    },
    Error {
        err: String,
    },
    TerminatedError {
        err: String,
    },
    ScenarioChanged {
        scenario_id: usize,
    },
    End,
}
