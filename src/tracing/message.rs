use std::time::Duration;

use chrono::{DateTime, Utc};

use super::task_event::{metrics::MetricValue, MetricSetKey};

/// Output Message genenerated by this tracing layer
#[derive(Debug, Clone)]
pub enum Message {
    TaskTime {
        execution_id: usize,
        scenario_id: usize,
        duration: Duration,
    },
    ExecutorStart {
        id: usize,
        start_time: DateTime<Utc>,
        prior_executor_duration: Duration,
    },
    ExecutorUpdate {
        id: usize,
        users: u64,
        max_users: u64,
        total_iteration: Option<u64>,
        total_duration: Option<Duration>,
        stage: Option<usize>,
        stage_duration: Option<Duration>,
        stages: Option<usize>,
        metrics: Vec<(MetricSetKey, MetricValue)>,
    },
    ExecutorEnd {
        id: usize,
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
