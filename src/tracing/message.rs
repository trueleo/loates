use std::time::{Duration, Instant};

/// Output Message genenerated by this tracing layer
#[derive(Debug)]
pub enum Message {
    TaskTime {
        execution_id: usize,
        scenario_id: usize,
        start_time: Instant,
        end_time: Instant,
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
    },
    TerminatedError {
        err: String,
    },
    ScenarioChanged {
        scenario_id: usize,
    },
    End,
}
