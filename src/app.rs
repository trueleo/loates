use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use chrono::{DateTime, Utc};

use crate::{
    logical::Executor,
    tracing::{
        message::Message,
        task_event::{metrics::MetricValue, MetricSetKey},
    },
};

pub mod tui;
// pub mod web;

#[derive(Debug)]
struct ExecutorState {
    config: Executor,
    users: u64,
    max_users: u64,
    iterations: u64,
    total_iteration: Option<u64>,
    prior_duration: Duration,
    start_time: Option<DateTime<Utc>>,
    total_duration: Option<Duration>,
    stage: Option<usize>,
    stage_duration: Option<Duration>,
    stages: Option<usize>,
    task_min_time: Duration,
    task_max_time: Duration,
    task_total_time: Duration,
    metrics: HashMap<MetricSetKey, VecDeque<MetricValue>>,
}

impl ExecutorState {
    fn duration(&self) -> Duration {
        let Some(start_time) = self.start_time else {
            return self.prior_duration;
        };
        self.prior_duration + (Utc::now() - start_time).abs().to_std().unwrap()
    }
}

pub struct Scenario {
    name: String,
    execs: Vec<ExecutorState>,
}

impl Scenario {
    pub fn new_from_scenario(scenario: &crate::logical::Scenario<'_>) -> Self {
        let name = scenario.label.to_string();
        let execs = scenario
            .execution_provider
            .iter()
            .map(|exec| ExecutorState {
                config: exec.config().clone(),
                users: Default::default(),
                max_users: Default::default(),
                iterations: Default::default(),
                total_iteration: Default::default(),
                prior_duration: Default::default(),
                start_time: Default::default(),
                total_duration: Default::default(),
                stage: Default::default(),
                stage_duration: Default::default(),
                stages: Default::default(),
                task_min_time: Default::default(),
                task_max_time: Default::default(),
                task_total_time: Default::default(),
                metrics: Default::default(),
            })
            .collect();

        Self { name, execs }
    }

    fn exec_names(&self) -> impl Iterator<Item = String> + '_ {
        self.execs.iter().map(|x| x.config.to_string())
    }
}

pub struct App {
    current_scenario: usize,
    scenarios: Vec<Scenario>,
}

impl App {
    pub fn new<'a, T>(scenarios: T) -> Self
    where
        T: IntoIterator<Item = &'a crate::logical::Scenario<'a>>,
    {
        let scenarios = scenarios
            .into_iter()
            .map(|scenario| Scenario::new_from_scenario(scenario))
            .collect();

        Self {
            current_scenario: 0,
            scenarios,
        }
    }

    fn current_scenario(&self) -> &Scenario {
        &self.scenarios[self.current_scenario]
    }

    fn current_scenario_mut(&mut self) -> &mut Scenario {
        &mut self.scenarios[self.current_scenario]
    }

    fn handle_message(&mut self, message: Message) {
        match message {
            Message::ScenarioChanged { scenario_id } => {
                self.current_scenario = scenario_id;
            }
            Message::TaskTime {
                execution_id: id,
                duration,
                ..
            } => {
                let exec = &mut self.current_scenario_mut().execs[id];
                exec.iterations += 1;
                exec.task_max_time = exec.task_max_time.max(duration);
                if exec.task_min_time == Duration::ZERO {
                    exec.task_min_time = duration;
                } else {
                    exec.task_min_time = exec.task_min_time.min(duration);
                }
                exec.task_total_time += duration;
            }
            Message::ExecutorUpdate {
                id,
                users,
                max_users,
                total_iteration,
                total_duration,
                stage,
                stages,
                stage_duration,
                metrics,
            } => {
                let exec = &mut self.current_scenario_mut().execs[id];
                exec.users = users;
                exec.max_users = max_users;
                exec.total_duration = total_duration;
                exec.total_iteration = total_iteration;
                exec.stage = stage;
                exec.stages = stages;
                exec.stage_duration = stage_duration;
                metrics.into_iter().for_each(|(key, value)| {
                    let entry = exec.metrics.entry(key).or_default();
                    if entry.len() >= 20 {
                        entry.pop_front();
                    }
                    entry.push_back(value)
                });
            }
            Message::ExecutorStart {
                id,
                start_time,
                prior_executor_duration,
            } => {
                let exec = &mut self.current_scenario_mut().execs[id];
                exec.start_time = Some(start_time);
                exec.prior_duration = prior_executor_duration;
            }
            Message::ExecutorEnd { id } => {
                let exec = &mut self.current_scenario_mut().execs[id];
                if let Some(start_time) = exec.start_time {
                    exec.prior_duration += (Utc::now() - start_time).abs().to_std().unwrap()
                }
                exec.start_time = None;
            }
            _ => (),
        }
    }
}
