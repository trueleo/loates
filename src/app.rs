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

#[cfg(feature = "tui")]
pub mod tui;
#[cfg(feature = "web")]
pub mod web;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
pub struct ExecutorState {
    ended: bool,
    config: Executor,
    users: u64,
    max_users: u64,
    iterations: u64,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    total_iteration: Option<u64>,
    prior_duration: Duration,
    #[cfg_attr(
        feature = "serde",
        serde(
            serialize_with = "serialize_to_rfc3339_opts",
            skip_serializing_if = "Option::is_none"
        )
    )]
    start_time: Option<DateTime<Utc>>,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    total_duration: Option<Duration>,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    stage: Option<usize>,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    stage_duration: Option<Duration>,
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    stages: Option<usize>,
    task_min_time: Duration,
    task_max_time: Duration,
    task_total_time: Duration,
    #[cfg_attr(feature = "serde", serde(serialize_with = "serialize_metric"))]
    metrics: HashMap<MetricSetKey, VecDeque<MetricValue>>,
}

impl ExecutorState {
    pub fn duration(&self) -> Duration {
        let Some(start_time) = self.start_time else {
            return self.prior_duration;
        };
        self.prior_duration + (Utc::now() - start_time).abs().to_std().unwrap()
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
#[derive(Debug, Clone)]
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
                ended: false,
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

    pub fn exec_names(&self) -> impl Iterator<Item = String> + '_ {
        self.execs.iter().map(|x| x.config.to_string())
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
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

    pub fn current_scenario(&self) -> &Scenario {
        &self.scenarios[self.current_scenario]
    }

    pub fn current_scenario_mut(&mut self) -> &mut Scenario {
        &mut self.scenarios[self.current_scenario]
    }

    pub fn handle_message(&mut self, message: Message) {
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
                exec.ended = true
            }
            _ => (),
        }
    }
}

#[cfg(feature = "web")]
pub fn serialize_to_rfc3339_opts<S: serde::Serializer>(
    t: &Option<DateTime<Utc>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    serde::Serialize::serialize(
        &t.as_ref()
            .map(|x| x.to_rfc3339_opts(chrono::SecondsFormat::Millis, false)),
        s,
    )
}

#[cfg(feature = "serde")]
fn serialize_metric<S: serde::Serializer>(
    t: &HashMap<MetricSetKey, VecDeque<MetricValue>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq as _;
    let mut seq = s.serialize_seq(Some(t.len()))?;
    for entry in t.iter() {
        seq.serialize_element(&entry)?;
    }
    seq.end()
}
