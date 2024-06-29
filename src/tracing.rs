pub mod message;
pub mod task_event;

use std::{
    collections::HashMap,
    ops::ControlFlow,
    str::FromStr,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use message::Message;
use task_event::{metrics::MetricType, MetricSet, TaskEvent, TaskSpanData};
use tracing::{
    field::{Field, Visit},
    span::{self, Id},
    Subscriber,
};

use tracing_subscriber::{
    registry::{LookupSpan, SpanRef},
    Layer,
};

use crate::{CRATE_NAME, SPAN_EXEC, SPAN_SCENARIO, SPAN_TASK, USER_TASK};

#[derive(Debug, Default)]
struct ErrorVisitor {
    err: String,
}

/// Tracked data that is associated with a task
#[derive(Debug)]
struct TaskData {
    scenario_id: usize,
    execution_id: usize,
    execution_span_id: Id,
    instant: Instant,
}

/// Tracked data associated with span of an execution.
#[derive(Debug)]
struct ExecutionData {
    id: usize,
    users: u64,
    max_users: u64,
    total_iteration: Option<u64>,
    duration: Duration,
    total_duration: Option<Duration>,
    stage: Option<usize>,
    stage_duration: Option<Duration>,
    total_stages: Option<usize>,
    metrics: MetricSet,
}

impl From<&ExecutionData> for Message {
    fn from(value: &ExecutionData) -> Self {
        Message::ExecutorUpdate {
            id: value.id,
            users: value.users,
            max_users: value.max_users,
            total_iteration: value.total_iteration,
            total_duration: value.total_duration,
            stage: value.stage,
            stages: value.total_stages,
            stage_duration: value.stage_duration,
            metrics: value.metrics.entries().collect(),
        }
    }
}

struct ExecutorTimings {
    start_time: DateTime<Utc>,
    prior_duration: Duration,
}

/// Tracked data associated with a span of a scenario .
struct ScenarioData {
    id: usize,
    executor_timings: HashMap<usize, ExecutorTimings>,
}

impl tracing::field::Visit for ExecutionData {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "id" => self.id = value as usize,
            "users" => self.users = value,
            "users_max" => self.max_users = value,
            "stages" => self.total_stages = Some(value as usize),
            "stage_duration" => self.stage_duration = Some(Duration::from_secs(value)),
            "stage" => self.stage = Some(value as usize),
            "duration" => self.duration = Duration::from_secs(value),
            "total_duration" => self.total_duration = Some(Duration::from_secs(value)),
            "total_iteration" => self.total_iteration = Some(value),
            _ => (),
        }
    }
}

impl tracing::field::Visit for ScenarioData {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "id" {
            self.id = value as usize
        }
    }
}

impl Visit for ErrorVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "err" {
            self.err = format!("{:?}", value)
        }
    }
}

pub trait Sender {
    fn send(&self, message: Message);
}

impl Sender for crate::Sender<Message> {
    fn send(&self, message: Message) {
        let _ = self.send(message);
    }
}

impl Sender for tokio::sync::broadcast::Sender<Message> {
    fn send(&self, message: Message) {
        let _ = self.send(message);
    }
}

// Tracing layer that tracks and generates message based on this crate's tracing events
pub struct TracerLayer<T: Sender> {
    // current_scenario: Mutex<String>,
    stats_sender: T,
}

impl<T: Sender> TracerLayer<T> {
    pub fn new(sender: T) -> Self {
        Self {
            stats_sender: sender,
        }
    }
}

impl<T: Sender + 'static, S: tracing::Subscriber + for<'a> LookupSpan<'a>> Layer<S>
    for TracerLayer<T>
{
    fn enabled(
        &self,
        metadata: &tracing::Metadata<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        let target = metadata.target();
        target == USER_TASK || target == CRATE_NAME
    }

    fn on_new_span(
        &self,
        attr: &span::Attributes<'_>,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else { return };
        if span.metadata().target() == USER_TASK {
            create_task_child_span(&span, attr);
            return;
        }

        if span.metadata().target() != CRATE_NAME {
            return;
        };

        match span.name() {
            SPAN_TASK => {
                create_task_span(&span);
            }
            SPAN_EXEC => {
                let message = create_exec_span(attr, &span);
                self.stats_sender.send(message);
            }
            SPAN_SCENARIO => {
                let id = create_scenario_span(attr, span);
                self.stats_sender
                    .send(Message::ScenarioChanged { scenario_id: id })
            }
            _ => (),
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == USER_TASK {
            handle_user_event(event, &ctx);
            return;
        }
        if event.metadata().target() == CRATE_NAME {
            match event.metadata().name() {
                "runner_exit" => {
                    self.stats_sender.send(Message::End);
                    return;
                }
                "termination_error" => {
                    let mut err = ErrorVisitor::default();
                    event.record(&mut err);
                    self.stats_sender
                        .send(Message::TerminatedError { err: err.err });
                    return;
                }
                "error" => {
                    let mut err = ErrorVisitor::default();
                    event.record(&mut err);
                    self.stats_sender.send(Message::Error { err: err.err });
                    return;
                }
                _ => {}
            }

            if let Some(message) = handle_crate_execution_event(event, &ctx) {
                self.stats_sender.send(message);
            }
        }
    }

    fn on_record(
        &self,
        span: &span::Id,
        values: &span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let Some(span) = ctx.span(span) else {
            return;
        };
        let mut span_data = span.extensions_mut();
        let Some(span_data) = span_data.get_mut::<TaskSpanData>() else {
            return;
        };
        values.record(span_data);
    }

    fn on_close(&self, id: span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };

        if span.metadata().name() == SPAN_EXEC {
            let message = close_exec_span(span);
            self.stats_sender.send(message);
            return;
        }

        if span.metadata().name() == SPAN_TASK {
            let messages = close_task_span(span, &ctx);
            for message in messages {
                self.stats_sender.send(message);
            }
            return;
        }

        if span.metadata().target() == USER_TASK {
            close_task_child_span(span, &ctx);
        }
    }
}

fn create_scenario_span<S: for<'a> LookupSpan<'a>>(
    attr: &span::Attributes,
    span: SpanRef<S>,
) -> usize {
    let mut visitor = ScenarioData {
        id: usize::MAX,
        executor_timings: HashMap::default(),
    };
    attr.values().record(&mut visitor);
    let id = visitor.id;
    let mut extentions = span.extensions_mut();
    extentions.insert(visitor);
    id
}

fn create_exec_span<'a, S: LookupSpan<'a>>(
    attr: &span::Attributes,
    span: &SpanRef<'a, S>,
) -> Message {
    let mut visitor = ExecutionData {
        id: usize::MAX,
        users: 0,
        max_users: 0,
        total_iteration: None,
        duration: Duration::ZERO,
        total_duration: None,
        total_stages: None,
        stage: None,
        stage_duration: None,
        metrics: MetricSet::default(),
    };
    attr.values().record(&mut visitor);
    let id = visitor.id;
    let mut extentions = span.extensions_mut();
    extentions.insert(visitor);

    let start_time = Utc::now();

    let scenario = span.parent().unwrap();
    let mut scenario = scenario.extensions_mut();
    let scenario_data = scenario
        .get_mut::<ScenarioData>()
        .unwrap()
        .executor_timings
        .entry(id)
        .or_insert_with(|| ExecutorTimings {
            start_time,
            prior_duration: Duration::ZERO,
        });

    scenario_data.start_time = start_time;

    Message::ExecutorStart {
        id,
        start_time,
        prior_executor_duration: scenario_data.prior_duration,
    }
}

fn create_task_span<'a, S: LookupSpan<'a>>(span: &SpanRef<'a, S>) {
    let Some(exec_span) = span.parent() else {
        return;
    };
    let Some(scenario_span) = exec_span.parent() else {
        return;
    };

    let scenario_id = scenario_span
        .extensions()
        .get::<ScenarioData>()
        .expect("exec parent is scenario")
        .id;

    let execution_id = exec_span
        .extensions()
        .get::<ExecutionData>()
        .expect("task parent is exec")
        .id;

    let execution_span_id = exec_span.id();

    let mut extentions = span.extensions_mut();
    extentions.insert(TaskData {
        instant: Instant::now(),
        scenario_id,
        execution_id,
        execution_span_id,
    });
}

fn create_task_child_span<'a, S: LookupSpan<'a>>(span: &SpanRef<'a, S>, attr: &span::Attributes) {
    let task_span = span
        .scope()
        .find(|span| span.metadata().name() == SPAN_TASK)
        .expect("span is child of an executor task");
    let execution_span_id = task_span
        .extensions()
        .get::<TaskData>()
        .unwrap()
        .execution_span_id
        .clone();
    let mut val = TaskSpanData {
        start_time: Instant::now(),
        attributes: vec![],
        execution_span_id,
    };
    attr.record(&mut val);
    span.extensions_mut().insert(val);
}

fn handle_user_event<S: Subscriber + for<'a> LookupSpan<'a>>(
    event: &tracing::Event,
    ctx: &tracing_subscriber::layer::Context<S>,
) -> ControlFlow<(), ()> {
    if event.metadata().target() != USER_TASK {
        return ControlFlow::Break(());
    }

    let Some(parent) = ctx.current_span().id().and_then(|id| ctx.span(id)) else {
        return ControlFlow::Break(());
    };

    let attributes: Vec<_> = parent
        .scope()
        .take_while(|x| x.metadata().target() == USER_TASK)
        .map(|x| x.id())
        .map(|id| {
            let span = ctx.span(&id).unwrap();
            let x = span
                .extensions()
                .get::<TaskSpanData>()
                .unwrap()
                .attributes
                .clone();
            x
        })
        .collect();

    let Some(exec_span) = parent.scope().find(|span| span.name() == SPAN_EXEC) else {
        return ControlFlow::Break(());
    };

    let Some((name, ty_str)) = event.metadata().name().split_once('.') else {
        return ControlFlow::Break(());
    };

    let Ok(metric_type) = MetricType::from_str(ty_str) else {
        return ControlFlow::Break(());
    };

    let mut task_event = TaskEvent::new(
        name,
        metric_type,
        attributes.into_iter().rev().flatten().collect(),
        task_event::Value::Number(0),
    );
    event.record(&mut task_event);

    let data = exec_span.extensions();
    let data = data.get::<ExecutionData>().unwrap();
    data.metrics.update(task_event);

    ControlFlow::Continue(())
}

fn handle_crate_execution_event<S: Subscriber + for<'a> LookupSpan<'a>>(
    event: &tracing::Event,
    ctx: &tracing_subscriber::layer::Context<S>,
) -> Option<Message> {
    if event.metadata().target() != CRATE_NAME {
        return None;
    }
    let parent = ctx.current_span().id().and_then(|id| ctx.span(id))?;
    let exec_span = parent.scope().find(|span| span.name() == SPAN_EXEC)?;
    let mut exec_ext = exec_span.extensions_mut();
    let exec_data = exec_ext.get_mut::<ExecutionData>()?;
    event.record(exec_data);
    Some(Message::from(&*exec_data))
}

fn close_exec_span<S: Subscriber + for<'a> LookupSpan<'a>>(span: SpanRef<S>) -> Message {
    let exec_id = span.extensions().get::<ExecutionData>().unwrap().id;
    let scenario = span.parent().unwrap();
    let mut scenario = scenario.extensions_mut();
    let scenario = scenario.get_mut::<ScenarioData>().unwrap();
    scenario
        .executor_timings
        .entry(exec_id)
        .and_modify(|x| x.prior_duration += (Utc::now() - x.start_time).abs().to_std().unwrap());
    Message::ExecutorEnd { id: exec_id }
}

fn close_task_span<'a, S: Subscriber + for<'lookup> LookupSpan<'lookup>>(
    span: SpanRef<'a, S>,
    ctx: &tracing_subscriber::layer::Context<'a, S>,
) -> [Message; 2] {
    let extention = span.extensions();
    let task_data = extention.get::<TaskData>().unwrap();
    let m1 = Message::TaskTime {
        execution_id: task_data.execution_id,
        scenario_id: task_data.scenario_id,
        duration: task_data.instant.elapsed(),
    };

    let exec = ctx.span(&task_data.execution_span_id).unwrap();
    let ext = exec.extensions();
    let exec_data = ext.get::<ExecutionData>().unwrap();
    let m2 = Message::from(exec_data);
    [m1, m2]
}

fn close_task_child_span<'a, S: Subscriber + for<'lookup> LookupSpan<'lookup>>(
    span: SpanRef<S>,
    ctx: &tracing_subscriber::layer::Context<S>,
) {
    let extention = span.extensions();
    let task_inner_span = extention.get::<TaskSpanData>().unwrap();

    let attributes: Vec<_> = span
        .scope()
        .take_while(|x| x.metadata().target() == USER_TASK)
        .map(|x| x.id())
        .collect();
    let mut attributes: Vec<_> = attributes
        .into_iter()
        .map(|id| {
            let span = ctx.span(&id).unwrap();
            let x = span
                .extensions()
                .get::<TaskSpanData>()
                .unwrap()
                .attributes
                .clone();
            x
        })
        .collect();

    attributes.reverse();
    let event = TaskEvent::new(
        span.name(),
        MetricType::Histogram,
        attributes.into_iter().flatten().collect(),
        task_inner_span.start_time.elapsed().into(),
    );

    let task_span = span
        .scope()
        .find(|x| x.metadata().name() == SPAN_EXEC)
        .unwrap();
    let task_span = ctx.span(&task_span.id()).unwrap();
    task_span
        .extensions()
        .get::<ExecutionData>()
        .unwrap()
        .metrics
        .update(event);
}
