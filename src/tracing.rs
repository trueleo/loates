pub mod message;
pub mod task_event;

use std::{
    ops::ControlFlow,
    str::FromStr,
    time::{Duration, Instant},
};

use message::Message;
use task_event::{metrics::MetricType, MetricSet, TaskEvent, TaskSpanData};
use tracing::{
    field::{Field, Visit},
    span, Subscriber,
};

use tracing_subscriber::{registry::LookupSpan, Layer};

use crate::{CRATE_NAME, SPAN_EXEC, SPAN_SCENARIO, SPAN_TASK, USER_TASK};

#[derive(Debug, Default)]
struct ErrorVisitor {
    err: String,
}

impl Visit for ErrorVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "err" {
            self.err = format!("{:?}", value)
        }
    }
}

/// Tracked data that is associated with a task
#[derive(Debug)]
struct TaskData {
    scenario_id: usize,
    execution_id: usize,
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

/// Tracked data associated with a span of a scenario .
struct ScenarioData {
    id: usize,
}

impl tracing::field::Visit for ScenarioData {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "id" {
            self.id = value as usize
        }
    }
}

// Tracing layer that tracks and generates message based on this crate's tracing events
pub struct TraceHttp {
    // current_scenario: Mutex<String>,
    stats_sender: crate::Sender<Message>,
}

impl TraceHttp {
    pub fn new() -> (Self, crate::Receiver<Message>) {
        let (tx, rx) = crate::channel();
        (Self { stats_sender: tx }, rx)
    }
}

impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> Layer<S> for TraceHttp {
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
            span.extensions_mut().insert(TaskSpanData::new());
            return;
        }

        if span.metadata().target() != CRATE_NAME {
            return;
        };

        match span.name() {
            SPAN_TASK => {
                let Some(exec_span) = span.parent() else {
                    return;
                };
                let extensions = &exec_span.extensions();
                let execution = extensions
                    .get::<ExecutionData>()
                    .expect("task parent is exec");

                let Some(scenario_span) = exec_span.parent() else {
                    return;
                };
                let extensions = &scenario_span.extensions();
                let scenario = extensions
                    .get::<ScenarioData>()
                    .expect("exec parent is scenario");

                let mut extentions = span.extensions_mut();
                extentions.insert(TaskData {
                    instant: Instant::now(),
                    scenario_id: scenario.id,
                    execution_id: execution.id,
                });
            }
            SPAN_EXEC => {
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
                let mut extentions = span.extensions_mut();
                extentions.insert(visitor);
            }
            SPAN_SCENARIO => {
                let mut visitor = ScenarioData { id: usize::MAX };
                attr.values().record(&mut visitor);
                let mut extentions = span.extensions_mut();
                extentions.insert(visitor);
            }
            _ => (),
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == CRATE_NAME {
            match event.metadata().name() {
                "runner_exit" => {
                    let _ = self.stats_sender.send(Message::End);
                    return;
                }
                "termination_error" => {
                    let mut err = ErrorVisitor::default();
                    event.record(&mut err);
                    let _ = self
                        .stats_sender
                        .send(Message::TerminatedError { err: err.err });
                    return;
                }
                "error" => {
                    let mut err = ErrorVisitor::default();
                    event.record(&mut err);
                    let _ = self.stats_sender.send(Message::Error { err: err.err });
                    return;
                }
                _ => (),
            }
        }

        if let ControlFlow::Continue(exec_update) = handle_crate_execution_event(event, &ctx) {
            let _ = self.stats_sender.send(exec_update);
            return;
        }
        if let ControlFlow::Continue(update) = handle_user_event(event, &ctx) {
            let _ = self.stats_sender.send(update);
        }
    }

    fn on_close(&self, id: span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let end_time = Instant::now();
        let Some(span) = ctx.span(&id) else { return };
        let mut extention = span.extensions_mut();

        if let Some(task_inner_span) = extention.get_mut::<TaskSpanData>() {
            let mut attributes: Vec<_> = span
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
            return;
        };

        let Some(task_data) = extention.get_mut::<TaskData>() else {
            return;
        };
        let _ = self.stats_sender.send(Message::TaskTime {
            start_time: task_data.instant,
            execution_id: task_data.execution_id,
            scenario_id: task_data.scenario_id,
            end_time,
        });
    }
}

fn handle_user_event<S: Subscriber + for<'a> LookupSpan<'a>>(
    event: &tracing::Event,
    ctx: &tracing_subscriber::layer::Context<S>,
) -> ControlFlow<(), Message> {
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

    ControlFlow::Continue(Message::from(data))
}

fn handle_crate_execution_event<S: Subscriber + for<'a> LookupSpan<'a>>(
    event: &tracing::Event,
    ctx: &tracing_subscriber::layer::Context<S>,
) -> ControlFlow<(), Message> {
    if event.metadata().target() != CRATE_NAME {
        return ControlFlow::Break(());
    }
    let Some(parent) = ctx.current_span().id().and_then(|id| ctx.span(id)) else {
        return ControlFlow::Break(());
    };
    let Some(exec_span) = parent.scope().find(|span| span.name() == SPAN_EXEC) else {
        return ControlFlow::Break(());
    };
    let mut exec_ext = exec_span.extensions_mut();
    let Some(exec_data) = exec_ext.get_mut::<ExecutionData>() else {
        return ControlFlow::Break(());
    };
    event.record(exec_data);
    ControlFlow::Continue(Message::from(&*exec_data))
}

impl From<&ExecutionData> for Message {
    fn from(value: &ExecutionData) -> Self {
        Message::ExecutorUpdate {
            id: value.id,
            users: value.users,
            max_users: value.max_users,
            total_iteration: value.total_iteration,
            duration: value.duration,
            total_duration: value.total_duration,
            stage: value.stage,
            stages: value.total_stages,
            stage_duration: value.stage_duration,
            metrics: value.metrics.entries().collect(),
        }
    }
}
