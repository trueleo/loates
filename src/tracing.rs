use std::{
    fmt::Display,
    ops::ControlFlow,
    time::{Duration, Instant},
};

use tracing::{
    field::{Field, Visit},
    span, Subscriber,
};

use tracing_subscriber::{registry::LookupSpan, Layer};

use crate::{CRATE_NAME, SPAN_EXEC, SPAN_SCENARIO, SPAN_TASK, TARGET_USER_EVENT};

/// Represents scalar values that are allowed to be in a user event's attribute set.
#[derive(Debug)]
pub enum Value {
    String(String),
    Number(i64),
    UnsignedNumber(u64),
    Float(f64),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(x) => write!(f, "{}", x),
            Value::Number(x) => write!(f, "{}", x),
            Value::UnsignedNumber(x) => write!(f, "{}", x),
            Value::Float(x) => write!(f, "{}", x),
        }
    }
}

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

/// Group of events that has occured inside of a user call grouped by event name
#[derive(Debug)]
pub struct TaskEventData {
    pub name: &'static str,
    pub values: Vec<(&'static str, Value)>,
}

impl Visit for TaskEventData {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.values
            .push((field.name(), Value::String(format!("{:?}", value))))
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.values
            .push((field.name(), Value::UnsignedNumber(value)))
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.values.push((field.name(), Value::Number(value)))
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.values.push((field.name(), Value::Float(value)))
    }
}

/// Tracked data that is associated with a task
#[derive(Debug)]
struct TaskData {
    scenario_id: usize,
    execution_id: usize,
    instant: Instant,
    events: Vec<TaskEventData>,
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
    total_stages: Option<usize>,
}

impl tracing::field::Visit for ExecutionData {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "id" => self.id = value as usize,
            "users" => self.users = value,
            "users_max" => self.max_users = value,
            "stages" => self.total_stages = Some(value as usize),
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

/// Output Message genenerated by this tracing layer
#[derive(Debug)]
pub enum Message {
    TaskTime {
        execution_id: usize,
        scenario_id: usize,
        start_time: Instant,
        end_time: Instant,
        events: Vec<TaskEventData>,
    },
    ExecutorUpdate {
        id: usize,
        users: u64,
        max_users: u64,
        total_iteration: Option<u64>,
        duration: Duration,
        total_duration: Option<Duration>,
        stage: Option<usize>,
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
        target == TARGET_USER_EVENT || target == CRATE_NAME
    }

    fn on_new_span(
        &self,
        attr: &span::Attributes<'_>,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else { return };
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
                    events: Vec::default(),
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
                _ => (),
            }
        }

        if let ControlFlow::Continue(exec_data) = handle_crate_execution_event(event, &ctx) {
            let _ = self.stats_sender.send(Message::ExecutorUpdate {
                id: exec_data.id,
                users: exec_data.users,
                max_users: exec_data.max_users,
                total_iteration: exec_data.total_iteration,
                duration: exec_data.duration,
                total_duration: exec_data.total_duration,
                stage: exec_data.stage,
                stages: exec_data.total_stages,
            });
            return;
        }
        if let ControlFlow::Continue(()) = handle_user_event(event, &ctx) {}
    }

    fn on_close(&self, id: span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let end_time = Instant::now();
        let Some(span) = ctx.span(&id) else { return };
        let mut extention = span.extensions_mut();
        let Some(task_data) = extention.get_mut::<TaskData>() else {
            return;
        };
        let events = std::mem::take(&mut task_data.events);
        let _ = self.stats_sender.send(Message::TaskTime {
            start_time: task_data.instant,
            execution_id: task_data.execution_id,
            scenario_id: task_data.scenario_id,
            end_time,
            events,
        });
    }
}

fn handle_user_event<S: Subscriber + for<'a> LookupSpan<'a>>(
    event: &tracing::Event,
    ctx: &tracing_subscriber::layer::Context<S>,
) -> ControlFlow<()> {
    if event.metadata().target() != TARGET_USER_EVENT {
        return ControlFlow::Break(());
    }
    let Some(parent) = ctx.current_span().id().and_then(|id| ctx.span(id)) else {
        return ControlFlow::Break(());
    };
    let Some(task_parent) = parent.scope().find(|span| span.name() == SPAN_TASK) else {
        return ControlFlow::Break(());
    };
    let mut task_ext = task_parent.extensions_mut();
    let Some(task_data) = task_ext.get_mut::<TaskData>() else {
        return ControlFlow::Break(());
    };

    if let Some(data) = task_data
        .events
        .iter_mut()
        .find(|data| data.name == event.metadata().name())
    {
        event.record(data);
    } else {
        let mut event_data = TaskEventData {
            name: event.metadata().name(),
            values: vec![],
        };
        event.record(&mut event_data);
        task_data.events.push(event_data);
    }
    ControlFlow::Continue(())
}

fn handle_crate_execution_event<S: Subscriber + for<'a> LookupSpan<'a>>(
    event: &tracing::Event,
    ctx: &tracing_subscriber::layer::Context<S>,
) -> ControlFlow<(), ExecutionData> {
    if event.metadata().target() != "rusher" {
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
    ControlFlow::Continue(ExecutionData {
        id: exec_data.id,
        users: exec_data.users,
        max_users: exec_data.max_users,
        total_iteration: exec_data.total_iteration,
        duration: exec_data.duration,
        total_duration: exec_data.total_duration,
        total_stages: exec_data.total_stages,
        stage: exec_data.stage,
    })
}
