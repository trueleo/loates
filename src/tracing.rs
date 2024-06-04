use std::{
    fmt::Write as _,
    ops::ControlFlow,
    sync::Arc,
    time::{Duration, Instant},
};

use tracing::{
    field::{Field, Visit},
    span, Subscriber,
};

use tracing_subscriber::{registry::LookupSpan, Layer};

/// Represents scalar values that are allowed to be in a user event's attribute set.
#[derive(Debug)]
pub enum Value {
    String(String),
    Number(i64),
    Float(f64),
}

/// Group of events that has occured inside of a user call grouped by event name
#[derive(Debug)]
pub struct TaskEventData {
    name: Arc<str>,
    values: Vec<(&'static str, Value)>,
}

impl Visit for TaskEventData {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.values
            .push((field.name(), Value::String(format!("{:?}", value))))
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.values
            .push((field.name(), Value::Number(value as i64)))
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.values.push((field.name(), Value::Number(value)))
    }
}

/// Tracked data that is associated with a task
#[derive(Debug)]
struct TaskData {
    instant: Instant,
    exec: Arc<str>,
    scenario: Arc<str>,
    events: Vec<TaskEventData>,
}

/// Tracked data associated with span of an execution.
#[derive(Debug)]
struct ExecutionData {
    name: Arc<str>,
    users: u64,
    max_users: u64,
    iterations: u64,
    total_iteration: Option<u64>,
    duration: Option<Duration>,
    total_duration: Option<Duration>,
}

impl tracing::field::Visit for ExecutionData {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "name" {
            self.name = value.into();
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "users" => self.users = value,
            "users_max" => self.max_users = value,
            "iterations" => self.iterations += value,
            "duration" => self.duration = Some(Duration::from_secs(value)),
            "total_duration" => self.total_duration = Some(Duration::from_secs(value)),
            "total_iteration" => self.total_iteration = Some(value),
            _ => (),
        }
    }
}

/// Tracked data associated with a span of a scenario .
struct ScenarioData {
    name: String,
}

impl tracing::field::Visit for ScenarioData {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "name" {
            write!(self.name, "{:?}", value).unwrap()
        }
    }
}

/// Output Message genenerated by this tracing layer
#[derive(Debug)]
pub enum Message {
    TaskTime {
        exec_name: Arc<str>,
        scenario_name: Arc<str>,
        duration: Duration,
        events: Vec<TaskEventData>,
    },
    ExecutorUpdate {
        name: Arc<str>,
        users: u64,
        max_users: u64,
        iterations: u64,
        total_iteration: Option<u64>,
        duration: Option<Duration>,
        total_duration: Option<Duration>,
    },
    ScenarioChanged {
        scenario_name: String,
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
        target == "load_test" || target == "rusher"
    }

    fn on_new_span(
        &self,
        attr: &span::Attributes<'_>,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else { return };
        if span.metadata().target() != "rusher" {
            return;
        };
        match span.name() {
            "task" => {
                let Some(exec_span) = span.parent() else {
                    return;
                };
                let extensions = &exec_span.extensions();
                let exec_name = extensions.get::<ExecutionData>().unwrap();

                let Some(scenario_span) = exec_span.parent() else {
                    return;
                };
                let extensions = &scenario_span.extensions();
                let scenario_name = extensions.get::<ScenarioData>().unwrap();

                let mut extentions = span.extensions_mut();
                extentions.insert(TaskData {
                    instant: Instant::now(),
                    exec: exec_name.name.to_string().into(),
                    scenario: scenario_name.name.to_string().into(),
                    events: Vec::default(),
                });
            }
            "exec" => {
                let mut visitor = ExecutionData {
                    name: "".to_string().into(),
                    users: 0,
                    max_users: 0,
                    iterations: 0,
                    total_iteration: None,
                    duration: None,
                    total_duration: None,
                };
                attr.values().record(&mut visitor);
                let mut extentions = span.extensions_mut();
                extentions.insert(visitor);
            }
            "scenario" => {
                let mut visitor = ScenarioData {
                    name: "".to_string(),
                };
                attr.values().record(&mut visitor);
                let mut extentions = span.extensions_mut();
                extentions.insert(visitor);
            }
            _ => (),
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == "rusher" && event.metadata().name() == "runner_exit" {
            let _ = self.stats_sender.unbounded_send(Message::End);
        }
        if let ControlFlow::Continue(exec_data) = handle_crate_execution_event(event, &ctx) {
            let _ = self.stats_sender.unbounded_send(Message::ExecutorUpdate {
                name: exec_data.name,
                users: exec_data.users,
                max_users: exec_data.max_users,
                iterations: exec_data.iterations,
                total_iteration: exec_data.total_iteration,
                duration: exec_data.duration,
                total_duration: exec_data.total_duration,
            });
            return;
        }
        if let ControlFlow::Continue(()) = handle_user_event(event, &ctx) {}
    }

    fn on_close(&self, id: span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let mut extention = span.extensions_mut();
        let Some(task_data) = extention.get_mut::<TaskData>() else {
            return;
        };
        let events = std::mem::take(&mut task_data.events);
        let _ = self.stats_sender.unbounded_send(Message::TaskTime {
            exec_name: task_data.exec.clone(),
            scenario_name: task_data.scenario.clone(),
            duration: task_data.instant.elapsed(),
            events,
        });
    }
}

fn handle_user_event<S: Subscriber + for<'a> LookupSpan<'a>>(
    event: &tracing::Event,
    ctx: &tracing_subscriber::layer::Context<S>,
) -> ControlFlow<()> {
    if event.metadata().target() != "load_test" {
        return ControlFlow::Break(());
    }
    let Some(parent) = ctx.current_span().id().and_then(|id| ctx.span(id)) else {
        return ControlFlow::Break(());
    };
    let Some(task_parent) = parent.scope().find(|span| span.name() == "task") else {
        return ControlFlow::Break(());
    };
    let mut task_ext = task_parent.extensions_mut();
    let Some(task_data) = task_ext.get_mut::<TaskData>() else {
        return ControlFlow::Break(());
    };

    if let Some(data) = task_data
        .events
        .iter_mut()
        .find(|data| &*data.name == event.metadata().name())
    {
        event.record(data);
    } else {
        let mut event_data = TaskEventData {
            name: event.metadata().name().to_string().into(),
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
    let Some(exec_span) = parent.scope().find(|span| span.name() == "exec") else {
        return ControlFlow::Break(());
    };
    let mut exec_ext = exec_span.extensions_mut();
    let Some(exec_data) = exec_ext.get_mut::<ExecutionData>() else {
        return ControlFlow::Break(());
    };
    event.record(exec_data);
    ControlFlow::Continue(ExecutionData {
        name: Arc::clone(&exec_data.name),
        users: exec_data.users,
        max_users: exec_data.max_users,
        iterations: exec_data.iterations,
        total_iteration: exec_data.total_iteration,
        duration: exec_data.duration,
        total_duration: exec_data.total_duration,
    })
}
