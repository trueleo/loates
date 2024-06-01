use std::{
    fmt::Write as _,
    time::{Duration, Instant},
};

use tracing::{
    field::{Field, Visit},
    span,
};

use tracing_subscriber::{registry::LookupSpan, Layer};

#[derive(Debug)]
pub enum Value {
    String(String),
    Number(i64),
    Float(f64),
}

#[derive(Debug)]
pub struct EventData {
    name: String,
    values: Vec<(String, Value)>,
}

impl Visit for EventData {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.values.push((
            field.name().to_string(),
            Value::String(format!("{:?}", value)),
        ))
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.values
            .push((field.name().to_string(), Value::Number(value as i64)))
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.values
            .push((field.name().to_string(), Value::Number(value)))
    }
}

#[derive(Debug)]
struct TaskData {
    instant: Instant,
    exec: String,
    scenario: String,
    events: Vec<EventData>,
}

impl TaskData {
    fn default() -> Self {
        Self {
            instant: Instant::now(),
            exec: "".to_string(),
            scenario: "".to_string(),
            events: vec![],
        }
    }
}

struct ExecData {
    name: String,
}

impl tracing::field::Visit for ExecData {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "name" {
            write!(self.name, "{:?}", value).unwrap()
        }
    }
}

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

#[derive(Debug)]
pub enum Message {
    TaskTime {
        exec_name: String,
        scenario_name: String,
        duration: Duration,
        events: Vec<EventData>,
    },
    ScenarioChanged {
        scenario_name: String,
    },
}

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
                let exec_name = extensions.get::<ExecData>().unwrap();

                let Some(scenario_span) = exec_span.parent() else {
                    return;
                };
                let extensions = &scenario_span.extensions();
                let scenario_name = extensions.get::<ScenarioData>().unwrap();

                let mut extentions = span.extensions_mut();
                extentions.insert(TaskData {
                    instant: Instant::now(),
                    exec: exec_name.name.to_string(),
                    scenario: scenario_name.name.to_string(),
                    events: Vec::default(),
                });
            }
            "exec" => {
                let mut visitor = ExecData {
                    name: "".to_string(),
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
        if event.metadata().target() != "load_test" {
            return;
        }
        let Some(parent) = ctx.current_span().id().and_then(|id| ctx.span(id)) else {
            return;
        };

        let Some(task_parent) = parent.scope().find(|span| span.name() == "task") else {
            return;
        };
        let mut task_ext = task_parent.extensions_mut();
        let Some(task_data) = task_ext.get_mut::<TaskData>() else {
            return;
        };

        if let Some(data) = task_data
            .events
            .iter_mut()
            .find(|data| data.name == event.metadata().name())
        {
            event.record(data);
        } else {
            let mut event_data = EventData {
                name: event.metadata().name().to_string(),
                values: vec![],
            };
            event.record(&mut event_data);
            task_data.events.push(event_data);
        }
    }

    fn on_close(&self, id: span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let mut extention = span.extensions_mut();
        let Some(task_data) = extention.get_mut::<TaskData>() else {
            return;
        };
        let task_data = std::mem::replace(task_data, TaskData::default());
        let _ = self.stats_sender.unbounded_send(Message::TaskTime {
            exec_name: task_data.exec,
            scenario_name: task_data.scenario,
            duration: task_data.instant.elapsed(),
            events: task_data.events,
        });
    }
}
