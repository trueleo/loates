use std::{
    fmt::Write as _,
    sync::Mutex,
    time::{Duration, Instant},
};

use tracing::{field::Field, span};
use tracing_subscriber::{registry::LookupSpan, Layer};

struct TaskData {
    instant: Instant,
    exec: String,
    scenario: String,
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

    fn on_close(&self, id: span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let extention = span.extensions();
        let Some(task_data) = extention.get::<TaskData>() else {
            return;
        };
        let _ = self.stats_sender.unbounded_send(Message::TaskTime {
            exec_name: task_data.exec.to_string(),
            scenario_name: task_data.scenario.to_string(),
            duration: task_data.instant.elapsed(),
        });
    }
}
