pub(crate) mod message;
pub(crate) mod task_event;

use crate::{
    metrics::{MetricSetKey, MetricType, Value},
    tracing::task_event::TaskEvent,
};
pub use message::Message;
use ulid::Ulid;

use std::{str::FromStr, sync::Arc};

use chrono::{DateTime, TimeZone as _, Utc};
use tracing::{
    field::{Field, Visit},
    span::{self},
    Subscriber,
};

use tracing_subscriber::{
    registry::{LookupSpan, SpanRef},
    Layer,
};

use crate::{
    tracing::task_event::TaskSpanRecord, CRATE_NAME, SPAN_EXEC, SPAN_SCENARIO, SPAN_TASK, USER_TASK,
};

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

#[derive(Debug, Clone)]
struct ScenarioSpanRecord {
    run_id: Ulid,
    name: Arc<str>,
}

impl Default for ScenarioSpanRecord {
    fn default() -> Self {
        Self {
            run_id: Ulid(0),
            name: Arc::from(""),
        }
    }
}

impl tracing::field::Visit for ScenarioSpanRecord {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "name" {
            self.name = Arc::from(value);
        }
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        if field.name() == "run_id" {
            self.run_id = Ulid(value)
        }
    }
}

#[derive(Debug)]
pub struct ExecutorSpanRecord {
    run_id: Ulid,
    scenario_name: Arc<str>,
    id: usize,
}

impl ExecutorSpanRecord {
    pub fn new(run_id: Ulid, scenario_name: Arc<str>) -> Self {
        Self {
            run_id,
            scenario_name,
            id: 0,
        }
    }
}

impl tracing::field::Visit for ExecutorSpanRecord {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "id" {
            self.id = value as usize;
        }
    }
}

#[derive(Debug)]
pub struct ExecutorEventRecord {
    users: u64,
    stage: usize,
    stage_start_time: DateTime<Utc>,
}

impl Default for ExecutorEventRecord {
    fn default() -> Self {
        Self {
            users: 0,
            stage: 0,
            stage_start_time: DateTime::<Utc>::MIN_UTC,
        }
    }
}

impl tracing::field::Visit for ExecutorEventRecord {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "users" => self.users = value,
            "stage" => self.stage = value as usize,
            "stage_start_time" => {
                self.stage_start_time = Utc.timestamp_millis_opt(value as i64).unwrap()
            }
            _ => (),
        }
    }
}

/// Trait for abstracting over sender type of a channel.
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

/// [Tracing subscriber layer](Layer) that tracks and generates [`Message`]s based on tracing events generated during a test.
pub struct TracerLayer<T: Sender> {
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
            create_task_span(&span, attr);
            return;
        }

        if span.metadata().target() != CRATE_NAME {
            return;
        };

        match span.name() {
            SPAN_EXEC => {
                let message = create_exec_span(attr, &span);
                self.stats_sender.send(message);
            }
            SPAN_SCENARIO => {
                let message = create_scenario_span(attr, span);
                self.stats_sender.send(message);
            }
            _ => (),
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == USER_TASK {
            if let Some(message) = handle_user_event(event, &ctx) {
                self.stats_sender.send(message);
            }
            return;
        }
        if event.metadata().target() == CRATE_NAME {
            match event.metadata().name() {
                "termination_error" => {
                    let mut err = ErrorVisitor::default();
                    event.record(&mut err);
                    self.stats_sender.send(Message::TerminatedError {
                        timestamp: Utc::now(),
                        err: err.err,
                    });
                }
                "error" => {
                    let mut err = ErrorVisitor::default();
                    event.record(&mut err);
                    self.stats_sender.send(Message::Error {
                        timestamp: Utc::now(),
                        err: err.err,
                    });
                }
                SPAN_EXEC => {
                    let message = handle_crate_execution_event(event, &ctx);
                    self.stats_sender.send(message);
                }
                _ => {}
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
        let Some(span_data) = span_data.get_mut::<TaskSpanRecord>() else {
            return;
        };
        values.record(span_data);
    }

    fn on_close(&self, id: span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };

        if span.metadata().name() == SPAN_SCENARIO {
            let message = close_scenario_span(span);
            self.stats_sender.send(message);
            return;
        }

        if span.metadata().name() == SPAN_EXEC {
            let message = close_execution_span(span);
            self.stats_sender.send(message);
            return;
        }

        if span.metadata().name() == SPAN_TASK {
            let message = close_task_span(span);
            self.stats_sender.send(message);
            return;
        }

        if span.metadata().target() == USER_TASK {
            let message = close_task_span(span);
            self.stats_sender.send(message);
        }
    }
}

fn create_scenario_span<S: for<'a> LookupSpan<'a>>(
    attributes: &span::Attributes,
    span: SpanRef<S>,
) -> Message {
    let start_time = Utc::now();
    let mut scenario_span_record = ScenarioSpanRecord::default();
    attributes.values().record(&mut scenario_span_record);
    let mut extentions = span.extensions_mut();
    let message = Message::ScenarioStarted {
        timestamp: Utc::now(),
        run_id: scenario_span_record.run_id,
        start_time,
        scenario_name: scenario_span_record.name.clone(),
    };
    extentions.insert(scenario_span_record);
    message
}

fn create_exec_span<'a, S: LookupSpan<'a>>(
    attr: &span::Attributes,
    span: &SpanRef<'a, S>,
) -> Message {
    let start_time = Utc::now();
    let scenario_span = span.parent().unwrap();
    let ScenarioSpanRecord {
        run_id,
        name: scenario_name,
        ..
    } = scenario_span
        .extensions()
        .get::<ScenarioSpanRecord>()
        .cloned()
        .unwrap();

    let mut executor_span_record = ExecutorSpanRecord::new(run_id, scenario_name.clone());
    attr.values().record(&mut executor_span_record);
    let mut extentions = span.extensions_mut();
    let message = Message::ExecutorStart {
        timestamp: Utc::now(),
        run_id,
        scenario_name,
        start_time,
        id: executor_span_record.id,
    };
    extentions.insert(executor_span_record);
    message
}

fn create_task_span<'a, S: LookupSpan<'a>>(span: &SpanRef<'a, S>, attr: &span::Attributes) {
    let start_time = Utc::now();

    let exec_span = span
        .scope()
        .find(|span| span.metadata().name() == SPAN_EXEC)
        .expect("task span is always inside a exec span");

    let parent_attributes = span
        .scope()
        .take_while(|s| s.metadata().name() != SPAN_EXEC)
        .filter(|s| s.metadata().target() == USER_TASK)
        .map(|span| {
            span.extensions()
                .get::<TaskSpanRecord>()
                .unwrap()
                .attributes
                .clone()
        })
        .next()
        .unwrap_or_default();

    let mut task_span_record = {
        let extensions = exec_span.extensions();
        let execution_span_record = extensions
            .get::<ExecutorSpanRecord>()
            .expect("task parent is exec");
        TaskSpanRecord {
            run_id: execution_span_record.run_id,
            scenario_id: execution_span_record.scenario_name.clone(),
            execution_id: execution_span_record.id,
            start_time,
            attributes: parent_attributes,
        }
    };
    attr.record(&mut task_span_record);
    span.extensions_mut().insert(task_span_record);
}

fn handle_user_event<S: Subscriber + for<'a> LookupSpan<'a>>(
    event: &tracing::Event,
    ctx: &tracing_subscriber::layer::Context<S>,
) -> Option<Message> {
    let parent_span = ctx
        .event_scope(event)
        .unwrap()
        .find(|x| x.metadata().target() == USER_TASK)?;

    let extensions = parent_span.extensions();
    let parent_span_record = extensions.get::<TaskSpanRecord>().unwrap();
    let attributes: Vec<_> = parent_span_record.attributes.clone();
    let (ty_str, name) = event.metadata().name().split_once('.')?;
    let metric_type = MetricType::from_str(ty_str).ok()?;

    let mut task_event = TaskEvent::new(
        name,
        metric_type,
        attributes,
        crate::metrics::Value::Number(0),
    );
    event.record(&mut task_event);

    Some(Message::Metric {
        timestamp: Utc::now(),
        run_id: parent_span_record.run_id,
        scenario_name: parent_span_record.scenario_id.clone(),
        executor_id: parent_span_record.execution_id,
        metric_set_key: task_event.key,
        metric_value: task_event.value,
    })
}

fn handle_crate_execution_event<S: Subscriber + for<'a> LookupSpan<'a>>(
    event: &tracing::Event,
    ctx: &tracing_subscriber::layer::Context<S>,
) -> Message {
    let parent = ctx
        .current_span()
        .id()
        .and_then(|id| ctx.span(id))
        .expect("event should be immediate child of execution span ");
    let mut exec_ext = parent.extensions_mut();
    let executor_span_record = exec_ext.get_mut::<ExecutorSpanRecord>().unwrap();
    let mut event_record = ExecutorEventRecord::default();
    event.record(&mut event_record);
    Message::ExecutorUpdate {
        timestamp: Utc::now(),
        run_id: executor_span_record.run_id,
        scenario_name: executor_span_record.scenario_name.clone(),
        executor_id: executor_span_record.id,
        users: event_record.users,
        stage: event_record.stage,
        stage_start_time: event_record.stage_start_time,
    }
}

fn close_scenario_span<S: Subscriber + for<'a> LookupSpan<'a>>(span: SpanRef<S>) -> Message {
    let end_time = Utc::now();
    let scenario = span.extensions();
    let scenario = scenario.get::<ScenarioSpanRecord>().unwrap();
    Message::ScenarioEnded {
        timestamp: Utc::now(),
        run_id: scenario.run_id,
        scenario_name: scenario.name.clone(),
        end_time,
    }
}

fn close_execution_span<S: Subscriber + for<'a> LookupSpan<'a>>(span: SpanRef<S>) -> Message {
    let end_time = Utc::now();
    let extensions = span.extensions();
    let exec_span_record = extensions.get::<ExecutorSpanRecord>().unwrap();
    Message::ExecutorEnd {
        timestamp: Utc::now(),
        run_id: exec_span_record.run_id,
        scenario_name: exec_span_record.scenario_name.clone(),
        id: exec_span_record.id,
        end_time,
    }
}

fn close_task_span<'a, S: Subscriber + for<'lookup> LookupSpan<'lookup>>(
    span: SpanRef<'a, S>,
) -> Message {
    let mut extention = span.extensions_mut();
    let task_span_record = extention.remove::<TaskSpanRecord>().unwrap();
    let time_delta = Utc::now() - task_span_record.start_time;
    let time_delta = time_delta.abs().to_std().unwrap();
    Message::Metric {
        timestamp: Utc::now(),
        run_id: task_span_record.run_id,
        scenario_name: task_span_record.scenario_id.clone(),
        executor_id: task_span_record.execution_id,
        metric_set_key: MetricSetKey {
            name: "task",
            metric_type: MetricType::Gauge,
            attributes: task_span_record.attributes,
        },
        metric_value: Value::Duration(time_delta),
    }
}
