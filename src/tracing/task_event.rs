use std::time::{Duration, Instant};

use metrics::MetricType;
use ordered_float::OrderedFloat;

/// During the execution, any span scope/event generated within task should be trimmed down to metrics.
/// This is done to reduce the scope of the UI and enrich the user with important metrics about calls made within the user task.
///
/// The general semantics for now is as such
///
/// event!(LEVEL, "counter.name", value="", attributes*, ) is converted to following metrics
/// `name_count{attributes} sum(value)`.
///
/// Similar stratergy is applied to other metric types
///
/// For any events within a span, attributes and name of the span are added as extra label for grouping the events.
///
/// Each span inherits all the attributes of the parent and each span is tracked for its duration and tracked in a histogram.

/// Type to capture arbritary spans
pub mod metrics;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct MetricSetKey {
    name: &'static str,
    metric_type: MetricType,
    attributes: Vec<Attribute>,
}

#[derive(Debug, Default)]
pub struct MetricSet {
    inner: dashmap::DashMap<MetricSetKey, metrics::Metric>,
}

impl MetricSet {
    pub fn update(&self, event: TaskEvent) {
        let metric = self.inner.get_mut(&event.key);

        if let Some(metric) = metric {
            metric.update(event.value, event.key.metric_type);
        } else {
            let v = metrics::Metric::new(event.key.metric_type);
            self.inner.insert(event.key, v);
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = (MetricSetKey, metrics::MetricValue)> + '_ {
        self.inner
            .iter()
            .map(|x| (x.key().clone(), x.value().value()))
    }
}

/// Represents scalar values that are allowed to be in a user eventErrorVisitor's attribute set.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Value {
    String(String),
    Number(i64),
    UnsignedNumber(u64),
    Float(OrderedFloat<f64>),
    Duration(Duration),
}

impl From<Duration> for Value {
    fn from(v: Duration) -> Self {
        Self::Duration(v)
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(x) => write!(f, "{}", x),
            Value::Number(x) => write!(f, "{}", x),
            Value::UnsignedNumber(x) => write!(f, "{}", x),
            Value::Float(x) => write!(f, "{}", x),
            Value::Duration(x) => write!(f, "{:?}", x),
        }
    }
}

pub type Attribute = (&'static str, Value);

pub struct TaskEvent {
    key: MetricSetKey,
    pub value: Value,
}

impl TaskEvent {
    pub fn new(
        name: &'static str,
        metric_type: MetricType,
        attributes: Vec<Attribute>,
        value: Value,
    ) -> Self {
        Self {
            key: MetricSetKey {
                name,
                metric_type,
                attributes,
            },
            value,
        }
    }
}

impl tracing::field::Visit for TaskEvent {
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.key
            .attributes
            .push((field.name(), Value::String(value.to_string())))
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        match field.name() {
            "value" => self.value = Value::Number(value),
            _ => self
                .key
                .attributes
                .push((field.name(), Value::Number(value))),
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "value" => self.value = Value::UnsignedNumber(value),
            _ => self
                .key
                .attributes
                .push((field.name(), Value::UnsignedNumber(value))),
        }
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        match field.name() {
            "value" => self.value = Value::Float(OrderedFloat(value)),
            _ => self
                .key
                .attributes
                .push((field.name(), Value::Float(OrderedFloat(value)))),
        }
    }
}

pub struct TaskSpanData {
    pub start_time: Instant,
    pub attributes: Vec<Attribute>,
}

impl TaskSpanData {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            ..Default::default()
        }
    }
}

impl Default for TaskSpanData {
    fn default() -> Self {
        Self::new()
    }
}

impl tracing::field::Visit for TaskSpanData {
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.attributes
            .push((field.name(), Value::String(value.to_string())))
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.attributes.push((field.name(), Value::Number(value)))
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.attributes
            .push((field.name(), Value::UnsignedNumber(value)))
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.attributes
            .push((field.name(), Value::Float(OrderedFloat(value))))
    }
}
