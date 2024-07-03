use std::time::{Duration, Instant};

use metrics::MetricType;
use ordered_float::OrderedFloat;
use tracing::span::Id;

pub(crate) mod metrics;

/// Key Value pair defining a label for a metric. For example - `("status", 200)`
pub type Attribute = (&'static str, Value);

/// A key identifying an observed metric related to this test.
///
/// They are guaranteed to be unique for each metric observed during runtime of an executor.  
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
pub struct MetricSetKey {
    /// Name of the metric
    pub name: &'static str,
    /// Type of the metric
    pub metric_type: MetricType,
    /// Attributes related to this metric
    pub attributes: Vec<Attribute>,
}

#[derive(Debug, Default)]
pub struct MetricSet {
    inner: dashmap::DashMap<MetricSetKey, metrics::Metric>,
}

impl MetricSet {
    pub fn update(&self, event: TaskEvent) {
        let metric = self.inner.get(&event.key);

        if let Some(metric) = metric {
            metric.update(event.value);
        } else {
            let v = metrics::Metric::new(event.key.metric_type, &event.value);
            v.update(event.value);
            self.inner.insert(event.key, v);
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = (MetricSetKey, metrics::MetricValue)> + '_ {
        self.inner
            .iter()
            .map(|x| (x.key().clone(), x.value().value()))
    }
}

/// Represents scalar values that are allowed to be in a user event's attribute set.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum Value {
    String(String),
    Number(i64),
    UnsignedNumber(u64),
    #[cfg_attr(feature = "serde", serde(serialize_with = "serialize_ordered_float"))]
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
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.key
            .attributes
            .push((field.name(), Value::String(format!("{:?}", value))))
    }

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

    // Captures duration in u64 range
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        match field.name() {
            "value" => self.value = Value::Duration(Duration::from_nanos(value as u64)),
            _ => self.key.attributes.push((
                field.name(),
                Value::Duration(Duration::from_nanos(value as u64)),
            )),
        }
    }
}

pub struct TaskSpanData {
    pub start_time: Instant,
    pub execution_span_id: Id,
    pub attributes: Vec<Attribute>,
}

impl tracing::field::Visit for TaskSpanData {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.attributes
            .push((field.name(), Value::String(format!("{:?}", value))));
    }

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
    // Captures duration in u64 range
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.attributes.push((
            field.name(),
            Value::Duration(Duration::from_nanos(value as u64)),
        ));
    }
}

#[cfg(feature = "serde")]
fn serialize_ordered_float<S: serde::Serializer>(
    x: &OrderedFloat<f64>,
    s: S,
) -> Result<S::Ok, S::Error> {
    serde::Serialize::serialize(&x.0, s)
}
