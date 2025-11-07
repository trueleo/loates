use chrono::{DateTime, Utc};
use ordered_float::OrderedFloat;
use std::{sync::Arc, time::Duration};
use ulid::Ulid;

use crate::metrics::{Attribute, MetricSetKey, MetricType, Value};

#[derive(Debug)]
pub struct TaskSpanRecord {
    pub run_id: Ulid,
    pub scenario_id: Arc<str>,
    pub execution_id: usize,
    pub start_time: DateTime<Utc>,
    pub attributes: Vec<Attribute>,
}

impl tracing::field::Visit for TaskSpanRecord {
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

pub struct TaskEvent {
    pub key: MetricSetKey,
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
