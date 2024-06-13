use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use atomic::Atomic;
use tdigest::TDigest;

use super::Value;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

impl FromStr for MetricType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "counter" => Ok(Self::Counter),
            "gauge" => Ok(Self::Gauge),
            "histogram" => Ok(Self::Histogram),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(((f64, f64, f64, f64), f64)),
}

#[derive(Debug)]
pub(crate) enum Metric {
    Counter(Counter),
    Gauge(Gauge),
    Histogram(Histogram),
}

impl Metric {
    pub fn new(ty: MetricType) -> Self {
        match ty {
            MetricType::Counter => Self::Counter(Counter::new()),
            MetricType::Gauge => Self::Gauge(Gauge::new()),
            MetricType::Histogram => Self::Histogram(Histogram::new()),
        }
    }

    pub(crate) fn update(&self, value: Value, metric_type: MetricType) {
        match (self, metric_type, value) {
            (Metric::Counter(x), MetricType::Counter, Value::UnsignedNumber(val)) => x.add(val),
            (Metric::Gauge(x), MetricType::Gauge, Value::Float(f)) => x.set(f.0),
            (Metric::Histogram(x), MetricType::Histogram, Value::Duration(f)) => {
                x.observe(f.as_secs_f64())
            }
            _ => {}
        }
    }

    pub fn value(&self) -> MetricValue {
        match self {
            Metric::Counter(x) => MetricValue::Counter(x.get()),
            Metric::Gauge(x) => MetricValue::Gauge(x.get()),
            Metric::Histogram(x) => MetricValue::Histogram((x.get_percentiles(), x.get_sum())),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Counter {
    pub(crate) value: AtomicU64,
}

impl Counter {
    pub(crate) fn new() -> Self {
        Counter {
            value: AtomicU64::new(0),
        }
    }

    pub(crate) fn add(&self, amount: u64) {
        self.value.fetch_add(amount, Ordering::Relaxed);
    }

    pub(crate) fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(crate) struct Gauge {
    pub(crate) value: Atomic<f64>,
}

impl Gauge {
    pub(crate) fn new() -> Self {
        Gauge {
            value: Atomic::new(0.),
        }
    }

    pub(crate) fn set(&self, value: f64) {
        self.value.swap(value, Ordering::Relaxed);
    }

    pub(crate) fn get(&self) -> f64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(crate) struct Histogram {
    inner: Mutex<(TDigest, Vec<f64>, f64)>,
}

impl Histogram {
    fn new() -> Self {
        Self {
            inner: Mutex::new((TDigest::new_with_size(100), Vec::default(), 0.)),
        }
    }

    fn observe(&self, value: f64) {
        let mut inner = self.inner.lock().unwrap();
        inner.1.push(value);
        if inner.1.len() >= 4096 {
            let values = std::mem::take(&mut inner.1);
            inner.0.merge_unsorted(values);
        }
        inner.2 += value;
    }

    fn get_percentile(&self, percentile: f64) -> f64 {
        self.inner
            .lock()
            .unwrap()
            .0
            .estimate_quantile(percentile / 100.0)
    }

    fn get_percentiles(&self) -> (f64, f64, f64, f64) {
        (
            self.get_percentile(50.0),
            self.get_percentile(90.0),
            self.get_percentile(95.0),
            self.get_percentile(99.0),
        )
    }

    fn get_sum(&self) -> f64 {
        self.inner.lock().unwrap().2
    }
}
