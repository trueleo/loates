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

#[allow(clippy::to_string_trait_impl)]
impl ToString for MetricType {
    fn to_string(&self) -> String {
        match self {
            MetricType::Counter => "counter".to_string(),
            MetricType::Gauge => "gauge".to_string(),
            MetricType::Histogram => "histogram".to_string(),
        }
    }
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
    /// histogram values ((p50, p90, p95, p99), sum)
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
                let val = f.as_nanos() as u64;
                x.observe(val as f64)
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
    inner: Mutex<(Option<TDigest>, Vec<f64>, f64)>,
}

impl Histogram {
    fn new() -> Self {
        Self {
            inner: Mutex::new((None, Vec::default(), 0.)),
        }
    }

    fn observe(&self, value: f64) {
        let mut inner = self.inner.lock().unwrap();
        inner.1.push(value);
        if inner.1.len() >= 4096 {
            let values = std::mem::take(&mut inner.1);
            if let Some(tdigest) = inner.0.as_mut() {
                tdigest.merge_unsorted(values);
            } else {
                let tdigest = TDigest::default();
                tdigest.merge_unsorted(values);
                inner.0 = Some(tdigest)
            }
        }
        inner.2 += value;
    }

    fn get_percentile(&self, u: usize, l: usize) -> f64 {
        let lock = self.inner.lock().unwrap();
        if let Some(tdigest) = &lock.0 {
            let quantile = u as f64 / l as f64;
            tdigest.estimate_quantile(quantile)
        } else {
            let index = (lock.1.len() * u) / l;
            lock.1[index]
        }
    }

    fn get_percentiles(&self) -> (f64, f64, f64, f64) {
        (
            self.get_percentile(1, 2),
            self.get_percentile(9, 10),
            self.get_percentile(95, 100),
            self.get_percentile(99, 100),
        )
    }

    fn get_sum(&self) -> f64 {
        self.inner.lock().unwrap().2
    }
}
