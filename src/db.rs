use chrono::{DateTime, Utc};
use duckdb::{
    params,
    types::{OrderedMap, Value},
};
use std::sync::Arc;
use ulid::Ulid;

use crate::metrics;

const CREATE_TYPE_METRIC_TYPE: &str = r#"
CREATE TYPE METRIC_TYPE AS ENUM ('gauge', 'counter', 'histogram')
"#;

const CREATE_RAW_METRIC_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS metrics_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP_TZ NOT NULL,
    run_id UUID NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    metric_type METRIC_TYPE NOT NULL,
    metric_attributes MAP(TEXT, TEXT) NOT NULL,
    metric_value UNION(f64 DOUBLE, i64 BIGINT, u64 UBIGINT, duration UBIGINT) NOT NULL
)
"#;

const CREATE_COUNTER_METRICS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS metrics_counter (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP_TZ NOT NULL,
    run_id UUID NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    metric_attributes MAP(TEXT, TEXT) NOT NULL,
    metric_value UBIGINT NOT NULL
)
"#;

const CREATE_GAUGE_METRICS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS metrics_gauge (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP_TZ NOT NULL,
    run_id UUID NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    metric_attributes MAP(TEXT, TEXT) NOT NULL,
    metric_value UNION(f64 DOUBLE, i64 BIGINT, u64 UBIGINT, duration UBIGINT) NOT NULL
)
"#;

const CREATE_HISTOGRAM_METRICS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS metrics_histogram (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP_TZ NOT NULL,
    run_id UUID NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    metric_attributes MAP(TEXT, TEXT) NOT NULL,
    metric_value JSON NOT NULL
)
"#;

const CREATE_EXECUTOR_UPDATES_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS executor_updates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP_TZ NOT NULL,
    run_id UUID NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER NOT NULL,
    users BIGINT NOT NULL,
    iterations BIGINT NOT NULL,
    stage INTEGER NOT NULL,
    stage_start_time TIMESTAMP_TZ NOT NULL
)
"#;

const CREATE_MESSAGES_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP_TZ NOT NULL,
    run_id UUID NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER NOT NULL,
    message_type TEXT NOT NULL,
    payload JSON NOT NULL
)
"#;

const INSERT_METRIC: &str = r#"
INSERT INTO metrics_raw (timestamp, run_id, scenario_name, executor_id, metric_name, metric_type, metric_attributes, metric_value)
VALUES (?, ?, ?, ?, ?, ?, ?, union_value(? := ?))
"#;

const INSERT_EXECUTOR_UPDATE: &str = r#"
INSERT INTO executor_updates (timestamp, run_id, scenario_name, executor_id, users, iterations, stage, stage_start_time)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"#;

const INSERT_MESSAGE: &str = r#"
INSERT INTO messages (timestamp, run_id, scenario_name, executor_id, message_type, payload)
VALUES (?, ?, ?, ?, ?, ?)
"#;

pub struct DatabaseConn {
    pub db_conn: duckdb::Connection,
}

impl DatabaseConn {
    pub fn new(db_conn: duckdb::Connection) -> Self {
        Self { db_conn }
    }

    pub fn create_all_tables(&self) -> Result<(), anyhow::Error> {
        self.db_conn.execute(CREATE_RAW_METRIC_TABLE, [])?;
        self.db_conn.execute(CREATE_COUNTER_METRICS_TABLE, [])?;
        self.db_conn.execute(CREATE_GAUGE_METRICS_TABLE, [])?;
        self.db_conn.execute(CREATE_HISTOGRAM_METRICS_TABLE, [])?;
        self.db_conn.execute(CREATE_EXECUTOR_UPDATES_TABLE, [])?;
        self.db_conn.execute(CREATE_MESSAGES_TABLE, [])?;
        Ok(())
    }

    pub fn write_message_type(
        &self,
        message: &crate::tracing::Message,
    ) -> Result<(), anyhow::Error> {
        match message {
            crate::tracing::Message::Metric {
                timestamp,
                run_id,
                scenario_name,
                executor_id,
                metric_set_key,
                metric_value,
            } => self.write_metric_message(
                timestamp,
                run_id,
                scenario_name,
                executor_id,
                metric_set_key,
                metric_value,
            ),

            crate::tracing::Message::ExecutorUpdate {
                timestamp,
                run_id,
                scenario_name,
                executor_id,
                users,
                iterations,
                stage,
                stage_start_time,
            } => self.write_executor_update_message(
                timestamp,
                run_id,
                scenario_name,
                executor_id,
                users,
                iterations,
                stage,
                stage_start_time,
            ),
            other => self.write_generic_message(other),
        }
    }

    fn write_metric_message(
        &self,
        timestamp: &DateTime<Utc>,
        run_id: &Ulid,
        scenario_name: &Arc<str>,
        executor_id: &usize,
        metric_set_key: &metrics::MetricSetKey,
        metric_value: &metrics::Value,
    ) -> Result<(), anyhow::Error> {
        // just for ownership
        let mut _own_duration = 0;
        let (metric_union_tag, metric_value): (&str, &dyn duckdb::ToSql) = match metric_value {
            metrics::Value::Number(x) => ("i64", x),
            metrics::Value::UnsignedNumber(x) => ("u64", x),
            metrics::Value::Float(ordered_float) => ("f64", ordered_float.as_ref()),
            metrics::Value::Duration(duration) => {
                _own_duration = duration.as_nanos() as u64;
                ("duration", &_own_duration)
            }
            metrics::Value::String(_) => unreachable!(),
        };

        let attributes: OrderedMap<Value, Value> = metric_set_key
            .attributes
            .iter()
            .map(|x| (Value::Text(x.0.to_string()), Value::Text(x.1.to_string())))
            .collect::<Vec<_>>()
            .into();

        self.db_conn.execute(
            INSERT_METRIC,
            params![
                timestamp,
                run_id.to_string(),
                scenario_name.as_ref(),
                *executor_id as i64,
                metric_set_key.name,
                metric_set_key.metric_type.to_string(),
                duckdb::types::Value::Map(attributes),
                metric_union_tag,
                metric_value,
            ],
        )?;
        Ok(())
    }

    fn write_executor_update_message(
        &self,
        timestamp: &DateTime<Utc>,
        run_id: &Ulid,
        scenario_name: &Arc<str>,
        executor_id: &usize,
        users: &u64,
        iterations: &u64,
        stage: &usize,
        stage_start_time: &DateTime<Utc>,
    ) -> Result<(), anyhow::Error> {
        self.db_conn.execute(
            INSERT_EXECUTOR_UPDATE,
            params![
                timestamp,
                run_id.to_string(),
                scenario_name.as_ref(),
                *executor_id as i64,
                *users as i64,
                *iterations as i64,
                *stage as i64,
                stage_start_time.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    fn write_generic_message(
        &self,
        message: &crate::tracing::Message,
    ) -> Result<(), anyhow::Error> {
        let timestamp = message.timestamp();
        let message_type = Self::extract_message_type(message);
        let payload = serde_json::to_string(message)
            .map_err(|e| anyhow::anyhow!("Failed to serialize message: {}", e))?;

        let (run_id, scenario_name) = Self::extract_common_fields(message);

        self.db_conn.execute(
            INSERT_MESSAGE,
            params![timestamp, run_id, scenario_name, message_type, payload],
        )?;
        Ok(())
    }

    /// Extract a readable message type name
    fn extract_message_type(message: &crate::tracing::Message) -> String {
        match message {
            crate::tracing::Message::Metric { .. } => "Metric",
            crate::tracing::Message::ExecutorUpdate { .. } => "ExecutorUpdate",
            crate::tracing::Message::Error { .. } => "Error",
            crate::tracing::Message::TerminatedError { .. } => "TerminatedError",
            crate::tracing::Message::ScenarioStarted { .. } => "ScenarioStarted",
            crate::tracing::Message::ScenarioEnded { .. } => "ScenarioEnded",
            crate::tracing::Message::ExecutorStart { .. } => "ExecutorStart",
            crate::tracing::Message::ExecutorEnd { .. } => "ExecutorEnd",
        }
        .to_string()
    }

    /// Extract shared fields for optional storage in messages table
    fn extract_common_fields(
        message: &crate::tracing::Message,
    ) -> (Option<String>, Option<String>) {
        match message {
            crate::tracing::Message::ScenarioStarted {
                run_id,
                scenario_name,
                ..
            }
            | crate::tracing::Message::ScenarioEnded {
                run_id,
                scenario_name,
                ..
            }
            | crate::tracing::Message::ExecutorStart {
                run_id,
                scenario_name,
                ..
            }
            | crate::tracing::Message::ExecutorEnd {
                run_id,
                scenario_name,
                ..
            } => (Some(run_id.to_string()), Some(scenario_name.to_string())),
            _ => (None, None),
        }
    }

    pub fn try_clone(&self) -> Result<Self, anyhow::Error> {
        Ok(Self {
            db_conn: self.db_conn.try_clone()?,
        })
    }
}
