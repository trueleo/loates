use chrono::{DateTime, Timelike, Utc};
use duckdb::{
    params,
    types::{TimeUnit, Value},
};
use serde_json::json;
use std::sync::Arc;
use ulid::Ulid;

use crate::metrics;

// Install datasketches extension, for approximate percentiles.
const INSTALL_DATASKETCHES: &str = r#"
INSTALL datasketches FROM community;
LOAD datasketches;
"#;

const CREATE_TYPE_METRIC_TYPE: &str = r#"
CREATE TYPE IF NOT EXISTS METRIC_TYPE AS ENUM ('gauge', 'counter', 'histogram')
"#;

// All metric points first land here
// How union types are mapped to metric values
// COUNTER - u64
// GAUGE - f64, i64, u64, duration
// HISTOGRAM - f64, duration
const CREATE_RAW_METRIC_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS metrics_raw (
    timestamp TIMESTAMPTZ NOT NULL,
    run_id UHUGEINT NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    metric_type METRIC_TYPE NOT NULL,
    metric_attributes MAP(TEXT, TEXT) NOT NULL,
    metric_value UNION(f64 DOUBLE, i64 BIGINT, u64 UBIGINT, duration UBIGINT) NOT NULL
)
"#;

// All counter metric points are aggregated here, granularity is 1 second
// Datapoints between two timestamp points are aggregated inside the later timestamp.
// metric_value is the sum of all values up to the current timestamp
const CREATE_COUNTER_METRICS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS metrics_counter (
    timestamp TIMESTAMPTZ NOT NULL,
    run_id UHUGEINT NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    metric_attributes MAP(TEXT, TEXT) NOT NULL,
    metric_value UBIGINT NOT NULL
)
"#;

// All gauge metric points are aggregated here, granularity is 1 second
// Datapoints between two timestamp points are aggregated inside the later timestamp.
// metric_value is the maximum value recorded in the 1 second window of the timestamp
const CREATE_GAUGE_METRICS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS metrics_gauge (
    timestamp TIMESTAMPTZ NOT NULL,
    run_id UHUGEINT NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    metric_attributes MAP(TEXT, TEXT) NOT NULL,
    metric_value UNION(f64 DOUBLE, i64 BIGINT, u64 UBIGINT, duration UBIGINT) NOT NULL
)
"#;

// All histogram metric points are aggregated here, granularity is 1 second
// Datapoints between two timestamp points are aggregated inside the later timestamp.
// metric_value is json encoded value, the schema for this json depends on the datasketch t-digest output
const CREATE_HISTOGRAM_METRICS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS metrics_histogram (
    timestamp TIMESTAMPTZ NOT NULL,
    run_id UHUGEINT NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    metric_attributes MAP(TEXT, TEXT) NOT NULL,
    metric_tag TEXT NOT NULL,
    metric_value sketch_tdigest_double NOT NULL,
)
"#;

const CREATE_EXECUTOR_UPDATES_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS executor_updates (
    timestamp TIMESTAMPTZ NOT NULL,
    run_id UHUGEINT NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER NOT NULL,
    users BIGINT NOT NULL,
    stage INTEGER NOT NULL,
    stage_start_time TIMESTAMPTZ NOT NULL
)
"#;

// Other app level message will land here
const CREATE_MESSAGES_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS messages (
    timestamp TIMESTAMPTZ NOT NULL,
    run_id UHUGEINT NOT NULL,
    scenario_name TEXT NOT NULL,
    executor_id INTEGER,
    message_type TEXT NOT NULL,
    payload JSON NOT NULL
)
"#;

// CAN'T FIX: Map type and Union types are unsupported in duckdb rs. We cast to the type from json as this is supported
// the eventual fix should be to let rust client insert into them directly via types that implement their Sql Value trait
const INSERT_METRIC: &str = r#"
INSERT INTO metrics_raw (timestamp, run_id, scenario_name, executor_id, metric_name, metric_type, metric_attributes, metric_value)
VALUES (?, ?, ?, ?, ?, ?, ?::JSON::MAP(TEXT, TEXT), ?::JSON::UNION(f64 DOUBLE, i64 BIGINT, u64 UBIGINT, duration UBIGINT))
"#;

const INSERT_EXECUTOR_UPDATE: &str = r#"
INSERT INTO executor_updates (timestamp, run_id, scenario_name, executor_id, users, stage, stage_start_time)
VALUES (?, ?, ?, ?, ?, ?, ?)
"#;

const INSERT_MESSAGE: &str = r#"
INSERT INTO messages (timestamp, run_id, scenario_name, executor_id, message_type, payload)
VALUES (?, ?, ?, ?, ?, ?)
"#;

// Sync updates from metric raw table into counter table. This is not a true streaming query.
// Delta update pattern from https://duckdb.org/2025/10/13/duckdb-streaming-patterns
pub(crate) const MERGE_METRIC_COUNTER: &str = r#"
MERGE INTO metrics_counter AS target
USING (
    WITH last_ts AS ( SELECT COALESCE(MAX(timestamp), TIMESTAMPTZ '1970-01-01') AS last_update FROM metrics_counter ),
    delta_raw AS ( SELECT r.* FROM metrics_raw r CROSS JOIN last_ts WHERE r.metric_type = 'counter' AND r.timestamp >= last_update ),
    SELECT
        date_trunc('second', timestamp) AS timestamp,
        run_id, scenario_name, executor_id, metric_name, metric_attributes,
        SUM(metric_value.u64) AS metric_value
    FROM delta_raw
    GROUP BY
        date_trunc('second', timestamp),
        run_id, scenario_name, executor_id, metric_name, metric_attributes
) AS src
ON target.timestamp          = src.timestamp
   AND target.run_id         = src.run_id
   AND target.scenario_name  = src.scenario_name
   AND target.executor_id    = src.executor_id
   AND target.metric_name    = src.metric_name
   AND target.metric_attributes = src.metric_attributes
WHEN MATCHED THEN
    UPDATE SET metric_value = src.metric_value
WHEN NOT MATCHED THEN
    INSERT ( timestamp, run_id, scenario_name, executor_id, metric_name, metric_attributes, metric_value )
    VALUES ( src.timestamp, src.run_id, src.scenario_name, src.executor_id, src.metric_name, src.metric_attributes, src.metric_value );
"#;

pub(crate) const MERGE_METRIC_GAUGE: &str = r#"
MERGE INTO metrics_gauge AS target
USING (
    WITH last_ts AS (
        SELECT COALESCE(MAX(timestamp), TIMESTAMPTZ '1970-01-01') AS last_update
        FROM metrics_gauge
    ),
    delta_raw AS (
        SELECT r.*
        FROM metrics_raw r
        CROSS JOIN last_ts
        WHERE r.metric_type = 'gauge'
          AND r.timestamp >= last_update
    ),
    agg AS (
        SELECT
            date_trunc('second', timestamp) AS timestamp,
            run_id,
            scenario_name,
            executor_id,
            metric_name,
            metric_attributes,
            MAX(metric_value.f64) AS max_f64,
            MAX(metric_value.i64) AS max_i64,
            MAX(metric_value.u64) AS max_u64,
            MAX(metric_value.duration) AS max_duration
        FROM delta_raw
        GROUP BY
            date_trunc('second', timestamp),
            run_id,
            scenario_name,
            executor_id,
            metric_name,
            metric_attributes
    )
    SELECT
        timestamp, run_id, scenario_name, executor_id, metric_name, metric_attributes,
        union_value(f64 := max_f64)::UNION(f64 DOUBLE, i64 BIGINT, u64 UBIGINT, duration UBIGINT) AS metric_value
    FROM agg
    WHERE max_f64 IS NOT NULL
    UNION ALL
    SELECT
        timestamp, run_id, scenario_name, executor_id, metric_name, metric_attributes,
        union_value(i64 := max_i64)::UNION(f64 DOUBLE, i64 BIGINT, u64 UBIGINT, duration UBIGINT) AS metric_value
    FROM agg
    WHERE max_i64 IS NOT NULL
    UNION ALL
    SELECT
        timestamp, run_id, scenario_name, executor_id, metric_name, metric_attributes,
        union_value(u64 := max_u64)::UNION(f64 DOUBLE, i64 BIGINT, u64 UBIGINT, duration UBIGINT) AS metric_value
    FROM agg
    WHERE max_u64 IS NOT NULL
    UNION ALL
    SELECT
        timestamp, run_id, scenario_name, executor_id, metric_name, metric_attributes,
        union_value(duration := max_duration)::UNION(f64 DOUBLE, i64 BIGINT, u64 UBIGINT, duration UBIGINT) AS metric_value
    FROM agg
    WHERE max_duration IS NOT NULL
) AS src
ON target.timestamp          = src.timestamp
   AND target.run_id         = src.run_id
   AND target.scenario_name  = src.scenario_name
   AND target.executor_id    = src.executor_id
   AND target.metric_name    = src.metric_name
   AND target.metric_attributes = src.metric_attributes
WHEN MATCHED THEN
    UPDATE SET metric_value = src.metric_value
WHEN NOT MATCHED THEN
    INSERT (
        timestamp,
        run_id,
        scenario_name,
        executor_id,
        metric_name,
        metric_attributes,
        metric_value
    )
    VALUES (
        src.timestamp,
        src.run_id,
        src.scenario_name,
        src.executor_id,
        src.metric_name,
        src.metric_attributes,
        src.metric_value
    );
"#;

pub(crate) const MERGE_METRIC_HISTOGRAM: &str = r#"
MERGE INTO metrics_histogram AS target
USING (
    WITH last_ts AS ( SELECT COALESCE(MAX(timestamp), TIMESTAMPTZ '1970-01-01') AS last_update FROM metrics_histogram ),
    delta_raw AS ( SELECT r.* FROM metrics_raw r CROSS JOIN last_ts WHERE r.metric_type = 'histogram' AND r.timestamp >= last_update ),
    agg as (
        SELECT
            date_trunc('second', timestamp) AS timestamp,
            run_id, scenario_name, executor_id, metric_name, metric_attributes,
            datasketch_tdigest(
                10,
                metric_value.f64
            ) AS digest_f64,
            datasketch_tdigest(
                10,
                metric_value.duration::DOUBLE
            ) AS digest_duration
        FROM delta_raw
        GROUP BY
            date_trunc('second', timestamp),
            run_id, scenario_name, executor_id, metric_name, metric_attributes
    )
    SELECT
        timestamp, run_id, scenario_name, executor_id, metric_name, metric_attributes, 'f64' as metric_tag,
        digest_f64 AS metric_value
    FROM agg
    WHERE digest_f64 IS NOT NULL
    UNION ALL
    SELECT
        timestamp, run_id, scenario_name, executor_id, metric_name, metric_attributes, 'duration' as metric_tag,
        digest_duration AS metric_value
    FROM agg
    WHERE digest_duration IS NOT NULL
) AS src
ON target.timestamp          = src.timestamp
   AND target.run_id         = src.run_id
   AND target.scenario_name  = src.scenario_name
   AND target.executor_id    = src.executor_id
   AND target.metric_name    = src.metric_name
   AND target.metric_attributes = src.metric_attributes
WHEN MATCHED THEN
    UPDATE SET metric_value = src.metric_value
WHEN NOT MATCHED THEN
    INSERT ( timestamp, run_id, scenario_name, executor_id, metric_name, metric_attributes, metric_tag, metric_value )
    VALUES ( src.timestamp, src.run_id, src.scenario_name, src.executor_id, src.metric_name, src.metric_attributes, src.metric_tag, src.metric_value );
"#;

pub struct DatabaseConn {
    pub db_conn: duckdb::Connection,
}

impl DatabaseConn {
    pub fn new(db_conn: duckdb::Connection) -> Self {
        Self { db_conn }
    }

    pub fn create_all_tables(&self) -> Result<(), anyhow::Error> {
        self.db_conn.execute(INSTALL_DATASKETCHES, [])?;
        self.db_conn.execute(CREATE_TYPE_METRIC_TYPE, [])?;
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
                stage,
                stage_start_time,
            } => self.write_executor_update_message(
                timestamp,
                run_id,
                scenario_name,
                executor_id,
                users,
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
        let metric_value: String = match metric_value {
            metrics::Value::Number(x) => json!({"i64": x}).to_string(),
            metrics::Value::UnsignedNumber(x) => json!({"u64": *x}).to_string(),
            metrics::Value::Float(ordered_float) => json!({"f64": ordered_float.0}).to_string(),
            metrics::Value::Duration(duration) => {
                _own_duration = duration.as_nanos() as u64;
                json!({"duration": _own_duration}).to_string()
            }
            metrics::Value::String(_) => unreachable!(),
        };

        let mut attributes = metric_set_key
            .attributes
            .iter()
            .fold(String::from("{"), |json, (name, value)| {
                json + &format!("\"{}\": \"{}\", ", name, value)
            });
        attributes = attributes.trim_end_matches(", ").to_string() + "}";

        self.db_conn.execute(
            INSERT_METRIC,
            [
                Value::Timestamp(TimeUnit::Nanosecond, timestamp.nanosecond() as i64),
                Value::HugeInt(run_id.0 as i128),
                Value::Text(scenario_name.to_string()),
                Value::Int(*executor_id as i32),
                Value::Text(metric_set_key.name.to_string()),
                Value::Text(metric_set_key.metric_type.to_string()),
                Value::Text(attributes),
                Value::Text(metric_value),
            ],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn write_executor_update_message(
        &self,
        timestamp: &DateTime<Utc>,
        run_id: &Ulid,
        scenario_name: &Arc<str>,
        executor_id: &usize,
        users: &u64,
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

        let (run_id, scenario_name, executor_id) = Self::extract_common_fields(message);

        self.db_conn.execute(
            INSERT_MESSAGE,
            [
                Value::Timestamp(TimeUnit::Nanosecond, timestamp.nanosecond() as i64),
                Value::HugeInt(run_id.map(|x| x.0 as i128).unwrap()),
                Value::Text(scenario_name.unwrap()),
                executor_id
                    .map(|x| Value::Int(x as i32))
                    .unwrap_or(Value::Null),
                Value::Text(message_type),
                Value::Text(payload),
            ],
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
    ) -> (Option<Ulid>, Option<String>, Option<usize>) {
        use crate::tracing::Message;

        let run_id = match message {
            Message::Metric { run_id, .. } => Some(*run_id),
            Message::ExecutorUpdate { run_id, .. } => Some(*run_id),
            Message::ScenarioStarted { run_id, .. } => Some(*run_id),
            Message::ScenarioEnded { run_id, .. } => Some(*run_id),
            Message::ExecutorStart { run_id, .. } => Some(*run_id),
            Message::ExecutorEnd { run_id, .. } => Some(*run_id),
            _ => None,
        };

        let scenario_name = match message {
            Message::ScenarioStarted { scenario_name, .. } => Some(scenario_name.to_string()),
            Message::ScenarioEnded { scenario_name, .. } => Some(scenario_name.to_string()),
            Message::ExecutorStart { scenario_name, .. } => Some(scenario_name.to_string()),
            Message::ExecutorEnd { scenario_name, .. } => Some(scenario_name.to_string()),
            Message::Metric { scenario_name, .. } => Some(scenario_name.to_string()),
            Message::ExecutorUpdate { scenario_name, .. } => Some(scenario_name.to_string()),
            _ => None,
        };

        let executor_id = match message {
            Message::ExecutorStart { id, .. } => Some(*id),
            Message::ExecutorEnd { id, .. } => Some(*id),
            Message::Metric { executor_id, .. } => Some(*executor_id),
            Message::ExecutorUpdate { executor_id, .. } => Some(*executor_id),
            _ => None,
        };

        (run_id, scenario_name, executor_id)
    }

    pub fn try_clone(&self) -> Result<Self, anyhow::Error> {
        Ok(Self {
            db_conn: self.db_conn.try_clone()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};
    use ulid::Ulid;

    use crate::{
        db::{DatabaseConn, MERGE_METRIC_COUNTER},
        metrics::{MetricSetKey, MetricType, Value},
        tracing::Message,
    };

    fn messages() -> Vec<Message> {
        let scenario_name: Arc<str> = Arc::from("scenario1");

        vec![
            Message::ScenarioStarted {
                run_id: Ulid::default(),
                scenario_name: scenario_name.clone(),
                timestamp: chrono::Utc::now(),
                start_time: chrono::Utc::now(),
            },
            Message::ExecutorStart {
                run_id: Ulid::default(),
                scenario_name: scenario_name.clone(),
                timestamp: chrono::Utc::now(),
                id: 1,
                start_time: chrono::Utc::now(),
            },
            Message::ExecutorUpdate {
                timestamp: chrono::Utc::now(),
                run_id: Ulid::default(),
                scenario_name: scenario_name.clone(),
                executor_id: 1,
                users: 10,
                stage: 0,
                stage_start_time: chrono::DateTime::<chrono::Utc>::MIN_UTC,
            },
            Message::Metric {
                timestamp: chrono::Utc::now(),
                run_id: Ulid::default(),
                scenario_name: scenario_name.clone(),
                executor_id: 1,
                metric_set_key: MetricSetKey {
                    name: "task",
                    metric_type: MetricType::Gauge,
                    attributes: vec![],
                },
                metric_value: Value::Duration(Duration::from_secs(1)),
            },
            Message::ExecutorEnd {
                run_id: Ulid::default(),
                scenario_name: scenario_name.clone(),
                timestamp: chrono::Utc::now(),
                id: 1,
                end_time: chrono::Utc::now(),
            },
            Message::ScenarioEnded {
                run_id: Ulid::default(),
                scenario_name: scenario_name.clone(),
                timestamp: chrono::Utc::now(),
                end_time: chrono::Utc::now(),
            },
        ]
    }

    #[tokio::test]
    async fn test_db_write_testrun() {
        let db = DatabaseConn::new(duckdb::Connection::open_in_memory().unwrap());
        db.create_all_tables().unwrap();
        for message in messages() {
            db.write_message_type(&message).unwrap();
        }

        fn row_count(table_name: &str, conn: &duckdb::Connection) -> i64 {
            conn.query_row(&format!("select count(*) from {}", table_name), [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap()
        }

        let metrics_row_count = row_count("metrics_raw", &db.db_conn);
        assert_eq!(metrics_row_count, 1);

        let executor_update_count = row_count("executor_updates", &db.db_conn);
        assert_eq!(executor_update_count, 1);

        let generic_message_count = row_count("messages", &db.db_conn);
        assert_eq!(generic_message_count, 4);

        let _ = db.db_conn.execute(MERGE_METRIC_COUNTER, []);
    }
}
