-- SQL version of pipelines/multi_flow (Databricks only). Configuration: source_flow_a, source_flow_b.

-- Bronze: one streaming table, two append flows (each with its own checkpoint)
CREATE OR REFRESH STREAMING TABLE bronze_events
COMMENT 'Raw device events from all landing zones (one append flow per source).';

CREATE FLOW flow_a AS INSERT INTO bronze_events BY NAME
SELECT *, 'flow_a' AS source_flow, _metadata.file_path AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files('${source_flow_a}', format => 'json',
                       schema => 'event_id STRING, device_id STRING, status STRING, amount DOUBLE, event_ts STRING');

CREATE FLOW flow_b AS INSERT INTO bronze_events BY NAME
SELECT *, 'flow_b' AS source_flow, _metadata.file_path AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files('${source_flow_b}', format => 'json',
                       schema => 'event_id STRING, device_id STRING, status STRING, amount DOUBLE, event_ts STRING');

-- Silver: expectations (warn / drop / fail) + Liquid Clustering
CREATE OR REFRESH STREAMING TABLE silver_events (
  CONSTRAINT non_negative_amount EXPECT (amount >= 0),
  CONSTRAINT valid_timestamp     EXPECT (event_ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_status        EXPECT (status IN ('ACTIVE', 'INACTIVE', 'PENDING', 'ERROR')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_event_id      EXPECT (event_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
CLUSTER BY (status, ingest_date)
COMMENT 'Validated device events.'
AS SELECT event_id, device_id, status, amount,
          to_timestamp(event_ts) AS event_ts, to_date(event_ts) AS ingest_date, source_flow
   FROM STREAM(bronze_events);

-- Quarantine: the inverse of the drop rules
CREATE OR REFRESH STREAMING TABLE silver_quarantine
COMMENT 'Rows rejected by silver_events drop rules.'
AS SELECT *,
          array_compact(array(
            CASE WHEN NOT coalesce(event_ts IS NOT NULL, false) THEN 'valid_timestamp' END,
            CASE WHEN NOT coalesce(status IN ('ACTIVE', 'INACTIVE', 'PENDING', 'ERROR'), false) THEN 'valid_status' END
          )) AS failed_expectations
   FROM STREAM(bronze_events)
   WHERE NOT coalesce(event_ts IS NOT NULL, false)
      OR NOT coalesce(status IN ('ACTIVE', 'INACTIVE', 'PENDING', 'ERROR'), false);

-- Gold: materialized view
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_metrics
CLUSTER BY (ingest_date)
COMMENT 'Daily event metrics per status.'
AS SELECT ingest_date, status,
          count(*) AS event_count, count(DISTINCT device_id) AS distinct_devices,
          round(sum(amount), 2) AS total_amount, round(avg(amount), 2) AS avg_amount
   FROM silver_events
   GROUP BY ingest_date, status;
