-- SQL version of pipelines/auto_cdc (Databricks only). Configuration: cdc_source.
--
-- Exam notes (Data Engineer Professional):
-- * AUTO CDC INTO is the new name of APPLY CHANGES INTO. The target is declared first with
--   CREATE OR REFRESH STREAMING TABLE (no query), and the flow must read a streaming source (STREAM(...)).
-- * KEYS and SEQUENCE BY are required; SEQUENCE BY orders late/out-of-order events per key.
-- * Default is STORED AS SCD TYPE 1. Without APPLY AS DELETE WHEN, delete events are just upserted.
--   APPLY AS TRUNCATE WHEN is SCD type 1 only.
-- * SCD type 2 adds __START_AT / __END_AT (same type as the SEQUENCE BY column); the current version
--   has __END_AT IS NULL.
-- * TRACK HISTORY ON * EXCEPT (cols): a change only in the excluded columns updates the current
--   version in place instead of opening a new one.

CREATE OR REFRESH STREAMING TABLE customers_cdc_bronze
COMMENT 'Raw CDC events, append-only.'
AS SELECT *, _metadata.file_path AS source_file
   FROM STREAM read_files('${cdc_source}', format => 'json',
        schema => 'customer_id BIGINT, name STRING, email STRING, city STRING, operation STRING, sequence_num BIGINT');

CREATE OR REFRESH STREAMING TABLE customers_cdc_clean (
  CONSTRAINT valid_id        EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_operation EXPECT (operation IN ('INSERT', 'UPDATE', 'DELETE')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_sequence  EXPECT (sequence_num IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT * EXCEPT (source_file) FROM STREAM(customers_cdc_bronze);

-- SCD type 1: current state
CREATE OR REFRESH STREAMING TABLE customers_scd1;

CREATE FLOW customers_scd1_flow AS AUTO CDC INTO customers_scd1
FROM STREAM(customers_cdc_clean)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num)
STORED AS SCD TYPE 1;

-- SCD type 2: history; an email-only change does not open a new version
CREATE OR REFRESH STREAMING TABLE customers_scd2;

CREATE FLOW customers_scd2_flow AS AUTO CDC INTO customers_scd2
FROM STREAM(customers_cdc_clean)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (email);

CREATE OR REFRESH MATERIALIZED VIEW customers_current
AS SELECT * EXCEPT (__START_AT, __END_AT) FROM customers_scd2 WHERE __END_AT IS NULL;
