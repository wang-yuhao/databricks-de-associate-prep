-- Additive migration: index-only changes for incremental/watermark reads.
-- Safe to run multiple times (IF NOT EXISTS). Does NOT drop, rename, or
-- rewrite any existing table or data in this database.
--
-- Purpose: speed up `WHERE timestamp > :last_ts` watermark queries used by
-- the incremental ingestion path in the companion stock-quant-project repo
-- (databricks-de-associate-prep/stock-quant-project), so it doesn't need to
-- scan entire per-symbol-year quote/trade tables (some are ~20GB). This is
-- the canonical copy of this migration, since this repo owns the schema;
-- an identical copy is kept in stock-quant-project/sql/ for that repo's
-- own reference/setup convenience.
--
-- Usage:
--   sudo docker exec -i <postgres_container> psql -U <user> -d <db> -f - < sql/001_additive_indexes.sql
-- or, from a host with psql installed:
--   psql -h localhost -U <user> -d <db> -f sql/001_additive_indexes.sql

-- Generates the CREATE INDEX statements for every table matching the
-- existing naming convention (<symbol>_quotes / <symbol>_trades / <symbol>_bars).
-- Adjust the LIKE patterns below if your naming convention differs.
DO $$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND (table_name LIKE '%_quotes' OR table_name LIKE '%_trades' OR table_name LIKE '%_bars')
    LOOP
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON %I(timestamp);',
            'idx_' || tbl.table_name || '_ts',
            tbl.table_name
        );
    END LOOP;
END $$;

-- Optional small control table for a future cross-table/cross-source
-- incremental watermark. Not read by any existing code in this repo or in
-- stock-quant-project today (stock-quant-project's --mode incremental
-- computes its own watermark from max(timestamp) in its bronze layer).
-- Kept here in case a future job wants a shared, explicit watermark.
CREATE TABLE IF NOT EXISTS ingestion_watermark (
    table_key   TEXT PRIMARY KEY,
    last_ts     TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
