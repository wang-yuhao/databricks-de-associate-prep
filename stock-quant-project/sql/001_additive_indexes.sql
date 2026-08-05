-- Additive migration: index-only changes for incremental/watermark reads.
-- Safe to run multiple times (IF NOT EXISTS). Does NOT drop, rename, or
-- rewrite any existing table or data in the trading-bot Postgres database.
--
-- Purpose: speed up `WHERE timestamp > :last_ts` watermark queries used by
-- the incremental ingestion path (postgres_to_bronze.py) so we don't need
-- to scan entire per-symbol-year quote/trade tables (some are ~20GB).
--
-- Usage: run manually against the existing trading-bot DB, e.g.
--   psql -h localhost -U <user> -d <db> -f 001_additive_indexes.sql
-- or wrap in a loop over your existing per-symbol table names.

-- Example (repeat per existing <symbol>_quotes / <symbol>_trades table):
-- CREATE INDEX IF NOT EXISTS idx_<symbol>_quotes_ts ON <symbol>_quotes(timestamp);
-- CREATE INDEX IF NOT EXISTS idx_<symbol>_trades_ts ON <symbol>_trades(timestamp);

-- Generic helper: generate the CREATE INDEX statements for every table
-- matching the existing naming convention, so you can review before running.
-- (Adjust the LIKE patterns if your naming convention differs.)
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

-- Optional small control table for incremental watermarks (additive, new table only).
CREATE TABLE IF NOT EXISTS ingestion_watermark (
    table_key   TEXT PRIMARY KEY,
    last_ts     TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
