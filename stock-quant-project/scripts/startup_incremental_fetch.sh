#!/usr/bin/env bash
# Additive, idempotent startup script for WSL/Windows.
#
# Purpose: on machine/WSL startup, run an incremental fetch of new
# bars/quotes/trades instead of re-pulling full history, then promote
# bronze -> silver. Safe to run repeatedly.
#
# Incremental logic note: `postgres_to_bronze.py --mode incremental` already
# computes its own watermark as max(timestamp) currently in bronze for that
# table, so it does NOT require the `ingestion_watermark` control table.
# The `ingestion_watermark` table added in sql/001_additive_indexes.sql is
# an optional building block for a future cross-table/cross-source watermark
# if you ever need one -- it is not read by the current ingestion code, so
# nothing here depends on it. The timestamp indexes from that migration do
# help this incremental mode by speeding up the max(timestamp) bronze scan
# and any downstream watermark filtering.
#
# How to wire this up in WSL (manual, one-time setup -- not run automatically
# by this script):
#   1) chmod +x scripts/startup_incremental_fetch.sh
#   2) Add a cron entry:  crontab -e
#        @reboot /full/path/to/stock-quant-project/scripts/startup_incremental_fetch.sh >> /tmp/incremental_fetch.log 2>&1
#      (WSL cron needs the service enabled: `sudo service cron start`)
#   3) Or call this script manually after `docker-compose up -d postgres`.
#
# This script assumes docker-compose (postgres) is already running; it will
# NOT start/stop containers or touch existing data destructively.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "[startup_incremental_fetch] project root: $PROJECT_ROOT"

# 1) Make sure postgres is reachable (does not create/drop anything).
if ! docker ps --format '{{.Names}}' | grep -q postgres; then
  echo "[startup_incremental_fetch] postgres container not running -- starting it (docker-compose up -d postgres)."
  docker-compose up -d postgres
fi

# 2) Run incremental ingestion for each table (uses postgres_to_bronze.py's
#    own max(timestamp)-in-bronze watermark; falls back to a full load the
#    first time there's no existing bronze data for that table).
for table in bars quotes trades; do
  echo "[startup_incremental_fetch] running incremental fetch for $table"
  python -m src.ingestion.postgres_to_bronze --mode incremental --table "$table"
done

# 3) Promote bronze -> silver (MERGE-based, safe to re-run).
python -m src.transform.bronze_to_silver --all

echo "[startup_incremental_fetch] done."
