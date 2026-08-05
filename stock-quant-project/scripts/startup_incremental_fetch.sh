#!/usr/bin/env bash
# Additive, idempotent startup script for WSL/Windows.
#
# Purpose: on machine/WSL startup, run an incremental (watermark-based) fetch
# of new bars/quotes/trades instead of re-pulling full history. Safe to run
# repeatedly -- relies on the `ingestion_watermark` table (see
# sql/001_additive_indexes.sql) and MERGE-based bronze_to_silver upserts, so
# duplicate runs do not duplicate or corrupt data.
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
# NOT start/stop containers or touch existing data.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "[startup_incremental_fetch] project root: $PROJECT_ROOT"

# 1) Make sure postgres is reachable (does not create/drop anything).
if ! docker ps --format '{{.Names}}' | grep -q postgres; then
  echo "[startup_incremental_fetch] postgres container not running -- starting it (docker-compose up -d postgres)."
  docker-compose up -d postgres
fi

# 2) Run incremental ingestion for each table, using existing watermark.
#    postgres_to_bronze.py should read/update `ingestion_watermark` per table_key
#    and only pull rows newer than the stored watermark (falls back to full
#    load on first run when no watermark row exists yet).
for table in bars quotes trades; do
  echo "[startup_incremental_fetch] running incremental fetch for $table"
  python -m src.ingestion.postgres_to_bronze --table "$table" --incremental
done

# 3) Promote bronze -> silver (MERGE-based, safe to re-run).
python -m src.transform.bronze_to_silver --all

echo "[startup_incremental_fetch] done."
