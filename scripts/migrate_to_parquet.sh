#!/usr/bin/env bash
set -euo pipefail

# Script to migrate legacy DuckDB event files to Parquet with the new schema.
# It will:
#   1) add the new columns to each DuckDB file (idempotent)
#   2) backfill event_type, error_level, error_payload from the JSON "event" column
#   3) export to Parquet in data/parquet/events_YYYY_MM_DD.parquet
#   4) optionally delete the original DuckDB file (if DELETE_ORIGINAL=1)
#
# Requirements:
#   - duckdb CLI available in PATH
#
# Usage:
#   ./scripts/migrate_to_parquet.sh
#   DELETE_ORIGINAL=1 ./scripts/migrate_to_parquet.sh   # if you want to remove duckdb files after export

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCK_DIR="${ROOT}/data/duck"
PARQUET_DIR="${ROOT}/data/parquet"

mkdir -p "${PARQUET_DIR}"

shopt -s nullglob
files=("${DUCK_DIR}"/events_*.duckdb)
shopt -u nullglob

if [[ ${#files[@]} -eq 0 ]]; then
  echo "No DuckDB files found in ${DUCK_DIR}"
  exit 0
fi

for f in "${files[@]}"; do
  base="$(basename "$f" .duckdb)"
  day="${base#events_}"
  parquet_path="${PARQUET_DIR}/${base}.parquet"

  echo "Processing ${f} -> ${parquet_path}"

  # Ensure schema, backfill parsed fields, and export to Parquet
  duckdb "$f" "
    ALTER TABLE events ADD COLUMN IF NOT EXISTS event_type VARCHAR;
    ALTER TABLE events ADD COLUMN IF NOT EXISTS error_level VARCHAR;
    ALTER TABLE events ADD COLUMN IF NOT EXISTS error_payload TEXT;

    UPDATE events SET
      event_type   = COALESCE(event_type, json_extract_string(event, '\$.type')),
      error_level  = COALESCE(error_level, json_extract_string(event, '\$.data.payload.level')),
      error_payload= COALESCE(error_payload, json_extract_string(event, '\$.data.payload.payload'))
    WHERE event_type IS NULL
       OR error_level IS NULL
       OR error_payload IS NULL;

    CREATE INDEX IF NOT EXISTS idx_error_level ON events(error_level);
    CREATE INDEX IF NOT EXISTS idx_error_payload ON events(error_payload);
    CREATE INDEX IF NOT EXISTS idx_ts ON events(ts);
    CREATE INDEX IF NOT EXISTS idx_session_app ON events(session_id, app_type);

    COPY (SELECT * FROM events) TO '${parquet_path}' (FORMAT PARQUET);
  "

  if [[ "${DELETE_ORIGINAL:-0}" == "1" ]]; then
    echo "Deleting source DuckDB file ${f}"
    rm -f "$f"
  fi
done

echo "Migration complete."

