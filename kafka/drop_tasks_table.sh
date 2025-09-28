#!/usr/bin/env bash
set -euo pipefail

DB_NAME="${DB_NAME:-dte2025}"
DB_SCHEMA="${DB_SCHEMA:-public}"
TABLE_NAME="${TABLE_NAME:-tasks}"

# connect via local socket as the postgres OS user (no password prompt)
PSQL_SUPERUSER="sudo -u postgres psql -v ON_ERROR_STOP=1 -d $DB_NAME"

echo "Dropping table if it exists: ${DB_SCHEMA}.${TABLE_NAME} in database ${DB_NAME}..."

$PSQL_SUPERUSER <<SQL
DROP TABLE IF EXISTS "${DB_SCHEMA}"."${TABLE_NAME}" CASCADE;
SQL

echo "✔ Done. If it existed, ${DB_SCHEMA}.${TABLE_NAME} has been dropped from '${DB_NAME}'."

