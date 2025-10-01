#!/usr/bin/env bash
set -euo pipefail

# ---- Config (override via env vars) ----
DB_NAME="${DB_NAME:-dte2025}"

# Admin connection method: local socket as postgres OS user (no password)
PSQL_SUPERUSER="sudo -u postgres psql -v ON_ERROR_STOP=1 --no-password -d postgres"

# Application user/creds (used by your app/Spark)
APP_USER="${APP_USER:-appuser}"
APP_PASSWORD="${APP_PASSWORD:-dte2025}"

DB_SCHEMA="${DB_SCHEMA:-public}"
TABLE_NAME="${TABLE_NAME:-tasks}"

# ---- 0) (Optional) create a login for your OS user WITHOUT superuser ----
# If you want your Linux user to be able to psql without password via peer, uncomment:
# sudo -u postgres createuser --login "$(whoami)" || true

# ---- 1) Ensure database exists (cannot be created inside a transaction) ----
DB_EXISTS=$($PSQL_SUPERUSER -Atc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME' LIMIT 1" || true)
if [[ "$DB_EXISTS" != "1" ]]; then
  echo "Creating database $DB_NAME..."
  sudo -u postgres createdb "$DB_NAME"
else
  echo "Database $DB_NAME already exists."
fi

# ---- 2) Create/alter app role with password (idempotent, no prompt) ----
$PSQL_SUPERUSER -c "
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$APP_USER') THEN
    CREATE ROLE $APP_USER LOGIN PASSWORD '$APP_PASSWORD';
  ELSE
    ALTER ROLE $APP_USER LOGIN PASSWORD '$APP_PASSWORD';
  END IF;
END
\$\$;
"

# ---- 3) Ensure schema and table exist; grant privileges to app user ----
sudo -u postgres psql -v ON_ERROR_STOP=1 --no-password -d "$DB_NAME" <<SQL
CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA AUTHORIZATION $APP_USER;

CREATE TABLE IF NOT EXISTS $DB_SCHEMA.$TABLE_NAME (
  id              VARCHAR(30) PRIMARY KEY,
  submission_time TIMESTAMP NOT NULL,
  duration        BIGINT NOT NULL,
  cpu_count       INTEGER NOT NULL,
  cpu_capacity    DOUBLE PRECISION NOT NULL,
  mem_capacity    BIGINT NOT NULL
);

-- Make sure app user can use the schema and table
GRANT USAGE ON SCHEMA $DB_SCHEMA TO $APP_USER;
GRANT SELECT, INSERT, UPDATE, DELETE ON $DB_SCHEMA.$TABLE_NAME TO $APP_USER;

-- Future-proof: default privileges for tables created later in this schema
ALTER DEFAULT PRIVILEGES IN SCHEMA $DB_SCHEMA
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $APP_USER;
SQL

echo "✔ Ready. Table $DB_SCHEMA.$TABLE_NAME ensured in database '$DB_NAME', user '$APP_USER' can access it."
