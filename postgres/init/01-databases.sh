#!/usr/bin/env bash
# Postgres runs this only on an empty volume, so every service's role must be created here up front.
set -euo pipefail

create_db() {
  local name=$1 password=$2
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" \
    -v name="$name" -v password="$password" <<'SQL'
CREATE ROLE :"name" LOGIN PASSWORD :'password';
CREATE DATABASE :"name" OWNER :"name";
SQL
}

create_db metastore "$METASTORE_DB_PASSWORD"
create_db airflow "$AIRFLOW_DB_PASSWORD"
create_db metabase "$METABASE_DB_PASSWORD"
