#!/bin/bash
# Runs ONCE, the first time the Postgres volume is initialised (the official
# image skips /docker-entrypoint-initdb.d when data already exists). To re-run
# it, reset the volume: `docker compose down -v`.
#
# One Postgres server, two databases, two owners:
#   market   (POSTGRES_DB)      analytical warehouse, owned by POSTGRES_USER
#   backend  (BACKEND_DB_NAME)  FastAPI app data (users, API keys)
# The backend role can't read or write warehouse tables, so a bug or SQL
# injection in the API can't touch analytics data, and vice versa.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v backend_user="$BACKEND_DB_USER" \
  -v backend_password="$BACKEND_DB_PASSWORD" \
  -v backend_db="$BACKEND_DB_NAME" \n  -v warehouse_db="$POSTGRES_DB" <<-'SQL'
	CREATE ROLE :"backend_user" WITH LOGIN PASSWORD :'backend_password';
	CREATE DATABASE :"backend_db" OWNER :"backend_user";
	REVOKE ALL ON DATABASE :"backend_db" FROM PUBLIC;
	REVOKE CONNECT ON DATABASE :"warehouse_db" FROM PUBLIC;
SQL
