#!/usr/bin/env bash
set -euo pipefail
# Only used by the disposable Compose database on its first initialization.
owner_password=$(cat /run/secrets/owner-password)
runtime_password=$(cat /run/secrets/runtime-password)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set=owner_password="$owner_password" --set=runtime_password="$runtime_password" <<'SQL'
CREATE ROLE heos_owner LOGIN PASSWORD :'owner_password';
CREATE ROLE heos_runtime LOGIN PASSWORD :'runtime_password';
CREATE DATABASE heos_control OWNER heos_owner;
CREATE DATABASE heos_test OWNER heos_owner;
REVOKE ALL ON DATABASE heos_control, heos_test FROM PUBLIC;
GRANT CONNECT ON DATABASE heos_control, heos_test TO heos_runtime;
SQL
for database in heos_control heos_test; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$database" <<'SQL'
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA heos AUTHORIZATION heos_owner;
GRANT USAGE ON SCHEMA heos TO heos_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE heos_owner IN SCHEMA heos GRANT SELECT ON TABLES TO heos_runtime;
SQL
done
