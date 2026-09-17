-- +goose Up
-- heos:min-runtime=1

CREATE SCHEMA IF NOT EXISTS heos;
REVOKE ALL ON SCHEMA heos FROM PUBLIC;
CREATE TABLE heos.schema_migrations (
    version integer PRIMARY KEY CHECK (version > 0),
    checksum text NOT NULL CHECK (length(checksum) = 64),
    min_runtime integer NOT NULL CHECK (min_runtime > 0 AND min_runtime <= version),
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE heos.journal_control (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    epoch text NOT NULL,
    ready boolean NOT NULL DEFAULT false
);
INSERT INTO heos.journal_control (epoch) VALUES ('');

CREATE TABLE heos.operations (
    id text PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('alarm', 'playback', 'volume', 'mute', 'transport', 'stop', 'cancel')),
    player text NOT NULL,
    device_key text NOT NULL,
    principal text NOT NULL,
    epoch text NOT NULL,
    state text NOT NULL CHECK (state IN ('accepted', 'running', 'succeeded', 'failed', 'cancelled', 'released', 'interrupted', 'uncertain')),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    phase text NOT NULL DEFAULT '',
    config_revision text NOT NULL,
    effective_arguments jsonb NOT NULL CHECK (jsonb_typeof(effective_arguments) = 'object' AND octet_length(effective_arguments::text) <= 32768),
    selected_album text,
    progress jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(progress) = 'object' AND octet_length(progress::text) <= 32768),
    outcome jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(outcome) = 'object' AND octet_length(outcome::text) <= 32768),
    error_code text NOT NULL DEFAULT '',
    scheduled_for timestamptz NOT NULL,
    not_after timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    CHECK ((state IN ('accepted', 'running')) = (finished_at IS NULL)),
    CHECK (not_after > scheduled_for),
    UNIQUE (id, device_key, epoch)
);
CREATE INDEX operations_retention ON heos.operations (finished_at, id) WHERE finished_at IS NOT NULL;
CREATE INDEX operations_recovery ON heos.operations (epoch, id) WHERE finished_at IS NULL;

CREATE TABLE heos.idempotency_records (
    principal text NOT NULL,
    method text NOT NULL,
    endpoint text NOT NULL,
    key text NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    operation_id text NOT NULL UNIQUE REFERENCES heos.operations (id) ON DELETE CASCADE,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (principal, method, endpoint, key)
);

CREATE TABLE heos.device_reservations (
    device_key text PRIMARY KEY,
    operation_id text NOT NULL UNIQUE,
    epoch text NOT NULL,
    FOREIGN KEY (operation_id, device_key, epoch) REFERENCES heos.operations (id, device_key, epoch)
);

-- This role is the deployment's runtime privilege group. Other login roles can
-- inherit it. Migration metadata remains read-only for runtime connections.
GRANT SELECT, INSERT, UPDATE, DELETE ON heos.operations, heos.idempotency_records,
    heos.device_reservations TO heos_runtime;
GRANT SELECT, UPDATE ON heos.journal_control TO heos_runtime;

GRANT USAGE ON SCHEMA heos TO heos_runtime;
GRANT SELECT ON heos.schema_migrations TO heos_runtime;
