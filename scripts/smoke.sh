#!/usr/bin/env bash
set -euo pipefail
# Operates only on this checkout's local Compose project, never a cluster/device.
# Leave PostgreSQL available after the check; stop the application on exit.
compose=(docker compose)
binary_pid=""
cleanup() {
  if [[ -n "$binary_pid" ]]; then
    kill -TERM "$binary_pid" >/dev/null 2>&1 || true
    wait "$binary_pid" >/dev/null 2>&1 || true
  fi
  "${compose[@]}" start postgres >/dev/null 2>&1 || true
  "${compose[@]}" --profile app stop heos-control >/dev/null 2>&1 || true
}
trap cleanup EXIT
status() {
  curl --silent --show-error --cacert .local/ca.crt --max-time 8 \
    -o /dev/null -w '%{http_code}' "https://localhost:8443/$1" 2>/dev/null || true
}
await_status() {
  for _ in {1..30}; do
    if [[ $(status "$1") == "$2" ]]; then return; fi
    sleep 1
  done
  echo "Expected $1 to return $2" >&2
  "${compose[@]}" logs --tail=30 heos-control >&2
  return 1
}
echo 'Smoke: start PostgreSQL, migrate and seed an interrupted operation'
"${compose[@]}" up -d --wait postgres
"${compose[@]}" --profile tools run --rm migrate
marker="op_smoke_$(date +%s)_$$"
"${compose[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -v marker="$marker" -U postgres -d heos_control >/dev/null <<'SQL'
INSERT INTO heos.operations (id, kind, player, device_key, principal, epoch, state, phase,
config_revision, effective_arguments, scheduled_for, not_after, created_at, updated_at, started_at)
VALUES (:'marker', 'playback', 'smoke', :'marker', 'smoke', 'stopped-smoke-process', 'running',
'playing', 'synthetic', '{}', now(), now()+interval '1 minute', now(), now(), now());
INSERT INTO heos.device_reservations (device_key, operation_id, epoch)
VALUES (:'marker', :'marker', 'stopped-smoke-process');
SQL
before=$("${compose[@]}" exec -T postgres psql -U postgres -d heos_control -Atc 'SELECT version || checksum || applied_at FROM heos.schema_migrations')
echo 'Smoke: check standalone binary HTTPS probes and graceful shutdown'
"${compose[@]}" --profile app stop heos-control
./bin/heos-control serve -config .local/runtime.json >.local/binary-smoke.log 2>&1 &
binary_pid=$!
await_status livez 200
await_status readyz 200
kill -TERM "$binary_pid"
wait "$binary_pid"
binary_pid=""
echo 'Smoke: check runtime container identity, read-only filesystem and TLS trust'
"${compose[@]}" --profile app up -d --no-deps heos-control
await_status livez 200
await_status readyz 200
id=$("${compose[@]}" ps -q heos-control)
[[ $(docker inspect --format '{{.Config.User}} {{.HostConfig.ReadonlyRootfs}}' "$id") == '10001:10001 true' ]]
if curl --silent --fail --max-time 3 https://localhost:8443/livez >/dev/null 2>&1; then
  echo 'API unexpectedly trusted without its development CA' >&2; exit 1
fi
echo 'Smoke: interrupt PostgreSQL and check liveness/readiness separately'
"${compose[@]}" stop postgres
await_status livez 200
await_status readyz 503
echo 'Smoke: replace the container while PostgreSQL is unavailable'
"${compose[@]}" --profile app up -d --force-recreate --no-deps heos-control
await_status livez 200
await_status readyz 503
echo 'Smoke: restore PostgreSQL and check persisted history and reservation recovery'
"${compose[@]}" start postgres
await_status readyz 200
after=$("${compose[@]}" exec -T postgres psql -U postgres -d heos_control -Atc 'SELECT version || checksum || applied_at FROM heos.schema_migrations')
[[ "$before" == "$after" ]]
recovered=$("${compose[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -v marker="$marker" -U postgres -d heos_control -At <<'SQL'
SELECT state || ':' || error_code FROM heos.operations WHERE id = :'marker' AND finished_at IS NOT NULL;
SELECT count(*) FROM heos.device_reservations WHERE operation_id = :'marker';
SQL
)
[[ "$recovered" == $'uncertain:process_interrupted\n0' ]]
echo 'Smoke: check container SIGTERM deadline, exit code and OOM state'
id=$("${compose[@]}" ps -q heos-control)
started=$SECONDS
"${compose[@]}" --profile app stop heos-control
[[ $((SECONDS-started)) -lt 30 ]]
[[ $(docker inspect --format '{{.State.ExitCode}} {{.State.OOMKilled}}' "$id") == '0 false' ]]
echo 'HTTPS, DB outage/recovery, container replacement, interrupted operation history, reservation release and SIGTERM passed.'
