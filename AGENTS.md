# Agent and contributor instructions

`heos-control` is a Go HTTP service for HEOS playback and bounded automation.
It owns its API, Docker build and reusable Helm chart. External clients own
scheduling, media selection and deployment-specific configuration.

## Read first

- [Architecture](docs/ARCHITECTURE.md): packages, lifecycle and ownership.
- [API usage](docs/API_USAGE.md) and [OpenAPI](api/openapi.yaml): public contract.
- [Queue decisions](docs/QUEUE_DECISIONS.md): ordered policies and execution boundary.
- [Device compatibility](docs/DEVICE_COMPATIBILITY.md): protocol/firmware evidence.
- [Configuration](docs/CONFIGURATION.md) and [operations](docs/OPERATIONS.md).
- [Migrations](docs/MIGRATIONS.md), [testing](docs/TESTING.md) and [releasing](docs/RELEASING.md).

Inspect the actual code. Keep current behavior documented alongside intentional
contract changes; do not turn guides into implementation diaries. Preserve
unrelated local files. Use English for repository text and identifiers.

## Work and validation

Complete authorized local changes and checks without repeated approval requests.
Do not commit, push, publish, deploy, change external schedules or write to a live
speaker without authorization. Local image builds and Helm rendering are checks,
not deployment. Keep secrets, device identities and personal media out of Git.
Do not redistribute or link to manufacturer protocol documents; cite the title,
version and relevant sections. Distinguish manufacturer semantics, observed
firmware behavior and application policy.

Use test-first changes for device/protocol/ownership behavior. Test with synthetic
TLS devices and injected clocks. For Go changes, run `make verify` and pinned
golangci-lint; run disposable PostgreSQL integration/coverage for persistence
changes. Run `make workflow-check`, `make chart-check` and container/smoke checks
when their areas change. Follow exclusive-database rules in the testing guide.
Do not lower coverage floors or exclude handwritten code to pass checks.

## Package and tool boundaries

- `cmd/heos-control` parses CLI; `internal/app` owns wiring/lifecycle. API/auth/SSE
  stay in `internal/api`, policy in `internal/control`, HEOS transport in
  `internal/heos`, persistence in `internal/journal`, config in `internal/config`.
- Prefer standard library, concrete types, explicit errors and small consumer-owned
  interfaces. No DI framework, generic repository or speculative storage backend.
- PostgreSQL is the durable store. Runtime uses native pgx/pgxpool; database/sql is
  limited to goose's pgx adapter. Keep explicit transactions and sqlc-generated
  types inside journal. Never hold a transaction across device I/O or timers.
- Edit SQL queries or OpenAPI inputs and run `make generate`. Do not hand-edit
  `internal/journal/dbgen` or `internal/api/api.gen.go`. Keep tooling modules
  separate from the service module; pin dependencies and aligned Go/build versions.
- Every goroutine needs ownership, cancellation and shutdown. Bound queues,
  frames, request bodies, catalog traversal and event replay. Use monotonic time.

## Behavior that must survive changes

- Reads, preflight, rejection and default startup issue no playback-changing
  commands. Verify physical identity and TLS pin before writes; no plaintext
  fallback. Writes default off and require a per-player verified volume ceiling.
- Authenticate before cache/idempotency access. Commit admission, request mapping
  and reservation atomically before 202; only confirmed new admission dispatches
  work. Accepted work survives client disconnect. Retries reuse the original key
  and input. Database retries never replay a device command.
- One controller owns live device connections and timers. Journal epochs fence
  database writes, not isolated device writers. On restart, mark unfinished work
  uncertain; do not resume automation or stop music without ownership evidence.
- Keep the existing single event consumer and synchronous ownership callback.
  Complete contiguous scalar events maintain confirmed state without per-setter
  full reads. Progress causes no reads. Metadata changes need targeted verification;
  queue/group/gap/invalid state requires full reconciliation. Events never renew
  the baseline full-observation age (300-second audit, 310-second TTL).
- Register command expectations before sending. Changed scalar fields need both
  successful reply and complete matching events, in either order. Preserve the
  12-second confirmation deadline and single final targeted fallback; never add
  per-second scalar polling, setter replay or speculative cleanup. Current-token
  send guards expire within five seconds and increases respect remaining duration.
- Queue/transport confirmation retains bounded readback. Preserve initial-loading
  and active-transition ordered policies, event revision checks, complete queue
  membership and MID/QID rules. Repeated events never extend deadlines. Stop/unknown
  and non-atomic media changes suspend writes up to 12 seconds within the original
  playback window; confirmed same-queue Play can resume. Pause/API Stop cancels
  immediately. Natural/manual in-queue navigation preserves the ramp/fade timeline.
- Manual intervention, group changes and lost continuity revoke future automation
  writes, including the stop timer. Unknown/stale state cannot permit increases.
  Device delivery uncertainty must remain visible; do not guess command origin.
- Journal lock order begins with `journal_control`. Resolve uncertain commits by
  rereading key/ID/revision. Preserve capacity, seven-day retention, bounded recovery,
  atomic terminal reservation release and journal-only finalization retries.
- History applies principal/operator/player authorization in SQL before pagination.
  Cursors never grant access. Completion evidence is bounded and scalar-only;
  event callbacks do no database I/O or private media logging.
- Metrics use closed labels and per-player authorization; count actual wire sends
  and durably terminal current-process work. Do not add sampling/read loops or
  expose operation IDs/media titles. Diagnostics stay bounded and read-only.

## Packaging and changes

Keep runtime non-root/read-only, one replica and `Recreate`, external PostgreSQL
and the automatic chart-owned migration Job. Do not add a bundled database/PVC,
manual migration toggle, alarm endpoint, preset selector or environment deployment.
Migrations are transactional Up-only with predecessor compatibility, checksums
and explicit minimum-runtime metadata; never add reset/Down/repair-on-start paths.
Readiness depends on the database; liveness does not. Preserve bounded shutdown.

Ordinary CI runs on main pushes and PR merge commits. Static checks precede tests
and runtime build. Release PRs use that same PR run with full container checks;
do not dispatch a duplicate branch run. Publication independently verifies the
release commit. Keep actions pinned, cache responsibilities explicit and steps
visible. Update runtime third-party notices when shipped dependencies change.
