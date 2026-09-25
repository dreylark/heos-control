# Operations

The service runs as one controller with external PostgreSQL. The reusable chart
lives in [charts/heos-control](../charts/heos-control); installation, routing,
Secrets, database provisioning, backups and scheduling belong to the operator.
See [configuration](CONFIGURATION.md), [migrations](MIGRATIONS.md) and
[release artifacts](RELEASING.md).

## Process and deployment

Keep one replica and `Recreate`. Stop the previous controller before starting its
replacement: database epochs fence journal writes, not an isolated connection to
a speaker. There is no scheduler, autoscaler, data PVC or startup playback.

The runtime image contains the static Go binary and CA bundle on scratch, runs
as UID/GID 10001 and supports a read-only root filesystem. The chart drops
capabilities and disables privilege escalation. Mount the configured files so
this user can read them; do not put Secrets in image build arguments.

| Endpoint | Meaning |
| --- | --- |
| `/livez` | Process health, independent of PostgreSQL and speakers |
| `/readyz` | PostgreSQL readiness, compatible schema and initial journal recovery |
| `/metrics` | Authenticated process/player telemetry; requires read scope |

Startup and liveness use `/livez`; readiness uses `/readyz`. PostgreSQL loss makes
readiness return 503 and suspends normal automation writes; speaker unavailability
does not fail process health. Readiness does not grant permission to play.

Default chart requests are 25m CPU / 64Mi memory, with limits of 500m / 128Mi.
These are starting budgets, not a capacity guarantee. Measure RSS, CPU throttling,
catalog size and concurrent demand for the intended workload.

NetworkPolicy defaults deny API ingress and database/device egress. Configure
`apiPeers`, `databasePeers` and `heosPeers`. The relevant ports are HTTPS 8443,
PostgreSQL 5432, pinned HEOS TLS 1265, and DNS TCP/UDP 53; API/database ports are
configurable. Check the default DNS selectors against the cluster. The speaker,
rather than this service, downloads media from the selected media server.

SIGTERM cancels future automation writes, closes connections/SSE, drains HTTP
requests and reconciles results. It does not issue a speculative Stop. Music can
continue after ownership loss or process exit. The default shutdown budget is
25 seconds within the chart's 30-second termination grace period.

Configuration, certificates and credentials are loaded at startup. Secret
rotation needs coordinated process replacement; files are not hot-reloaded.
Optional `deploymentAnnotations` can integrate an existing reload controller,
but replacement can interrupt an active run.

## Install with Helm

Provision the external PostgreSQL database/roles and these existing Secrets in
the application's namespace before installing the chart:

| Value | Required Secret keys |
| --- | --- |
| `api.tlsSecret` | `tls.crt`, `tls.key` for the API's actual DNS name |
| `api.credentialsSecret` | `credentials.json` with hashed client tokens and player grants |
| `database.credentialsSecret` | `password` for the runtime role, or `database.passwordKey` |
| `database.caSecret` | `ca.crt`, or `database.caKey` |
| `migration.credentialsSecret` | `password` for the schema owner, or `migration.passwordKey` |

Build consumer values using [the chart defaults](../charts/heos-control/values.yaml)
and [configuration](CONFIGURATION.md). At minimum, replace the database hostname,
role/Secret names and network peers. Add verified players, sources and explicit
write policy only when ready. Network peers use standard Kubernetes NetworkPolicy
selectors or CIDRs; empty lists deny the relevant traffic. DNS selectors must
match the cluster. Certificate issuance, routing, database provisioning and
Secret management are not installed by this chart.

After selecting an available published chart version:

```sh
VERSION='<published-version-without-v>'
helm upgrade --install heos-control \
  oci://ghcr.io/dreylark/heos-control/charts/heos-control \
  --version "$VERSION" --namespace heos-control --create-namespace \
  --values values.yaml --wait --timeout 10m
```

The published chart selects an immutable image digest. Source-chart defaults use
a local development image; if installing directly from the checkout, explicitly
provide an available image repository and digest. Supply `imagePullSecrets` for
a private image; Helm's registry login and Kubernetes image-pull credentials are
separate. Package visibility is independent of source-repository visibility.

The chart always runs its migration Job before Helm install/upgrade. Use separate
owner credentials and inspect failed Job logs before retrying; see
[migrations](MIGRATIONS.md). Keep one controller even when rolling back the
application. Neither Helm uninstall nor rollback reverses committed migrations.

## Interactive API reference

Documentation defaults off. Enable it in consumer values:

```yaml
api:
  docs:
    enabled: true
```

This renders runtime `docs_enabled: true`. The service then exposes `/docs`, its
small local page/settings resources and `/openapi.yaml` without authentication.
The API and `/metrics` still require their usual credentials and grants. When
disabled, documentation paths return 404. The schema is the exact document
embedded in the running binary, not a separately deployed website.

Scalar loads from a pinned CDN URL with SRI and CSP; the browser needs network
access on first use. The API reference uses the current service origin. No
renderer bundle or frontend build is shipped. To update Scalar, change the
version, integrity hash and allowed CSP resource together in `internal/api/docs`,
check in a browser and retain the offline HTTP/security tests. Theme/settings
belong to that small page, not Helm values.

## Check an installation

Run from the same network and mounted-file context as the service:

```sh
heos-control config check -config /etc/heos-control/config.json
heos-control config check -config /etc/heos-control/config.json -format json
heos-control doctor -config /etc/heos-control/config.json -timeout 30s
heos-control doctor -config /etc/heos-control/config.json -format json
```

`config check` is offline: it checks strict JSON, device/source policy, credential
scopes/player references, API certificate/key validity and matching, database
password and CA files. Independent file failures are reported together. Structural
configuration errors stop dependent checks. Referenced files must resolve to
regular files of at most 1 MiB; Secret symlinks are supported. Reports identify
fields and array indexes without echoing their values.

Offline success does not prove remote trust, hostname matching, port availability
or connectivity. PostgreSQL requires one TCP host/IP and a separate port; Unix
sockets are rejected because they would bypass TLS. Passwords use runtime's
whitespace trimming. Driver-default certificate/password/service files are
disabled; unset unsupported `PGSERVICE`.

`doctor` first performs those local checks. If they pass, it runs independent
probes, at most four concurrently, and reports results in configuration order:

| Check | A pass establishes |
| --- | --- |
| `database.connection` | Runtime credentials connect with verified PostgreSQL TLS |
| `database.schema` | Required tables and compatible migration versions/checksums exist |
| `database.permissions` | The login role has required effective schema/table ACLs, including inherited grants |
| `players[N]` | Pinned TLS connects, physical identity resolves uniquely, and a valid state response arrives |

Database probes use one short-lived, server-enforced read-only connection pool.
They perform no migration, recovery, advisory lock, epoch change or trial write.
Use runtime credentials; effective ACL checks do not prove every future query
will succeed.

Each device probe has writes disabled and sends three commands on success:
event registration, `player/get_players`, and `player/get_play_state`. It does
not browse media, read queues, run observer polling or change a certificate pin.
Firmware state `unknown` is valid for connectivity checks. A pass does not verify
a volume ceiling, establish write ownership or replace API preflight.

The default overall timeout is 30 seconds; `-timeout` accepts 1s through 2m.
Device probes have a five-second cap; database checks also obey
`database.timeout_seconds`. Cancellation joins started probes and closes their
connections. A failed network probe does not suppress independent probes; a
local configuration failure suppresses all network activity. Dependent database
stages report `not_checked` after a connection failure.

Text output has one PASS/FAIL line per check. JSON contains `version: 1`,
`command`, overall `ok`, and ordered `checks` with `field`, `code`, `message`
and `ok`. Reports omit credentials, hashes, private keys, pins, serials, network
addresses, media, file contents and raw driver/device errors.

| Exit status | Meaning |
| --- | --- |
| 0 | All checks passed; also used for command help |
| 1 | A check failed or was cancelled, or output could not be written |
| 2 | Invalid command arguments |

Authentication and TLS failures have distinct database codes.
`runtime_permissions_missing` identifies missing runtime grants;
`schema_permission_denied` identifies inaccessible migration metadata.
Investigate schema failures using the migration Job; the doctor never repairs
history. Device `pin_mismatch` requires independent certificate verification
before any configuration change. Other device codes distinguish identity,
reachability, TLS, protocol, timeout and unstable-observation failures.

## History and recovery

Authenticated `GET /v1/operations` exposes a live, filtered history with
creator/operator and player authorization. Pages contain at most 100 operations.
Continue with the returned cursor and unchanged filters/page size. State changes,
new admissions and retention can affect later pages; this is not a frozen audit
export. See [API usage](API_USAGE.md).

Terminal `outcome.diagnostics` records a safe reason, optional decision rule,
last active phase, detection time and bounded scalar evidence. `error_code`
is the coarse classification; `outcome.delivery` expresses device-delivery
certainty. `playback.level` is the last confirmed level, not a pending target.
Cancellation and its target have separate evidence; a target may temporarily
show `cancellation_requested`.

Retention is seven days for terminal operations, while unexpired idempotency
records protect their operations. The journal is bounded at 10,000 operations;
history reads neither extend retention nor contact a speaker. Requests rejected
before durable admission have no operation-history entry.

On restart, unfinished work becomes uncertain with `process_interrupted`;
reservations are released. Recovery never resumes a run, recreates its queue or
stops playback. It preserves the last persisted phase and uses a null detection
time. If PostgreSQL was unavailable when the process died, an uncommitted cause
cannot be reconstructed. Older records may omit diagnostics.

## Metrics

`GET /metrics` serves Prometheus text 0.0.4 over HTTPS with bearer authentication
and read scope. Only authorized configured players are exposed; process gauges
are aggregate. Scraping sends no HEOS commands. Its only database access is the
existing journal-readiness check with a two-second budget.

| Metric and labels | Type | Meaning |
| --- | --- | --- |
| `heos_journal_ready` | Gauge | Journal readiness within the scrape budget |
| `heos_shutting_down` | Gauge | Current shutdown flag |
| `heos_go_goroutines` | Gauge | Go runtime goroutine count |
| `heos_go_heap_objects_bytes` | Gauge | Heap object bytes, not process RSS |
| `heos_player_connected{player}` | Gauge | Current connection snapshot |
| `heos_player_stale{player}` | Gauge | Observed state cannot currently authorize writes |
| `heos_player_writes_enabled{player}` | Gauge | Configured opt-in, not current write authorization |
| `heos_wire_commands_total{player,command,result}` | Counter | Completed exchanges with bytes sent |
| `heos_wire_command_duration_seconds{player,command}` | Histogram | Duration for the same wire-exchange population |
| `heos_command_confirmation_duration_seconds{player,kind,result}` | Histogram | Writer invocation through application-state confirmation or failure |
| `heos_confirmation_fallbacks_total{player,kind}` | Counter | Targeted fallback attempts after missing scalar events |
| `heos_observation_refreshes_total{player,scope,trigger}` | Counter | Observation read sequences started, including failures |
| `heos_reconnects_total{player}` | Counter | Successful subscriptions after the first usable connection |
| `heos_event_gaps_total{player,reason}` | Counter | Continuity gaps: connection closure or event-buffer overflow |
| `heos_operations_finished_total{player,kind,state,reason}` | Counter | Durably terminal operations admitted and executed by this process |
| `heos_active_operations{player,phase}` | Gauge | One active phase per player at most; all zero when idle |
| `heos_player_last_full_observation_timestamp_seconds{player}` | Gauge | Last complete observation's Unix timestamp; absent before the first |

Wire results are `reply_success`, `reply_rejected` or `uncertain`. Subscription,
heartbeat, queue-page and fallback commands count too. A partial write counts as
uncertain; `NotSent` counts neither an exchange nor a duration. A successful
reply is not proof of state confirmation.

Confirmation kinds are `volume`, `mute`, `mode`, `transport` and `queue`;
results are `confirmed`, `timeout`, `released`, `rejected`, `uncertain` or
`cancelled`. The interval includes writer waiting, reply and event/readback
verification, but excludes admission and prewrite journal work. Not-sent attempts
are excluded. Histograms expose `_bucket`, `_sum`, `_count` with second-based
buckets from 0.01 through 60 and +Inf. Labeled counters/histograms appear after
their first sample.

Observation scopes are `full`, `media`, `scalars` and `playback`; triggers are
`startup`, `event`, `audit`, `recovery`, `preflight`, `admission`,
`prewrite`, `confirmation`, `fallback` and `manual`. Cache hits and valid
event projections do not count as refreshes. A fallback counts attempts:
volume/mute normally uses two GETs, play mode one. Only full observations advance
the full-observation timestamp.

Initial or failed connection attempts are not reconnects. Connection-closed gaps
include intentional shutdown; this is not a network-failure counter. Completion
counters exclude retries, history reads and startup recovery, and count deferred
terminal persistence once. Phase `persisting` means execution ended while
durable finalization remains pending; it does not mean music is playing.
Counters/histograms reset at restart and are not rebuilt from history.

Labels are bounded and exclude operation IDs, principals, media and physical
identifiers. A database outage normally leaves metrics HTTP 200 with readiness 0.
Configure a dedicated read credential, TLS verification, a scrape timeout longer
than the database budget and monitoring ingress in `apiPeers`. No
ServiceMonitor/operator is installed by the chart.

```promql
sum by (player, command) (rate(heos_wire_commands_total[5m]))
```

```promql
histogram_quantile(0.95,
  sum by (player, kind, le) (
    rate(heos_command_confirmation_duration_seconds_bucket{result="confirmed"}[1h])
  )
)
```

Sparse command traffic may not support useful percentile estimates.

## Logs

JSON stdout logs identify startup version/revision and operation lifecycle.
INFO ownership-loss records retain the first precise cause, phase, confirmed
commands and safe comparison/event evidence. An unexpected event does not prove
manual intervention; firmware duplicates can also cause it.

Set runtime `log_level` or Helm `logging.level` to `debug` for sanitized
command/reply/event parameters, observation summaries, timing and delivery
classification. Changes require restart. Even DEBUG omits raw payloads,
credentials, account/media labels, URLs and free-form device errors; opaque
identity/media values use truncated fingerprints. Return to INFO after diagnosis.
Collection and retention belong to the operator.

Queue request diagnostics include the numeric `aid` and fixed-size
`cid_fingerprint`/`mid_fingerprint` values when those IDs are present. Compare
these fingerprints with the preceding browse requests to follow the selected
container without disclosing its ID or name. Final browse responses include
`browse_options.state` (`absent`, `valid`, or `invalid`) and
`browse_options.playable_container`, reflecting HEOS option 21 for the currently
browsed container. This is advertised capability, not confirmation that queue
replacement will succeed. These fields are diagnostic only; they do not grant
playback ownership or change admission. Malformed or oversized optional data is
marked invalid without rejecting an otherwise valid response.

When queue replacement is acknowledged but playback does not change, retain the
DEBUG interval from catalog browsing through the operation's terminal result.
Confirmation compares the observed queue and transport state; a successful
native reply alone cannot establish that the requested queue was applied.

## Backup and restore

Back up application tables, idempotency records, reservations, journal control
and both migration ledgers together. Roles, Secrets and TLS configuration must
be recoverable separately. A database restore can lose idempotency records for
requests after its recovery point.

1. Pause callers and stop the previous controller. Record the recovery point and
   any potentially active physical playback.
2. Restore a consistent backup to an isolated assessment target with device
   writes disabled; keep the original target recoverable.
3. Verify roles, PostgreSQL version, TLS identities, grants and migration
   checksums. Start one controller with all `writes_enabled: false`.
4. Check readiness and history. Recovered unfinished work must not resume.
5. Reconcile jobs after the recovery point using external records. Missing
   idempotency history is not evidence that playback never happened; do not
   automatically replay those requests.
6. Inspect device state, reject expired jobs, then deliberately restore normal
   operation with one controller.

`make qualify` rehearses dump/restore and retained/missing keys on a disposable
local database. It does not establish production RPO/RTO or reconstruct lost
history. See [testing](TESTING.md).
