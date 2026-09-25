# Testing and local validation

Automated tests use synthetic devices and disposable PostgreSQL, never a real
speaker or production database. Protocol/replay tests are evidence about the
implementation; hardware/firmware compatibility requires separate authorized
qualification. See [device compatibility](DEVICE_COMPATIBILITY.md).

Use the Go version pinned in [go.mod](../go.mod). Race tests need CGO and a C
compiler. Docker/Compose are needed for the local PostgreSQL and container
checks; chart checks need Helm. Workflow checks also need Node.js and ShellCheck.
Use the golangci-lint version in [.golangci-lint-version](../.golangci-lint-version),
rather than an unrelated system installation.

## Select checks for a change

| Command | Scope |
| --- | --- |
| `make generate` | Regenerate sqlc queries and OpenAPI bindings |
| `make generate-check` | Compare generated output without a running database |
| `make lint` | Generation, Go formatting and `go vet` |
| `make verify` | Lint, ordinary tests, race tests and Go build |
| `golangci-lint run ./...` | Additional configured analysis, including tagged tests |
| `make chart-check` | Helm rendering, resource/policy and invalid-values contracts |
| `make workflow-check` | Workflow formatting, actionlint, shell lint, CI tooling tests |
| `make build` | Local runtime binary at `bin/heos-control` |
| `make image` | Runtime image build only; no test or publication |
| `make image-test` | Docker verification and compilation of the integration-test image |
| `make smoke` | Build and exercise runtime-container TLS, probes, DB outage and shutdown |
| `make qualify` | Build and run recovery, dump/restore and resource qualification |

`make verify` does not include integration, coverage, chart or workflow checks.
Run the relevant checks for the changed area, without treating image creation or
Helm rendering as deployment. Generated Go must be regenerated from SQL/OpenAPI,
not edited manually. Do not weaken behavioral assertions or coverage floors to
make a check pass.

Ordinary CI runs for main pushes, pull requests targeting main and manual
dispatches. Static checks run first; tests and runtime build/smoke follow in
parallel. Pull requests test the merge commit. Container integration and
qualification also run for releases and manual full checks.
See [releasing](RELEASING.md) for publication gates.

## Disposable PostgreSQL

```sh
make db-up
make integration
```

`db-up` first runs `dev-init`, which creates ignored development credentials and
TLS files under `.local`, then starts the Compose PostgreSQL service. It retains
existing initialized credentials; do not create `.local` yourself for build
outputs. Compose uses PostgreSQL 18.6, with a loopback host port of 15432.

Integration tests require explicit owner/runtime configs and reject nonlocal
targets or databases other than `heos_test`. They reset its application schema.
The Make target supplies `.local/test-owner.json` and
`.local/test-runtime.json`; missing configuration is a failure, not a skip.

**Integration, coverage, smoke and qualification must not overlap.** Their database
and container changes need exclusive use of the disposable environment. A normal
unit/race run without integration tags needs no PostgreSQL.

To exercise the integration-test container after building it:

```sh
make db-up image-test
docker compose --profile tests run --rm integration
```

The resulting test image contains a compiled test binary and Debian slim, not a
Go compiler or cache. `make down` stops the Compose services without removing
the database volume.

## Coverage

```sh
make db-up
make coverage
cat coverage/summary.tsv
```

Open `coverage/coverage.html` in a browser. Coverage runs one full, uncached,
race-enabled unit/integration invocation using atomic cross-package
instrumentation for `cmd` and `internal`. It does not run a second ordinary
suite or merge profiles. PostgreSQL must already be running and exclusively
available. Optional `HEOS_TEST_OWNER_CONFIG`/`HEOS_TEST_RUNTIME_CONFIG` overrides
must point to disposable local test configurations.

The ignored `coverage/` directory is recreated for each run:

| Output | Purpose |
| --- | --- |
| `coverage.raw.out` | Complete atomic profile, including generated code |
| `coverage.out` | Profile after exact generated-file exclusions |
| `coverage.functions.txt`, `coverage.html` | Native Go function/source reports |
| `summary.json`, `summary.tsv` | Maintained statement counts, percentages and policy |
| `scope.txt`, `toolchain.txt`, `revision.txt` | Instrumentation scope, toolchain and revision |

[policy.json](../scripts/coverage/policy.json) defines the overall and per-package
floors and exact generated-file exclusions. Handwritten executable code remains
in scope, including startup/error paths. New or missing packages, missing
excluded files, malformed profiles and threshold regressions fail. Reports
remain available after a threshold failure.

Development/qualification tools and isolated generator modules are outside the
service denominator and have separate tests. Coverage measures executed
statements, not branch completeness or correctness; race tests, protocol
assertions and physical-device qualification remain necessary.

## Incident replay

```sh
go test ./internal/control -run '^TestReplay' -count=1
go test -race ./internal/control -run '^TestReplay' -count=20
```

The [corpus guide](../internal/control/testdata/replay/README.md) defines fixtures,
provenance and request budgets. Replays exercise the real coordinator using
synthetic devices and an injected clock. A TLS subset also exercises actual
transport, observer and event delivery over loopback.

Preserve these checks when adding or changing a scenario:

- State, diagnostic reason, command identity and required steps must match; wrong,
  unexpected or unconsumed steps fail rather than being skipped.
- Verify progress as well as cancellation: a successful run reaches its target,
  follows the original ramp/hold/fade timeline and stops at the original end.
- Test reply/event ordering, duplicate/partial events, initial Stop/Pause/unknown,
  transitional MID/QID pairs, queue navigation and manual intervention.
- Keep confirmation and playback deadlines fixed across duplicate hints. Pending
  events and unresolved media must not grant write permission.
- Count actual wire commands, including startup/background observation and
  targeted fallbacks. GETs serialize current state; extra reads must not make a
  track start or advance a scenario.
- Check connection/goroutine cleanup on failure, including an expectation failure
  while the transport waits for its reply.
- Record synthetic assumptions and distinguish a reconstructed incident from a
  captured trace. Remove private addresses, identifiers, URLs and credentials.

Controller waits use virtual time; socket deadlines, observer timers and TLS
watchdogs still use real time. A virtual long-running envelope does not exercise
real observer heartbeat/audit intervals. Separate clock tests cover those.
Do not add long sleeps to emulate playback.

## Native fuzzing

Ordinary `go test` runs handwritten `f.Add` seeds and committed failure corpus
entries. Longer mutation campaigns are explicit local work, not part of ordinary
CI. Select one target and package per invocation:

```sh
go test ./internal/heos -run='^$' -fuzz='^FuzzHEOSEventProjection$' \
  -fuzztime=60s -fuzzminimizetime=10s -parallel=2
go test ./internal/control -run='^$' -fuzz='^FuzzQueueTransitionPolicy$' \
  -fuzztime=60s -fuzzminimizetime=10s -parallel=2
```

| Package | Target | Boundary |
| --- | --- | --- |
| `internal/heos` | `FuzzHEOSCommandEncoding` | Command encoding and argument separation |
| `internal/heos` | `FuzzHEOSFrameBoundaries` | Framing, split/coalesced input and malformed replies |
| `internal/heos` | `FuzzHEOSEventProjection` | Event decoding/projection and incomplete evidence |
| `internal/control` | `FuzzQueueStartPolicy` | Initial queue confirmation |
| `internal/control` | `FuzzQueueTransitionPolicy` | Owned queue transitions and fixed deadlines |
| `internal/control` | `FuzzOrderedQueueAppend` | Ordered repeats, preserved prefixes and partial suffixes |

For policy/protocol changes, run the related seeds and a bounded campaign for
each affected target. These structured mutations are bounded to 4 KiB inputs,
eight queue items and 64 steps. Targets use input-derived logical time without
network sockets, PostgreSQL, shared mutable state or real sleeps. Large framing
limits are covered by separate fixed regressions.

Preserve preconditions and independent assertions: policy `pass` delegates
further checks and is not permission to write. Cover both safety and successful
progress; do not derive expected outcomes by calling the implementation again.

Keep Go's minimized failing input under `testdata/fuzz/<Target>/`, inspect it
for private data and add a named regression when that clarifies the failure.
Reproduce it with `go test ./path/to/package -run='^Target/corpus-hash$'`.
Minimization can take additional time after the search budget. Useful nonfailing
generated inputs may stay in the local Go cache. Execution counts and coverage
growth are not correctness proofs.

## Container smoke and recovery qualification

Run these sequentially after `make db-up`:

```sh
make smoke
make qualify
```

Smoke exercises the non-root, read-only runtime container, API TLS/probes,
PostgreSQL outage/reconnection, container replacement and bounded SIGTERM.
It interrupts the local database and replaces the local application container.

Qualification runs a separate non-race runtime process against the dedicated
local test database and a synthetic TLS device on loopback port 1265. That port
must be free. It samples process RSS/CPU externally, performs a bounded playback,
kills/restarts the process and uses real `pg_dump`/`pg_restore`. It checks
retained idempotency keys, missing keys after restore, write opt-out, recovery
without device writes and shutdown. Results, synthetic credentials and dumps
remain ignored under `.local/qualification`.

The harness is Linux-oriented and uses `/proc`; resource samples are not
production sizing guarantees. No physical-device compatibility, production
backup objective or Kubernetes deployment is established by these checks.
