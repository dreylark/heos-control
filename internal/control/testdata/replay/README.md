# HEOS incident replay corpus

These fixtures drive `TestReplayIncidents` through the real coordinator and a
fake device/operation clock. `TestReplayTLS` also runs `delayed-mode`,
`duplicate-volume` and `loading-unknown` through the real TLS client, observer,
ownership callback and ordinary event consumer. Everything uses synthetic local
identities; no fixture can select a network address.

## Evidence

The [device compatibility guide](../../../../docs/DEVICE_COMPATIBILITY.md)
records protocol assumptions and firmware findings used by these scenarios. Current fixtures
are **reconstructed** from partial observations or **synthetic** variations,
never complete raw captures. All firmware versions are unknown (`null`). Notes
identify invented timing, symbolic intermediate media and successful continuations
that were not present in the original evidence. `captured` is reserved for future
sanitized captures whose order and payloads are actually recorded.

MID/QID relationships, local source `1024`, duplicate events and reply/event order
are significant. Player `1`, server `900`, album `green`, and track identifiers
are synthetic; they do not identify a deployment or private media. Review new
fixtures for URLs, addresses, serials, titles and credentials before committing.

## Format version 1

The strict loader rejects unknown fields, trailing JSON, unsupported versions,
duplicate request anchors, invalid actions/times, more than 256 steps, or files
over 256 KiB. A fixture contains:

| Field | Meaning |
| --- | --- |
| `name`, `family`, `provenance`, `source`, `model`, `firmware`, `notes` | Evidence and scenario description; `source` is relative to the repository root |
| `initial` | Stop/Pause, volume, mute, repeat and shuffle; baseline has one old track |
| `automation` | Bounded playback settings, using the existing API-independent Go type |
| `scripts` | Exceptions to the documented immediate-response synthetic baseline |
| `expect` | Outcome, command/read budgets, exact volume progression and timing |

The requested operation starts at volume 10, replaces the old queue with two
selected tracks, sets repeat off/shuffle on and uses the fixture's automation.
Volumes are synthetic; these tests never control a speaker.

Each script matches `on` (canonical command), one-based `occurrence`, and the
declared subset of `args`. Each script must be consumed. Its ordered steps use
`at_ms` relative to that request's arrival, and one action:

- `apply`: change device state via a partial `patch`. Queue is `old` or `selected`;
  media explicitly names `mid`, `qid` and `source`.
- `event`: send an independent notification. The fake-device layer also accepts
  an explicit transport `gap`; it is not invented as a HEOS wire event.
- `reply`: acknowledge with `success` or `rejected`.
- `disconnect`: close delivery with an uncertain outcome.

Each script has exactly one reply or disconnect. Events and state changes may
precede or follow it, including at the same logical time. GETs only serialize
current state: additional reads cannot make a track start. The TLS layer echoes
the real request's arguments and SEQUENCE; it does not disable correlation.
Unexpected command names, mismatched arguments and unconsumed required steps
fail. Negative tests exercise these failures, including cleanup while a client
is awaiting a reply.

The baseline implements immediate state changes for unscripted valid setters and
synthetic catalog/GET responses. It is deliberately separate from incident
evidence. This is a small scenario format, not a general protocol simulator.

## Expectations and budgets

`state` and diagnostic `reason` must match exactly. `volume_levels` is the exact
ordered list of attempted volume writes; `queue_writes` is exact too. Successful
runs must reach zero and end with Stop, with the original playback duration in
persisted progress. `end_ms`, optional exact `volume_at_ms`/`stop_at_ms`, and
half-open `no_writes` intervals are checked by the deterministic executor layer.
The full-envelope case checks each volume timestamp and Stop at 1200 seconds
even after navigation at 268 seconds; delayed completion cannot conceal early
fade or Stop.

`max_full_reads` counts full observation calls after the established baseline.
`max_scalar_reads` is also checked for equality: a required fallback must really
happen, and an event-confirmed case must not perform one. `max_wire_writes` bounds
all setters/queue replacement. `max_wire_gets` is checked by the TLS cases and
includes **all nonmutation commands**, including subscription, catalog and
background observation. Server logs are the independent authority; Prometheus
wire counters must agree with them. Existing wire-budget assertions additionally
restrict where reads may occur during ramp/fade/queue confirmation.

The three TLS scenarios cap nonmutation commands at 60, 60 and 76 respectively,
including allowance for one full eight-command stale-read retry, and cap writes
at 11 each. These are whole-scenario budgets with connection setup and
initial/final confirmation, not the per-volume-step cost. A scalar fallback is
zero in these three cases.

Controller waits use virtual time. Socket deadlines, observer heartbeat/audit
timers and the TLS watchdog still use real time. A virtual 1200-second run does
not establish background observer timer behavior; existing dedicated clock tests
cover that separately. Do not add real sleeps to imitate long playback.

## Add or run a case

1. Start from a small relevant fixture, preserve identifier relationships, and
   explain provenance and synthetic assumptions.
2. State the expected contract result independently of current output. Include
   successful progress as well as cancellation/timeout cases. Do not regenerate
   expected outcomes or raise budgets merely to make a regression pass.
3. Run `go test ./internal/control -run '^TestReplay' -count=1`. To repeat with
   race detection, use `go test -race ./internal/control -run '^TestReplay' -count=20`.
4. For a production failure, keep the failing scenario before implementing a fix.
   Fuzz minimized failures belong in Go's `testdata/fuzz/<Target>/` layout after
   review, with a named regression where that makes the defect easier to read.

The [testing guide](../../../../docs/TESTING.md#native-fuzzing) lists all five
native fuzz targets, campaign commands and remaining limitations.
