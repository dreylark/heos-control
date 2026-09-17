# API usage

[OpenAPI](../api/openapi.yaml) defines the exact schemas and responses. This guide
explains how to combine them. Optional `/docs` renders the running binary's
contract; enable it through configuration or Helm. Requests require a bearer
token, explicit scopes and player grants as described in
[configuration](CONFIGURATION.md#credentials-and-scopes).

## Observe and browse

For a local instance, use the generated CA; for an installed service, use its
trusted CA and DNS name. Never disable certificate verification to make examples
work.

```sh
HEOS_URL=https://localhost:8443
HEOS_TOKEN=$(cat .local/client-token)
PLAYER=room-speaker
heos() {
  curl --fail-with-body --silent --show-error --cacert .local/ca.crt \
    --header "Authorization: Bearer $HEOS_TOKEN" "$@"
}
heos "$HEOS_URL/v1/players" | jq
heos "$HEOS_URL/v1/players/$PLAYER" | jq
heos "$HEOS_URL/v1/sources" | jq
heos "$HEOS_URL/v1/sources/music-library/items" | jq
```

Player results include availability, freshness, an observation revision,
capabilities and visible active operation. Unknown/stale state does not authorize
writes. The revision is also exposed as a quoted `ETag` on player detail.

Catalog references are opaque and expire: pass a browsable element's
`item_ref` as the next request's `parent_ref`, and follow returned `next_cursor`
values for pagination, including every page needed for your selection. Use
`--get --data-urlencode` when passing references in query parameters. Do not
construct references from titles or HEOS identifiers, choose the first ambiguous
match, or assume a displayed album has complete playable membership.

## Prepare and submit playback

The client selects one playable container and retains its current `item_ref`.
The same body is accepted by preflight and playback. This is an illustrative
request; choose levels within the player's verified ceiling:

```json
{
  "item_ref": "replace-with-current-item-reference",
  "queue_mode": "replace",
  "shuffle": true,
  "repeat": "off",
  "initial_volume": {"unit": "heos", "level": 2},
  "takeover": false,
  "automation": {
    "target_volume": {"unit": "heos", "level": 5},
    "ramp_seconds": 60,
    "duration_seconds": 600,
    "fade_seconds": 30
  }
}
```

Save the chosen body as `.local/playback.json` in the ignored local directory. Preflight needs
`read` scope and does not reserve the player or change playback. A request with
`takeover: true` additionally requires `operator` scope.

```sh
heos --request POST --header 'Content-Type: application/json' \
  --data-binary @.local/playback.json \
  "$HEOS_URL/v1/players/$PLAYER/preflight" > .local/preflight.json
jq . .local/preflight.json
```

Proceed only when `ready` is true; inspect warnings and the player observation.
Playback needs `operator` scope. Persist the body, key and precondition before
submitting, so an ambiguous response can be retried exactly:

Create the submission record **once for this new operation** (keep an existing
record after an ambiguous response):

```sh
umask 077
REVISION=$(jq -er '.player.revision' .local/preflight.json)
IDEMPOTENCY_KEY=$(openssl rand -hex 16)
jq -n --arg key "$IDEMPOTENCY_KEY" --arg revision "$REVISION" \
  --slurpfile body .local/playback.json \
  '{idempotency_key: $key, revision: $revision, body: $body[0]}' \
  > .local/submission.json
```

Send or retry from that saved record, without generating another key or selecting
media again:

```sh
REVISION=$(jq -er '.revision' .local/submission.json)
IDEMPOTENCY_KEY=$(jq -er '.idempotency_key' .local/submission.json)
jq '.body' .local/submission.json > .local/submission-body.json
heos --request POST --header 'Content-Type: application/json' \
  --header "Idempotency-Key: $IDEMPOTENCY_KEY" \
  --header "If-Match: \"$REVISION\"" \
  --data-binary @.local/submission-body.json \
  "$HEOS_URL/v1/players/$PLAYER/playback" > .local/operation.json
jq . .local/operation.json
```

**202 means the operation and reservation were committed, not that music has
started.** Follow the returned `Location` or operation ID until terminal.
Preflight is optional and can become outdated before submission: playback checks
admission independently. A regular player read can supply the initial revision.

Omit `automation` for a short playback operation that completes after confirmed
queue/start and leaves music playing without a timer. With automation, all four
fields are required: duration 1..7200 seconds, fade 0..60 and less than duration,
ramp from 0 through `duration - fade`, and `0 <= initial <= target <= ceiling`.
Repeat must be off. The ramp and final fade fit inside the original duration;
track changes do not restart the clock.

A paused/stopped unreserved player can start with `takeover: false`. A playing
player is rejected for bounded playback unless explicit takeover is allowed.
The service confirms preparatory Stop when replacing currently playing media,
then establishes the selected native queue and confirmed playback.

## Ownership and intervention

An accepted bounded run belongs to the service, independent of the client's
connection. Natural track transitions and manual Next/Previous preserve it while
the complete verified queue is unchanged. Pause, conflicting volume/mute/mode
changes, queue/source/group changes and missing event continuity can release
ownership. Future writes, including the final stop, are then removed.

HEOS does not identify the origin of every event. During a same-queue transition,
Stop/unknown or a temporarily inconsistent media pair suspends writes for up to
12 seconds, capped by the original playback deadline. Confirmed Play and unchanged
ownership evidence can resume the run. Manual Stop followed by Play within that
window may therefore preserve automation too. API `/stop` and Pause cancel
immediately. See [queue decisions](QUEUE_DECISIONS.md) for the precise policies.

After a service restart, unfinished operations become uncertain/interrupted work;
they are not resumed and the service does not stop the speaker speculatively.
Database loss also suspends further normal automation writes. Inspect operation
outcomes before starting another run.

## Direct controls, stop and cancellation

| Route | Scope | Body |
| --- | --- | --- |
| `PUT /v1/players/{player}/volume` | `control` | `unit: heos`, `level`, `takeover` |
| `PUT /v1/players/{player}/mute` | `control` | `muted`, `takeover` |
| `PUT /v1/players/{player}/transport` | `control` | `state: play/pause/stop`, `takeover` |
| `POST /v1/players/{player}/stop` | `operator` | `fade_seconds: 0..60` |
| `POST /v1/operations/{operation}/cancel` | `control` | `mode: release/stop_owned`, optional `fade_seconds` |

All mutations use `Idempotency-Key`; ordinary player mutations also require a
quoted `If-Match`. Stop and cancellation do not require a player revision.
`takeover: true` adds an operator-scope requirement. Creator/operator and player
authorization still apply to cancellation.

`release` abandons future automation without stopping music and cannot specify a
nonzero fade. `stop_owned` stops only while the target operation still owns
playback. Operator `/stop` is an independent priority command and may interrupt
another operation. It still requires a reachable device, database and valid
admission evidence; it is not an emergency hardware-off mechanism.

The API does not expose arbitrary HEOS commands, media URLs or next/previous
buttons. Native-app navigation is handled as an observed transition.

## Retries and errors

Idempotency keys are 1..256 ASCII letters/digits or `.`, `_`, `:`, `-`. After
authorization, a matching key and input returns the original operation before
mutable revision/reference checks. Reusing a key for different input returns
409. Keep the original body and preconditions on retries, even if the current
revision has advanced. Do not choose a new album or key after a lost response.

| Response | Typical action |
| --- | --- |
| 400 | Correct malformed input, duplicate/unknown fields or headers |
| 401 / 403 | Correct credentials, scopes or player grants |
| 409 | Inspect ownership/conflict or changed idempotent input |
| 412 / 428 | Refresh the revision for a genuinely new request / supply `If-Match` |
| 422 | Correct unsupported values or device policy violations |
| 503 | Inspect readiness and delivery; resolve ambiguous submission with the original key/input |

Use the structured error code and delivery outcome in addition to HTTP status.
Device commands with ambiguous delivery are not automatically replayed. Keys and
terminal operations are retained for at least seven days; this is not permanent
deduplication storage.

## History and events

`GET /v1/operations/{id}` returns a permitted operation. `GET /v1/operations`
returns `{items, next_cursor}` using PostgreSQL only. With `read` scope, callers
see their own operations for granted players; `operator` additionally permits
other creators' operations on those players.

History supports `player`, `state`, `kind`, `delivery`, `created_from` (inclusive),
`created_before` (exclusive), `limit` (1..100, default 50) and `cursor`. Times use
RFC 3339 with timezone. Continue with the same normalized filters and page size.
Pages are a live descending `(created_at, id)` view, not a snapshot export or a
total count; updates and retention can affect later pages.

Terminal `outcome.diagnostics` can explain the reason, decision rule, last active
phase, detection time and changed scalar fields. It excludes private media and
raw protocol text. Diagnostics may be absent for old/unfinished records and
cannot reconstruct in-memory evidence lost in a crash. Delivery certainty is
independent of the explanatory reason; tolerate new reason values.

`GET /v1/events` streams authorized SSE invalidations. Use notifications to reread
resources; they are not a durable history or proof that a command succeeded.
Reconnect with `Last-Event-ID` when available and refresh snapshots after a gap.
Read clients must handle unknown/stale observations without treating them as
permission to write. Slow subscribers are bounded and can be disconnected.
