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
capabilities, `volume_ceiling` and visible active operation. `volume_ceiling` is
the highest HEOS level a write to that player may request, or null when no
verified ceiling is configured. It is configuration, so it does not by itself
change `revision`. Unknown/stale state does not authorize writes. The revision
is also exposed as a quoted `ETag` on player detail.

Player list, detail and preflight always include `now_playing`. For example,
the field can contain:

```json
{
  "song": "Example track",
  "album": "Example album",
  "artist": "Example artist",
  "queue_id": "2",
  "media_id": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "stale": false
}
```

Clients that validate responses against an older strict schema need the updated
OpenAPI contract containing this field.

Play and Pause retain observed media. Stop, unknown state, disconnection and
absence of a verified observation for the current connection return `null`.
An object with empty song/album/artist strings means the device supplied media
with incomplete labels; it differs from `null`. While connected Play/Pause is
being verified, the last known object remains available with `stale: true`.
It is display data, not confirmed new media. A state-only return from Stop or
unknown to Play cannot make old metadata fresh; that requires an accepted media
read while playing or paused. Until then it can remain stale, including until
the existing background audit if no new metadata event arrives.

`queue_id` is optional and identifies an entry within this player's current
queue. `media_id` is an optional opaque service identifier derived from native
media identity, without exposing the native ID or a media URL. It supports
equality within the same player/source and running process; display-text changes
do not change it, but a service restart may. Repeated occurrences of the same
media may have different queue IDs. Neither identifier is a playable `item_ref`
or a Gerbera library ID. Missing identifiers are omitted.

Media identity, labels and freshness participate in player `revision` and the
detail ETag. A metadata change can therefore invalidate `If-Match` for a new
mutation; accepted retries keep their original key and input. Revision is an
opaque equality token, not a track identifier or a numeric sequence.
`Player.observed_at` remains the last full-observation time; targeted metadata
updates do not renew it.

`now_playing.progress` is an optional last sample of the playhead:
`position_ms`, optional `duration_ms`, and `sampled_at`. Units are milliseconds.
`duration_ms` is absent when the device does not know the duration. The object
is present for fresh Play or Pause after a sample bound to that media. It is
absent while media is stale or unverified, and whenever `now_playing` is null.
A full observation or a change of media identity clears progress. Delayed samples
received before that boundary are discarded.
The service does not interpolate between samples and does not provide seek.
Playhead samples are outside player `revision` and the detail ETag, so
`If-Match` and `player_changed` stay tied to control and media changes. To move
a progress display, poll the player about once per second. The poll does not
cause a device read. An unchanged `sampled_at` means no newer sample has arrived.

Current media comes from the native now-playing read, without inferring queue
position. Firmware can briefly report mixed media/queue identifiers, so even
fresh display data does not establish atomic queue membership or playback
ownership. Queue pages are not current-track evidence. To continue a queue
traversal, pass `next_offset` with its `revision`; restart at offset zero after
`stale_reference`, including when a media change advances the player revision.

Catalog references are opaque and expire: pass a browsable element's
`item_ref` as the next request's `parent_ref`, and follow returned `next_cursor`
values for pagination, including every page needed for your selection. Use
`--get --data-urlencode` when passing references in query parameters. Do not
construct references from titles or HEOS identifiers, choose the first ambiguous
match, or assume a displayed album has complete playable membership.

## Prepare and submit playback

The client selects a playable media item or container and retains its current
`item_ref`. A catalog item with `kind: media` and `playable: true` can start a
single song/track through the same playback endpoint.
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
| `POST /v1/players/{player}/skip` | `control` | `direction: next/previous`, `takeover` |
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

The API does not expose arbitrary HEOS commands or media URLs. `POST /skip`
navigates the current queue by one native next or previous command (HEOS CLI
Protocol Specification 1.17, 4.2.21 and 4.2.22). It is admitted only while the
player is playing or paused, the volume is at or below the configured ceiling,
and the current entry is an exact member of the complete queue. With repeat off,
a single entry is not skippable in either direction, and with shuffle also off
the last entry rejects next and the first entry rejects previous. Those cases
return 422 `not_skippable` and send nothing. Repeat `on_all` and `on_one` are
still sent. Acceptance does not mean the entry changed: confirmation requires a
different entry in that same queue and settled play or pause within twelve
seconds. The native success reply identifies only the player. An acknowledged
command that never shows a new entry is not replayed; retry the original
idempotency key to read that outcome.
Transitional Stop/unknown or a mixed MID/QID pair from that queue permits only
waiting. Confirmation needs an exact pair and reconciliation of newer events;
repeated notifications do not extend the twelve-second deadline.

A native refusal such as `eid=17` finishes as `failed` / `device_rejected`.
The service attempts one bounded read-only refresh so later volume and transport
commands can use current state. If that refresh fails, ordinary controls remain
unavailable until observation recovers. The rejected command is never replayed.

Skip follows the other direct controls. While an operation owns the player,
`takeover: false` returns 409. `takeover: true` requires operator scope, releases
that run, including its stop timer, and then skips. Observed manual or natural
Next/Previous during an owned run still preserves the ramp, fade and stop
deadline; that observation is not this command. Shuffle can make the confirmed
entry non-adjacent. Repeat-one and an unchanged entry are not reported as a
confirmed skip.

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

`player_changed` covers current-media identity, display-text and freshness
changes as well as other player state. Its data remains identifier-only:

```text
event: player_changed
data: {"player":"room-speaker"}
```

Refetch that player and compare confirmed `now_playing` identity, queue entry
and labels before emitting a consumer `TRACK_CHANGE`. A volume change, audit or
duplicate native hint is not itself a new track. A refresh can first return
stale old metadata; verification completion changes the revision and produces
another invalidation if the first was already published. Null means no usable
current-media observation, not proof that a Stop command succeeded.

Notifications converge on the latest observed snapshot. The existing 250 ms
metadata debounce and 500 ms in-memory revision watcher can coalesce rapid
transitions; SSE is not an exactly-once history of every track. Device response
time and recovery affect latency. A gap/restart requires fresh snapshots;
process-scoped media IDs must not be compared across service restarts.
