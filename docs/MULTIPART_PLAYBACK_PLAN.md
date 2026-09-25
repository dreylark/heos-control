# Ordered multipart playback

## Scope and ownership

The goal is to play a client-selected, explicitly ordered collection through
small native HEOS queue commands. Fresh Gerbera playlists have been qualified
on a Home 150 at volume 0: replacement with 16 reversed tracks and append of
45 permuted tracks preserved the complete order and existing MID/QID prefix.
The append took approximately 1.3 seconds including readback. These observations
are not a firmware track-count limit.

Only the **heos-control** work below is authorized for this implementation.
Stentor, fleet, deployment, releases and further physical-device qualification
are separate work. Existing unrelated changes must be preserved.

| Component | Responsibility |
| --- | --- |
| Stentor UI | Selection, sort order, starting track, preparation/loading display |
| Stentor Go BFF | Expand the complete selection, freeze its order, generate immutable playlist parts, resolve fresh opaque references, submit one playback operation |
| fleet / Gerbera | Persistent playlist directory, scoped mounts, playlist MIME mapping and a separate bounded autoscan |
| heos-control | Durable admission, ordered replace/append execution, complete queue confirmation, ownership, volume and the original automation timeline |

## Client and Gerbera work (deferred)

1. Fix the complete ordered selection before playback, including unloaded UI
   pages. Shuffle, when requested, produces one saved permutation; native shuffle
   stays off for an ordered plan.
2. Generate new playlist files for each request using atomic publication.
   Retries reuse the same saved plan and idempotency input. Do not rewrite a
   playlist that is already being used.
3. Start with configurable 30-track and 50-track parts, subject to physical
   qualification. Chunk sizes are client policy, not a HEOS protocol limit.
4. Wait for all parts and their complete contents to appear in Gerbera, then
   resolve their opaque `item_ref` values through the public heos-control API.
5. Submit the complete reference list once. Playback and queue loading after
   admission must not depend on the browser or BFF remaining connected.
6. Keep playlist files across BFF restarts. Cleanup must account for queues that
   remain on players after the submitting operation finishes; request completion
   or a short TTL alone is insufficient evidence that a playlist is unused.

The successful hardware test used local filesystem paths in M3U. Stentor's CDS
adapter receives resource URLs. Before implementing its generator, qualify URL
entries, metadata, formats and navigation at volume 0; otherwise explicitly
design a Gerbera adapter for local paths. Do not silently couple Stentor to
Gerbera's database schema or assume opaque HEOS references are Gerbera IDs.

## heos-control contract

- Extend playback and its read-only preflight with `item_refs`: an ordered,
  nonempty list of at most 32 references and 10,000 tracks in total. Exactly one of `item_ref` and `item_refs` is required.
  Existing single-item requests retain their behavior.
- An ordered plan requires native shuffle off. Each part is an individual local
  track or a leaf container with playable local tracks. Preserve repeated tracks
  and references; do not reduce an ordered sequence to a membership set.
- Resolve every reference and freeze each part's ordered contents before
  admission and before any device write. Bound parts, traversal, total tracks,
  JSON size and execution time. Reject empty, stale, ambiguous and invalid plans
  without changing playback.
- Persist the normalized request, selected-plan identity and loading progress
  through the existing journal. Matching retries return the admitted operation;
  a different order under the same idempotency key conflicts. Restart recovery
  remains interrupted/uncertain and never replays a device command.
- Expose confirmed/total part and track counts separately from the automation
  timeline. Partial loading and uncertain delivery remain visible on failures,
  cancellation and history reads.

## Execution and confirmation

1. Use the existing single worker, connection and synchronous event callback.
   Prepare volume/mode/mute and replace-and-play the first part with native
   `aid=4`.
2. Confirm the complete first part in the requested order and its current
   MID/QID before enabling ownership. ACK alone is never completion.
3. Anchor automation to that first confirmed playback. Append remaining parts
   using native `aid=3` between automation actions. Do not start a second writer,
   move the timeline, replay missed volume steps or postpone priority Stop.
4. Register each append expectation before send. Its bounded confirmation must
   preserve every existing MID/QID entry in order and establish the exact new
   suffix, complete count, unique QIDs and a valid current pair. Legitimate
   in-queue navigation retains the timeline; unexpected queue edits, Pause,
   controls, groups and lost continuity retain cancellation behavior.
5. Queue/media reads may straddle a transition. Discard stale/incomplete reads
   and retry observations within the original confirmation deadline; never
   replay a queue command to repair uncertainty. Events do not extend deadlines.
6. Do not start additional appends once the playback window is ending. Final
   fade/Stop and cancellation retain their existing safety guards. An unfinished
   load must not be reported as fully successful.

## Delivery and validation

1. Add failing API, selection, protocol and ownership tests before behavior.
2. Implement ordered-plan admission and native container append.
3. Implement append confirmation, serialized loading/automation and persisted
   progress; preserve single-item behavior and API compatibility.
4. Update OpenAPI and current architecture/API/queue documentation; generate
   bindings from their inputs.
5. Cover exact order and repeats, preserved prefixes, async replies/events,
   stale pagination, navigation, manual intervention, cancellation, deadlines,
   client disconnect, idempotency and restart recovery with synthetic devices
   and injected clocks.
6. Run `make verify`, pinned golangci-lint, relevant bounded fuzz campaigns and
   exclusive disposable PostgreSQL integration/coverage for persistence changes.
7. Qualify the integrated client flow separately on the speaker at volume 0
   before considering the production feature complete.

## Implementation status

- [x] Record the agreed cross-repository plan and implementation scope.
- [x] Ordered playback input, bounded resolution and read-only preflight.
- [x] Native container append and ordered queue confirmation.
- [x] Serialized loading with the original automation timeline.
- [x] Durable loading progress and public contract/documentation.
- [x] Required automated verification and review.
- [ ] Stentor/fleet integration and additional physical qualification (deferred).

## heos-control validation

Completed on 2026-09-25:

- `make generate` and `make verify`: generated bindings, formatting, vet, unit
  tests, race tests and build passed.
- Pinned golangci-lint v2.13.2: no issues, including tagged tests.
- `make coverage`: exclusive disposable PostgreSQL integration and race-enabled
  coverage passed. Overall maintained-code coverage is 90.42%; control is 90.55%.
  Existing coverage floors and exclusions are unchanged.
- Four 30-second fuzz campaigns passed: HEOS command encoding, initial queue
  policy, active transition policy and ordered append invariants.
- Synthetic pinned-TLS tests cover multiple parts with both reply/event orders,
  ordinary and bounded playback. Injected-clock regressions cover repeats,
  partial first loading, delayed/no-op append, stale pagination, navigation,
  intervention, priority Stop, fade cutoff and closure of event attribution.
- PostgreSQL tests retain confirmed partial counts and frozen plan identity
  across recovery, reject reordered retries and never create replay work.

The current implementation has not been deployed or qualified end to end on a
physical speaker. Earlier native playlist tests establish the protocol evidence
listed above; integrated client generation and this controller version need their
separate volume-0 qualification. No Stentor or fleet implementation is included.
