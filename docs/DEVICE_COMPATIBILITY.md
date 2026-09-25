# Device compatibility

The implementation follows the command and event definitions in **HEOS CLI
Protocol Specification, version 1.17**. Section references below identify that
contract. Manufacturer material is not redistributed in this repository.
Observed device behavior and service safety policy are described separately;
neither extends the manufacturer's guarantees.

## Qualification boundary

Physical checks cover one ungrouped **Denon Home 150**, including identity and
catalog reads, starts from Stop and Pause, muted preparation, explicit takeover
of playing media, low-volume ramps, fades and final Stop. Local DLNA browsing
and playback through Gerbera were exercised through the HEOS device.

The earlier checks did not retain a firmware build; the separate native
container checks below did. Neither set is a firmware compatibility matrix.
Short low-volume checks do not qualify all levels, long-running playback,
group behavior or other models. Other HEOS devices, including the AVC-X3800H,
remain unverified and must not inherit the
Home 150's volume policy. Configure a ceiling only after verifying the target;
writes default off. See [Configuration](CONFIGURATION.md).

Synthetic TLS, replay and fuzz tests verify implementation behavior under
controlled inputs; they do not establish physical-device compatibility. See
[Testing](TESTING.md) for that distinction and the regression workflow.

## Connection and protocol boundary

The manufacturer's document describes plaintext CLI on TCP **1255**. This
service uses the separately observed Secure CLI endpoint on TCP **1265**, with
a configured SHA-256 certificate pin and physical serial/model checks before
writes. TLS support on that port is observed device behavior, not a TLS guarantee
in version 1.17. There is no plaintext fallback or automatic acceptance of a
changed certificate.

The service connects to configured endpoints; it does not discover or bootstrap
devices through SSDP. Commands use bounded CRLF framing, exact IDs and validated
reply correlation. Numeric pagination ranges retain their literal comma
separator; other parameter values, including opaque IDs, are escaped once.
On the tested Home 150, `range=0%2C99` failed for Get Queue while `range=0,99`
succeeded (4.2.15). Catalog acceptance of an escaped comma did not generalize to
queue commands.

Event registration precedes the initial state read to avoid losing changes
between reading and subscribing. This deliberately differs from the document's
recommended initial read-then-register ordering. No state from before the
subscription authorizes writes.

## Implemented scope

| Capability | Current scope |
| --- | --- |
| Identity and groups | Verify the configured player and detect grouping; grouped targets cannot be mutated |
| Observation | Playback state, volume/mute, repeat/shuffle, current media and bounded queue reads |
| Catalog | Configured local sources, bounded HEOS browsing and expiring opaque item references |
| Direct controls | Play/Pause/Stop, next/previous, volume, mute, repeat/shuffle and native queue replacement |
| Bounded playback | Confirmed local media selection, ramp/hold/fade and owned Stop |
| Buffered queue | Ordered part loading with confirmed reserve, played-prefix removal and retained Previous history |
| Events | Registration, typed validated fields, continuity/gap handling and idle heartbeat |
| Diagnostics | Read-only identity/connectivity probes and redacted completion evidence |

Queue replacement uses native `browse/add_to_queue` with `aid=4` (4.4.11), keeping
the selected HEOS source/container identity. It does not convert local media to
an arbitrary stream URL. Source configuration identifies a catalog that the
HEOS device can browse; the service does not implement its own DLNA player.

Account sign-in, cloud-service setup, search, group creation/control, QuickSelect,
firmware updates, arbitrary stream playback and general queue editing are not
exposed. Next and previous use `player/play_next` and `player/play_previous`
(4.2.21, 4.2.22). Their success replies carry only the player id, so the service
confirms a different current entry by bounded readback. Handling Next/Previous
notifications inside an owned queue remains a separate observation path and does
not itself perform that command. API skip is not in the recorded Home 150
qualification set. The exact supported HTTP surface is defined by
[OpenAPI](../api/openapi.yaml) and [API usage](API_USAGE.md).

## Observed Home 150 behavior

| Observation | Consequence in this service |
| --- | --- |
| A successful play-mode reply echoed the requested mode before a later event confirmed application; an immediate GET still showed the previous value | Scalar confirmation waits for a successful reply and matching changed-field events, in either order; it does not treat an immediate old value as proof of intervention |
| Changed volume produced duplicate notifications; unchanged volume could also produce a notification (5.9) | Do not expect an exact count. Identical notifications are harmless only at the currently confirmed volume **and** mute state |
| Setting volume, even to its current level, could clear mute | A volume setter must confirm the target level and valid mute state; an unrelated mute-on change is not accepted |
| Replacing paused media produced intermediate Stop and `unknown` before Play | An acknowledged queue command gets a bounded loading wait; neither state confirms Play or permits a ramp |
| State, queue and now-playing metadata changed separately; an old MID could coexist with a transitional QID (4.2.3, 4.2.5, 4.2.15) | Require the selected final MID/QID pair and complete queue before bounded playback begins |
| During initial loading, `unknown` and a complete selected queue appeared with a nonempty local MID outside the old and selected media | Before the first Play only, wait read-only for metadata to settle; its origin is not inferred from the available evidence |
| Stop could reset current media to the first entry of an unchanged queue | Stop confirmation permits that reset or cleared media, while retaining queue/control checks |
| Manual Next could report Stop before Play in the unchanged queue | Suspend writes during a bounded same-queue transition wait; do not infer manual versus natural origin from the event |
| A now-playing change could temporarily combine IDs from different owned entries while state remained Play | Both IDs must belong to the confirmed queue to permit a read-only wait; only an exact pair can resume automation |

The last observation is supported by a device trace whose final settled pair was
not retained. The permitted wait and rejection boundaries have synthetic
regressions; that trace alone does not demonstrate successful end-to-end handling
of every natural track transition. Unknown intermediate media IDs likewise do
not establish where those IDs came from.

### Native local container playback

Separate checks on **Denon Home 150 firmware 3.139.170**, through pinned TLS at
volume **0**, confirmed single-track playback and native queue replacement for
an 11-track album, a 10-track generic folder, a 49-track genre container and a
99-track artist collection. The generic folder reported `playable=no` in its
parent listing and option 21 in its browse response (4.4.4).

Two library-wide containers containing 199 tracks advertised option 21 and
acknowledged native queue replacement, but left the queue unchanged beyond the
service's 12-second confirmation window. One remained unchanged during a
45-second observation. Tested changes to queue criteria, optional parameters
and browsing on the same connection did not resolve these cases. Read-only
event captures of official HEOS app playback confirmed sustained Play at volume
0 with 199 queue entries. One capture matched all 199 All Audio MIDs and their
metadata; that queue was already present in the initial snapshot, so its
construction was not observed. Events do not expose the app's commands.

Explicit pagination checks on the same firmware at volume 0 did not make a
large container playable in portions. Browsing `range=0,98` or `range=99,197`
and then replacing the queue with that same container left the queue empty
for 15 seconds. Adding `range` directly to `browse/add_to_queue` with `aid=4`
also left it empty for `0,98`, `99,197` and `198,198`. With `aid=3`, ranges
`0,98` and `99,197` left an existing 11-track playing queue unchanged for
15 seconds. The control album played normally: `range=0,0` with `aid=4`
loaded all 11 tracks, and with `aid=3` appended all 11 (22 total), instead of
selecting one track. Replies echoed the supplied `range` and reported success;
complete queue readbacks showed that it did not restrict membership. These
checks provide no range-based queue-loading alternative on this firmware.
The test queue was cleared and the player left stopped at volume 0.

A separate app packet capture contained encrypted TLS application data on TCP
10101. Plaintext renderer HTTP on TCP 60006 contained only `GetPositionInfo`
requests. Queue notifications reported 30 and then 199 entries, but the capture
did not reveal the queue-changing command or its parameters. The encrypted
payload size does not establish whether the app submitted a container or a list
of tracks.

Server-side captures separated successful catalog delivery from the native
queue failure. Ordinary browsing requested 100 entries per DLNA call and
received pages of 100 and 99. Native replacement requested 1000 entries in one
`BrowseDirectChildren` call; Gerbera returned HTTP 200 with all 199 selected MIDs
in a complete, parseable 317,046-byte SOAP body in approximately 0.8 seconds.
The queue and Pause state still remained unchanged after twelve seconds.
A separate official-app replacement produced the same 199-MID queue without
any captured catalog request to Gerbera. That observation does not distinguish
cached metadata from metadata supplied by the app.

Documented single-track additions with `mid` and `aid=3` successfully extended
an already playing queue while preserving its existing MID/QID prefix and
current track, at volume 0. A sequential test confirmed 30 entries after about
48 seconds and 109 entries before its 180-second test budget expired. This was
a test deadline, not a rejected append; it did not qualify a complete 199-track
load. Two subsequent three-track sequences with one final queue read each took
about 4.1 seconds each. Moving readback to a group boundary therefore did not
remove the substantial per-command latency. A separate `aid=3` addition of the
whole 199-track container acknowledged success but left a one-track playing
queue unchanged for more than twelve seconds. Neither parallel writes nor
unconfirmed batch continuation is enabled by these diagnostic tests.

A subsequent volume-0 check with a Gerbera virtual batch layout loaded the
199-track collection through six leaf containers of 16, 45, 45, 45, 45 and 3
tracks. Native `aid=4` started the first part; five `aid=3` container additions
extended the queue from 16 to 61, 106, 151, 196 and finally 199 entries. Every
addition preserved the existing MID/QID prefix, the current MID/QID pair and
Play at volume 0; progress events continued advancing. Complete readbacks
matched the ordered union of the browsed parts, without missing or duplicate
MIDs. The five additions took approximately 7.5 seconds including confirmation,
with each addition confirmed in approximately 1.3–1.4 seconds. This timing
excludes starting the first part. Native success replies arrived before the
queue changes and were not used as completion evidence. These direct CLI
checks qualify this collection on the tested firmware. The public API now
accepts an ordered `item_refs` plan for automatic multi-container loading; see
[API usage](API_USAGE.md). That implementation has synthetic TLS coverage;
these earlier direct CLI checks do not qualify the integrated controller path.

A volume-0 check also qualified newly generated local-file M3U playlists on
Gerbera 3.3.0. A temporary instance used a separate catalog and the same media
mounted read-only; its standard playlist importer picked up fresh files after
startup. One playlist reversed the 16 tracks of the first part; another arranged
45 different tracks in a specified permutation. DLNA and HEOS browse results
preserved both requested orders. Native `aid=4` started the reverse playlist,
and one `aid=3` command appended all 45 tracks in approximately 1.3 seconds
including confirmation. The complete 61-entry queue matched the ordered
playlists, retaining the existing MID/QID prefix and current track. Next selected
the expected second track; progress advanced on both played tracks without
playback errors. Cleanup confirmed Stop, volume 0 and an empty test queue before
removing the temporary server. This qualifies fresh immutable playlist containers
with M3U import enabled; it does not qualify rewriting an existing playlist or
loading all 199 tracks through one playlist command.

Separate static analysis of **Android HEOS 3.139.350** identified a Play All
handler that builds an explicit media list. Its native SDK has a Protobuf queue
path that splits lists into Play/Add actions, with track metadata and playable
resource URLs. A fast-response portion defaults to 30 entries; queue/message
limits can be updated from the device. This is consistent with the captured
30-to-199 queue notifications, but does not establish the iPhone app's exact
commands or a workaround through the supported CLI. Protobuf message defaults
do not establish a limit on DLNA SOAP response size.

The cause of the failure after catalog delivery remains undetermined; these
checks do not establish a track-count or response-size limit. An advertised
playable container and a successful command reply do not confirm queue
application. The service retains an uncertain outcome for an unconfirmed
replacement and does not replay it automatically.

### Played-prefix removal

Separate native CLI checks on **Denon Home 150 firmware 3.139.170**, through
pinned TLS at volume **0**, exercised `player/remove_from_queue` (4.2.17).
An 11-track album was queued twice, retaining repeated MIDs as distinct queue
occurrences. After navigation to the fourth entry, removing the first two
produced exactly the expected 20-entry ordered suffix. The device reassigned
remaining QIDs from `1..22` to `1..20`; the current QID changed from 4 to 2 while
the current MID, its logical occurrence and Play were preserved. A second
played-prefix removal reduced 20 entries to 19 with the same guarantees.
Complete confirmations took approximately 1.18 and 1.20 seconds.

For the first removal, the final success reply arrived before a now-playing
notification and then a queue notification. The first complete queue read and
subsequent media read already showed the settled suffix and rebased current
occurrence. This trace does not establish an atomic update or universal event
order. Progress continued advancing. Next worked, and Previous immediately
after that Next returned to the retained preceding occurrence. An earlier
Previous after longer playback briefly reported the preceding entry and then
returned to the original entry with reset progress; it was not treated as a
confirmed navigation to a different entry. No playback errors were reported.
Cleanup confirmed Stop, an empty queue and volume 0.

These native checks alone qualify the removal path on that device and firmware,
not all capacities, long-running sessions or controller integration.
In particular, they do not qualify removal racing with natural navigation or
ambiguous repeated-current-MID transitions. The controller retains conservative
bounded confirmation for those cases and never assumes QID stability.

### Buffered controller playback

A subsequent check used the implemented controller through its public HTTPS API,
an isolated local journal and a configured volume ceiling of **0**, on the same
Home 150 firmware. The plan repeated an 11-track album three times, with refill
threshold 10, resident limit 24 and one retained Previous entry. The first two
parts loaded to 22 entries in approximately 1.9 seconds; the third remained
unloaded while the reserve exceeded the threshold.

Eleven same-principal API Next operations completed as independent Skip results
while the playback operation retained ownership. The eleventh reached the refill
threshold: the controller removed ten played entries and appended the last part.
The completed plan reported 33 cumulatively confirmed tracks, ten pruned tracks,
23 resident tracks and absolute current index 11. A complete native read matched
the exact ordered suffix, including repeated MIDs, with current playback at the
second resident occurrence after QID reassignment. The runtime sent exactly three
queue additions, one removal and eleven Next commands; its sole volume setter
requested 0. No playback errors were recorded.

This qualifies that bounded refill/prune and same-owner Skip sequence, not a
10,000-track session, arbitrary queue capacity, long-duration recovery or pruning
concurrent with uncontrolled navigation. Cleanup used confirmed API Stop, ended
the test controller and cleared the test queue; the final state was Stop with an
empty queue and volume 0.

## Event semantics and failure limits

State events (5.4) report state, not transition origin. Now-playing and queue
notifications (5.5, 5.8) do not contain enough information to prove current media
membership. Valid volume/mute, repeat and shuffle events (5.9–5.11) can confirm
their reported controls. Progress (5.6) is display telemetry only: it triggers no
read, it is not ownership evidence, and a stored sample does not confirm a command.
The Home 150 qualification does not measure its emission interval.
Missing, duplicate or malformed **fields** cannot confirm a command.
Unknown events remain visible, and stream gaps invalidate continuity.

The service's twelve-second confirmation and transition limits are application
policy, not measured firmware latency guarantees. Normal scalar steps use the
existing event stream; one targeted fallback is reserved for missing events near
the confirmation deadline. A full baseline is audited every 300 seconds, and
events do not renew its age. [Architecture](ARCHITECTURE.md) describes the read
budgets and [Queue decisions](QUEUE_DECISIONS.md) describes ordered wait rules.

A device reply acknowledging a setter does not prove application, and no event
proves exclusive control. Changes that cannot be safely attributed revoke
ownership. A transition wait necessarily also permits manual Stop followed by
Play within its window; API Stop and Pause cancel immediately. Disconnects,
identity/pin changes, incomplete queues and uncertain commands do not authorize
new writes. Ambiguous writes are never automatically replayed, and restart does
not resume playback. Operational recovery is covered in [Operations](OPERATIONS.md).
