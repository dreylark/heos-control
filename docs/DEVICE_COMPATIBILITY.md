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

The recorded evidence does not identify a firmware build, so these findings are
not a firmware compatibility matrix. Short low-volume checks do not qualify all
levels, long-running playback, group behavior or other models. Other HEOS
devices, including the AVC-X3800H, remain unverified and must not inherit the
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

## Event semantics and failure limits

State events (5.4) report state, not transition origin. Now-playing and queue
notifications (5.5, 5.8) do not contain enough information to prove current media
membership. Valid volume/mute, repeat and shuffle events (5.9–5.11) can confirm
their reported controls; progress (5.6) is not ownership evidence and triggers no
read. Missing, duplicate or malformed **fields** cannot confirm a command.
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
