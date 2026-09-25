# Queue decision policies

Queue confirmation and active track transitions use separate ordered decision
tables. Initial confirmation establishes ownership of the requested queue;
active transitions preserve an already confirmed complete queue. Their permitted
intermediate media therefore differ.

The pure policies live in
[`queue_start_policy.go`](../internal/control/queue_start_policy.go) and
[`queue_policy.go`](../internal/control/queue_policy.go). Each returns an action,
a rule identifier and, when needed, an error classification. The first matching
rule wins. Inputs include the current time and immutable snapshots; policies do
not read clocks, perform I/O, log, cancel contexts or mutate their inputs. There
is no runtime rule language or configurable rule order.

For the wider lifecycle and device evidence, see
[Architecture](ARCHITECTURE.md) and [Device compatibility](DEVICE_COMPATIBILITY.md).

## Execution boundary

[`events.go`](../internal/control/events.go) applies event decisions under the
execution mutex. [`queue_transition.go`](../internal/control/queue_transition.go)
applies observation decisions under the same mutex, then performs waiting,
database checks and device I/O outside it. Pending-event revision checks and
clearing a transition wait are atomic with respect to the callback: an older
Play snapshot cannot confirm a Stop that arrived after it.

`queuePolicyState` is a captured value, not an additional mutable state machine.
Its zero `WaitUntil` means no transition is pending; a nonzero value preserves
the first transition's deadline.

| Action | Meaning |
| --- | --- |
| `pass` | Continue ordinary command attribution or ownership checks; no permission to write |
| `ignore` | No additional transition action; do not renew a deadline |
| `observe` | Request coalesced verification; the event itself is insufficient |
| `wait` | Suspend writes while the existing confirmation budget remains |
| `confirm` / `resume` | Finish this confirmation/wait; subsequent writes still need ownership, journal and wire guards |
| `abort` / `release` | End with the classified error; do not replay the queue command or send speculative cleanup |

## Initial queue confirmation

After native replace-and-play succeeds (HEOS CLI Protocol Specification 1.17,
4.4.11), [`confirmation.go`](../internal/control/confirmation.go) evaluates the
selected membership, snapshots and queue-start phase against a fixed twelve-second
deadline. Events wake the existing loop; full readback has a one-second backup
and 250 ms minimum spacing. Replies and events do not renew the deadline.

The first observed or reported Play closes the loading allowance. Before that
boundary only, `unknown` with a nonempty local MID outside old/selected identities
may wait if the **complete new queue** already matches the selected members.
Its total and positions must be valid, with no remaining pages. Controls,
physical identity and connection must remain unchanged. This permits read-only
waiting, not a conclusion about the MID's origin or permission to write.

| Priority / rule | Condition | Action |
| --- | --- | --- |
| 1. `queue_start_inactive` | No queue command awaits confirmation | `abort` |
| 2. `queue_start_timeout` | Original confirmation deadline reached | `abort`: deadline exceeded |
| 3. `operation_cancelled` | Operation cancelled | `abort`: preserve its cause |
| 4. `observation_unsafe` | Ordinary device safety fails | `abort`: preserve the safety error |
| 5. `queue_controls_changed` | Generation, identity, volume/mute, modes or group changed | `abort` |
| 6. `queue_state_changed` | State outside the permitted loading transition | `abort` |
| 7. `queue_items_changed` | An observed queue item is outside both old and selected sets | `abort` |
| 8. `queue_media_changed` | Media outside old/selected identities and the narrow unresolved-MID allowance | `abort` |
| 9. `queue_start_confirmed` | Requested Play verified with no uncovered newer event | `confirm` |
| 10. `events_pending` | A newer event needs reconciliation | `wait` |
| 11. `loading_media_pending` | Unresolved local MID under the conditions above | `wait` |
| Default: `queue_loading` | Other permitted loading observations | `wait` |

A bounded run needs selected Play, the complete queue and the exact current
MID/QID pair before its ramp/fade/Stop timeline starts. Ordinary short queue
commands retain their own result validation; the unresolved-MID allowance still
requires a complete selected queue.

The executor records Play and checks event revisions under the execution mutex.
A full snapshot with the exact current token and a player revision advanced
beyond the pre-command observation can cover events received during that read.
An older snapshot or partial event projection cannot clear those pending hints.

## Ordered replacement and append

`item_refs` adds an exact ordered check to initial confirmation. An observed
proper prefix of the requested first part waits (`queue_order_pending`) within
the same deadline; a different sequence aborts (`queue_order_changed`). Complete
confirmation includes repeated MIDs and unique QIDs. Legacy `item_ref` membership
semantics remain unchanged.

Each subsequent `aid=3` uses the existing command/observation loop with append
expectations registered before send. [`queue_append.go`](../internal/control/queue_append.go)
checks these boundaries under the event mutex:

1. Cancellation, device safety, continuity and unchanged controls still apply.
2. Every previously confirmed MID/QID must remain at its position. Observed
   additional entries must be an exact prefix of the expected suffix, with no
   missing old entries, extra tracks or reused QIDs.
3. Stop/unknown, empty media and hybrid identifiers from that same queue permit
   read-only waiting. Pause, foreign media or queue edits revoke ownership.
4. Only Play with the entire requested sequence, a valid current MID/QID and no
   uncovered newer event confirms the append. Current playback may advance
   naturally or by manual navigation within the queue.

Queue-change hints may repeat only during the pending append window. They never
extend its twelve-second deadline or the first active-transition deadline.
Outside that window a queue event retains ordinary cancellation behavior. Full
readback retries stale pagination; a device command with uncertain delivery is
never replayed. An automation append is additionally capped by the original
playback end. Ordinary multipart loading uses the same transition policy without
a service Stop timer.

## Buffered refill and prefix removal

Buffered mode chooses maintenance from the exact current MID/QID occurrence in
the complete confirmed queue. It counts resident entries after that occurrence,
not numeric QID differences or elapsed track duration. If the reserve is above
`refill_threshold`, it sends no queue command. Otherwise it validates the next
immutable part and either appends it or first makes room within
`max_queue_tracks`. Missing or stale position never authorizes maintenance.

Pruning removes only a played prefix, excludes the current entry and retains
`retain_previous` entries. The executor rechecks that prefix against the final
fresh observation before sending, so a concurrent Previous can make pruning
ineligible. One maintenance iteration sends at most one command; navigation,
Stop/cancel and volume deadlines are reconsidered before an append follows a
prune. Queue maintenance stops at the automation fade boundary.

Removal uses `player/remove_from_queue` (HEOS CLI Protocol Specification 1.17,
4.2.17), with an expectation registered before send and the existing twelve-second
confirmation deadline. Full readback must be the exact remaining ordered suffix,
including duplicates, with complete membership and unique nonempty QIDs. QIDs
may be reassigned after removal; they are never treated as permanent positions.
An unchanged queue must still retain its original QIDs. Partial removal and
permitted non-atomic state/media updates allow only bounded read-only waiting.

Removal confirmation requires the pre-command current occurrence rebased into
the remaining suffix. A repeated MID paired with an old QID could accidentally
identify a different valid occurrence after renumbering, so mere membership is
insufficient. Concurrent navigation that cannot establish the rebased current
occurrence stays pending and can end in uncertainty; the service does not guess
which occurrence the user intended. Changed controls, unexpected membership,
Pause or lost continuity revoke future writes. No uncertain removal is replayed.

After confirmed removal, the occurrence offset advances by the removed count;
cumulative loaded counts do not decrease. New native QIDs become the basis for
subsequent observations and guards. Prefix removal alone neither starts a new
track nor renews the playback/session deadline.

## Active event rules

Before this table, the callback validates the target player and continuity and
attempts attribution to the pending command. Gaps and unrelated changes retain
ordinary cancellation behavior. Our own confirmed Stop is attributed before
these rules and must not start another transition wait.

| Priority / rule | Condition | Action |
| --- | --- | --- |
| 1. `queue_not_owned` | No confirmed queue ownership | `pass` |
| 2. `media_notification` | Now-playing notification | `observe`: verify metadata against the retained queue |
| 3. `unexpected_play` | Play while expected state is not Play | `pass`; retain the observation hint if already waiting |
| 4. `play_requires_observation` | Play while waiting | `observe`; the event does not confirm membership |
| 5. `play_unchanged` | Otherwise expected Play | `ignore` |
| 6. `transport_already_pending` | Stop/unknown during automation or multipart loading, already waiting | `ignore`; preserve the deadline |
| 7. `transport_transition` | Stop/unknown during automation or multipart loading, not waiting | `wait`; establish the deadline and request observation |
| Default: `event_not_attributed` | Anything else, including Pause and queue edits | `pass` to ordinary cancellation |

State events report state, not who initiated a transition (5.4); a now-playing
notification does not report queue membership (5.5). Natural advancement and
manual Next/Previous therefore follow the same unchanged-queue policy. Manual
Stop followed by Play inside the wait window may also preserve the run. API Stop
and Pause cancel immediately. `POST /skip` is not an event in this table.
For buffered playback, a same-principal Skip can be admitted as a durable child
and dispatched by the existing worker without changing the parent deadline.
Other active-owner cases require takeover, which releases the run first.

## Active observation rules

An active policy needs an owned complete queue, active automation or multipart
loading, and expected Play. Stop/unknown starts a wait if none exists. So does Play with a hybrid
MID/QID: both identifiers occur in the owned queue, but in different entries.
A hybrid pair permits waiting only.

The first deadline is `now + 12s`, capped by the original monotonic playback
deadline. Repeated notifications and observations cannot renew either limit.
While waiting, targeted playback-state and media reads retain the verified
complete queue, with event wakes, a one-second backup and 250 ms minimum spacing.
A required baseline audit or invalid observation still needs a full read.

| Priority / rule | Condition | Action |
| --- | --- | --- |
| 1. `outside_active_queue` | Policy is inactive | `pass` |
| 2. `no_transition` | No transition wait | `pass` |
| 3. `transition_timeout` | Effective deadline reached or its context timed out | `release`: `queue_transition_timeout` |
| 4. `operation_cancelled` | Operation cancelled | `release`: preserve the first cause |
| 5. `observation_unsafe` | Observation fails ordinary device safety | `release`: preserve the safety error |
| 6. `queue_controls_changed` | Queue, controls, generation or media outside the allowed transition changed | `release`: `queue_transition_changed` with changed fields |
| 7. `play_confirmed` | Play, exact owned MID/QID pair, no newer uncovered event | `resume`: clear the wait |
| 8. `events_pending` | An event still needs reconciliation | `wait` |
| 9. `transport_pending` | Still Stop/unknown | `wait` |
| Default: `media_pending` | Play with permitted unresolved media | `wait` |

Timeout precedes cancellation because cancelling an expired read can itself
close the connection and cause a secondary gap. Intervention still takes
precedence over apparently valid Play or pending events. Missing media may be
tolerated during an existing wait; Stop/unknown may retain an owned MID with a
transitional QID. Neither confirms playback. Pause, foreign media, queue edits
and other control changes cannot use these allowances.

## Diagnostics and regression checks

INFO transition waiting and confirmation records include the deciding `rule`.
INFO ownership loss retains the first cause and safe changed-field diagnostics;
DEBUG records include release rules and initial queue decisions when the rule
changes. Duplicate events do not produce individual INFO records. Completion
evidence is bounded and persisted without raw media or device payloads; see
[Operations](OPERATIONS.md).

Pure policy tests check priority, immutable inputs, deadlines and forbidden
confirmation. Executor tests add fake time, event/read races, cancellation and
the complete envelope. Synthetic TLS tests count actual commands, including
background observation. Useful starting points are
[`queue_start_policy_test.go`](../internal/control/queue_start_policy_test.go),
[`queue_policy_test.go`](../internal/control/queue_policy_test.go),
[`queue_loading_test.go`](../internal/control/queue_loading_test.go),
[`queue_transition_test.go`](../internal/control/queue_transition_test.go) and
[`wire_test.go`](../internal/control/wire_test.go). Buffered preparation,
refill/pruning and child navigation also have focused regressions in
[`buffered_playback_test.go`](../internal/control/buffered_playback_test.go),
[`buffered_execution_test.go`](../internal/control/buffered_execution_test.go) and
[`progressive_skip_test.go`](../internal/control/progressive_skip_test.go). Validation commands and the
limits of synthetic evidence are in [Testing](TESTING.md).
