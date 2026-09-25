package control

import (
	"context"
	"errors"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

// Queue confirmation has a one-way boundary: once play is seen, loading Stop
// can no longer be accepted, even while media/queue readback is still pending.
type queueStartPhase uint8

const (
	queueNotStarting queueStartPhase = iota
	queueAwaitingPlay
	queuePlayObserved
)

// Home 150 can acknowledge queue replacement (Denon 4.4.11), transport (4.2.4)
// or play mode (4.2.14) before the observed state changes. Confirm using
// bounded reads, never by repeating a setter. The operation's context continues
// to enforce event-based intervention and shutdown cancellation.
func (c *Coordinator) confirmReadback(l *lane, r *execution, m heos.Mutation, before heos.Snapshot) (heos.Snapshot, error) {
	if scalarMutation(m) {
		return c.confirmScalar(l, r, m)
	}
	deadline := c.clock.Now().Add(playbackConfirmationTimeout)
	if (m.Append || m.Kind == heos.MutationKindRemove || (r.buffered != nil && m.Kind == heos.MutationKindSkip)) && !r.playbackDeadline.IsZero() {
		deadline = minTime(deadline, r.playbackDeadline)
	}
	ctx, cancel := context.WithTimeout(heos.WithObservationTrigger(r.ctx, "confirmation"), max(0, deadline.Sub(c.clock.Now())))
	defer cancel()
	reuse := false
	lastQueueRule := ""
	for {
		if err := context.Cause(ctx); err != nil {
			return heos.Snapshot{}, err
		}
		if !c.clock.Now().Before(deadline) {
			return heos.Snapshot{}, context.DeadlineExceeded
		}
		after, err := c.observeRun(ctx, l, r, reuse)
		reuse = true
		if err == nil && m.Kind == heos.MutationKindSkip {
			after, err = completeQueue(ctx, l, after)
		}
		if !c.clock.Now().Before(deadline) {
			return after, context.DeadlineExceeded
		}
		if err == nil {
			c.traceSnapshot(r, "pending_readback", after)
			switch m.Kind {
			case heos.MutationKindQueue:
				if m.Append {
					if confirmed, err := c.appendObservation(ctx, l, r, before, after); confirmed || err != nil {
						return after, err
					}
					break
				}
				d := c.queueStartObservation(ctx, l, r, before, after, deadline)
				if c.logger != nil && d.Rule != lastQueueRule {
					c.logger.Debug("queue start confirmation decision", "operation_id", r.id, "action", d.Action, "rule", d.Rule, "timeout_at", deadline)
				}
				lastQueueRule = d.Rule
				switch d.Action {
				case queueStartConfirm:
					return after, nil // Caller retains final ownership/write checks.
				case queueStartAbort:
					return after, d.Err
				}
			case heos.MutationKindRemove:
				if confirmed, err := c.removeObservation(ctx, l, r, before, after, len(m.QueueIDs)); confirmed || err != nil {
					return after, err
				}
			case heos.MutationKindSkip:
				if err = deviceObservationSafety(l.device.Config, after); err != nil {
					return after, err
				}
				if confirmed, err := r.acceptSkipReadback(ctx, before, after); confirmed || err != nil {
					return after, err
				}
			default:
				if err = deviceObservationSafety(l.device.Config, after); err != nil {
					return after, err
				}
				if after.State == m.State {
					return after, nil // Caller still verifies all remaining control state.
				}
				if changes := pendingChanges(before, after, m); len(changes) != 0 {
					return after, ownershipMismatch("pending_readback_changed", changes, before, after)
				}
			}

		} else if !errors.Is(err, heos.ErrStale) || context.Cause(ctx) != nil {
			return after, err
		}
		if !c.clock.Now().Before(deadline) {
			return after, context.DeadlineExceeded
		}
		if err = c.waitForObservation(ctx, r, deadline); err != nil {
			return after, err
		}
	}
}

func (c *Coordinator) queueStartObservation(ctx context.Context, l *lane, r *execution, before, after heos.Snapshot, deadline time.Time) queueStartDecision {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observeQueueState(after.State)
	// Refresh may internally retry after an event and return a full snapshot
	// that already includes it. The exact current token and an advanced player
	// revision prove coverage; a projection or an older read cannot do this.
	if !after.EventUpdated && after.Token == r.expected.Token && after.Token.Player > before.Token.Player {
		r.observedRevision = r.eventRevision
	}
	d := decideQueueStart(queueStartFacts{
		Now: c.clock.Now(), Deadline: deadline, Phase: r.queueStart,
		Before: before, Observed: after, Bounded: r.bounded, Members: r.members,
		Cancellation: context.Cause(ctx), Unsafe: deviceObservationSafety(l.device.Config, after),
		PendingEvents: r.eventRevision != r.observedRevision,
	})
	if d.Action == queueStartConfirm && r.ordered != nil && !orderedQueue(after.Queue, r.ordered) {
		if appendQueueProblem(heos.QueuePage{}, after.Queue, r.ordered) {
			d = queueStartDecision{Action: queueStartAbort, Rule: "queue_order_changed", Err: ownershipMismatch("pending_readback_changed", []string{"queue_items"}, before, after)}
		} else {
			d = queueStartDecision{Action: queueStartWait, Rule: "queue_order_pending"}
		}
	}
	if d.Action == queueStartAbort {
		d.Err = r.captureDecision(d.Err, d.Rule, c.clock.Now())
	}
	return d
}

// A read can confirm play before its notification or before the queue is ready.
// Close the loading-stop allowance immediately, retaining a late play event.
// Call with r.mu held, together with the initial queue decision and event check.
func (r *execution) observeQueueState(state heos.PlayState) {
	if state == heos.PlayStatePlay {
		r.queueStart = queuePlayObserved
		name := "event/player_state_changed"
		pending := r.events[name][:0]
		for _, want := range r.events[name] {
			if want["state"] != string(heos.PlayStateStop) {
				pending = append(pending, want)
			}
		}
		r.events[name] = pending
	}
}

func (r *execution) queueResultMatches(s heos.Snapshot) bool {
	return r.queueResultProblem(s) == ""
}

func (r *execution) queueResultProblem(s heos.Snapshot) string {
	if r.ordered != nil && !orderedQueue(s.Queue, r.ordered) {
		return "queue_incomplete_or_invalid"
	}
	return queueResultProblem(s, r.members, r.bounded)
}

// Ordinary transport/mode confirmation retains its separate media comparison.
func pendingChanges(before, after heos.Snapshot, m heos.Mutation) []string {
	if before.Token.Generation != after.Token.Generation {
		return []string{"connection_generation"}
	}
	switch m.Kind {
	case heos.MutationKindMode:
		// Only unchanged or requested values may be pending (Denon
		// 4.2.13/4.2.14). Keep every other control and queue check intact.
		if after.Repeat == m.Repeat {
			before.Repeat = after.Repeat
		}
		if after.Shuffle == m.Shuffle {
			before.Shuffle = after.Shuffle
		}
	case heos.MutationKindTransport:
		if after.State == heos.PlayStateUnknown {
			before.State = after.State
		}
		before = transportMediaReadback(before, after, m.State, true)
	case heos.MutationKindSkip:
		if after.State.Known() {
			before.State = after.State
		}
		// MID and QID can settle separately. Only identifiers from this same
		// queue permit waiting; skipConfirmed still requires an exact pair.
		if emptyMedia(after.Media) || queuedMedia(before.Queue, after.Media) || transitionalQueueMedia(before.Queue, after.Media) {
			before.Media = after.Media
		}
	}
	return stateChanges(before, after)
}

// A stopping Home 150 can clear media or reset its position in the unchanged
// queue. This never accepts a foreign MID/QID or hides other control changes.
func stoppedReadback(before, after heos.Snapshot) heos.Snapshot {
	if after.State == heos.PlayStateUnknown || after.State == heos.PlayStateStop {
		before.State = after.State
		if emptyMedia(after.Media) || queuedMedia(before.Queue, after.Media) {
			before.Media = after.Media
		}
	}
	return before
}

// Resuming a stopped shuffled queue can select another queued track. While
// loading, its MID can precede its QID; final Play still needs the exact pair.
// Transport never authorizes a different queue or foreign source/media.
func transportMediaReadback(before, after heos.Snapshot, target heos.PlayState, pending bool) heos.Snapshot {
	switch target {
	case heos.PlayStateStop:
		return stoppedReadback(before, after)
	case heos.PlayStatePlay:
		if queuedMedia(before.Queue, after.Media) {
			before.Media = after.Media
		} else if pending && after.State != heos.PlayStatePlay && (emptyMedia(after.Media) || queuedIdentity(before.Queue, after.Media)) {
			before.Media = after.Media
		}
	}
	return before
}

func queuedIdentity(queue heos.QueuePage, media *heos.Media) bool {
	if media == nil || media.Source != "1024" || media.ID == "" {
		return false
	}
	for _, item := range queue.Items {
		if item.ID == media.ID {
			return true
		}
	}
	return false
}

// Only read-only confirmation uses these transitional states. Admission and
// every subsequent mutation still require an ordinary known playback state.
func queueStatePending(before, after heos.PlayState, started bool) bool {
	switch after {
	case heos.PlayStatePlay, heos.PlayStateUnknown:
		return true
	case heos.PlayStateStop:
		return !started && (before == heos.PlayStateStop || before == heos.PlayStatePause)
	case heos.PlayStatePause:
		return !started && before == heos.PlayStatePause
	default:
		return false
	}
}
