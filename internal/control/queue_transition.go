package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

const playbackConfirmationTimeout = 12 * time.Second

// The automation loop must recompute its level after waiting during a final
// pre-write observation. This is a local scheduling signal, never a wire retry.
var errPlaybackTimelineChanged = errors.New("playback timeline advanced during queue transition")

var errQueueTransitionTimeout = &ownershipError{reason: "queue_transition_timeout"}

// awaitOwnedPlayback is the single read-only gate for active track transitions.
// It is used by periodic checks, before setters, and after a volume setter whose
// readback overlapped Next. The expected control state may include that setter;
// the owned queue, generation and all unrelated controls must remain unchanged.
// A return to play must have a valid MID/QID in that exact complete queue.
func (c *Coordinator) awaitOwnedPlayback(l *lane, r *execution, before, fresh heos.Snapshot) (heos.Snapshot, bool, error) {
	d := c.queueObservationDecision(r.ctx, l, r, before, fresh)
	if d.Action == queuePass {
		return fresh, false, nil
	}
	deadline := d.Deadline
	ctx, cancel := context.WithTimeoutCause(r.ctx, max(0, deadline.Sub(c.clock.Now())), errQueueTransitionTimeout)
	defer cancel()
	waited := false
	// The post statement also runs after continue: interrupted I/O must return
	// to the same policy, preserving timeout vs cancellation priority.
	for ; ; d = c.queueObservationDecision(ctx, l, r, before, fresh) {
		switch d.Action {
		case queuePass:
			return fresh, waited, nil
		case queueRelease:
			if c.logger != nil {
				c.logger.Debug("playback queue transition released", "operation_id", r.id, "rule", d.Rule)
			}
			return fresh, waited, d.Err
		case queueResume:
			if c.logger != nil {
				c.logger.Info("playback queue transition confirmed", "operation_id", r.id, "rule", d.Rule)
			}
			return fresh, waited, nil
		}
		if !waited && c.logger != nil {
			c.logger.Info("waiting for playback queue transition", "operation_id", r.id, "timeout_at", deadline, "rule", d.Rule)
		}
		waited = true
		if err := c.waitForObservation(ctx, r, deadline); err != nil {
			continue // Resolve deadline vs intervention at the top of the loop.
		}
		if !c.clock.Now().Before(deadline) {
			continue
		}
		ready, stop := context.WithTimeout(ctx, 2*time.Second)
		err := c.db.Ready(ready)
		stop()
		if ctx.Err() != nil {
			continue
		}
		if err != nil {
			return fresh, waited, fmt.Errorf("%w: %w", errJournalUnavailable, err)
		}
		fresh, err = c.reconcileOwned(ctx, l, r)
		if ctx.Err() != nil {
			continue
		}
		if err != nil {
			return fresh, waited, err
		}
		c.traceSnapshot(r, "queue_transition", fresh)
	}
}

func (c *Coordinator) queueObservationDecision(ctx context.Context, l *lane, r *execution, before, fresh heos.Snapshot) queueDecision {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := decideQueueObservation(queuePolicyState{
		Owned: r.queueOwned, Automating: r.automating, ExpectedState: before.State,
		WaitUntil: r.queueWait, PlaybackUntil: r.playbackDeadline,
	}, queueObservationFacts{
		Now: c.clock.Now(), Before: before, Observed: fresh,
		Cancellation: context.Cause(r.ctx), Unsafe: deviceObservationSafety(l.device.Config, fresh),
		Expired: errors.Is(context.Cause(ctx), errQueueTransitionTimeout), PendingEvents: r.eventRevision != r.observedRevision,
	})
	r.queueWait = d.WaitUntil
	if d.Action == queueRelease {
		d.Err = r.captureDecision(d.Err, d.Rule, c.clock.Now())
	}
	return d
}
