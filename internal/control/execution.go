package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
	"time"
)

func (c *Coordinator) transition(ctx context.Context, r *execution, u journal.Update) error {
	if r.progress != nil {
		u.Progress, _ = json.Marshal(r.progress)
	}
	o, e := c.db.Transition(ctx, r.op.ID, r.op.Revision, u)
	if e != nil { // Resolve the journal write, never repeat a HEOS command.
		resolve, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		current, getErr := c.db.Get(resolve, r.op.ID)
		if getErr == nil && current.Revision == r.op.Revision+1 && matchesUpdate(current, u) {
			o, e = current, nil
		}
	}
	if e == nil {
		r.op = o
		r.mu.Lock()
		r.phase = o.Phase
		r.mu.Unlock()
		if o.FinishedAt != nil {
			r.metrics.finish(o)
		} else {
			r.metrics.phase(o.Phase)
		}
		if c.logger != nil {
			c.logger.Debug("operation phase changed", "operation_id", o.ID, "player", o.Player, "state", o.State, "phase", o.Phase, "revision", o.Revision)
		}
		c.emit(o)
	} else {
		return fmt.Errorf("%w: %w", errJournalUnavailable, e)
	}
	return e
}
func (c *Coordinator) write(l *lane, r *execution, m heos.Mutation) error {
	var err error
	for range 3 {
		err = c.writeOnce(l, r, m)
		var command *heos.CommandError
		// A media transition may invalidate the final wire guard. Only a proven
		// NotSent stale guard may be retried, after reobserving queue ownership.
		if !r.bounded || !errors.As(err, &command) || command.Delivery != heos.NotSent || !errors.Is(err, heos.ErrStale) || context.Cause(r.ctx) != nil {
			return err
		}
		r.mu.Lock()
		r.events = map[string][]map[string]string{}
		r.mu.Unlock()
	}
	return err
}

func (c *Coordinator) writeOnce(l *lane, r *execution, m heos.Mutation) (err error) {
	if e := context.Cause(r.ctx); e != nil {
		return e
	}
	fresh, e := c.prewriteObservation(l, r, m)
	if e != nil {
		return e
	}
	r.mu.Lock()
	before := r.expected
	r.mu.Unlock()
	fresh, waited, e := c.awaitOwnedPlayback(l, r, before, fresh)
	if e != nil {
		return e
	}
	if waited && m.Kind == "volume" {
		return errPlaybackTimelineChanged
	}
	if r.automating && m.Kind == "volume" && m.Level > 0 && !c.clock.Now().Before(r.playbackDeadline) {
		return errPlaybackTimelineChanged
	}
	if e := mutationSafety(l, fresh, m.Kind); e != nil {
		return e
	}
	if m.Kind == "skip" {
		before, fresh, e = c.prepareSkip(r.ctx, l, m.Direction, before, fresh)
		if e != nil {
			return e
		}
	}
	c.traceSnapshot(r, "before_write", fresh)
	r.mu.Lock()
	changes := r.ownedChanges(before, fresh)
	same := len(changes) == 0
	if same {
		r.expected = fresh
	}
	r.mu.Unlock()
	if !same {
		return ownershipMismatch("before_write_changed", changes, before, fresh)
	}
	if e := c.transition(r.ctx, r, journal.Update{State: journal.Running, Phase: "sending_" + m.Kind}); e != nil {
		r.mu.Lock()
		r.uncertain = true
		r.mu.Unlock()
		return fmt.Errorf("journal unavailable: %w", e)
	}
	guardDuration := 5 * time.Second
	if r.automating && m.Kind == "volume" && m.Level > 0 {
		remaining := r.playbackDeadline.Sub(c.clock.Now())
		if remaining <= 0 {
			return errPlaybackTimelineChanged
		}
		guardDuration = min(guardDuration, remaining)
	}
	r.mu.Lock()
	r.expect(m)
	r.inflight = true
	r.mu.Unlock()
	m.Player = fresh.Player.ID
	began := c.clock.Now()
	response, writeErr := l.writer.Write(r.ctx, m, heos.Guard{Token: fresh.Token, ExpiresAt: time.Now().Add(guardDuration)})
	defer func() { c.recordConfirmation(l, r, m.Kind, began, writeErr, err) }()
	e = writeErr
	r.mu.Lock()
	r.inflight = false
	if e == nil {
		r.unconfirmed = true
		// The command revision fences a second write even for an unchanged target.
		// Never replace newer event history with an earlier reply token.
		if response.Token.Generation == r.expected.Token.Generation && response.Token.Player == r.expected.Token.Player {
			r.expected.Token = response.Token
		}
	}
	if e != nil {
		var ce *heos.CommandError
		if !errors.As(e, &ce) || ce.Delivery == heos.Uncertain {
			r.uncertain = true
		}
		r.rejectedWrite = ce != nil && ce.Delivery == heos.Rejected
	}
	r.mu.Unlock()
	if e != nil {
		return e
	}
	after, e := c.confirmReadback(l, r, m, fresh)
	if e != nil {
		r.mu.Lock()
		r.uncertain = true
		r.mu.Unlock()
		return e
	}
	expected := fresh
	switch m.Kind {
	case "mode":
		expected.Repeat = m.Repeat
		expected.Shuffle = m.Shuffle
	case "volume":
		v := m.Level
		expected.Volume = &v
		if fresh.Muted != nil && *fresh.Muted {
			expected.Muted = after.Muted // Setting volume may clear an existing mute.
		}
	case "mute":
		v := m.Muted
		expected.Muted = &v
	case "transport":
		expected.State = m.State
		expected = transportMediaReadback(expected, after, m.State, false)
	case "skip":
		if !skipConfirmed(fresh, after) {
			return ownershipMismatch("readback_changed", stateChanges(fresh, after), fresh, after)
		}
		expected.State = after.State
		expected.Media = after.Media
	case "queue":
		expected.State = "play"
		expected.Media = after.Media
		expected.Queue = after.Queue
		if !r.queueResultMatches(after) {
			return ownershipMismatch(r.queueResultProblem(after), []string{"queue_membership"}, fresh, after)
		}
	}
	if m.Kind == "volume" {
		after, _, e = c.awaitOwnedPlayback(l, r, expected, after)
		if e != nil {
			return e
		}
	}
	if e = safety(l, after); e != nil {
		return e
	}
	r.mu.Lock()
	if changes := r.ownedChanges(expected, after); len(changes) != 0 {
		r.mu.Unlock()
		return ownershipMismatch("readback_changed", changes, expected, after)
	}
	// An unchanged duplicate can arrive after confirmation was assembled. Keep
	// its revision so the next guard does not manufacture a full-read retry.
	if r.expected.Token.Generation == after.Token.Generation && r.expected.Token.Player >= after.Token.Player {
		after.Token = r.expected.Token
	}
	r.expected = after
	if m.Kind == "queue" && r.bounded {
		r.queueOwned = true
	}
	r.unconfirmed = false
	r.confirmed++
	// Confirmation closes this command's acknowledgement window. Later events
	// cannot borrow a previous command's expected value to hide manual input.
	r.events = map[string][]map[string]string{}
	r.queueStart = queueNotStarting
	r.scalar = nil
	r.keepExpectations = false
	r.mu.Unlock()
	return context.Cause(r.ctx)
}
func (c *Coordinator) execute(l *lane, r *execution, cmd Command, item heos.Item) {
	budget := time.Duration(cmd.FadeSeconds+30) * time.Second
	if cmd.Automation != nil {
		budget += time.Duration(cmd.Automation.DurationSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(r.ctx, budget)
	defer cancel()
	r.ctx = ctx
	write := func(m heos.Mutation) error { return c.write(l, r, m) }
	var e error
	switch cmd.Kind {
	case "volume":
		e = write(heos.Mutation{Kind: "volume", Level: cmd.Level})
	case "mute":
		e = write(heos.Mutation{Kind: "mute", Muted: cmd.Muted})
	case "transport":
		e = write(heos.Mutation{Kind: "transport", State: cmd.State})
	case "skip":
		e = write(heos.Mutation{Kind: "skip", Direction: cmd.Direction})
	case "playback":
		e = c.startPlayback(l, r, cmd, item)
		if e == nil && cmd.Automation != nil {
			e = c.automate(l, r, *cmd.Automation)
		}
	case "stop", "cancel":
		if cmd.Kind == "cancel" && cmd.Mode == "release" {
			break
		}
		r.mu.Lock()
		start := *r.expected.Volume
		r.mu.Unlock()
		began := c.clock.Now()
		last := start
		for cmd.FadeSeconds > 0 {
			elapsed := c.clock.Now().Sub(began)
			level := max(0, start-int(elapsed*time.Duration(start)/(time.Duration(cmd.FadeSeconds)*time.Second)))
			if level < last {
				e = write(heos.Mutation{Kind: "volume", Level: level})
				if e != nil {
					break
				}
				last = level
			}
			if elapsed >= time.Duration(cmd.FadeSeconds)*time.Second {
				break
			}
			if e = c.clock.Wait(r.ctx, 100*time.Millisecond, nil); e != nil {
				break
			}
		}
		if e == nil {
			e = write(heos.Mutation{Kind: "transport", State: "stop"})
		}
	default:
		e = heos.ErrBounds
	}
	if errors.Is(context.Cause(r.ctx), errSuperseded) {
		if cmd.Kind == "cancel" {
			c.finishOrphan(cmd.Target, r.targetMetrics)
		}
		return
	}
	state, code, outcome := journal.Succeeded, "", "confirmed"
	if e != nil {
		state, code, outcome = journal.Failed, "device_unavailable", "not_sent"
		if c.ctx.Err() != nil {
			state, code = journal.Interrupted, "service_stopping"
		}
		if errors.Is(e, ErrOwnership) || errors.Is(context.Cause(r.ctx), ErrOwnership) {
			state, code = journal.Released, "ownership_lost"
			c.logOwnershipLoss(r, e)
		}
		r.mu.Lock()
		uncertain := r.uncertain || r.unconfirmed
		r.mu.Unlock()
		if uncertain {
			state, outcome = journal.Uncertain, "unknown"
		}
		if errors.Is(e, heos.ErrRejected) {
			code, outcome = "device_rejected", "rejected"
		}
		if errors.Is(e, ErrNotSkippable) {
			code = "not_skippable"
		}
		if errors.Is(e, errJournalUnavailable) && c.ctx.Err() == nil {
			code = "journal_unavailable"
		}
	}
	r.mu.Lock()
	mayContinue := r.expected.State != "stop" || state != journal.Succeeded
	confirmed := r.confirmed
	r.mu.Unlock()
	final := journal.Update{State: state, Phase: "complete", ErrorCode: code, Outcome: json.RawMessage(fmt.Sprintf(`{"delivery":%q,"playback_may_continue":%t,"commands_confirmed":%d}`, outcome, mayContinue, confirmed))}
	final.Outcome = c.annotateOutcome(final.Outcome, c.completionDiagnostics(r, e, code))
	if r.rejectedWrite {
		// Even a rejected send advances the local write revision. Restore the
		// observer baseline for later admission without replaying the command.
		// Capture the final outcome first: recovery failures or their events
		// must not replace the evidence of the original rejection.
		recovery, stop := context.WithTimeout(heos.WithObservationTrigger(r.ctx, "recovery"), 5*time.Second)
		_ = l.device.Observer.Refresh(recovery)
		stop()
	}
	// Device work is finished; only target/worker journal reconciliation remains.
	r.metrics.phase("persisting")
	if cmd.Kind == "cancel" {
		targetState := journal.Cancelled
		if state != journal.Succeeded {
			targetState = state
		}
		// The cancellation worker owns its own command count and observations.
		// Its target retains the target's phase/progress and a separate explanation.
		targetOutcome := json.RawMessage(fmt.Sprintf(`{"delivery":%q,"playback_may_continue":%t}`, outcome, mayContinue))
		c.persistByID(cmd.Target, journal.Update{State: targetState, Phase: "cancelled", ErrorCode: code, Outcome: targetOutcome}, r.targetMetrics)
	}
	// Keep only journal reconciliation pending during an outage. No playback replay.
	for {
		if errors.Is(context.Cause(r.ctx), errSuperseded) {
			if cmd.Kind == "cancel" {
				c.finishOrphan(cmd.Target, r.targetMetrics)
			}
			return
		}
		finish, stop := context.WithTimeout(context.Background(), 2*time.Second)
		err := c.transition(finish, r, final)
		stop()
		if err == nil || errors.Is(err, journal.ErrStaleEpoch) || c.ctx.Err() != nil {
			return
		}
		if c.clock.Wait(c.ctx, time.Second, nil) != nil {
			return
		}
	}
}
