package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

// A buffered Skip has its own durable result, but only the existing playback
// worker may dispatch it. All fields except immutable direction/metrics are
// protected by the parent's mu until ready; thereafter the worker owns them.
// The single slot remains occupied until journal-only finalization finishes.
type bufferedSkip struct {
	direction string
	op        journal.Operation
	metrics   *operationMetrics
	ready     bool
}

func (c *Coordinator) submitBufferedSkip(ctx context.Context, l *lane, parent *execution, owner journal.Operation, request journal.Request, cmd Command) (journal.Operation, error) {
	if owner.Principal != request.Principal {
		return journal.Operation{}, journal.ErrBusy
	}
	child := &bufferedSkip{direction: cmd.Direction, metrics: &operationMetrics{player: l.device.Metrics, kind: "skip"}}
	parent.mu.Lock()
	if !parent.queueOwned || parent.bufferedSkipsClosed || context.Cause(parent.ctx) != nil || parent.uncertain || parent.unconfirmed {
		parent.mu.Unlock()
		return journal.Operation{}, journal.ErrBusy
	}
	if parent.bufferedSkip != nil {
		parent.mu.Unlock()
		return journal.Operation{}, heos.ErrQueueFull
	}
	if parent.expected.State != heos.PlayStatePlay {
		parent.mu.Unlock()
		return journal.Operation{}, ErrNotSkippable
	}
	if err := skipAdmissible(l.device.Config.VolumeCeiling, cmd.Direction, parent.expected); err != nil {
		parent.mu.Unlock()
		return journal.Operation{}, err
	}
	parent.bufferedSkip = child
	parent.mu.Unlock()

	configuration, _ := json.Marshal(l.device.Config)
	args, _ := json.Marshal(struct {
		OwnerID string  `json:"owner_operation_id"`
		Command Command `json:"command"`
	}{owner.ID, cmd})
	now := c.clock.Now()
	proposal := journal.Proposal{Kind: "skip", OwnerID: owner.ID, DeviceKey: l.device.Config.Serial,
		ConfigRevision: fmt.Sprintf("direct:%x", sha256.Sum256(configuration)), EffectiveArguments: args,
		NotBefore: now, NotAfter: now.Add(time.Minute)}
	// Track the admission through shutdown even if the parent exits while its
	// child commit is being resolved. Close must not finish before that child's
	// unsent result has received its bounded journal finalization attempt.
	c.lifecycle.Lock()
	if c.closing || ctx.Err() != nil {
		c.lifecycle.Unlock()
		parent.mu.Lock()
		if parent.bufferedSkip == child {
			parent.bufferedSkip = nil
		}
		parent.mu.Unlock()
		return journal.Operation{}, ErrUnavailable
	}
	c.wg.Add(1)
	c.lifecycle.Unlock()
	defer c.wg.Done()
	a, err := c.db.Admit(ctx, request, proposal)
	parent.mu.Lock()
	closed := parent.bufferedSkipsClosed || context.Cause(parent.ctx) != nil
	if err != nil || !a.Created {
		if parent.bufferedSkip == child {
			parent.bufferedSkip = nil
		}
		parent.mu.Unlock()
		return a.Operation, err
	}
	child.op = a.Operation
	child.ready = !closed
	parent.mu.Unlock()
	c.emit(a.Operation)
	if closed {
		c.finishBufferedSkip(parent, child, journal.Update{State: journal.Released, Phase: "complete", ErrorCode: "ownership_revoked",
			Outcome: json.RawMessage(`{"delivery":"not_sent","playback_may_continue":true,"commands_confirmed":0}`)})
	} else {
		select {
		case parent.wake <- struct{}{}:
		default:
		}
	}
	return a.Operation, nil
}

// serviceBufferedSkip must run on the owning playback worker, between device
// commands and before preparing/appending another part. Even on rejection the
// caller should recompute position/timeline before scheduling more work.
func (c *Coordinator) serviceBufferedSkip(l *lane, parent *execution) (bool, error) {
	parent.mu.Lock()
	child := parent.bufferedSkip
	if child == nil || !child.ready || parent.bufferedSkipsClosed {
		parent.mu.Unlock()
		return false, nil
	}
	child.ready = false
	confirmed := parent.confirmed
	parent.mu.Unlock()

	err := context.Cause(parent.ctx)
	if err == nil {
		err = c.checkJournal(parent)
	}
	if err == nil {
		childRun := &execution{op: child.op}
		err = c.transition(parent.ctx, childRun, journal.Update{State: journal.Running, Phase: "sending_skip"})
		child.op = childRun.op
	}
	if err == nil {
		err = c.write(l, parent, heos.Mutation{Kind: heos.MutationKindSkip, Direction: child.direction})
	}
	parent.mu.Lock()
	count := parent.confirmed - confirmed
	uncertain := parent.uncertain || parent.unconfirmed
	parent.mu.Unlock()
	state, code, delivery := journal.Succeeded, "", "confirmed"
	if err != nil {
		state, code, delivery = journal.Failed, "device_unavailable", "not_sent"
		switch {
		case errors.Is(err, ErrNotSkippable):
			code = "not_skippable"
		case errors.Is(err, errJournalUnavailable):
			code = "journal_unavailable"
		case c.ctx.Err() != nil:
			state, code = journal.Interrupted, "service_stopping"
		case errors.Is(err, ErrOwnership), errors.Is(err, errSuperseded), errors.Is(err, context.Canceled), errors.Is(err, errPlaybackTimelineChanged):
			state, code = journal.Released, "ownership_revoked"
		case errors.Is(err, heos.ErrRejected):
			code, delivery = "device_rejected", "rejected"
		}
		if count > 0 {
			delivery = "confirmed"
		}
		if uncertain {
			state, delivery = journal.Uncertain, "unknown"
		}
	}
	c.finishBufferedSkip(parent, child, journal.Update{State: state, Phase: "complete", ErrorCode: code,
		Outcome: json.RawMessage(fmt.Sprintf(`{"delivery":%q,"playback_may_continue":true,"commands_confirmed":%d}`, delivery, count))})
	if errors.Is(err, ErrNotSkippable) || errors.Is(err, errPlaybackTimelineChanged) {
		return true, nil // Recompute the owner timeline after an unsent navigation boundary.
	}
	return true, err
}

// closeBufferedSkips is called by the playback worker before its exit. A child
// whose admission is still in flight is finalized by submitBufferedSkip after
// the commit is known. An executing child is finalized by serviceBufferedSkip.
func (c *Coordinator) closeBufferedSkips(parent *execution) {
	parent.mu.Lock()
	parent.bufferedSkipsClosed = true
	child := parent.bufferedSkip
	if child == nil || !child.ready {
		parent.mu.Unlock()
		return
	}
	child.ready = false
	parent.mu.Unlock()
	c.finishBufferedSkip(parent, child, journal.Update{State: journal.Released, Phase: "complete", ErrorCode: "ownership_revoked",
		Outcome: json.RawMessage(`{"delivery":"not_sent","playback_may_continue":true,"commands_confirmed":0}`)})
}

// Finalization has no device access. It cannot delay priority Stop behind a
// database outage and never turns an uncertain Skip into a replay.
func (c *Coordinator) finishBufferedSkip(parent *execution, child *bufferedSkip, update journal.Update) {
	finish := func() {
		c.persistBufferedSkip(child, update)
		parent.mu.Lock()
		if parent.bufferedSkip == child {
			parent.bufferedSkip = nil
		}
		parent.mu.Unlock()
	}
	c.lifecycle.Lock()
	if c.closing {
		c.lifecycle.Unlock()
		finish() // Shutdown permits one bounded journal attempt; recovery handles failure.
		return
	}
	c.wg.Go(finish)
	c.lifecycle.Unlock()
}

// Child completion has independent diagnostics. The cancellation-target
// finalizer must not turn successful navigation into a failed handoff.
func (c *Coordinator) persistBufferedSkip(child *bufferedSkip, update journal.Update) {
	at := c.clock.Now().UTC()
	reason := update.ErrorCode
	if update.State == journal.Succeeded {
		reason = "completed"
	}
	update.Outcome = c.annotateOutcome(update.Outcome, journal.Diagnostics{Reason: reason, Source: "controller", Phase: "skipping", DetectedAt: &at})
	// Reuse optimistic commit resolution while keeping the parent's active
	// phase metric intact; the child has its own terminal metric receipt.
	r := &execution{op: child.op}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		current, err := c.db.Get(ctx, child.op.ID)
		if err == nil && current.FinishedAt != nil {
			child.metrics.finish(current)
			cancel()
			return
		}
		if err == nil {
			r.op = current
			err = c.transition(ctx, r, update)
		}
		cancel()
		if err == nil {
			child.metrics.finish(r.op)
			return
		}
		if errors.Is(err, journal.ErrStaleEpoch) || c.ctx.Err() != nil {
			return
		}
		if c.clock.Wait(c.ctx, time.Second, nil) != nil {
			return
		}
	}
}
