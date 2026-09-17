package control

import (
	"context"
	"errors"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

const (
	activeObservationInterval  = heos.IdleObservationInterval
	pendingObservationInterval = time.Second
	observationEventSpacing    = 250 * time.Millisecond
)

// Called under r.mu by the existing synchronous event callback. The wake is a
// read hint, not state confirmation. No second event reader or timer goroutine.
func (r *execution) notifyObservation() {
	r.eventRevision++
	r.signal()
}

func (r *execution) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Once a command is confirmed, the synchronous event consumer maintains this
// operation's state. A fresh wire guard fences that exact checked revision;
// events do not renew the full baseline or its audit deadline.
func (c *Coordinator) prewriteObservation(l *lane, r *execution, m heos.Mutation) (heos.Snapshot, error) {
	r.mu.Lock()
	s := r.expected
	confirmed := r.confirmed > 0
	pending := r.eventRevision != r.observedRevision || !r.queueWait.IsZero()
	r.mu.Unlock()
	age := c.clock.Now().Sub(s.ObservedAt)
	view := l.device.Client.PlayerView(s.Player.ID)
	// A background audit may discover an intervention without a notification.
	// Consume that newer evidence before relying on the event-maintained chain.
	if confirmed && l.device.Observer.Snapshot().ObservedAt.After(s.ObservedAt) {
		return c.reconcileOwned(r.ctx, l, r)
	}
	if confirmed && scalarMutation(m) && !pending && age >= 0 && age < activeObservationInterval &&
		s.Connected && !s.Stale && view.Connected {
		if view.Token == s.Token {
			return s, context.Cause(r.ctx)
		}
		// A notification can advance the revision between these snapshots.
		// Let the existing consumer reconcile it before choosing any GET.
		return c.reconcileOwned(r.ctx, l, r)
	}
	if confirmed && r.queueOwned && pending {
		return c.reconcileOwned(r.ctx, l, r)
	}
	return c.observeRun(heos.WithObservationTrigger(r.ctx, "prewrite"), l, r, false)
}

type reconciler interface{ Reconcile(context.Context) error }
type playbackReader interface{ RefreshPlayback(context.Context) error }

// A now-playing notification lacks MID/QID. Resolve that metadata against the
// already confirmed complete queue; only invalid history or the rare audit
// needs a full observation. Simple events are handled synchronously above.
func (c *Coordinator) reconcileOwned(ctx context.Context, l *lane, r *execution) (heos.Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var s heos.Snapshot
	var err error
	for range 3 {
		if err = context.Cause(ctx); err != nil {
			return s, err
		}
		s, err = c.reconcileOwnedOnce(ctx, l, r)
		if !errors.Is(err, heos.ErrStale) {
			return s, err
		}
	}
	return s, err
}

func (c *Coordinator) reconcileOwnedOnce(ctx context.Context, l *lane, r *execution) (heos.Snapshot, error) {
	o, ok := l.device.Observer.(reconciler)
	if !ok {
		return c.observeRun(ctx, l, r, true)
	}
	r.mu.Lock()
	before, revision, waiting := r.expected, r.eventRevision, !r.queueWait.IsZero()
	r.mu.Unlock()
	read := o.Reconcile
	if reader, ok := l.device.Observer.(playbackReader); ok && waiting {
		read = reader.RefreshPlayback
		ctx = heos.WithObservationTrigger(ctx, "confirmation")
	}
	if err := read(ctx); err != nil {
		return heos.Snapshot{}, err
	}
	s := l.device.Observer.Snapshot()
	if err := context.Cause(ctx); err != nil {
		return s, err
	}
	view := l.device.Client.PlayerView(s.Player.ID)
	if s.Stale || !s.Connected || !view.Connected || s.Token != view.Token {
		return s, heos.ErrStale
	}
	if s.ObservedAt.After(before.ObservedAt) && r.bounded {
		var err error
		s, err = completeQueue(ctx, l, s)
		if err != nil {
			return s, err
		}
	} else {
		s.Queue = before.Queue
	}
	r.lastReadAttempt = c.clock.Now()
	if s.ObservedAt.After(before.ObservedAt) {
		r.lastRead = c.clock.Now()
	}
	r.observedAt = s.ObservedAt
	r.mu.Lock()
	r.observedRevision = revision
	r.mu.Unlock()
	return s, nil
}

func (r *execution) observationPending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.eventRevision != r.observedRevision
}

// Initial admission to a write chain, queue/transport confirmation and recovery
// still require complete observations. Read-only confirmation may reuse a newer
// full snapshot from the background observer; event projections cannot replace
// initial queue membership verification or renew the full baseline.
func (c *Coordinator) observeRun(ctx context.Context, l *lane, r *execution, reuse bool) (heos.Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := context.Cause(ctx); err != nil {
		return heos.Snapshot{}, err
	}
	r.mu.Lock()
	revision := r.eventRevision
	r.mu.Unlock()
	s := l.device.Observer.Snapshot()
	age := c.clock.Now().Sub(s.ObservedAt)
	view := l.device.Client.PlayerView(s.Player.ID)
	canReuse := reuse && s.ObservedAt.After(r.observedAt) && age >= 0 && age <= time.Second &&
		!s.EventUpdated && !s.Stale && s.Connected && view.Connected && view.Token == s.Token
	var err error
	if canReuse {
		if r.bounded {
			s, err = completeQueue(ctx, l, s)
		}
	} else {
		s, err = observe(ctx, l, r.bounded)
	}
	if cause := context.Cause(ctx); cause != nil {
		err = cause // Cache reuse is subject to the same cancellation as wire reads.
	}
	r.lastReadAttempt = c.clock.Now()
	if err == nil {
		r.lastRead, r.observedAt = c.clock.Now(), s.ObservedAt
		r.mu.Lock()
		r.observedRevision = revision
		r.mu.Unlock()
	}
	return s, err
}

// Events can wake a pending confirmation before its one-second fallback.
// Duplicate hints cannot cause a full read more often than every 250 ms; neither
// events nor reads extend the caller's original deadline. A buffered event that
// arrived during a read remains pending until a later read reconciles it.
func (c *Coordinator) waitForObservation(ctx context.Context, r *execution, deadline time.Time) error {
	next := minTime(r.lastReadAttempt.Add(pendingObservationInterval), deadline)
	for c.clock.Now().Before(next) {
		if r.observationPending() {
			break
		}
		if err := c.clock.Wait(ctx, next.Sub(c.clock.Now()), r.wake); err != nil {
			return err
		}
	}
	earliest := minTime(r.lastReadAttempt.Add(observationEventSpacing), deadline)
	if c.clock.Now().Before(earliest) {
		if err := c.clock.Wait(ctx, earliest.Sub(c.clock.Now()), nil); err != nil {
			return err
		}
	}
	return context.Cause(ctx)
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
