package control

import (
	"context"
	"errors"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

// A command reply acknowledges delivery; Denon 5.9–5.11 observe its result.
// Keep one pending scalar change, registered before send, under execution.mu.
type scalarConfirmation struct {
	command                             heos.Mutation
	observed                            heos.Snapshot
	volumeSeen, repeatSeen, shuffleSeen bool
}

func scalarMutation(m heos.Mutation) bool {
	return m.Kind == "volume" || m.Kind == "mute" || m.Kind == "mode"
}

func newScalarConfirmation(m heos.Mutation, before heos.Snapshot) *scalarConfirmation {
	p := &scalarConfirmation{command: m, observed: before}
	switch m.Kind {
	case "volume":
		// Even an unchanged volume can clear an existing mute on Home 150.
		p.volumeSeen = before.Volume != nil && *before.Volume == m.Level && before.Muted != nil && !*before.Muted
	case "mute":
		p.volumeSeen = before.Muted != nil && *before.Muted == m.Muted
	case "mode":
		p.repeatSeen, p.shuffleSeen = before.Repeat == m.Repeat, before.Shuffle == m.Shuffle
	}
	return p
}

func (p *scalarConfirmation) ready() bool {
	if p.command.Kind == "mode" {
		return p.repeatSeen && p.shuffleSeen
	}
	return p.volumeSeen
}

func (p *scalarConfirmation) result(s heos.Snapshot) heos.Snapshot {
	if p.command.Kind == "mode" {
		s.Repeat, s.Shuffle = p.observed.Repeat, p.observed.Shuffle
	} else {
		s.Volume, s.Muted = p.observed.Volume, p.observed.Muted
	}
	s.EventUpdated = true
	return s
}

// Exact repeated current values are harmless. Once a pending field reaches its
// target, an older value is a reversal, not an acknowledgement of a prior SET.
func (r *execution) acceptScalarEvent(e heos.Event) bool {
	data := e.Data()
	if !data.Valid {
		return false
	}
	p := r.scalar
	switch data.Kind {
	case heos.EventVolume:
		if p == nil || p.command.Kind == "mode" {
			return confirmedVolumeEvent(e, r.expected)
		}
		want := r.expected
		m := p.command
		if m.Kind == "volume" {
			v := m.Level
			want.Volume = &v
		} else {
			v := m.Muted
			want.Muted = &v
		}
		match := confirmedVolumeEvent(e, want)
		if !match && m.Kind == "volume" && r.expected.Muted != nil && *r.expected.Muted {
			muted := false
			want.Muted = &muted
			match = confirmedVolumeEvent(e, want)
		}
		if !match {
			return !p.volumeSeen && confirmedVolumeEvent(e, r.expected)
		}
		if p.volumeSeen && !confirmedVolumeEvent(e, p.observed) {
			return false
		}
		p.observed.Volume, p.observed.Muted = want.Volume, want.Muted
		p.volumeSeen = true
	case heos.EventRepeat:
		if p == nil || p.command.Kind != "mode" {
			return data.Repeat == r.expected.Repeat
		}
		if data.Repeat != p.command.Repeat {
			return !p.repeatSeen && data.Repeat == r.expected.Repeat
		}
		p.observed.Repeat, p.repeatSeen = data.Repeat, true
	case heos.EventShuffle:
		if p == nil || p.command.Kind != "mode" {
			return data.Shuffle == r.expected.Shuffle
		}
		if data.Shuffle != p.command.Shuffle {
			return !p.shuffleSeen && data.Shuffle == r.expected.Shuffle
		}
		p.observed.Shuffle, p.shuffleSeen = data.Shuffle, true
	default:
		return false
	}
	if p.ready() {
		r.signal()
	}
	return true
}

type scalarReader interface {
	RefreshScalars(context.Context, string) error
}
type scalarPublisher interface{ ConfirmScalars(heos.Snapshot) error }

func (c *Coordinator) confirmScalar(l *lane, r *execution, m heos.Mutation) (heos.Snapshot, error) {
	deadline := c.clock.Now().Add(playbackConfirmationTimeout)
	fallbackAt := deadline.Add(-time.Second)
	// The terminal zero belongs to finishing playback, like the following
	// Stop. Its acknowledgement may arrive after the envelope ends; no later
	// positive volume is allowed. Other pending steps cannot extend playback.
	finishing := m.Kind == "volume" && m.Level == 0 && !r.playbackDeadline.IsZero() && !c.clock.Now().Before(r.playbackDeadline)
	if !finishing && !r.playbackDeadline.IsZero() && r.playbackDeadline.Before(deadline) {
		deadline = r.playbackDeadline
		fallbackAt = deadline // A short remaining window must not trigger an early GET.
	}
	ctx, cancel := context.WithTimeout(r.ctx, max(0, deadline.Sub(c.clock.Now())))
	defer cancel()
	// Reserve the final second of the existing timeout for one targeted fallback.
	// Waiting does no device I/O; journal readiness remains independent of HEOS.
	for {
		if err := context.Cause(r.ctx); err != nil {
			return heos.Snapshot{}, err
		}
		if !c.clock.Now().Before(deadline) {
			return heos.Snapshot{}, context.DeadlineExceeded
		}
		r.mu.Lock()
		ready := r.scalar.ready()
		s := r.scalar.result(r.expected)
		pending := r.eventRevision != r.observedRevision || !r.queueWait.IsZero()
		r.mu.Unlock()
		if ready {
			if publisher, ok := l.device.Observer.(scalarPublisher); ok && !pending {
				// The asynchronous cache consumer can lag or already have newer
				// events. Its CAS must not overwrite either case or cause GETs.
				if err := publisher.ConfirmScalars(s); err != nil && !errors.Is(err, heos.ErrStale) {
					return s, err
				}
			}
			c.traceSnapshot(r, "event_confirmation", s)
			return s, context.Cause(r.ctx)
		}
		if err := context.Cause(ctx); err != nil {
			return s, err
		}
		if !c.clock.Now().Before(fallbackAt) {
			break
		}
		if err := c.checkJournal(r); err != nil {
			return s, err
		}
		if err := c.clock.Wait(ctx, min(time.Second, fallbackAt.Sub(c.clock.Now())), r.wake); err != nil {
			return s, err
		}
	}
	if !c.clock.Now().Before(deadline) {
		return heos.Snapshot{}, context.DeadlineExceeded
	}
	reader, ok := l.device.Observer.(scalarReader)
	if !ok {
		return heos.Snapshot{}, context.DeadlineExceeded
	}
	if c.logger != nil {
		c.logger.Info("scalar confirmation event missing; reading requested controls", "operation_id", r.id, "command", m.Kind)
	}
	l.device.Metrics.Fallback(m.Kind)
	if err := reader.RefreshScalars(heos.WithObservationTrigger(ctx, "fallback"), m.Kind); err != nil {
		return heos.Snapshot{}, err
	}
	if err := context.Cause(ctx); err != nil {
		return heos.Snapshot{}, err
	}
	if !c.clock.Now().Before(deadline) {
		return heos.Snapshot{}, context.DeadlineExceeded
	}
	s := l.device.Observer.Snapshot()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := context.Cause(r.ctx); err != nil {
		return s, err
	}
	if s.Stale || !s.Connected || s.Token.Generation != r.expected.Token.Generation {
		return s, heos.ErrStale
	}
	if m.Kind == "mode" {
		if s.Repeat != m.Repeat || s.Shuffle != m.Shuffle {
			return s, context.DeadlineExceeded
		}
	} else {
		want := r.expected
		if m.Kind == "volume" {
			level := m.Level
			want.Volume = &level
			if want.Muted != nil && *want.Muted {
				want.Muted = s.Muted
			}
		} else {
			muted := m.Muted
			want.Muted = &muted
		}
		if s.Volume == nil || s.Muted == nil || *s.Volume != *want.Volume || *s.Muted != *want.Muted {
			return s, context.DeadlineExceeded
		}
	}
	// Only the explicitly read fields can repair a missing scalar event.
	r.scalar.observed = s
	result := r.scalar.result(r.expected)
	result.Token = s.Token
	return result, nil
}
