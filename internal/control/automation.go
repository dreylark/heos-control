package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

// Automation is an optional, bounded envelope for already selected playback.
// Scheduling and album selection belong to the caller.
type Automation struct {
	TargetLevel     int `json:"target_level"`
	RampSeconds     int `json:"ramp_seconds"`
	DurationSeconds int `json:"duration_seconds"`
	FadeSeconds     int `json:"fade_seconds"`
}

func (a Automation) validate(cmd Command, ceiling *int) error {
	if cmd.Kind != CommandKindPlayback || cmd.Repeat != heos.RepeatOff || ceiling == nil ||
		a.TargetLevel < cmd.Level || a.TargetLevel > *ceiling || a.TargetLevel > 100 ||
		a.DurationSeconds < 1 || a.DurationSeconds > 7200 ||
		a.FadeSeconds < 0 || a.FadeSeconds > 60 || a.FadeSeconds >= a.DurationSeconds ||
		a.RampSeconds < 0 || a.RampSeconds > a.DurationSeconds-a.FadeSeconds {
		return heos.ErrBounds
	}
	return nil
}

// PlaybackProgress is a persisted observation, not a promise that a future stop
// will execute. UTC timestamps are derived from the confirmed monotonic start.
type PlaybackProgress struct {
	PlaybackStartedAt time.Time `json:"playback_started_at"`
	StopAt            time.Time `json:"stop_at"`
	ElapsedSeconds    int       `json:"elapsed_seconds"`
	Level             int       `json:"level"` // last confirmed HEOS volume
}

// Journal health is checked independently of the device observation cadence.
// Reducing device reads must not postpone suspension on database failure.
func (c *Coordinator) checkJournal(r *execution) error {
	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	err := c.db.Ready(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("%w: %w", errJournalUnavailable, err)
	}
	return nil
}

func (c *Coordinator) checkOwnership(l *lane, r *execution) error {
	fresh, err := c.reconcileOwned(r.ctx, l, r)
	if err != nil {
		return err
	}
	r.mu.Lock()
	before := r.expected
	r.mu.Unlock()
	fresh, _, err = c.awaitOwnedPlayback(l, r, before, fresh)
	if err != nil {
		return err
	}
	if err := safety(l, fresh); err != nil {
		if errors.Is(err, ErrGrouped) {
			return &ownershipError{reason: "grouped_target"}
		}
		return err
	}
	r.mu.Lock()
	changes := r.ownedChanges(before, fresh)
	same := len(changes) == 0
	if same {
		r.expected = fresh
	}
	r.mu.Unlock()
	if !same {
		return ownershipMismatch("active_state_changed", changes, before, fresh)
	}
	return context.Cause(r.ctx)
}

func (c *Coordinator) automate(l *lane, r *execution, a Automation) error {
	began := c.clock.Now()
	duration := time.Duration(a.DurationSeconds) * time.Second
	ramp := time.Duration(a.RampSeconds) * time.Second
	fade := time.Duration(a.FadeSeconds) * time.Second
	r.mu.Lock()
	r.automating = true
	initial := *r.expected.Volume
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.automating = false; r.mu.Unlock() }()
	r.playbackDeadline = began.Add(duration)
	r.progress = &PlaybackProgress{PlaybackStartedAt: began.UTC(), StopAt: began.Add(duration).UTC(), Level: initial}
	last, fadeFrom := initial, -1
	phase := ""
	nextJournal := began
	for {
		if err := context.Cause(r.ctx); err != nil {
			return err
		}
		if !c.clock.Now().Before(nextJournal) {
			if err := c.checkJournal(r); err != nil {
				return err
			}
			nextJournal = c.clock.Now().Add(time.Second)
		}
		// Recompute after I/O. Delayed ticks issue one current level, never a
		// burst of missed steps or a newly shifted stop deadline.
		elapsed := max(time.Duration(0), c.clock.Now().Sub(began))
		r.progress.ElapsedSeconds = int(min(elapsed, duration) / time.Second)
		level, nextPhase := a.TargetLevel, "playing"
		switch {
		case elapsed >= duration:
			nextPhase = "stopping"
			if fade > 0 {
				level = 0
			} else {
				level = last
			}
		case fade > 0 && elapsed >= duration-fade:
			nextPhase = "fading"
			if fadeFrom < 0 {
				fadeFrom = last
			}
			level = fadeFrom - int((elapsed-(duration-fade))*time.Duration(fadeFrom)/fade)
		case ramp > 0 && elapsed < ramp:
			nextPhase = "ramping"
			level = initial + int(elapsed*time.Duration(a.TargetLevel-initial)/ramp)
		}
		// Scalar events confirm steps without GETs. Reconcile media changes
		// and the rare full audit; journal health has its own one-second check.
		quiet := level == last && elapsed < duration
		due := c.clock.Now().Sub(r.lastRead) >= activeObservationInterval ||
			(r.observationPending() && c.clock.Now().Sub(r.lastRead) >= observationEventSpacing)
		if quiet && due {
			if err := c.checkOwnership(l, r); err != nil {
				return err
			}
			continue // Recompute the envelope after I/O or a track-transition wait.
		}
		if nextPhase != phase {
			if err := c.transition(r.ctx, r, journal.Update{State: journal.Running, Phase: nextPhase}); err != nil {
				return err
			}
			phase = nextPhase
		}
		if level != last {
			if err := c.write(l, r, heos.Mutation{Kind: heos.MutationKindVolume, Level: level}); err != nil {
				if errors.Is(err, errPlaybackTimelineChanged) {
					continue
				}
				return err
			}
			last, r.progress.Level = level, level
			if err := c.transition(r.ctx, r, journal.Update{State: journal.Running, Phase: phase}); err != nil {
				return err
			}
		}
		if elapsed >= duration {
			return c.write(l, r, heos.Mutation{Kind: heos.MutationKindTransport, State: heos.PlayStateStop})
		}
		if err := c.clock.Wait(r.ctx, min(250*time.Millisecond, duration-elapsed), r.wake); err != nil {
			return err
		}
	}
}
