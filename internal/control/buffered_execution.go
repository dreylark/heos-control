package control

import (
	"context"
	"errors"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

var errBufferedSessionExpired = errors.New("buffered playback session expired")
var errBufferedCapacity = errors.New("cannot safely make room for the next part")

func (r *execution) updateBufferedProgress() (bool, error) {
	if r.buffered == nil {
		return false, nil
	}
	r.mu.Lock()
	s := r.expected
	r.mu.Unlock()
	position, err := bufferedPosition(s, r.ordered)
	if err != nil {
		return false, err
	}
	p := r.queueLoading
	resident, remaining, current, pruned := len(s.Queue.Items), len(s.Queue.Items)-position-1, r.buffered.pruned+position, r.buffered.pruned
	changed := p.BufferedTracks == nil || *p.BufferedTracks != resident || p.RemainingTracks == nil || *p.RemainingTracks != remaining || p.CurrentIndex == nil || *p.CurrentIndex != current || p.PrunedTracks == nil || *p.PrunedTracks != pruned
	p.Mode = "buffered"
	p.BufferedTracks = &resident
	p.RemainingTracks = &remaining
	p.CurrentIndex = &current
	p.PrunedTracks = &pruned
	if !r.buffered.expires.IsZero() {
		at := r.buffered.expires.UTC()
		p.SessionExpiresAt = &at
	}
	return changed, nil
}

// The same worker owns waiting, catalog preparation, pruning, appending and Skip.
// Journal health has a separate cadence; progress never initiates device reads.
func (c *Coordinator) loadBuffered(l *lane, r *execution) error {
	nextJournal := c.clock.Now()
	for r.moreParts() {
		if err := context.Cause(r.ctx); err != nil {
			return err
		}
		now := c.clock.Now()
		if !now.Before(r.buffered.expires) {
			return errBufferedSessionExpired
		}
		if !now.Before(nextJournal) {
			if err := c.checkJournal(r); err != nil {
				return err
			}
			nextJournal = c.clock.Now().Add(time.Second)
		}
		if handled, err := c.serviceBufferedSkip(l, r); handled || err != nil {
			if err != nil {
				return err
			}
			continue
		}
		pending := r.observationPending()
		if (!pending || c.clock.Now().Sub(r.lastReadAttempt) >= observationEventSpacing) &&
			(pending || c.clock.Now().Sub(r.lastRead) >= activeObservationInterval || l.device.Observer.Snapshot().ObservedAt.After(r.observedAt)) {
			if err := c.checkOwnership(l, r); err != nil {
				return err
			}
		}
		if !r.observationPending() {
			changed, err := r.updateBufferedProgress()
			if err != nil {
				return err
			}
			if changed {
				if err := c.transition(r.ctx, r, journal.Update{State: journal.Running, Phase: "playing"}); err != nil {
					return err
				}
			}
			if acted, err := c.bufferedStep(l, r); acted || err != nil {
				if err != nil && !errors.Is(err, errPlaybackTimelineChanged) {
					return err
				}
				continue
			}
		}
		next := minTime(nextJournal, r.buffered.expires)
		if r.observationPending() {
			next = minTime(next, r.lastReadAttempt.Add(observationEventSpacing))
		}
		if err := c.clock.Wait(r.ctx, max(0, next.Sub(c.clock.Now())), r.wake); err != nil {
			return err
		}
	}
	return nil
}

// A maintenance step issues at most one command. The caller recomputes envelope
// and navigation priority between pruning and append, and after all I/O.
func (c *Coordinator) bufferedStep(l *lane, r *execution) (bool, error) {
	if !r.moreParts() {
		return false, nil
	}
	if !c.clock.Now().Before(r.buffered.expires) {
		return false, errBufferedSessionExpired
	}
	if !r.appendUntil.IsZero() && !c.clock.Now().Before(r.appendUntil) {
		return false, nil
	}
	r.mu.Lock()
	s := r.expected
	r.mu.Unlock()
	position, err := bufferedPosition(s, r.ordered)
	if err != nil {
		return false, err
	}
	if len(s.Queue.Items)-position-1 > r.buffered.policy.RefillThreshold {
		return false, nil
	}
	part := &r.parts[r.queueLoading.ConfirmedParts]
	if err := prepareBufferedPart(r.ctx, l.device, part); err != nil {
		return false, err
	}
	if len(s.Queue.Items)+part.Count > r.buffered.policy.MaxQueueTracks {
		remove := position - r.buffered.policy.RetainPrevious
		if remove <= 0 || len(s.Queue.Items)-remove+part.Count > r.buffered.policy.MaxQueueTracks {
			return false, errBufferedCapacity
		}
		return true, c.pruneBuffered(l, r, remove, s.Queue)
	}
	return true, c.appendNext(l, r)
}

func (c *Coordinator) pruneBuffered(l *lane, r *execution, n int, before heos.QueuePage) error {
	ids := make([]heos.ID, n)
	for i := range ids {
		ids[i] = before.Items[i].QueueID
	}
	previous := r.ordered
	r.ordered = append([]heos.ID(nil), previous[n:]...)
	r.members = idMembership(r.ordered)
	if err := c.write(l, r, heos.Mutation{Kind: heos.MutationKindRemove, QueueIDs: ids}); err != nil {
		r.ordered = previous
		r.members = idMembership(previous)
		return err
	}
	r.buffered.pruned += n
	if _, err := r.updateBufferedProgress(); err != nil {
		return err
	}
	return c.transition(r.ctx, r, journal.Update{State: journal.Running, Phase: "queue_loading"})
}

// Re-evaluate the removable prefix at the last checked revision. Previous during
// preparation may make an earlier pruning decision unsafe; it is not a send.
func (r *execution) removalAllowed(s heos.Snapshot, m heos.Mutation) bool {
	if r.buffered == nil || s.State != heos.PlayStatePlay || !queuedMedia(s.Queue, s.Media) {
		return false
	}
	n := len(m.QueueIDs)
	if n == 0 || n >= len(s.Queue.Items) {
		return false
	}
	current := -1
	for i, item := range s.Queue.Items {
		if i < n && item.QueueID != m.QueueIDs[i] {
			return false
		}
		if item.QueueID == s.Media.QueueID && item.ID == s.Media.ID {
			current = i
		}
	}
	return current-n >= r.buffered.policy.RetainPrevious
}
