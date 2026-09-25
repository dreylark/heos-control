package control

import (
	"context"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

// Home 150 renumbers remaining QIDs after removing a prefix. Compare ordered
// occurrences, retaining the exact original QIDs until any removal is observed.
func removedQueuePrefix(before, after heos.QueuePage, maximum int) (int, bool) {
	removed := len(before.Items) - len(after.Items)
	if removed < 0 || removed > maximum || after.Total == nil || *after.Total != len(after.Items) || after.Next != nil {
		return 0, false
	}
	seen := make(map[heos.ID]bool, len(after.Items))
	for i, item := range after.Items {
		original := before.Items[i+removed]
		if item.ID != original.ID || item.QueueID == "" || seen[item.QueueID] || (removed == 0 && item.QueueID != original.QueueID) {
			return 0, false
		}
		seen[item.QueueID] = true
	}
	return removed, true
}

func (c *Coordinator) removeObservation(ctx context.Context, l *lane, r *execution, before, after heos.Snapshot, n int) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return false, err
	}
	if !r.queueWait.IsZero() && !c.clock.Now().Before(r.queueWait) {
		return false, errQueueTransitionTimeout
	}
	if err := deviceObservationSafety(l.device.Config, after); err != nil {
		return false, err
	}
	if fields := queueLoadingControls(before, after); len(fields) != 0 {
		return false, ownershipMismatch("pending_readback_changed", fields, before, after)
	}
	removed, valid := removedQueuePrefix(before.Queue, after.Queue, n)
	if !valid {
		return false, ownershipMismatch("pending_readback_changed", []string{"queue_items"}, before, after)
	}
	// QIDs can be reassigned before now-playing catches up. With repeated
	// MIDs, the old pair may already identify a different occurrence in the
	// new queue. Only the original occurrence mapped into the remaining suffix
	// confirms uninterrupted playback; concurrent navigation stays pending.
	position := queueIndex(before)
	if !queuedMedia(before.Queue, before.Media) || position < removed || position-removed >= len(after.Queue.Items) {
		return false, ownershipMismatch("pending_readback_changed", []string{"media"}, before, after)
	}
	current := after.Queue.Items[position-removed]
	sameOccurrence := after.Media != nil && after.Media.ID == current.ID && after.Media.QueueID == current.QueueID
	if after.State != heos.PlayStatePlay && after.State != heos.PlayStateStop && after.State != heos.PlayStateUnknown {
		return false, ownershipMismatch("pending_readback_changed", []string{"state"}, before, after)
	}
	validMedia := queuedMedia(after.Queue, after.Media)
	pendingMedia := emptyMedia(after.Media) || transitionalQueueMedia(after.Queue, after.Media) || queuedMedia(before.Queue, after.Media)
	if !validMedia && !pendingMedia {
		return false, ownershipMismatch("pending_readback_changed", []string{"media"}, before, after)
	}
	if !after.EventUpdated && after.Token == r.expected.Token && after.Token.Player > before.Token.Player {
		r.observedRevision = r.eventRevision
	}
	if removed == n && after.State == heos.PlayStatePlay && validMedia && sameOccurrence && orderedQueue(after.Queue, r.ordered) && r.eventRevision == r.observedRevision {
		r.queueWait = time.Time{}
		delete(r.events, "event/player_queue_changed")
		delete(r.events, "event/player_now_playing_changed")
		return true, nil
	}
	return false, nil
}
