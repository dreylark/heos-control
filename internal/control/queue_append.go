package control

import (
	"context"
	"errors"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

var errQueueLoadingIncomplete = errors.New("playback window ended before queue loading completed")

type QueueLoadingProgress struct {
	TotalParts      int `json:"total_parts"`
	ConfirmedParts  int `json:"confirmed_parts"`
	TotalTracks     int `json:"total_tracks"`
	ConfirmedTracks int `json:"confirmed_tracks"`
}

// Keep the existing timeline fields at the root of journal progress, so old
// operations and predecessor runtimes can still read their playback timestamps.
type operationProgress struct {
	*PlaybackProgress
	QueueLoading *QueueLoadingProgress `json:"queue_loading,omitempty"`
}

func (r *execution) moreParts() bool {
	return r.queueLoading != nil && r.queueLoading.ConfirmedParts < len(r.parts)
}
func (c *Coordinator) confirmPart(r *execution) error {
	if r.queueLoading == nil {
		return nil
	}
	r.queueLoading.ConfirmedTracks += len(r.parts[r.queueLoading.ConfirmedParts].IDs)
	r.queueLoading.ConfirmedParts++
	r.mu.Lock()
	r.loading = r.moreParts()
	r.mu.Unlock()
	return c.transition(r.ctx, r, journal.Update{State: journal.Running, Phase: "queue_loading"})
}
func (c *Coordinator) appendNext(l *lane, r *execution) error {
	part := r.parts[r.queueLoading.ConfirmedParts]
	previous := r.ordered
	r.ordered = append(append([]heos.ID(nil), previous...), part.IDs...)
	r.members = idMembership(r.ordered)
	err := c.write(l, r, heos.Mutation{Kind: heos.MutationKindQueue, Item: part.Item, Append: true})
	if err != nil {
		r.ordered = previous
		r.members = idMembership(previous)
		return err
	}
	return c.confirmPart(r)
}
func (c *Coordinator) loadRemaining(l *lane, r *execution) error {
	for r.moreParts() {
		if err := c.checkJournal(r); err != nil {
			return err
		}
		if err := c.appendNext(l, r); err != nil {
			return err
		}
	}
	return nil
}

// Exact ordered identity includes multiplicity. QIDs must be unique even when
// the same MID occurs more than once. Metadata/title differences are irrelevant.
func orderedQueue(q heos.QueuePage, ids []heos.ID) bool {
	if len(q.Items) != len(ids) || q.Total == nil || *q.Total != len(ids) || q.Next != nil {
		return false
	}
	seen := make(map[heos.ID]bool, len(ids))
	for i, m := range q.Items {
		if m.ID != ids[i] || m.QueueID == "" || seen[m.QueueID] {
			return false
		}
		seen[m.QueueID] = true
	}
	return true
}

// A pending append can expose the unchanged queue or a growing exact suffix.
// It can never replace/reorder an old entry, reuse a QID or add foreign media.
func appendQueueProblem(before, after heos.QueuePage, ids []heos.ID) bool {
	if after.Total == nil || *after.Total != len(after.Items) || after.Next != nil || len(after.Items) < len(before.Items) || len(after.Items) > len(ids) {
		return true
	}
	seen := make(map[heos.ID]bool, len(after.Items))
	for i, m := range after.Items {
		if m.ID != ids[i] || m.QueueID == "" || seen[m.QueueID] {
			return true
		}
		if i < len(before.Items) && (m.QueueID != before.Items[i].QueueID || m.ID != before.Items[i].ID) {
			return true
		}
		seen[m.QueueID] = true
	}
	return false
}

func (c *Coordinator) appendObservation(ctx context.Context, l *lane, r *execution, before, after heos.Snapshot) (bool, error) {
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
	if appendQueueProblem(before.Queue, after.Queue, r.ordered) {
		return false, ownershipMismatch("pending_readback_changed", []string{"queue_items"}, before, after)
	}
	if after.State != heos.PlayStatePlay && after.State != heos.PlayStateStop && after.State != heos.PlayStateUnknown {
		return false, ownershipMismatch("pending_readback_changed", []string{"state"}, before, after)
	}
	validMedia := queuedMedia(after.Queue, after.Media)
	pendingMedia := emptyMedia(after.Media) || transitionalQueueMedia(after.Queue, after.Media) || (after.State != heos.PlayStatePlay && queuedIdentity(after.Queue, after.Media))
	if !validMedia && !pendingMedia {
		return false, ownershipMismatch("pending_readback_changed", []string{"media"}, before, after)
	}
	if !after.EventUpdated && after.Token == r.expected.Token && after.Token.Player > before.Token.Player {
		r.observedRevision = r.eventRevision
	}
	if after.State == heos.PlayStatePlay && validMedia && orderedQueue(after.Queue, r.ordered) && r.eventRevision == r.observedRevision {
		r.queueWait = time.Time{}
		// Close attribution under the same mutex as confirmation. A later
		// queue event must not borrow this command while journal work runs.
		delete(r.events, "event/player_queue_changed")
		return true, nil
	}
	return false, nil
}
