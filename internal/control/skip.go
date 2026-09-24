package control

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

// ErrNotSkippable means the current observation has no queued Play/Pause entry
// that a next/previous command can be confirmed against. The command is not sent.
var ErrNotSkippable = errors.New("current queue entry cannot be skipped")

// skipAdmissible requires a complete queue and an exact current member.
// HEOS CLI Protocol Specification 1.17 sections 4.2.21 and 4.2.22 acknowledge
// play_next/play_previous with pid only, so confirmation needs this baseline.
// Repeat off at a boundary that cannot move is rejected before any command:
// a single entry in either direction, or, with shuffle also off, next on the
// last entry and previous on the first. Repeat on_all and on_one are still sent.
func skipAdmissible(ceiling *int, direction string, s heos.Snapshot) error {
	if ceiling == nil || s.Volume == nil || *s.Volume > *ceiling {
		return heos.ErrBounds
	}
	if !s.State.Active() || !completeQueuePage(s.Queue) || !queuedMedia(s.Queue, s.Media) {
		return ErrNotSkippable
	}
	if atSkipBoundary(direction, s) {
		return ErrNotSkippable
	}
	return nil
}

// atSkipBoundary is true when repeat is off and this direction has nowhere to go.
// Shuffle can leave the current index, so a longer queue is not judged by index.
// A single entry has nowhere to go even when shuffle is on.
func atSkipBoundary(direction string, s heos.Snapshot) bool {
	if s.Repeat != heos.RepeatOff || (direction != "next" && direction != "previous") {
		return false
	}
	if len(s.Queue.Items) == 1 {
		return true
	}
	if s.Shuffle {
		return false
	}
	index := queueIndex(s)
	if index < 0 {
		return false
	}
	return (direction == "next" && index == len(s.Queue.Items)-1) || (direction == "previous" && index == 0)
}

func queueIndex(s heos.Snapshot) int {
	if s.Media == nil {
		return -1
	}
	for i, item := range s.Queue.Items {
		if item.QueueID == s.Media.QueueID && item.ID == s.Media.ID {
			return i
		}
	}
	return -1
}

func completeQueuePage(q heos.QueuePage) bool {
	return q.Total != nil && *q.Total == len(q.Items) && len(q.Items) > 0 && q.Next == nil
}

// queueExtension is true when full is the same queue with later pages appended.
func queueExtension(prefix, full heos.QueuePage) bool {
	if !completeQueuePage(full) || len(prefix.Items) == 0 || len(prefix.Items) > len(full.Items) {
		return false
	}
	if prefix.Total != nil && *prefix.Total != *full.Total {
		return false
	}
	return reflect.DeepEqual(prefix.Items, full.Items[:len(prefix.Items)])
}

// skipConfirmed is a different current queue entry in the same complete queue.
// Settled state is play or pause. Direction is not re-derived from index order:
// shuffle can select a non-adjacent member, and the native reply has no QID.
func skipConfirmed(before, after heos.Snapshot) bool {
	if !after.State.Active() || before.Media == nil || after.Media == nil {
		return false
	}
	if after.Media.QueueID == "" || after.Media.QueueID == before.Media.QueueID || !queuedMedia(before.Queue, after.Media) {
		return false
	}
	expected := before
	expected.State = after.State
	expected.Media = after.Media
	return len(stateChanges(expected, after)) == 0
}

// Check event coverage and close the transition allowance atomically with the
// event callback. A newer Stop or media hint cannot borrow a completed skip's
// expectations while writeOnce finishes its remaining ownership checks.
func (r *execution) acceptSkipReadback(ctx context.Context, before, after heos.Snapshot) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return false, err
	}
	if changes := pendingChanges(before, after, heos.Mutation{Kind: heos.MutationKindSkip}); len(changes) != 0 {
		return false, ownershipMismatch("pending_readback_changed", changes, before, after)
	}
	// A full read may already cover an event received during Refresh's retry.
	// As with initial queue confirmation, only the exact latest token and an
	// advanced player revision establish that coverage, never a projection.
	if !after.EventUpdated && after.Token == r.expected.Token && after.Token.Player > before.Token.Player {
		r.observedRevision = r.eventRevision
	}
	if r.eventRevision != r.observedRevision || !skipConfirmed(before, after) {
		return false, nil
	}
	r.events = map[string][]map[string]string{}
	r.keepExpectations = false
	return true, nil
}

func (c *Coordinator) prepareSkip(ctx context.Context, l *lane, direction string, before, fresh heos.Snapshot) (heos.Snapshot, heos.Snapshot, error) {
	readctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	completed, err := completeQueue(readctx, l, fresh)
	if err != nil {
		return before, fresh, err
	}
	if err = skipAdmissible(l.device.Config.VolumeCeiling, direction, completed); err != nil {
		return before, fresh, err
	}
	if queueExtension(before.Queue, completed.Queue) {
		before.Queue = completed.Queue
	}
	return before, completed, nil
}
