package control

import (
	"context"
	"log/slog"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

type queueStartAction string

const (
	queueStartWait    queueStartAction = "wait"
	queueStartConfirm queueStartAction = "confirm"
	queueStartAbort   queueStartAction = "abort"
)

type queueStartFacts struct {
	Now, Deadline        time.Time
	Phase                queueStartPhase
	Bounded              bool
	Members              map[heos.ID]bool
	Before, Observed     heos.Snapshot
	Cancellation, Unsafe error
	PendingEvents        bool
}

type queueStartDecision struct {
	Action queueStartAction
	Rule   string
	Err    error
}

// Initial confirmation has no owned queue yet. Its rules deliberately differ
// from active transition policy: only the requested result can establish ownership.
// Facts include a fixed deadline and the existing one-way Play phase. This pure
// policy cannot send, read, advance time, consume events or mutate its inputs.
func decideQueueStart(f queueStartFacts) queueStartDecision {
	controls := queueLoadingControls(f.Before, f.Observed)
	mediaAllowed := queueLoadingMedia(f.Before, f.Observed, f.Members)
	// The unknown MID in the incident is not evidence of selected playback.
	// Before the first Play only, a complete selected queue permits read-only
	// waiting for local metadata to settle (Denon 4.2.3, 4.2.5 and 4.2.15).
	unresolved := f.Phase == queueAwaitingPlay && f.Observed.State == heos.PlayStateUnknown &&
		f.Observed.Media != nil && f.Observed.Media.Source == "1024" && f.Observed.Media.ID != "" &&
		!mediaAllowed && completeOwnedQueue(f.Observed.Queue, f.Members)
	for _, rule := range []struct {
		name   string
		when   bool
		action queueStartAction
		err    error
		fields []string
	}{
		{"queue_start_inactive", f.Phase == queueNotStarting, queueStartAbort, ErrOwnership, nil},
		{"queue_start_timeout", !f.Now.Before(f.Deadline), queueStartAbort, context.DeadlineExceeded, nil},
		{"operation_cancelled", f.Cancellation != nil, queueStartAbort, f.Cancellation, nil},
		{"observation_unsafe", f.Unsafe != nil, queueStartAbort, f.Unsafe, nil},
		{"queue_controls_changed", len(controls) != 0, queueStartAbort, nil, controls},
		{"queue_state_changed", !queueStatePending(f.Before.State, f.Observed.State, f.Phase == queuePlayObserved), queueStartAbort, nil, []string{"state"}},
		{"queue_items_changed", !queueLoadingItems(f.Before.Queue, f.Observed.Queue, f.Members), queueStartAbort, nil, []string{"queue_items"}},
		{"queue_media_changed", !mediaAllowed && !unresolved, queueStartAbort, nil, []string{"media"}},
		{"queue_start_confirmed", f.Observed.State == heos.PlayStatePlay && queueResultProblem(f.Observed, f.Members, f.Bounded) == "" && !f.PendingEvents, queueStartConfirm, nil, nil},
		{"events_pending", f.PendingEvents, queueStartWait, nil, nil},
		{"loading_media_pending", unresolved, queueStartWait, nil, nil},
	} {
		if rule.when {
			d := queueStartDecision{Action: rule.action, Rule: rule.name, Err: rule.err}
			if len(rule.fields) != 0 {
				detail := ownershipMismatch("pending_readback_changed", rule.fields, f.Before, f.Observed)
				detail.attrs = append(detail.attrs, slog.String("rule", rule.name))
				d.Err = detail
			}
			return d
		}
	}
	return queueStartDecision{Action: queueStartWait, Rule: "queue_loading"}
}

func queueLoadingControls(before, after heos.Snapshot) []string {
	if before.Token.Generation != after.Token.Generation {
		return []string{"connection_generation"}
	}
	before.State, before.Media, before.Queue = after.State, after.Media, after.Queue
	return stateChanges(before, after)
}

func queueLoadingMedia(before, after heos.Snapshot, members map[heos.ID]bool) bool {
	media := after.Media
	if emptyMedia(media) {
		return true
	}
	previous := before.Media != nil && before.Media.Source == media.Source && before.Media.ID == media.ID
	selected := media.Source == "1024" && members[media.ID]
	return previous || selected
}

func queueLoadingItems(before, after heos.QueuePage, members map[heos.ID]bool) bool {
	old := make(map[heos.ID]bool, len(before.Items))
	for _, item := range before.Items {
		old[item.ID] = true
	}
	for _, item := range after.Items {
		if !old[item.ID] && !members[item.ID] {
			return false
		}
	}
	return true
}

func queueResultProblem(s heos.Snapshot, members map[heos.ID]bool, bounded bool) string {
	if s.Media == nil || s.Media.Source != "1024" || s.Media.ID == "" || s.Media.QueueID == "" || !members[s.Media.ID] {
		return "queue_media_not_selected"
	}
	for _, track := range s.Queue.Items {
		if !members[track.ID] {
			return "queue_contains_unselected_media"
		}
	}
	if bounded {
		if !completeOwnedQueue(s.Queue, members) {
			return "queue_incomplete_or_invalid"
		}
		if !queuedMedia(s.Queue, s.Media) {
			return "queue_media_not_found"
		}
	}
	return ""
}
