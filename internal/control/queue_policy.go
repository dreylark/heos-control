package control

import (
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

type queueAction string

const (
	queuePass    queueAction = "pass" // Continue ordinary command/ownership checks.
	queueIgnore  queueAction = "ignore"
	queueObserve queueAction = "observe"
	queueWait    queueAction = "wait"
	queueResume  queueAction = "resume"
	queueRelease queueAction = "release"
)

// These are values captured under the execution mutex, not another mutable FSM.
type queuePolicyState struct {
	Owned, Automating bool
	ExpectedState     string
	WaitUntil         time.Time
	PlaybackUntil     time.Time
}

type queueObservationFacts struct {
	Now                    time.Time
	Before, Observed       heos.Snapshot
	Cancellation, Unsafe   error
	Expired, PendingEvents bool
}

type queueDecision struct {
	Action              queueAction
	Rule                string
	WaitUntil, Deadline time.Time
	Notify              bool
	Err                 error
}

// The caller has already checked event validity, player identity, continuity and
// pending command expectations. Denon 5.4/5.5 can request observation; they cannot
// establish queue membership or confirm a transition by themselves.
func decideQueueEvent(state queuePolicyState, event heos.Event, now time.Time) queueDecision {
	data := event.Data()
	waiting := !state.WaitUntil.IsZero()
	play := data.Kind == heos.EventState && data.State == "play"
	transition := data.Kind == heos.EventState && state.Automating &&
		(data.State == "stop" || data.State == "unknown")
	d := queueDecision{Action: queuePass, Rule: "event_not_attributed", WaitUntil: state.WaitUntil}
	// First matching rule wins. Attribution to our own Stop happens before this
	// table; it must never start another transition wait.
	for _, rule := range []struct {
		name   string
		when   bool
		action queueAction
		notify bool
	}{
		{"queue_not_owned", !state.Owned, queuePass, false},
		{"media_notification", data.Kind == heos.EventNowPlaying, queueObserve, true},
		{"unexpected_play", play && state.ExpectedState != "play", queuePass, waiting},
		{"play_requires_observation", play && waiting, queueObserve, true},
		{"play_unchanged", play, queueIgnore, false},
		{"transport_already_pending", transition && waiting, queueIgnore, false},
		{"transport_transition", transition, queueWait, true},
	} {
		if rule.when {
			d.Action, d.Rule, d.Notify = rule.action, rule.name, rule.notify
			break
		}
	}
	if d.Action == queueWait {
		d.WaitUntil = now.Add(playbackConfirmationTimeout)
	}
	return d
}

// This policy decides only whether active queue transitions block further work.
// Passing it never bypasses the caller's ordinary safety and write guards.
// Snapshots and deadlines are values; the caller applies the decision under the
// same mutex as the event callback so an older Play cannot clear a newer Stop.
func decideQueueObservation(state queuePolicyState, facts queueObservationFacts) queueDecision {
	active := state.Owned && state.Automating && state.ExpectedState == "play"
	pending := facts.Observed.State == "stop" || facts.Observed.State == "unknown" ||
		(facts.Observed.State == "play" && transitionalQueueMedia(facts.Before.Queue, facts.Observed.Media))
	waitUntil := state.WaitUntil
	if active && pending && waitUntil.IsZero() {
		waitUntil = facts.Now.Add(playbackConfirmationTimeout)
	}
	d := queueDecision{Action: queueWait, Rule: "media_pending", WaitUntil: waitUntil,
		Deadline: minTime(waitUntil, state.PlaybackUntil)}
	var changed error
	if active && !waitUntil.IsZero() {
		if fields := transitionChanges(facts.Before, facts.Observed); len(fields) != 0 {
			changed = ownershipMismatch("queue_transition_changed", fields, facts.Before, facts.Observed)
		}
	}
	// First matching rule wins. In particular, timeout precedes the secondary
	// connection gap that cancelling an in-flight read can produce.
	for _, rule := range []struct {
		name   string
		when   bool
		action queueAction
		err    error
	}{
		{"outside_active_queue", !active, queuePass, nil},
		{"no_transition", waitUntil.IsZero(), queuePass, nil},
		{"transition_timeout", facts.Expired || !facts.Now.Before(d.Deadline), queueRelease, errQueueTransitionTimeout},
		{"operation_cancelled", facts.Cancellation != nil, queueRelease, facts.Cancellation},
		{"observation_unsafe", facts.Unsafe != nil, queueRelease, facts.Unsafe},
		{"queue_controls_changed", changed != nil, queueRelease, changed},
		{"play_confirmed", facts.Observed.State == "play" && queuedMedia(facts.Before.Queue, facts.Observed.Media) && !facts.PendingEvents, queueResume, nil},
		{"events_pending", facts.PendingEvents, queueWait, nil},
		{"transport_pending", facts.Observed.State != "play", queueWait, nil},
	} {
		if rule.when {
			d.Action, d.Rule, d.Err = rule.action, rule.name, rule.err
			break
		}
	}
	if d.Action == queueResume {
		d.WaitUntil = time.Time{}
	}
	return d
}

func transitionChanges(before, after heos.Snapshot) []string {
	if before.Token.Generation != after.Token.Generation {
		return []string{"connection_generation"}
	}
	switch after.State {
	case "stop", "unknown", "play":
		before.State = after.State
	default:
		return []string{"state"}
	}
	// Home 150 can update MID/QID separately, even while state remains Play
	// (Denon 4.2.5/4.2.15, event 5.5). Only read-only waiting accepts that pair;
	// confirmation still requires exact membership in the unchanged queue.
	pendingIdentity := after.State != "play" && queuedIdentity(before.Queue, after.Media)
	if emptyMedia(after.Media) || queuedMedia(before.Queue, after.Media) || pendingIdentity || transitionalQueueMedia(before.Queue, after.Media) {
		before.Media = after.Media
	}
	return stateChanges(before, after)
}

// Both identifiers must belong to the confirmed queue, but identify different
// entries. This narrowly permits waiting, never a write or ownership confirmation.
func transitionalQueueMedia(queue heos.QueuePage, media *heos.Media) bool {
	if !queuedIdentity(queue, media) || media.QueueID == "" {
		return false
	}
	for _, item := range queue.Items {
		if item.QueueID == media.QueueID {
			return item.ID != media.ID
		}
	}
	return false
}
