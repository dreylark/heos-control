package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

func queuePolicyFixture() (queuePolicyState, queueObservationFacts) {
	now := time.Date(2026, 9, 12, 7, 0, 0, 0, time.UTC)
	volume, muted, total := 20, false, 2
	first := heos.Media{Source: "1024", ID: "first", QueueID: "1"}
	second := heos.Media{Source: "1024", ID: "second", QueueID: "2"}
	before := heos.Snapshot{State: "play", Volume: &volume, Muted: &muted, Repeat: "off", Shuffle: true,
		Media: &first, Queue: heos.QueuePage{Items: []heos.Media{first, second}, Total: &total}}
	after := before
	after.Media = &second
	return queuePolicyState{Owned: true, Automating: true, ExpectedState: "play", PlaybackUntil: now.Add(20 * time.Minute)},
		queueObservationFacts{Now: now, Before: before, Observed: after}
}

func TestQueueEventDecisionTable(t *testing.T) {
	for _, tc := range []struct {
		name, command, value   string
		owned, active, waiting bool
		action                 queueAction
		rule                   string
		notify                 bool
	}{
		{"no-queue", "player_now_playing_changed", "", false, true, false, queuePass, "queue_not_owned", false},
		{"metadata", "player_now_playing_changed", "", true, true, false, queueObserve, "media_notification", true},
		{"metadata-waiting", "player_now_playing_changed", "", true, true, true, queueObserve, "media_notification", true},
		{"play-duplicate", "player_state_changed", "play", true, true, false, queueIgnore, "play_unchanged", false},
		{"play-awaiting-read", "player_state_changed", "play", true, true, true, queueObserve, "play_requires_observation", true},
		{"stop", "player_state_changed", "stop", true, true, false, queueWait, "transport_transition", true},
		{"unknown", "player_state_changed", "unknown", true, true, false, queueWait, "transport_transition", true},
		{"stop-duplicate", "player_state_changed", "stop", true, true, true, queueIgnore, "transport_already_pending", false},
		{"unknown-duplicate", "player_state_changed", "unknown", true, true, true, queueIgnore, "transport_already_pending", false},
		{"stop-not-active", "player_state_changed", "stop", true, false, false, queuePass, "event_not_attributed", false},
		{"pause", "player_state_changed", "pause", true, true, true, queuePass, "event_not_attributed", false},
		{"queue-edit", "player_queue_changed", "", true, true, true, queuePass, "event_not_attributed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, f := queuePolicyFixture()
			s.Owned, s.Automating = tc.owned, tc.active
			if tc.waiting {
				s.WaitUntil = f.Now.Add(3 * time.Second)
			}
			e := heos.Event{Command: "event/" + tc.command, Params: url.Values{"pid": {"1"}, "state": {tc.value}}}
			d := decideQueueEvent(s, e, f.Now)
			if d.Action != tc.action || d.Rule != tc.rule || d.Notify != tc.notify || d.Err != nil {
				t.Fatalf("decision=%+v; want %s/%s notify=%t", d, tc.action, tc.rule, tc.notify)
			}
			wantDeadline := s.WaitUntil
			if tc.action == queueWait {
				wantDeadline = f.Now.Add(12 * time.Second)
			}
			if d.WaitUntil != wantDeadline {
				t.Fatal("changed transition deadline", d)
			}
		})
	}
}

func TestQueueObservationDecisionTable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*queuePolicyState, *queueObservationFacts)
		action queueAction
		rule   string
		err    error
	}{
		{"not-owned", func(s *queuePolicyState, _ *queueObservationFacts) { s.Owned = false }, queuePass, "outside_active_queue", nil},
		{"not-automating", func(s *queuePolicyState, _ *queueObservationFacts) { s.Automating = false }, queuePass, "outside_active_queue", nil},
		{"expected-not-play", func(s *queuePolicyState, _ *queueObservationFacts) { s.ExpectedState = "stop" }, queuePass, "outside_active_queue", nil},
		{"already-settled", func(s *queuePolicyState, _ *queueObservationFacts) { s.WaitUntil = time.Time{} }, queuePass, "no_transition", nil},
		{"confirmed", func(_ *queuePolicyState, _ *queueObservationFacts) {}, queueResume, "play_confirmed", nil},
		{"new-stop", func(s *queuePolicyState, f *queueObservationFacts) {
			s.WaitUntil = time.Time{}
			f.Observed.State = "stop"
		}, queueWait, "transport_pending", nil},
		{"unknown", func(_ *queuePolicyState, f *queueObservationFacts) { f.Observed.State = "unknown" }, queueWait, "transport_pending", nil},
		{"stop-transitional-qid", func(_ *queuePolicyState, f *queueObservationFacts) {
			f.Observed.State = "stop"
			f.Observed.Media.QueueID = "transitional"
		}, queueWait, "transport_pending", nil},
		{"hybrid", func(s *queuePolicyState, f *queueObservationFacts) {
			s.WaitUntil = time.Time{}
			f.Observed.Media.ID = "first"
		}, queueWait, "media_pending", nil},
		{"missing-media-after-stop", func(_ *queuePolicyState, f *queueObservationFacts) { f.Observed.Media = nil }, queueWait, "media_pending", nil},
		{"new-event-after-snapshot", func(_ *queuePolicyState, f *queueObservationFacts) { f.PendingEvents = true }, queueWait, "events_pending", nil},
		{"timeout-before-confirmation", func(s *queuePolicyState, f *queueObservationFacts) { s.WaitUntil = f.Now }, queueRelease, "transition_timeout", errQueueTransitionTimeout},
		{"playback-deadline", func(s *queuePolicyState, f *queueObservationFacts) { s.PlaybackUntil = f.Now }, queueRelease, "transition_timeout", errQueueTransitionTimeout},
		{"timeout-before-cleanup-gap", func(_ *queuePolicyState, f *queueObservationFacts) { f.Expired = true; f.Cancellation = ErrOwnership }, queueRelease, "transition_timeout", errQueueTransitionTimeout},
		{"timeout-before-all-faults", func(_ *queuePolicyState, f *queueObservationFacts) {
			f.Expired, f.PendingEvents = true, true
			f.Cancellation, f.Unsafe = context.Canceled, heos.ErrStale
			f.Observed.State = "pause"
		}, queueRelease, "transition_timeout", errQueueTransitionTimeout},
		{"cancel-before-confirmation", func(_ *queuePolicyState, f *queueObservationFacts) { f.Cancellation = context.Canceled }, queueRelease, "operation_cancelled", context.Canceled},
		{"cancel-before-unsafe-and-intervention", func(_ *queuePolicyState, f *queueObservationFacts) {
			f.Cancellation, f.Unsafe = context.Canceled, heos.ErrStale
			f.Observed.State = "pause"
		}, queueRelease, "operation_cancelled", context.Canceled},
		{"unsafe-before-confirmation", func(_ *queuePolicyState, f *queueObservationFacts) { f.Unsafe = heos.ErrStale }, queueRelease, "observation_unsafe", heos.ErrStale},
		{"unsafe-before-intervention", func(_ *queuePolicyState, f *queueObservationFacts) {
			f.Unsafe = heos.ErrStale
			f.Observed.State = "pause"
		}, queueRelease, "observation_unsafe", heos.ErrStale},
		{"intervention-before-wait", func(_ *queuePolicyState, f *queueObservationFacts) {
			v := 21
			f.Observed.Volume = &v
			f.Observed.Media.ID = "first"
		}, queueRelease, "queue_controls_changed", ErrOwnership},
		{"intervention-before-new-event", func(_ *queuePolicyState, f *queueObservationFacts) {
			f.PendingEvents = true
			f.Observed.State = "pause"
		}, queueRelease, "queue_controls_changed", ErrOwnership},
		{"pause", func(_ *queuePolicyState, f *queueObservationFacts) { f.Observed.State = "pause" }, queueRelease, "queue_controls_changed", ErrOwnership},
		{"foreign-mid", func(_ *queuePolicyState, f *queueObservationFacts) { f.Observed.Media.ID = "foreign" }, queueRelease, "queue_controls_changed", ErrOwnership},
		{"foreign-qid", func(_ *queuePolicyState, f *queueObservationFacts) { f.Observed.Media.QueueID = "foreign" }, queueRelease, "queue_controls_changed", ErrOwnership},
		{"foreign-source", func(_ *queuePolicyState, f *queueObservationFacts) { f.Observed.Media.Source = "foreign" }, queueRelease, "queue_controls_changed", ErrOwnership},
		{"generation", func(_ *queuePolicyState, f *queueObservationFacts) { f.Observed.Token.Generation++ }, queueRelease, "queue_controls_changed", ErrOwnership},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, f := queuePolicyFixture()
			s.WaitUntil = f.Now.Add(12 * time.Second)
			tc.change(&s, &f)
			beforeState := s
			// Serialize nested pointers/slices too; a shallow copy would miss
			// accidental mutation of the confirmed queue or media.
			beforeFacts, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			d := decideQueueObservation(s, f)
			if d.Action != tc.action || d.Rule != tc.rule || !errors.Is(d.Err, tc.err) {
				t.Fatalf("decision=%+v; want %s/%s err=%v", d, tc.action, tc.rule, tc.err)
			}
			afterFacts, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			if s != beforeState || !bytes.Equal(beforeFacts, afterFacts) {
				t.Fatal("decision modified its inputs")
			}
			if d.Action == queueResume && !d.WaitUntil.IsZero() {
				t.Fatal("confirmed Play retained the wait", d)
			}
		})
	}
}

func TestQueueEventUnexpectedPlayStillRequiresOrdinaryAttribution(t *testing.T) {
	for _, waiting := range []bool{false, true} {
		s, f := queuePolicyFixture()
		s.ExpectedState = "stop"
		if waiting {
			s.WaitUntil = f.Now.Add(3 * time.Second)
		}
		d := decideQueueEvent(s, heos.Event{Command: "event/player_state_changed", Params: url.Values{"state": {"play"}}}, f.Now)
		if d.Action != queuePass || d.Rule != "unexpected_play" || d.Notify != waiting || d.WaitUntil != s.WaitUntil {
			t.Fatal("unexpected Play attributed to the queue", d)
		}
	}
}

func TestQueuePolicyWaitNeverRenewsDeadlineOrConfirmsPendingEvents(t *testing.T) {
	s, f := queuePolicyFixture()
	f.Observed.Media.ID = "first"
	first := decideQueueObservation(s, f)
	if first.Action != queueWait {
		t.Fatal("hybrid pair did not enter wait", first)
	}
	s.WaitUntil = first.WaitUntil
	s.PlaybackUntil = f.Now.Add(2 * time.Second)
	for tick := range 9 {
		f.Now = f.Now.Add(250 * time.Millisecond)
		for range 10 {
			e := decideQueueEvent(s, heos.Event{Command: "event/player_now_playing_changed"}, f.Now)
			if e.WaitUntil != first.WaitUntil {
				t.Fatal("duplicate renewed deadline", e)
			}
		}
		f.PendingEvents = true
		d := decideQueueObservation(s, f)
		if tick < 7 && d.Action != queueWait || tick >= 7 && !errors.Is(d.Err, errQueueTransitionTimeout) {
			t.Fatal("pending media escaped fixed deadline", tick, d)
		}
		if d.WaitUntil != first.WaitUntil || d.Deadline != s.PlaybackUntil {
			t.Fatal("duplicate shifted transition/playback deadline", d)
		}
	}
}
