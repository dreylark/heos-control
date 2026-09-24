package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

func queueStartFixture() queueStartFacts {
	now := time.Date(2026, 9, 13, 7, 0, 0, 0, time.UTC)
	v, muted, total := 10, false, 2
	old := heos.Media{Source: "1024", ID: "old", QueueID: "1"}
	first := heos.Media{Source: "1024", ID: "first", QueueID: "1"}
	second := heos.Media{Source: "1024", ID: "second", QueueID: "2"}
	before := heos.Snapshot{State: heos.PlayStateStop, Volume: &v, Muted: &muted, Repeat: heos.RepeatOff, Shuffle: true,
		Media: &old, Queue: heos.QueuePage{Items: []heos.Media{old}}}
	after := before
	after.State, after.Media = "play", &second
	after.Queue = heos.QueuePage{Items: []heos.Media{first, second}, Total: &total}
	return queueStartFacts{Now: now, Deadline: now.Add(12 * time.Second), Phase: queueAwaitingPlay,
		Bounded: true, Members: map[heos.ID]bool{"first": true, "second": true}, Before: before, Observed: after}
}

func unresolvedQueueStart(f *queueStartFacts) {
	f.Observed.State = heos.PlayStateUnknown
	f.Observed.Media.ID = "loading-mid"
}

func TestQueueStartDecisionTable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*queueStartFacts)
		action queueStartAction
		rule   string
		err    error
	}{
		{"selected-play", func(*queueStartFacts) {}, queueStartConfirm, "queue_start_confirmed", nil},
		{"not-starting", func(f *queueStartFacts) { f.Phase = queueNotStarting }, queueStartAbort, "queue_start_inactive", ErrOwnership},
		{"expired-play", func(f *queueStartFacts) { f.Now = f.Deadline }, queueStartAbort, "queue_start_timeout", context.DeadlineExceeded},
		{"timeout-before-secondary-gap", func(f *queueStartFacts) { f.Now = f.Deadline; f.Cancellation = ErrOwnership }, queueStartAbort, "queue_start_timeout", context.DeadlineExceeded},
		{"cancelled-play", func(f *queueStartFacts) { f.Cancellation = context.Canceled }, queueStartAbort, "operation_cancelled", context.Canceled},
		{"cancel-before-other-faults", func(f *queueStartFacts) {
			f.Cancellation, f.Unsafe, f.Observed.State = context.Canceled, heos.ErrStale, "pause"
		}, queueStartAbort, "operation_cancelled", context.Canceled},
		{"unsafe-play", func(f *queueStartFacts) { f.Unsafe = heos.ErrStale }, queueStartAbort, "observation_unsafe", heos.ErrStale},
		{"new-event-before-confirmation", func(f *queueStartFacts) { f.PendingEvents = true }, queueStartWait, "events_pending", nil},
		{"old-state", func(f *queueStartFacts) { f.Observed = f.Before }, queueStartWait, "queue_loading", nil},
		{"missing-media", func(f *queueStartFacts) { f.Observed.Media = nil }, queueStartWait, "queue_loading", nil},
		{"selected-unknown", func(f *queueStartFacts) { f.Observed.State = heos.PlayStateUnknown }, queueStartWait, "queue_loading", nil},
		{"selected-hybrid-pair", func(f *queueStartFacts) { f.Observed.Media.QueueID = "1" }, queueStartWait, "queue_loading", nil},
		{"short-command-keeps-existing-confirmation", func(f *queueStartFacts) {
			f.Bounded = false
			f.Observed.Queue.Total = nil
		}, queueStartConfirm, "queue_start_confirmed", nil},
		{"unresolved-local-mid", unresolvedQueueStart, queueStartWait, "loading_media_pending", nil},
		{"unresolved-mid-short-command-complete-queue", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			f.Bounded = false
		}, queueStartWait, "loading_media_pending", nil},
		{"unresolved-after-play", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			f.Phase = queuePlayObserved
		}, queueStartAbort, "queue_media_changed", ErrOwnership},
		{"unresolved-while-stopped", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			f.Observed.State = heos.PlayStateStop
		}, queueStartAbort, "queue_media_changed", ErrOwnership},
		{"unselected-play", func(f *queueStartFacts) { f.Observed.Media.ID = "foreign" }, queueStartAbort, "queue_media_changed", ErrOwnership},
		{"unresolved-foreign-source", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			f.Observed.Media.Source = "foreign"
		}, queueStartAbort, "queue_media_changed", ErrOwnership},
		{"empty-mid-is-not-unresolved-mid", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			f.Observed.Media.ID = ""
		}, queueStartAbort, "queue_media_changed", ErrOwnership},
		{"unresolved-incomplete-queue", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			n := 3
			f.Observed.Queue.Total = &n
		}, queueStartAbort, "queue_media_changed", ErrOwnership},
		{"unresolved-paged-queue", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			n := 2
			f.Observed.Queue.Next = &n
		}, queueStartAbort, "queue_media_changed", ErrOwnership},
		{"unresolved-queue-without-total", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			f.Observed.Queue.Total = nil
		}, queueStartAbort, "queue_media_changed", ErrOwnership},
		{"unresolved-empty-queue", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			f.Observed.Queue.Items = nil
			n := 0
			f.Observed.Queue.Total = &n
		}, queueStartAbort, "queue_media_changed", ErrOwnership},
		{"unresolved-duplicate-position", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			f.Observed.Queue.Items[1].QueueID = "1"
		}, queueStartAbort, "queue_media_changed", ErrOwnership},
		{"unresolved-old-queue", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			f.Observed.Queue = f.Before.Queue
		}, queueStartAbort, "queue_media_changed", ErrOwnership},
		{"unresolved-foreign-queue", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			f.Observed.Queue.Items[0].ID = "foreign"
		}, queueStartAbort, "queue_items_changed", ErrOwnership},
		{"pause-before-wait", func(f *queueStartFacts) { unresolvedQueueStart(f); f.Observed.State = heos.PlayStatePause }, queueStartAbort, "queue_state_changed", ErrOwnership},
		{"stop-after-play", func(f *queueStartFacts) { f.Phase = queuePlayObserved; f.Observed.State = heos.PlayStateStop }, queueStartAbort, "queue_state_changed", ErrOwnership},
		{"generation-before-wait", func(f *queueStartFacts) { unresolvedQueueStart(f); f.Observed.Token.Generation++ }, queueStartAbort, "queue_controls_changed", ErrOwnership},
		{"volume-before-wait", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			v := 11
			f.Observed.Volume = &v
		}, queueStartAbort, "queue_controls_changed", ErrOwnership},
		{"mute-before-wait", func(f *queueStartFacts) {
			unresolvedQueueStart(f)
			muted := true
			f.Observed.Muted = &muted
		}, queueStartAbort, "queue_controls_changed", ErrOwnership},
		{"mode-before-wait", func(f *queueStartFacts) { unresolvedQueueStart(f); f.Observed.Repeat = heos.RepeatOnAll }, queueStartAbort, "queue_controls_changed", ErrOwnership},
		{"shuffle-before-wait", func(f *queueStartFacts) { unresolvedQueueStart(f); f.Observed.Shuffle = false }, queueStartAbort, "queue_controls_changed", ErrOwnership},
		{"group-before-wait", func(f *queueStartFacts) { unresolvedQueueStart(f); f.Observed.Grouped = true }, queueStartAbort, "queue_controls_changed", ErrOwnership},
		{"identity-before-wait", func(f *queueStartFacts) { unresolvedQueueStart(f); f.Observed.Player.Serial = "other" }, queueStartAbort, "queue_controls_changed", ErrOwnership},
		{"controls-before-confirmation-and-events", func(f *queueStartFacts) {
			f.Observed.Shuffle, f.PendingEvents = false, true
		}, queueStartAbort, "queue_controls_changed", ErrOwnership},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := queueStartFixture()
			tc.change(&f)
			before, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			d := decideQueueStart(f)
			if d.Action != tc.action || d.Rule != tc.rule || !errors.Is(d.Err, tc.err) {
				t.Errorf("decision=%+v; want %s/%s err=%v", d, tc.action, tc.rule, tc.err)
			}
			after, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Error("queue-start decision modified its inputs")
			}
		})
	}
}

func TestQueueStartDeadlineCannotBeRenewedByPendingMetadata(t *testing.T) {
	f := queueStartFixture()
	unresolvedQueueStart(&f)
	deadline := f.Deadline
	for i := range 49 {
		f.Now = deadline.Add(time.Duration(i-48) * 250 * time.Millisecond)
		f.PendingEvents = i%2 == 0
		d := decideQueueStart(f)
		if i < 48 && d.Action != queueStartWait || i == 48 && !errors.Is(d.Err, context.DeadlineExceeded) {
			t.Fatal("unresolved media escaped the fixed deadline", i, d)
		}
		if f.Deadline != deadline {
			t.Fatal("deadline was renewed")
		}
	}
}

func TestQueueStartFullSnapshotCanCoverEventsReceivedDuringRead(t *testing.T) {
	for _, tc := range []struct {
		name      string
		player    uint64
		projected bool
		want      queueStartAction
	}{
		{"full-covers-events", 6, false, queueStartConfirm},
		{"full-before-last-event", 5, false, queueStartWait},
		{"projection-is-not-full-readback", 6, true, queueStartWait},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, base, _ := fixtureCoordinator(t)
			f := queueStartFixture()
			c.clock = &modeEventClock{now: f.Now}
			f.Before.Player, f.Observed.Player = base.s.Player, base.s.Player
			f.Observed.Connected, f.Observed.Verified = true, true
			f.Observed.EventUpdated = tc.projected
			f.Before.Token = heos.Token{Generation: 1, Player: 4}
			f.Observed.Token = heos.Token{Generation: 1, Player: tc.player}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			r := &execution{ctx: ctx, cancel: cancel, expected: f.Before, queueStart: queueAwaitingPlay,
				bounded: true, members: f.Members, events: map[string][]map[string]string{},
				eventRevision: 2, observedRevision: 0}
			r.expected.Token = heos.Token{Generation: 1, Player: 6}
			d := c.queueStartObservation(ctx, c.lanes["room"], r, f.Before, f.Observed, f.Deadline)
			if d.Action != tc.want {
				t.Fatal("incorrect event coverage for initial confirmation", d)
			}
			if tc.want == queueStartConfirm && r.observedRevision != r.eventRevision {
				t.Fatal("confirmed events would cause redundant later reads")
			}
		})
	}
}
