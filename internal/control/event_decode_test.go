package control

import (
	"context"
	"errors"
	"net/url"
	"testing"

	"github.com/dreylark/heos-control/internal/heos"
)

// Malformed notifications must not confirm an expected command, extend a queue
// transition or disappear as supposedly harmless progress from another player.
func TestMalformedEventCannotPreserveOwnership(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event heos.Event
	}{
		{"duplicate-state", heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play", "pause"}}}},
		{"duplicate-target", heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1", "2"}, "state": {"play"}}}},
		{"ambiguous-other-player", heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"2", "1"}, "state": {"play"}}}},
		{"incomplete-progress", heos.Event{Command: "event/player_now_playing_progress", Params: url.Values{"pid": {"1"}, "cur_pos": {"10"}}}},
		{"duplicate-progress-player", heos.Event{Command: "event/player_now_playing_progress", Params: url.Values{"pid": {"2", "1"}, "cur_pos": {"10"}, "duration": {"100"}}}},
		{"duplicate-volume", heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"20", "21"}, "mute": {"off"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, r, _, _ := modeEventFixture(t)
			r.queueOwned, r.automating = true, true
			r.expect(heos.Mutation{Kind: heos.MutationKindTransport, State: heos.PlayStatePlay})
			r.event(tc.event)
			if !errors.Is(context.Cause(r.ctx), ErrOwnership) {
				t.Fatal("malformed event preserved ownership", context.Cause(r.ctx))
			}
			if !r.queueWait.IsZero() {
				t.Fatal("malformed event started a transition wait")
			}
		})
	}
}

func TestPartialEventKeepsOnlyValidatedDiagnosticFields(t *testing.T) {
	for _, tc := range []struct {
		name       string
		params     url.Values
		wantVolume bool
		wantMute   bool
	}{
		{"missing-mute", url.Values{"pid": {"1"}, "level": {"21"}}, true, false},
		{"invalid-volume", url.Values{"pid": {"1"}, "level": {"101"}, "mute": {"on"}}, false, true},
		{"duplicate-mute", url.Values{"pid": {"1"}, "level": {"21"}, "mute": {"on", "off"}}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, r, _, _ := modeEventFixture(t)
			r.expect(heos.Mutation{Kind: heos.MutationKindVolume, Level: 21})
			r.event(heos.Event{Command: "event/player_volume_changed", Params: tc.params})
			var lost *ownershipError
			if !errors.As(context.Cause(r.ctx), &lost) {
				t.Fatal("partial event confirmed a command", context.Cause(r.ctx))
			}
			observed := lost.evidence.Observed
			if observed == nil || (observed.Volume != nil) != tc.wantVolume || (observed.Muted != nil) != tc.wantMute {
				t.Fatalf("wrong partial evidence: %+v", observed)
			}
			if observed.State != nil || observed.Repeat != nil || observed.Shuffle != nil || observed.Grouped != nil {
				t.Fatal("partial event invented unrelated evidence", observed)
			}
		})
	}
}

func TestMalformedScalarForUnambiguousOtherPlayerDoesNotCancel(t *testing.T) {
	for _, gap := range []bool{false, true} {
		_, r, _, _ := modeEventFixture(t)
		r.event(heos.Event{Command: "event/player_volume_changed", Gap: gap, Params: url.Values{"pid": {"2"}, "level": {"invalid"}}})
		if errors.Is(context.Cause(r.ctx), ErrOwnership) != gap {
			t.Fatalf("unrelated scalar or gap attribution changed: gap=%t, err=%v", gap, context.Cause(r.ctx))
		}
	}
}

func TestControlUsesDecodedEventValues(t *testing.T) {
	t.Run("scalar-confirmation", func(t *testing.T) {
		_, r, _, _ := modeEventFixture(t)
		r.expect(heos.Mutation{Kind: heos.MutationKindVolume, Level: 21})
		e := volumeEvent("21", "off").Decode()
		e.Params.Set("level", "99")
		e.Params.Set("mute", "on")
		e.Params.Set("pid", "other")
		r.event(e)
		if err := context.Cause(r.ctx); err != nil || !r.scalar.ready() || *r.scalar.observed.Volume != 21 || *r.scalar.observed.Muted {
			t.Fatalf("confirmation reread raw fields: scalar=%+v, err=%v", r.scalar, err)
		}
	})
	t.Run("transport-confirmation", func(t *testing.T) {
		_, r, _, _ := modeEventFixture(t)
		r.expect(heos.Mutation{Kind: heos.MutationKindTransport, State: heos.PlayStatePlay})
		e := heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play"}}}.Decode()
		e.Params.Set("state", "pause")
		r.event(e)
		if err := context.Cause(r.ctx); err != nil || len(r.events[e.Command]) != 0 {
			t.Fatal("transport confirmation reread raw state", err)
		}
	})
	t.Run("queue-transition", func(t *testing.T) {
		s, f := queuePolicyFixture()
		e := heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"unknown"}}}.Decode()
		e.Params.Set("state", "pause")
		if d := decideQueueEvent(s, e, f.Now); d.Action != queueWait || d.Rule != "transport_transition" {
			t.Fatal("transition reread raw state", d)
		}
	})
	t.Run("partial-diagnostics", func(t *testing.T) {
		_, r, _, _ := modeEventFixture(t)
		r.expect(heos.Mutation{Kind: heos.MutationKindVolume, Level: 21})
		e := heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"21"}}}.Decode()
		e.Params.Set("level", "99")
		e.Params.Set("mute", "on")
		r.event(e)
		var lost *ownershipError
		if !errors.As(context.Cause(r.ctx), &lost) || lost.evidence.Observed == nil || lost.evidence.Observed.Volume == nil {
			t.Fatal("lost partial diagnostic evidence", context.Cause(r.ctx))
		}
		if *lost.evidence.Observed.Volume != 21 || lost.evidence.Observed.Muted != nil {
			t.Fatal("diagnostics reread raw fields", lost.evidence.Observed)
		}
	})
}
