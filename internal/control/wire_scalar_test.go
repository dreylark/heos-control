package control

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/journal"
)

func assertWireScalarFallback(t *testing.T, requests []string, times []time.Time) {
	t.Helper()
	set := slices.Index(requests, "player/set_volume")
	next := slices.Index(requests, "player/set_play_mode")
	if next < 0 {
		next = len(requests) // A fallback that disproves application ends the operation.
	}
	if set < 0 || next <= set {
		t.Fatal("fixture did not verify the volume setter", requests)
	}
	want := []string{"player/get_volume", "player/get_mute"}
	if got := requests[set+1 : next]; !slices.Equal(got, want) {
		t.Fatalf("missing scalar event produced %v; want exactly one targeted fallback %v, with no SET replay", got, want)
	}
	if elapsed := times[set+1].Sub(times[set]); elapsed < 11*time.Second || elapsed >= 12*time.Second {
		t.Fatalf("targeted fallback ran after %v; want event wait followed by fallback in the final second of the twelve-second budget", elapsed)
	}
}

func assertWireScalarNotConfirmed(t *testing.T, requests []string, operation journal.Operation, rejected bool) {
	t.Helper()
	set := slices.Index(requests, "player/set_volume")
	if set < 0 {
		t.Fatal("fixture did not send the volume setter", requests)
	}
	var want []string
	if rejected {
		// A definite device refusal permits one full read-only recovery of the
		// consumed write token. Manual intervention still stops all device I/O.
		want = []string{"player/get_players", "group/get_groups", "player/get_play_state", "player/get_volume", "player/get_mute", "player/get_play_mode", "player/get_now_playing_media", "player/get_queue"}
	}
	if got := requests[set+1:]; !slices.Equal(got, want) {
		t.Errorf("unexpected wire commands after rejected/interrupted setter: got %v, want %v", got, want)
	}
	var outcome struct {
		Confirmed int    `json:"commands_confirmed"`
		Delivery  string `json:"delivery"`
	}
	if err := json.Unmarshal(operation.Outcome, &outcome); err != nil {
		t.Fatal(err)
	}
	if outcome.Confirmed != 0 {
		t.Errorf("target event incorrectly confirmed a rejected/interrupted command: %s", operation.Outcome)
	}
	if rejected && (operation.State != journal.Failed || operation.ErrorCode != "device_rejected" || outcome.Delivery != "rejected") {
		t.Errorf("recovery changed the rejected setter outcome: %+v", operation)
	}
}
