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

func assertWireScalarNotConfirmed(t *testing.T, requests []string, operation journal.Operation) {
	t.Helper()
	set := slices.Index(requests, "player/set_volume")
	if set < 0 || set != len(requests)-1 {
		t.Errorf("rejected/interrupted setter triggered further wire commands: %v", requests)
	}
	var outcome struct {
		Confirmed int `json:"commands_confirmed"`
	}
	if err := json.Unmarshal(operation.Outcome, &outcome); err != nil {
		t.Fatal(err)
	}
	if outcome.Confirmed != 0 {
		t.Errorf("target event incorrectly confirmed a rejected/interrupted command: %s", operation.Outcome)
	}
}
