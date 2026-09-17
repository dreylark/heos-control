package control

import (
	"slices"
	"strings"
	"testing"
)

func assertQueueLoadingWireBudget(t *testing.T, requests []string) {
	t.Helper()
	queueAt, rampAt, lastVolume := slices.Index(requests, "browse/add_to_queue"), -1, -1
	for i := queueAt + 1; i < len(requests); i++ {
		if requests[i] == "player/set_volume" {
			if rampAt < 0 {
				rampAt = i
			}
			lastVolume = i
		}
	}
	if queueAt < 0 || rampAt < 0 {
		t.Fatal("loading fixture did not exercise queue confirmation and ramp", requests)
	}
	loading := requests[queueAt+1 : rampAt]
	fullReads := 0
	for _, request := range loading {
		if !strings.Contains(request, "/get_") {
			t.Error("write before queue confirmation or repeated queue command", request)
		}
		if request == "player/get_queue" {
			fullReads++
		}
	}
	// This fixture settles after three state reads. A state notification during
	// GET can require an immediate stale-read retry, including a fourth state
	// GET. Bound actual commands, including the background observer, not just
	// successful Refresh calls: at most three complete eight-command reads.
	if fullReads > 3 || len(loading) > 32 {
		t.Error("excessive loading reads", loading)
	}
	for _, request := range requests[rampAt : lastVolume+1] {
		if strings.Contains(request, "/get_") {
			t.Error("confirmed ramp/fade required a GET", request)
		}
	}
	stop := lastVolume + 1 + slices.Index(requests[lastVolume+1:], "player/set_play_state")
	if stop <= lastVolume || stop-lastVolume-1 > 8 {
		t.Error("missing Stop or extra reads beyond its full prewrite guard", requests[lastVolume+1:])
	}
}
