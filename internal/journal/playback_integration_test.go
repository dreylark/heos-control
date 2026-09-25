//go:build integration

package journal

import (
	"encoding/json"
	"errors"
	"testing"
)

// The bounded playback timeline uses existing JSON columns. A restarted controller exposes
// the original timeline and settings but releases the reservation, never resumes.
func TestBoundedPlaybackTimelineSurvivesRecovery(t *testing.T) {
	f := newJournalFixture(t)
	req, proposal := f.request("bounded", "physical")
	req.Endpoint = "/v1/players/test/playback"
	req.Body = json.RawMessage(`{"item_ref":"opaque","automation":{"duration_seconds":1200}}`)
	proposal.Kind = "playback"
	proposal.EffectiveArguments = json.RawMessage(`{"command":{"automation":{"duration_seconds":1200,"target_level":40}}}`)
	a, err := f.store.Admit(f.ctx, req, proposal)
	if err != nil {
		t.Fatal(err)
	}
	progress := json.RawMessage(`{"playback_started_at":"2026-09-06T05:30:00Z","stop_at":"2026-09-06T05:50:00Z","elapsed_seconds":300,"level":40}`)
	running, err := f.store.Transition(f.ctx, a.Operation.ID, a.Operation.Revision, Update{State: Running, Phase: "playing", Progress: progress})
	if err != nil {
		t.Fatal(err)
	}
	fresh := f.open()
	if err := fresh.Recover(f.ctx); err != nil {
		t.Fatal(err)
	}
	o, err := fresh.Lookup(f.ctx, req)
	if err != nil || o.ID != running.ID || o.State != Uncertain || o.ErrorCode != "process_interrupted" || o.FinishedAt == nil {
		t.Fatal(o, err)
	}
	if string(o.Progress) != string(running.Progress) || string(o.EffectiveArguments) != string(running.EffectiveArguments) {
		t.Fatal("recovery changed the frozen timeline/settings", o)
	}
	if _, err := fresh.Active(f.ctx, req.Player); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	replay, err := fresh.Admit(f.ctx, req, proposal)
	if err != nil || replay.Created || replay.Operation.ID != o.ID {
		t.Fatal(replay, err)
	}
}

func TestOrderedPlaybackProgressAndIdempotencySurviveRecovery(t *testing.T) {
	f := newJournalFixture(t)
	req, proposal := f.request("multipart", "physical")
	req.Endpoint = "/v1/players/test/playback"
	req.Body = json.RawMessage(`{"item_refs":["part-b","part-a","part-b"],"shuffle":false}`)
	proposal.Kind = "playback"
	proposal.EffectiveArguments = json.RawMessage(`{"ordered_plan":{"sha256":"frozen","part_tracks":[30,50,30]}}`)
	a, err := f.store.Admit(f.ctx, req, proposal)
	if err != nil {
		t.Fatal(err)
	}
	progress := json.RawMessage(`{"playback_started_at":"2026-09-06T05:30:00Z","stop_at":"2026-09-06T05:50:00Z","elapsed_seconds":2,"level":2,"queue_loading":{"total_parts":3,"confirmed_parts":1,"total_tracks":110,"confirmed_tracks":30}}`)
	running, err := f.store.Transition(f.ctx, a.Operation.ID, a.Operation.Revision, Update{State: Running, Phase: "sending_queue", Progress: progress})
	if err != nil {
		t.Fatal(err)
	}
	changed := req
	changed.Body = json.RawMessage(`{"item_refs":["part-a","part-b","part-b"],"shuffle":false}`)
	if _, err := f.store.Lookup(f.ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("reordered retry did not conflict", err)
	}
	fresh := f.open()
	if err := fresh.Recover(f.ctx); err != nil {
		t.Fatal(err)
	}
	o, err := fresh.Lookup(f.ctx, req)
	if err != nil || o.State != Uncertain || string(o.Progress) != string(running.Progress) || string(o.EffectiveArguments) != string(running.EffectiveArguments) {
		t.Fatal(o, err)
	}
	replay, err := fresh.Admit(f.ctx, req, proposal)
	if err != nil || replay.Created || replay.Operation.ID != o.ID {
		t.Fatal(replay, err)
	}
}
