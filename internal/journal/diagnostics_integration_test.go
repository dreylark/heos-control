//go:build integration

package journal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
)

func operationDiagnostics(t *testing.T, o Operation) Diagnostics {
	t.Helper()
	var out struct {
		Diagnostics *Diagnostics `json:"diagnostics"`
	}
	if err := json.Unmarshal(o.Outcome, &out); err != nil || out.Diagnostics == nil {
		t.Fatalf("missing durable explanation: %s, %v", o.Outcome, err)
	}
	return *out.Diagnostics
}

func TestJournalDiagnosticHandoff(t *testing.T) {
	for _, kind := range []string{"stop", "playback", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			f := newJournalFixture(t)
			_, target := f.admit("target", "physical")
			var err error
			target, err = f.store.Transition(f.ctx, target.ID, target.Revision, Update{State: Running, Phase: "ramping", Progress: []byte(`{"playback":{"level":7}}`), Outcome: []byte(`{"commands_confirmed":3}`)})
			if err != nil {
				t.Fatal(err)
			}
			r, p := f.request("replacement", "physical")
			p.Kind, p.ReplaceID, p.ReplaceRevision = kind, target.ID, target.Revision
			f.store.commit = func(ctx context.Context, tx pgx.Tx) error {
				if err := tx.Commit(ctx); err != nil {
					return err
				}
				return io.ErrUnexpectedEOF
			}
			a, err := f.store.Admit(f.ctx, r, p)
			if err != nil || !a.Created || !a.RecoveredCommit {
				t.Fatalf("handoff failed: %+v %v", a, err)
			}
			got, err := f.store.Get(f.ctx, target.ID)
			if err != nil {
				t.Fatal(err)
			}
			d := operationDiagnostics(t, got)
			reason := "superseded"
			if kind == "cancel" {
				reason = "cancellation_requested"
				if got.State != Running || got.Phase != "cancelling" || got.FinishedAt != nil {
					t.Fatalf("cancel target prematurely completed: %+v", got)
				}
			} else if got.State != Released || got.FinishedAt == nil {
				t.Fatalf("target was not released: %+v", got)
			}
			if d.Reason != reason || d.Source != "controller" || d.Phase != "ramping" || d.DetectedAt == nil || !d.DetectedAt.Equal(f.now) || d.Observed != nil {
				t.Fatalf("incorrect target evidence: %+v", d)
			}
			var out struct {
				Commands int `json:"commands_confirmed"`
			}
			if err := json.Unmarshal(got.Outcome, &out); err != nil || out.Commands != 3 {
				t.Fatalf("handoff dropped progress: %s", got.Outcome)
			}
			f.now = f.now.Add(Retention)
			replay, err := f.store.Admit(f.ctx, r, p)
			if err != nil || replay.Created || replay.Operation.ID != a.Operation.ID {
				t.Fatalf("replay failed: %+v %v", replay, err)
			}
			again, err := f.store.Get(f.ctx, target.ID)
			if err != nil || string(again.Outcome) != string(got.Outcome) {
				t.Fatal("handoff retry replaced explanation", err)
			}
			f.counts(2, 2, 1)
		})
	}
}

func TestJournalDiagnosticRecovery(t *testing.T) {
	f := newJournalFixture(t)
	_, target := f.admit("target", "physical")
	target, err := f.store.Transition(f.ctx, target.ID, target.Revision, Update{State: Running, Phase: "holding", Progress: []byte(`{"playback":{"level":7}}`)})
	if err != nil {
		t.Fatal(err)
	}
	next := f.open()
	if err := next.Recover(f.ctx); err != nil {
		t.Fatal(err)
	}
	got, err := next.Get(f.ctx, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	d := operationDiagnostics(t, got)
	if d.Reason != "process_interrupted" || d.Phase != "holding" || d.Source != "recovery" || d.DetectedAt != nil || len(d.ChangedFields) != 0 || d.Expected != nil || d.Observed != nil {
		t.Fatalf("recovery invented evidence: %+v", d)
	}
	if got.State != Uncertain || got.Phase != "restart" || got.ErrorCode != "process_interrupted" || got.FinishedAt == nil {
		t.Fatalf("incorrect recovery classification: %+v", got)
	}
	if _, err := next.Transition(f.ctx, target.ID, got.Revision, Update{State: Succeeded, Phase: "complete"}); !errors.Is(err, ErrRevision) {
		t.Fatalf("terminal explanation mutable: %v", err)
	}
	if err := next.Recover(f.ctx); err != nil {
		t.Fatal(err)
	}
	again, err := next.Get(f.ctx, target.ID)
	if err != nil || string(again.Outcome) != string(got.Outcome) {
		t.Fatal("repeated recovery replaced evidence", err)
	}
	f.counts(1, 1, 0)
}

func TestJournalDiagnosticTerminalResultIsAtomic(t *testing.T) {
	f := newJournalFixture(t)
	_, target := f.admit("target", "physical")
	outcome, err := WithDiagnostics([]byte(`{"delivery":"confirmed","commands_confirmed":2}`), Diagnostics{Reason: "completed", Phase: "holding", Source: "controller", DetectedAt: &f.now})
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.store.Transition(f.ctx, target.ID, target.Revision, Update{State: Succeeded, Phase: "complete", Progress: []byte(`{"playback":{"level":7}}`), Outcome: outcome})
	if err != nil {
		t.Fatal(err)
	}
	if operationDiagnostics(t, got).Reason != "completed" || got.FinishedAt == nil {
		t.Fatalf("incomplete terminal commit: %+v", got)
	}
	f.counts(1, 1, 0)
	other := f.open()
	reloaded, err := other.Get(f.ctx, target.ID)
	if err != nil || string(reloaded.Outcome) != string(got.Outcome) {
		t.Fatal("terminal evidence lost after pool replacement", err)
	}
	if _, err := f.store.Transition(f.ctx, target.ID, got.Revision, Update{State: Failed, Phase: "failed"}); !errors.Is(err, ErrRevision) {
		t.Fatalf("terminal evidence could be overwritten: %v", err)
	}
}
