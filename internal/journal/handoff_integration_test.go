//go:build integration

package journal

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"io"
	"testing"
)

func TestReservationHandoffIsAtomicAndReplayable(t *testing.T) {
	f := newJournalFixture(t)
	_, old := f.admit("old", "physical")
	r, p := f.request("stop", "physical")
	p.Kind = "stop"
	p.ReplaceID, p.ReplaceRevision = old.ID, old.Revision+1
	if _, e := f.store.Admit(f.ctx, r, p); !errors.Is(e, ErrRevision) {
		t.Fatal(e)
	}
	active, e := f.store.Active(f.ctx, "test")
	if e != nil || active.ID != old.ID {
		t.Fatal(active, e)
	}
	f.counts(1, 1, 1)
	p.ReplaceRevision = old.Revision
	a, e := f.store.Admit(f.ctx, r, p)
	if e != nil || !a.Created {
		t.Fatal(a, e)
	}
	previous, e := f.store.Get(f.ctx, old.ID)
	if e != nil || previous.State != Released {
		t.Fatal(previous, e)
	}
	active, e = f.store.Active(f.ctx, "test")
	if e != nil || active.ID != a.Operation.ID {
		t.Fatal(active, e)
	}
	replay, e := f.store.Admit(f.ctx, r, p)
	if e != nil || replay.Created || replay.Operation.ID != a.Operation.ID {
		t.Fatal(replay, e)
	}
	f.counts(2, 2, 1)
}

func TestCancelHandoffKeepsTargetPendingAndLostCommitDoesNotDuplicate(t *testing.T) {
	f := newJournalFixture(t)
	_, old := f.admit("old", "physical")
	r, p := f.request("cancel", "physical")
	p.Kind = "cancel"
	p.ReplaceID, p.ReplaceRevision = old.ID, old.Revision
	f.store.maxOperations = 1
	if _, e := f.store.Admit(f.ctx, r, p); !errors.Is(e, ErrCapacity) {
		t.Fatal(e)
	}
	f.counts(1, 1, 1)
	f.store.maxOperations = 100
	f.store.commit = func(ctx context.Context, tx pgx.Tx) error {
		if e := tx.Commit(ctx); e != nil {
			return e
		}
		return io.ErrUnexpectedEOF
	}
	a, e := f.store.Admit(f.ctx, r, p)
	if e != nil || !a.Created || !a.RecoveredCommit {
		t.Fatal(a, e)
	}
	target, e := f.store.Get(f.ctx, old.ID)
	if e != nil || target.State != Running || target.Phase != "cancelling" || target.FinishedAt != nil {
		t.Fatal(target, e)
	}
	active, e := f.store.Active(f.ctx, "test")
	if e != nil || active.ID != a.Operation.ID {
		t.Fatal(active, e)
	}
	replay, e := f.store.Admit(f.ctx, r, p)
	if e != nil || replay.Created || replay.Operation.ID != a.Operation.ID {
		t.Fatal(replay, e)
	}
	f.counts(2, 2, 1)
	if _, e = f.store.Transition(f.ctx, target.ID, target.Revision, Update{State: Cancelled, Phase: "cancelled"}); e != nil {
		t.Fatal(e)
	}
	if _, e = f.store.Transition(f.ctx, a.Operation.ID, a.Operation.Revision, Update{State: Succeeded, Phase: "complete"}); e != nil {
		t.Fatal(e)
	}
	f.counts(2, 2, 0)
}
