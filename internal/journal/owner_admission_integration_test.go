//go:build integration

package journal

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
)

func admitPlaybackOwner(t *testing.T, f *journalFixture) Operation {
	t.Helper()
	r, p := f.request("playback-owner", "physical")
	p.Kind = "playback"
	a, err := f.store.Admit(f.ctx, r, p)
	if err != nil || !a.Created {
		t.Fatal(a, err)
	}
	return a.Operation
}

func TestOwnedSkipAdmissionKeepsParentReservation(t *testing.T) {
	f := newJournalFixture(t)
	owner := admitPlaybackOwner(t, f)
	r, p := f.request("owned-skip", "physical")
	p.Kind, p.OwnerID = "skip", owner.ID
	a, err := f.store.Admit(f.ctx, r, p)
	if err != nil || !a.Created {
		t.Fatal(a, err)
	}
	f.counts(2, 2, 1)
	active, err := f.store.Active(f.ctx, "test")
	if err != nil || active.ID != owner.ID {
		t.Fatal(active, err)
	}
	if _, err = f.store.Transition(f.ctx, a.Operation.ID, a.Operation.Revision, Update{State: Succeeded}); err != nil {
		t.Fatal(err)
	}
	active, err = f.store.Active(f.ctx, "test")
	if err != nil || active.ID != owner.ID {
		t.Fatal("child completion released parent reservation", active, err)
	}
	replay, err := f.store.Admit(f.ctx, r, p)
	if err != nil || replay.Created || replay.Operation.ID != a.Operation.ID || replay.Operation.State != Succeeded {
		t.Fatal(replay, err)
	}
	if _, err = f.store.Transition(f.ctx, owner.ID, owner.Revision, Update{State: Released}); err != nil {
		t.Fatal(err)
	}
	replay, err = f.store.Admit(f.ctx, r, p)
	if err != nil || replay.Created || replay.Operation.ID != a.Operation.ID {
		t.Fatal("replay must not need a live owner", replay, err)
	}
	f.counts(2, 2, 0)
}

func TestOwnedSkipAdmissionRejectsWrongOwner(t *testing.T) {
	for _, scenario := range []string{"missing", "wrong-device", "wrong-player", "wrong-principal", "terminal", "wrong-kind", "replace", "not-playback"} {
		t.Run(scenario, func(t *testing.T) {
			f := newJournalFixture(t)
			owner := admitPlaybackOwner(t, f)
			r, p := f.request("owned-skip", "physical")
			p.Kind, p.OwnerID = "skip", owner.ID
			want := ErrRevision
			switch scenario {
			case "missing":
				p.OwnerID = "op_missing"
			case "wrong-device":
				p.DeviceKey = "other"
			case "wrong-player":
				r.Player = "other"
			case "wrong-principal":
				r.Principal = "other"
			case "terminal":
				if _, err := f.store.Transition(f.ctx, owner.ID, owner.Revision, Update{State: Released}); err != nil {
					t.Fatal(err)
				}
			case "wrong-kind":
				p.Kind, want = "volume", ErrInvalid
			case "replace":
				p.ReplaceID, p.ReplaceRevision, want = owner.ID, owner.Revision, ErrInvalid
			case "not-playback":
				f.exec("UPDATE heos.operations SET kind = 'volume' WHERE id = $1", owner.ID)
			}
			if _, err := f.store.Admit(f.ctx, r, p); !errors.Is(err, want) {
				t.Fatalf("got %v, want %v", err, want)
			}
			reservations := 1
			if scenario == "terminal" {
				reservations = 0
			}
			f.counts(1, 1, reservations)
		})
	}
}

func TestOwnedSkipCommitRecoveryAndRestart(t *testing.T) {
	f := newJournalFixture(t)
	owner := admitPlaybackOwner(t, f)
	r, p := f.request("owned-skip", "physical")
	p.Kind, p.OwnerID = "skip", owner.ID
	f.store.commit = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	a, err := f.store.Admit(f.ctx, r, p)
	if err != nil || !a.Created || !a.RecoveredCommit {
		t.Fatal(a, err)
	}
	f.counts(2, 2, 1)
	newOwner := f.open()
	if err = newOwner.Recover(f.ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{owner.ID, a.Operation.ID} {
		op, getErr := newOwner.Get(f.ctx, id)
		if getErr != nil || op.State != Uncertain || op.FinishedAt == nil {
			t.Fatal(op, getErr)
		}
	}
	f.counts(2, 2, 0)
}

func TestOwnedSkipLateCompletionCannotReleaseSuccessor(t *testing.T) {
	f := newJournalFixture(t)
	owner := admitPlaybackOwner(t, f)
	r, p := f.request("owned-skip", "physical")
	p.Kind, p.OwnerID = "skip", owner.ID
	child, err := f.store.Admit(f.ctx, r, p)
	if err != nil || !child.Created {
		t.Fatal(child, err)
	}
	stopRequest, stopProposal := f.request("priority-stop", "physical")
	stopProposal.Kind = "stop"
	stopProposal.ReplaceID, stopProposal.ReplaceRevision = owner.ID, owner.Revision
	stop, err := f.store.Admit(f.ctx, stopRequest, stopProposal)
	if err != nil || !stop.Created {
		t.Fatal(stop, err)
	}
	if _, err = f.store.Transition(f.ctx, child.Operation.ID, child.Operation.Revision, Update{State: Released, Phase: "complete"}); err != nil {
		t.Fatal(err)
	}
	active, err := f.store.Active(f.ctx, "test")
	if err != nil || active.ID != stop.Operation.ID {
		t.Fatal("late child completion released successor", active, err)
	}
	r.Key = "late-skip"
	if _, err = f.store.Admit(f.ctx, r, p); !errors.Is(err, ErrRevision) {
		t.Fatal(err)
	}
	f.counts(3, 3, 1)
}
