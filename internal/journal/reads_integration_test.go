//go:build integration

package journal

import (
	"errors"
	"testing"
)

func TestActiveOperationTracksReservation(t *testing.T) {
	f := newJournalFixture(t)
	if _, e := f.store.Active(f.ctx, "test"); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	r, p := f.request("read-active", "physical")
	a, e := f.store.Admit(f.ctx, r, p)
	if e != nil {
		t.Fatal(e)
	}
	active, e := f.store.Active(f.ctx, "test")
	if e != nil || active.ID != a.Operation.ID {
		t.Fatal(active, e)
	}
	if _, e = f.store.Transition(f.ctx, active.ID, active.Revision, Update{State: Cancelled, Phase: "cancelled", Progress: []byte(`{}`), Outcome: []byte(`{}`)}); e != nil {
		t.Fatal(e)
	}
	if _, e = f.store.Active(f.ctx, "test"); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
}
