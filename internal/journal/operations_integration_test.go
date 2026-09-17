//go:build integration

package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type journalFixture struct {
	t       *testing.T
	ctx     context.Context
	owner   *pgx.Conn
	store   *Store
	runtime config.Database
	now     time.Time
}

func newJournalFixture(t *testing.T) *journalFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	ownerCfg := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG")
	runtimeCfg := loadIntegrationConfig(t, "HEOS_TEST_RUNTIME_CONFIG")
	runtimeCfg.Database.MaxConnections = 1
	runtimeCfg.Database.LockTimeoutSeconds = 1
	pc, err := poolConfig(ownerCfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := pgx.ConnectConfig(ctx, pc.ConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(context.Background()) })
	f := &journalFixture{t: t, ctx: ctx, owner: owner, runtime: runtimeCfg.Database,
		now: time.Date(2026, 9, 6, 5, 30, 0, 0, time.UTC)}
	f.exec(`DROP SCHEMA IF EXISTS heos CASCADE; CREATE SCHEMA heos;
		GRANT USAGE ON SCHEMA heos TO heos_runtime;
		ALTER DEFAULT PRIVILEGES IN SCHEMA heos GRANT SELECT ON TABLES TO heos_runtime`)
	if err := Migrate(ctx, ownerCfg.Database); err != nil {
		t.Fatal(err)
	}
	f.store = f.open()
	if err := f.store.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *journalFixture) open() *Store {
	f.t.Helper()
	s, err := Open(f.ctx, f.runtime)
	if err != nil {
		f.t.Fatal(err)
	}
	s.now = func() time.Time { return f.now }
	f.t.Cleanup(s.Close)
	return s
}

func (f *journalFixture) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.owner.Exec(f.ctx, sql, args...); err != nil {
		f.t.Fatal(err)
	}
}

func (f *journalFixture) request(key, device string) (Request, Proposal) {
	body, err := json.Marshal(map[string]any{"preset": "morning", "scheduled_for": f.now, "not_after": f.now.Add(time.Minute)})
	if err != nil {
		f.t.Fatal(err)
	}
	return Request{Principal: "scheduler", Method: "POST", Endpoint: "/v1/players/test/alarms", Key: key, Player: "test", IfMatch: "observation:old", Body: body},
		Proposal{Kind: "alarm", DeviceKey: device, ConfigRevision: "preset:1", EffectiveArguments: []byte(`{"volume":40,"albums":["Green","Awake"]}`),
			NotBefore: f.now, NotAfter: f.now.Add(time.Minute)}
}

func (f *journalFixture) admit(key, device string) (Request, Operation) {
	f.t.Helper()
	r, p := f.request(key, device)
	a, err := f.store.Admit(f.ctx, r, p)
	if err != nil || !a.Created {
		f.t.Fatalf("admit: %+v %v", a, err)
	}
	return r, a.Operation
}

func (f *journalFixture) counts(operations, records, reservations int) {
	f.t.Helper()
	for table, expected := range map[string]int{"operations": operations, "idempotency_records": records, "device_reservations": reservations} {
		var count int
		// Table names are test constants, never caller input.
		if err := f.owner.QueryRow(f.ctx, "SELECT count(*) FROM heos."+table).Scan(&count); err != nil || count != expected {
			f.t.Fatalf("%s: got %d want %d: %v", table, count, expected, err)
		}
	}
}

func TestJournalAdmission(t *testing.T) {
	t.Run("same key concurrently creates one operation", func(t *testing.T) {
		f := newJournalFixture(t)
		r, p := f.request("same", "physical-1")
		// Use independent pools with the same execution epoch to exercise actual
		// PostgreSQL contention, not only one pool's acquisition queue.
		peer := f.open()
		peer.epoch = f.store.epoch
		peer.initialized.Store(true)
		var wg sync.WaitGroup
		results := make(chan Admission, 24)
		for i := range 24 {
			wg.Go(func() {
				s := f.store
				if i%2 == 0 {
					s = peer
				}
				a, err := s.Admit(f.ctx, r, p)
				if err != nil {
					t.Error(err)
					return
				}
				results <- a
			})
		}
		wg.Wait()
		close(results)
		created, total, id := 0, 0, ""
		for a := range results {
			total++
			if a.Created {
				created++
			}
			if id == "" {
				id = a.Operation.ID
			}
			if a.Operation.ID != id {
				t.Fatal("duplicate operation")
			}
		}
		if created != 1 || total != 24 {
			t.Fatalf("created %d of %d", created, total)
		}
		f.counts(1, 1, 1)
		r.Body = []byte(`{"preset":"changed"}`)
		if _, err := f.store.Admit(f.ctx, r, p); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	})

	t.Run("different keys and aliases contend for physical device", func(t *testing.T) {
		f := newJournalFixture(t)
		peer := f.open()
		peer.epoch = f.store.epoch
		peer.initialized.Store(true)
		var wg sync.WaitGroup
		var mu sync.Mutex
		created, busy := 0, 0
		var winner Operation
		for i := range 16 {
			wg.Go(func() {
				r, p := f.request(fmt.Sprint(i), "one-device")
				r.Player = fmt.Sprintf("alias-%d", i)
				r.Endpoint = "/v1/players/" + r.Player + "/alarms"
				s := f.store
				if i%2 == 0 {
					s = peer
				}
				a, err := s.Admit(f.ctx, r, p)
				mu.Lock()
				defer mu.Unlock()
				if errors.Is(err, ErrBusy) {
					busy++
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
				if a.Created {
					created++
					winner = a.Operation
				}
			})
		}
		wg.Wait()
		if created != 1 || busy != 15 {
			t.Fatalf("created %d busy %d", created, busy)
		}
		f.counts(1, 1, 1) // Failed reservations roll back operation AND key.
		if _, err := f.store.Transition(f.ctx, winner.ID, winner.Revision, Update{State: Released}); err != nil {
			t.Fatal(err)
		}
		f.admit("after-release", "one-device")
		f.counts(2, 2, 1)
	})

	t.Run("replay precedes changed defaults and expired preconditions", func(t *testing.T) {
		f := newJournalFixture(t)
		r, original := f.admit("replay", "physical-1")
		selected, err := f.store.SelectAlbum(f.ctx, original.ID, original.Revision, "Green")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.SelectAlbum(f.ctx, selected.ID, selected.Revision, "Awake"); !errors.Is(err, ErrRevision) {
			t.Fatal(err)
		}
		running, err := f.store.Transition(f.ctx, selected.ID, selected.Revision, Update{State: Running, Phase: "ramp", Progress: []byte(`{"volume":15}`)})
		if err != nil {
			t.Fatal(err)
		}
		finished, err := f.store.Transition(f.ctx, running.ID, running.Revision, Update{State: Succeeded, Outcome: []byte(`{"stopped":true}`)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.Transition(f.ctx, finished.ID, finished.Revision, Update{State: Running}); !errors.Is(err, ErrRevision) {
			t.Fatal(err)
		}
		f.now = f.now.Add(8 * 24 * time.Hour)
		a, err := f.store.Admit(f.ctx, r, Proposal{ConfigRevision: "preset:99", EffectiveArguments: []byte(`{"volume":5}`)})
		if err != nil || a.Created || a.Operation.ID != original.ID || a.Operation.ConfigRevision != "preset:1" || a.Operation.State != Succeeded ||
			a.Operation.SelectedAlbum == nil || *a.Operation.SelectedAlbum != "Green" {
			t.Fatalf("replay %+v %v", a, err)
		}
		if a.Operation.StartedAt == nil || a.Operation.FinishedAt == nil {
			t.Fatal("missing timestamps")
		}
		if !a.Operation.ScheduledFor.Equal(original.ScheduledFor) || !a.Operation.NotAfter.Equal(original.NotAfter) || !original.NotAfter.After(original.ScheduledFor) {
			t.Fatal("original admission window was not preserved")
		}
		r.IfMatch = "new-revision"
		if _, err := f.store.Lookup(f.ctx, r); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
		f.counts(1, 1, 0)
	})

	t.Run("method endpoint and principal scope the key", func(t *testing.T) {
		f := newJournalFixture(t)
		for i := range 4 {
			r, p := f.request("shared-key", fmt.Sprint(i))
			switch i {
			case 1:
				r.Principal = "operator"
			case 2:
				r.Method = "PUT"
			case 3:
				r.Endpoint = "/v1/players/test/stop"
			}
			a, err := f.store.Admit(f.ctx, r, p)
			if err != nil || !a.Created {
				t.Fatalf("scope %d: %+v %v", i, a, err)
			}
		}
		f.counts(4, 4, 4)
	})

	t.Run("future and expired new requests do not reserve", func(t *testing.T) {
		f := newJournalFixture(t)
		r, p := f.request("window", "device")
		f.now = p.NotBefore.Add(-time.Microsecond)
		if _, err := f.store.Admit(f.ctx, r, p); !errors.Is(err, ErrFuture) {
			t.Fatal(err)
		}
		f.now = p.NotAfter
		if _, err := f.store.Admit(f.ctx, r, p); !errors.Is(err, ErrExpired) {
			t.Fatal(err)
		}
		f.counts(0, 0, 0)
	})
}

func TestJournalCommitResolution(t *testing.T) {
	t.Run("unavailable resolution remains uncertain until restart", func(t *testing.T) {
		f := newJournalFixture(t)
		r, p := f.request("unresolved", "device")
		f.store.commit = func(ctx context.Context, tx pgx.Tx) error {
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			f.store.pool.Close() // No connection is available for resolution.
			return io.ErrUnexpectedEOF
		}
		if _, err := f.store.Admit(f.ctx, r, p); !errors.Is(err, ErrCommitUncertain) {
			t.Fatal(err)
		}
		f.counts(1, 1, 1)
		fresh := f.open()
		if err := fresh.Recover(f.ctx); err != nil {
			t.Fatal(err)
		}
		a, err := fresh.Admit(f.ctx, r, p)
		if err != nil || a.Created || a.Operation.State != Uncertain {
			t.Fatalf("restart replay %+v %v", a, err)
		}
		f.counts(1, 1, 0)
	})
	t.Run("recovery between commit and reply prevents old dispatch", func(t *testing.T) {
		f := newJournalFixture(t)
		r, p := f.request("restart-at-commit", "device")
		f.store.commit = func(ctx context.Context, tx pgx.Tx) error {
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			// A replacement has claimed its epoch but has not yet reconciled
			// this accepted row in a later recovery batch.
			f.exec("UPDATE heos.journal_control SET epoch='replacement', ready=false")
			return io.ErrUnexpectedEOF
		}
		a, err := f.store.Admit(f.ctx, r, p)
		if err != nil || a.Created || !a.RecoveredCommit || a.Operation.State != Accepted {
			t.Fatalf("unsafe dispatch: %+v %v", a, err)
		}
		f.counts(1, 1, 1)
	})
	t.Run("lost reply after real commit and client disconnect", func(t *testing.T) {
		f := newJournalFixture(t)
		r, p := f.request("lost-reply", "device")
		ctx, cancel := context.WithCancel(f.ctx)
		defer cancel()
		f.store.commit = func(ctx context.Context, tx pgx.Tx) error {
			if err := tx.Commit(ctx); err != nil {
				return err
			}
			cancel() // The HTTP caller disappears after PostgreSQL commits.
			return io.ErrUnexpectedEOF
		}
		a, err := f.store.Admit(ctx, r, p)
		if err != nil || !a.Created || !a.RecoveredCommit {
			t.Fatalf("resolution %+v %v", a, err)
		}
		again, err := f.store.Admit(f.ctx, r, p)
		if err != nil || again.Created || again.Operation.ID != a.Operation.ID {
			t.Fatalf("retry %+v %v", again, err)
		}
		f.counts(1, 1, 1)
	})
	t.Run("failure before commit leaves no partial rows", func(t *testing.T) {
		f := newJournalFixture(t)
		r, p := f.request("no-commit", "device")
		commit := f.store.commit
		f.store.commit = func(context.Context, pgx.Tx) error { return io.ErrUnexpectedEOF }
		if _, err := f.store.Admit(f.ctx, r, p); !errors.Is(err, ErrCommitUncertain) {
			t.Fatal(err)
		}
		if _, err := f.store.Lookup(f.ctx, r); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		f.counts(0, 0, 0)
		f.store.commit = commit
		if a, err := f.store.Admit(f.ctx, r, p); err != nil || !a.Created {
			t.Fatalf("retry %+v %v", a, err)
		}
		f.counts(1, 1, 1)
	})
}

func TestJournalBounds(t *testing.T) {
	t.Run("stale transitions cannot release a successor reservation", func(t *testing.T) {
		f := newJournalFixture(t)
		_, o := f.admit("old", "device")
		peer := f.open()
		peer.epoch = f.store.epoch
		peer.initialized.Store(true)
		var wg sync.WaitGroup
		results := make(chan error, 2)
		for _, s := range []*Store{f.store, peer} {
			wg.Go(func() { _, err := s.Transition(f.ctx, o.ID, o.Revision, Update{State: Released}); results <- err })
		}
		wg.Wait()
		close(results)
		ok, conflict := 0, 0
		for err := range results {
			if err == nil {
				ok++
			} else if errors.Is(err, ErrRevision) {
				conflict++
			} else {
				t.Fatal(err)
			}
		}
		if ok != 1 || conflict != 1 {
			t.Fatalf("transitions %d conflicts %d", ok, conflict)
		}
		f.admit("successor", "device")
		if _, err := f.store.Transition(f.ctx, o.ID, o.Revision, Update{State: Cancelled}); !errors.Is(err, ErrRevision) {
			t.Fatal(err)
		}
		f.counts(2, 2, 1)
	})
	t.Run("lock waits honor caller and SQL deadlines", func(t *testing.T) {
		f := newJournalFixture(t)
		tx, err := f.owner.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer rollback(tx)
		if _, err := tx.Exec(f.ctx, "SELECT epoch FROM heos.journal_control FOR UPDATE"); err != nil {
			t.Fatal(err)
		}
		r, p := f.request("locked", "device")
		ctx, cancel := context.WithTimeout(f.ctx, 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		if _, err := f.store.Admit(ctx, r, p); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if time.Since(start) > time.Second {
			t.Fatal("caller deadline exceeded")
		}
		start = time.Now()
		_, err = f.store.Admit(f.ctx, r, p)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "55P03" {
			t.Fatalf("SQL lock timeout: %v", err)
		}
		if time.Since(start) > 3*time.Second {
			t.Fatal("SQL deadline exceeded")
		}
		rollback(tx)
		f.counts(0, 0, 0)
		f.admit("after-lock", "device")
	})
	t.Run("pool exhaustion is bounded", func(t *testing.T) {
		f := newJournalFixture(t)
		conn, err := f.store.pool.Acquire(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		r, p := f.request("pool", "device")
		ctx, cancel := context.WithTimeout(f.ctx, 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		if _, err := f.store.Admit(ctx, r, p); err == nil {
			t.Fatal("admitted without a connection")
		}
		if time.Since(start) > time.Second {
			t.Fatal("acquisition unbounded")
		}
		f.counts(0, 0, 0)
	})
	t.Run("capacity does not evict unexpired or active work", func(t *testing.T) {
		f := newJournalFixture(t)
		f.store.maxOperations = 2
		f.admit("active", "active-device")
		r, done := f.admit("terminal", "terminal-device")
		if _, err := f.store.Transition(f.ctx, done.ID, done.Revision, Update{State: Succeeded}); err != nil {
			t.Fatal(err)
		}
		if n, err := f.store.Prune(f.ctx, MaxBatch); err != nil || n != 0 {
			t.Fatalf("prune %d %v", n, err)
		}
		third, p := f.request("third", "third-device")
		if _, err := f.store.Admit(f.ctx, third, p); !errors.Is(err, ErrCapacity) {
			t.Fatal(err)
		}
		if _, err := f.store.Lookup(f.ctx, r); err != nil {
			t.Fatal(err)
		}
		f.counts(2, 2, 1)
	})
}

func TestJournalRecoveryAndRetention(t *testing.T) {
	t.Run("restart reconciles several batches and fences old journal writes", func(t *testing.T) {
		f := newJournalFixture(t)
		var request Request
		var original Operation
		for i := range MaxBatch + 1 {
			request, original = f.admit(fmt.Sprint(i), fmt.Sprint(i))
		}
		fresh := f.open()
		if err := fresh.Ready(f.ctx); !errors.Is(err, ErrNotInitialized) {
			t.Fatal(err)
		}
		if err := fresh.Recover(f.ctx); err != nil {
			t.Fatal(err)
		}
		if err := fresh.Ready(f.ctx); err != nil {
			t.Fatal(err)
		}
		f.counts(MaxBatch+1, MaxBatch+1, 0)
		var unfinished int
		if err := f.owner.QueryRow(f.ctx, "SELECT count(*) FROM heos.operations WHERE finished_at IS NULL").Scan(&unfinished); err != nil || unfinished != 0 {
			t.Fatal(unfinished, err)
		}
		o, err := fresh.Lookup(f.ctx, request)
		if err != nil || o.ID != original.ID || o.State != Uncertain || o.ErrorCode != "process_interrupted" || o.Epoch != original.Epoch {
			t.Fatalf("recovered %+v %v", o, err)
		}
		if err := f.store.Ready(f.ctx); !errors.Is(err, ErrStaleEpoch) {
			t.Fatal(err)
		}
		if _, err := f.store.Transition(f.ctx, original.ID, original.Revision, Update{State: Running}); !errors.Is(err, ErrStaleEpoch) {
			t.Fatal(err)
		}
		r, p := f.request("new", "new-device")
		if _, err := f.store.Admit(f.ctx, r, p); !errors.Is(err, ErrStaleEpoch) {
			t.Fatal(err)
		}
		if err := f.store.Recover(f.ctx); err != nil {
			t.Fatal(err)
		}
		if err := fresh.Ready(f.ctx); err != nil {
			t.Fatal("old store reactivated", err)
		}
		if a, err := fresh.Admit(f.ctx, r, p); err != nil || !a.Created {
			t.Fatal(a, err)
		}
		// Repeated initialization must not interrupt this process's accepted work.
		if err := fresh.Recover(f.ctx); err != nil {
			t.Fatal(err)
		}
		f.counts(MaxBatch+2, MaxBatch+2, 1)
	})
	t.Run("retention boundaries batches and expired alarm retry", func(t *testing.T) {
		f := newJournalFixture(t)
		activeRequest, _ := f.admit("active", "active-device")
		var expiredRequest Request
		var expiredProposal Proposal
		var protectedID string
		for i := range 4 {
			r, p := f.request(fmt.Sprint(i), fmt.Sprint(i))
			a, err := f.store.Admit(f.ctx, r, p)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.Transition(f.ctx, a.Operation.ID, 1, Update{State: Succeeded}); err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				expiredRequest, expiredProposal = r, p
			}
			if i == 3 {
				protectedID = a.Operation.ID
			}
		}
		// Unexpired mapping wins even when the operation's terminal age expires.
		f.exec("UPDATE heos.idempotency_records SET expires_at=$1 WHERE operation_id=$2", f.now.Add(2*Retention), protectedID)
		f.now = f.now.Add(Retention - time.Microsecond)
		if n, err := f.store.Prune(f.ctx, MaxBatch); err != nil || n != 0 {
			t.Fatal(n, err)
		}
		f.now = f.now.Add(time.Microsecond)
		if n, err := f.store.Prune(f.ctx, 2); err != nil || n != 2 {
			t.Fatal(n, err)
		}
		if n, err := f.store.Prune(f.ctx, 2); err != nil || n != 1 {
			t.Fatal(n, err)
		}
		f.counts(2, 2, 1)
		if _, err := f.store.Lookup(f.ctx, expiredRequest); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if _, err := f.store.Admit(f.ctx, expiredRequest, expiredProposal); !errors.Is(err, ErrExpired) {
			t.Fatal(err)
		}
		if _, err := f.store.Lookup(f.ctx, activeRequest); err != nil {
			t.Fatal("active key expired", err)
		}
		f.now = f.now.Add(30 * 24 * time.Hour)
		if n, err := f.store.Prune(f.ctx, MaxBatch); err != nil || n != 1 {
			t.Fatal(n, err)
		}
		f.counts(1, 1, 1)
	})
	t.Run("terminal retention starts at completion", func(t *testing.T) {
		f := newJournalFixture(t)
		r, o := f.admit("long", "device")
		f.now = f.now.Add(2 * Retention)
		if _, err := f.store.Transition(f.ctx, o.ID, o.Revision, Update{State: Uncertain}); err != nil {
			t.Fatal(err)
		}
		if n, err := f.store.Prune(f.ctx, MaxBatch); err != nil || n != 0 {
			t.Fatal(n, err)
		}
		if _, err := f.store.Lookup(f.ctx, r); err != nil {
			t.Fatal(err)
		}
	})
}
