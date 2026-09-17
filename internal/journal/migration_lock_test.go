package journal

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

type migrationSessionLockerProbe struct {
	lock   func(context.Context, *sql.Conn) error
	unlock func(context.Context, *sql.Conn) error
}

func (p migrationSessionLockerProbe) SessionLock(ctx context.Context, conn *sql.Conn) error {
	return p.lock(ctx, conn)
}

func (p migrationSessionLockerProbe) SessionUnlock(ctx context.Context, conn *sql.Conn) error {
	return p.unlock(ctx, conn)
}

func TestMigrationSessionUnlockBoundsDetachedCleanup(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	conn := new(sql.Conn)
	want := errors.New("unlock failed")
	var cleanup <-chan struct{}
	before := time.Now()
	locker := boundedSessionLocker{SessionLocker: migrationSessionLockerProbe{
		unlock: func(ctx context.Context, got *sql.Conn) error {
			cleanup = ctx.Done()
			if got != conn {
				t.Fatal("cleanup used a different connection")
			}
			if err := ctx.Err(); err != nil {
				t.Fatalf("detached cleanup inherited cancellation: %v", err)
			}
			deadline, ok := ctx.Deadline()
			if !ok || deadline.Before(before.Add(time.Second)) || deadline.After(time.Now().Add(time.Second)) {
				t.Fatalf("cleanup must have a one-second deadline: %v, present=%v", deadline, ok)
			}
			return want
		},
	}}
	if err := locker.SessionUnlock(context.WithoutCancel(parent), conn); !errors.Is(err, want) {
		t.Fatalf("unlock error = %v; want %v", err, want)
	}
	select {
	case <-cleanup:
	default:
		t.Fatal("unlock completion did not cancel its cleanup context")
	}
}

func TestMigrationSessionUnlockPreservesEarlierDeadline(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	locker := boundedSessionLocker{SessionLocker: migrationSessionLockerProbe{
		unlock: func(ctx context.Context, _ *sql.Conn) error {
			if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
				t.Fatalf("cleanup extended the caller deadline: %v, present=%v", got, ok)
			}
			return ctx.Err()
		},
	}}
	if err := locker.SessionUnlock(ctx, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unlock error = %v; want deadline exceeded", err)
	}
}

func TestMigrationSessionLockDelegatesUnchanged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := new(sql.Conn)
	want := errors.New("lock failed")
	locker := boundedSessionLocker{SessionLocker: migrationSessionLockerProbe{
		lock: func(got context.Context, connection *sql.Conn) error {
			if got != ctx || connection != conn {
				t.Fatal("lock acquisition changed the context or connection")
			}
			return want
		},
	}}
	if err := locker.SessionLock(ctx, conn); !errors.Is(err, want) {
		t.Fatalf("lock error = %v; want %v", err, want)
	}
}
