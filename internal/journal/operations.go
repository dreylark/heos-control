package journal

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dreylark/heos-control/internal/journal/dbgen"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func timestamp(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t.UTC(), Valid: true} }

func operation(row dbgen.HeosOperation) Operation {
	o := Operation{ID: row.ID, Kind: row.Kind, Player: row.Player, DeviceKey: row.DeviceKey,
		Principal: row.Principal, Epoch: row.Epoch, State: State(row.State), Revision: row.Revision,
		Phase: row.Phase, ConfigRevision: row.ConfigRevision, EffectiveArguments: row.EffectiveArguments,
		Progress: row.Progress, Outcome: row.Outcome, ErrorCode: row.ErrorCode,
		ScheduledFor: row.ScheduledFor.Time, NotAfter: row.NotAfter.Time,
		CreatedAt: row.CreatedAt.Time, UpdatedAt: row.UpdatedAt.Time}
	if row.SelectedAlbum.Valid {
		o.SelectedAlbum = &row.SelectedAlbum.String
	}
	if row.StartedAt.Valid {
		o.StartedAt = &row.StartedAt.Time
	}
	if row.FinishedAt.Valid {
		o.FinishedAt = &row.FinishedAt.Time
	}
	return o
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// All journal mutations lock one control row first. At MVP traffic levels this
// keeps capacity, recovery and reservation changes serializable under READ
// COMMITTED without transaction retries. No device I/O runs under this lock.
func (s *Store) beginLocked(ctx context.Context) (pgx.Tx, *dbgen.Queries, dbgen.LockJournalRow, error) {
	if err := s.checkSchema(ctx); err != nil {
		return nil, nil, dbgen.LockJournalRow{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, dbgen.LockJournalRow{}, fmt.Errorf("begin journal transaction: %w", err)
	}
	q := dbgen.New(tx)
	control, err := q.LockJournal(ctx)
	if err != nil {
		rollback(tx)
		return nil, nil, control, fmt.Errorf("lock journal: %w", err)
	}
	return tx, q, control, nil
}

func (s *Store) active(control dbgen.LockJournalRow) error {
	if !s.initialized.Load() || !control.Ready {
		return ErrNotInitialized
	}
	if control.Epoch != s.epoch {
		return ErrStaleEpoch
	}
	return nil
}

func lookup(ctx context.Context, q *dbgen.Queries, request Request, hash []byte) (Operation, error) {
	record, err := q.LookupRequest(ctx, dbgen.LookupRequestParams{
		Principal: request.Principal, Method: request.Method, Endpoint: request.Endpoint, Key: request.Key})
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrNotFound
	}
	if err != nil {
		return Operation{}, fmt.Errorf("lookup idempotency record: %w", err)
	}
	if !bytes.Equal(record.RequestHash, hash) {
		return Operation{}, ErrConflict
	}
	row, err := q.GetOperation(ctx, record.OperationID)
	if err != nil {
		return Operation{}, fmt.Errorf("read admitted operation: %w", err)
	}
	return operation(row), nil
}

// Lookup must precede mutable configuration, freshness and admission-window checks.
// Its shared control lock also waits for an outstanding COMMIT to resolve.
// An expired mapping remains replayable until retention actually removes it.
func (s *Store) Lookup(ctx context.Context, request Request) (Operation, error) {
	hash, err := request.fingerprint()
	if err != nil {
		return Operation{}, err
	}
	o, _, err := s.lookupCommitted(ctx, request, hash)
	return o, err
}

func (s *Store) lookupCommitted(ctx context.Context, request Request, hash []byte) (Operation, dbgen.ReadJournalRow, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Operation{}, dbgen.ReadJournalRow{}, fmt.Errorf("begin request lookup: %w", err)
	}
	defer rollback(tx)
	q := dbgen.New(tx)
	control, err := q.ReadJournal(ctx)
	if err != nil {
		return Operation{}, control, fmt.Errorf("wait for journal writes: %w", err)
	}
	o, err := lookup(ctx, q, request, hash)
	return o, control, err
}

// Get returns journal data; the caller must restrict it to its creator or an
// authorized operator for this player before returning it across an API boundary.
func (s *Store) Get(ctx context.Context, id string) (Operation, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	row, err := dbgen.New(s.pool).GetOperation(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrNotFound
	}
	if err != nil {
		return Operation{}, fmt.Errorf("get operation: %w", err)
	}
	return operation(row), nil
}

// Active returns the currently reserved operation. Authorization is owned by
// the caller, exactly as for Get; absence is distinct from database failure.
func (s *Store) Active(ctx context.Context, player string) (Operation, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	row, err := dbgen.New(s.pool).ActiveOperation(ctx, player)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrNotFound
	}
	if err != nil {
		return Operation{}, fmt.Errorf("active operation: %w", err)
	}
	return operation(row), nil
}

func (s *Store) Admit(ctx context.Context, request Request, proposal Proposal) (Admission, error) {
	hash, err := request.fingerprint()
	if err != nil {
		return Admission{}, err
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tx, q, control, err := s.beginLocked(ctx)
	if err != nil {
		return Admission{}, err
	}
	defer rollback(tx)
	previous, err := lookup(ctx, q, request, hash)
	if err == nil {
		return Admission{Operation: previous}, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Admission{}, err
	}
	if err := s.active(control); err != nil {
		return Admission{}, err
	}
	if !textWithin(proposal.DeviceKey, 256) || !textWithin(proposal.ConfigRevision, 256) {
		return Admission{}, ErrInvalid
	}
	switch proposal.Kind {
	case "alarm", "playback", "volume", "mute", "transport", "skip", "stop", "cancel":
	default:
		return Admission{}, ErrInvalid
	}
	effective, err := canonicalObject(proposal.EffectiveArguments)
	if err != nil {
		return Admission{}, err
	}
	now := s.now().UTC()
	if proposal.NotBefore.IsZero() || !proposal.NotAfter.After(proposal.NotBefore) {
		return Admission{}, ErrInvalid
	}
	if now.Before(proposal.NotBefore) {
		return Admission{}, ErrFuture
	}
	if !now.Before(proposal.NotAfter) {
		return Admission{}, ErrExpired
	}
	count, err := q.CountOperations(ctx, int32(s.maxOperations))
	if err != nil {
		return Admission{}, fmt.Errorf("check journal capacity: %w", err)
	}
	if count >= s.maxOperations {
		return Admission{}, ErrCapacity
	}
	if proposal.OwnerID != "" {
		if proposal.Kind != "skip" || proposal.ReplaceID != "" || proposal.ReplaceRevision != 0 || proposal.ReplaceUncertain {
			return Admission{}, ErrInvalid
		}
		owner, ownerErr := q.ActiveOperation(ctx, request.Player)
		if ownerErr != nil && !errors.Is(ownerErr, pgx.ErrNoRows) {
			return Admission{}, fmt.Errorf("read reservation for owned command: %w", ownerErr)
		}
		if ownerErr != nil || owner.ID != proposal.OwnerID || owner.DeviceKey != proposal.DeviceKey ||
			owner.Epoch != s.epoch || owner.Kind != "playback" || owner.Principal != request.Principal || owner.FinishedAt.Valid {
			return Admission{}, ErrRevision
		}
	}
	if proposal.ReplaceID != "" {
		old, e := q.ActiveOperation(ctx, request.Player)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return Admission{}, fmt.Errorf("read reservation for handoff: %w", e)
		}
		if e != nil || old.ID != proposal.ReplaceID || old.DeviceKey != proposal.DeviceKey || old.Revision != proposal.ReplaceRevision {
			return Admission{}, ErrRevision
		}
		update, e := handoffUpdate(operation(old), proposal, now)
		if e != nil {
			return Admission{}, e
		}
		_, e = q.TransitionOperation(ctx, dbgen.TransitionOperationParams{ID: old.ID, Revision: old.Revision, Epoch: s.epoch,
			State: string(update.State), Phase: update.Phase, Progress: update.Progress, Outcome: update.Outcome, ErrorCode: update.ErrorCode, UpdatedAt: timestamp(now)})
		if e != nil {
			return Admission{}, e
		}
		if e = finish(ctx, q, []string{old.ID}, now); e != nil {
			return Admission{}, e
		}
	}
	row, err := q.InsertOperation(ctx, dbgen.InsertOperationParams{
		ID: "op_" + rand.Text(), Kind: proposal.Kind, Player: request.Player, DeviceKey: proposal.DeviceKey,
		Principal: request.Principal, Epoch: s.epoch, ConfigRevision: proposal.ConfigRevision,
		EffectiveArguments: effective, CreatedAt: timestamp(now), ScheduledFor: timestamp(proposal.NotBefore), NotAfter: timestamp(proposal.NotAfter)})
	if err != nil {
		return Admission{}, fmt.Errorf("insert operation: %w", err)
	}
	if err := q.InsertRequest(ctx, dbgen.InsertRequestParams{Principal: request.Principal, Method: request.Method,
		Endpoint: request.Endpoint, Key: request.Key, RequestHash: hash, OperationID: row.ID, ExpiresAt: timestamp(now.Add(Retention))}); err != nil {
		return Admission{}, fmt.Errorf("insert idempotency record: %w", err)
	}
	if proposal.OwnerID == "" {
		if err := q.ReserveDevice(ctx, dbgen.ReserveDeviceParams{DeviceKey: proposal.DeviceKey, OperationID: row.ID, Epoch: s.epoch}); err != nil {
			var pgerr *pgconn.PgError
			if errors.As(err, &pgerr) && pgerr.Code == "23505" && pgerr.ConstraintName == "device_reservations_pkey" {
				return Admission{}, ErrBusy
			}
			return Admission{}, fmt.Errorf("reserve device: %w", err)
		}
	}
	if err := s.commit(ctx, tx); err != nil {
		// Release the transaction/connection before resolving, including pools of
		// size one. This boundary is replaceable in tests to drop a COMMIT reply
		// after a real PostgreSQL commit, or abort it before it reaches PostgreSQL.
		rollback(tx)
		resolution, stop := context.WithTimeout(context.WithoutCancel(parent), s.timeout)
		defer stop()
		resolved, current, lookupErr := s.lookupCommitted(resolution, request, hash)
		if lookupErr != nil {
			return Admission{}, fmt.Errorf("%w: %w", ErrCommitUncertain, errors.Join(err, lookupErr))
		}
		return Admission{Operation: resolved, Created: current.Epoch == s.epoch && current.Ready && resolved.ID == row.ID && resolved.Epoch == s.epoch && resolved.State == Accepted,
			RecoveredCommit: true}, nil
	}
	return Admission{Operation: operation(row), Created: true}, nil
}

func handoffUpdate(old Operation, proposal Proposal, now time.Time) (Update, error) {
	u := Update{State: Released, Phase: "superseded", Progress: old.Progress, Outcome: old.Outcome}
	reason := "superseded"
	if proposal.ReplaceUncertain {
		u.State, u.ErrorCode = Uncertain, "command_interrupted"
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(old.Outcome, &fields); err != nil || fields == nil {
			return Update{}, ErrInvalid
		}
		fields["delivery"], fields["playback_may_continue"] = json.RawMessage(`"unknown"`), json.RawMessage(`true`)
		var err error
		u.Outcome, err = json.Marshal(fields)
		if err != nil {
			return Update{}, ErrInvalid
		}
	}
	if proposal.Kind == "cancel" {
		u.State, u.Phase, reason = Running, "cancelling", "cancellation_requested"
	}
	var err error
	u.Outcome, err = WithDiagnostics(u.Outcome, Diagnostics{Reason: reason, Phase: old.Phase, Source: "controller", DetectedAt: &now})
	return u, err
}

func (s *Store) Transition(ctx context.Context, id string, revision int64, update Update) (Operation, error) {
	if revision < 1 || len(update.Phase) > 128 || len(update.ErrorCode) > 128 ||
		(update.State != Running && !update.State.terminal()) {
		return Operation{}, ErrInvalid
	}
	if update.Progress == nil {
		update.Progress = []byte("{}")
	}
	if update.Outcome == nil {
		update.Outcome = []byte("{}")
	}
	progress, err := canonicalObject(update.Progress)
	if err != nil {
		return Operation{}, err
	}
	outcome, err := canonicalObject(update.Outcome)
	if err != nil {
		return Operation{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tx, q, control, err := s.beginLocked(ctx)
	if err != nil {
		return Operation{}, err
	}
	defer rollback(tx)
	if err := s.active(control); err != nil {
		return Operation{}, err
	}
	now := s.now().UTC()
	row, err := q.TransitionOperation(ctx, dbgen.TransitionOperationParams{ID: id, State: string(update.State), Phase: update.Phase,
		Progress: progress, Outcome: outcome, ErrorCode: update.ErrorCode, UpdatedAt: timestamp(now), Revision: revision, Epoch: s.epoch})
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrRevision
	}
	if err != nil {
		return Operation{}, fmt.Errorf("transition operation: %w", err)
	}
	if update.State.terminal() {
		if err := finish(ctx, q, []string{id}, now); err != nil {
			return Operation{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Operation{}, fmt.Errorf("%w: transition: %w", ErrCommitUncertain, err)
	}
	return operation(row), nil
}

func finish(ctx context.Context, q *dbgen.Queries, ids []string, now time.Time) error {
	if err := q.ReleaseReservations(ctx, ids); err != nil {
		return fmt.Errorf("release reservations: %w", err)
	}
	if err := q.ExtendRetention(ctx, dbgen.ExtendRetentionParams{Column1: ids, ExpiresAt: timestamp(now.Add(Retention))}); err != nil {
		return fmt.Errorf("extend terminal retention: %w", err)
	}
	return nil
}

// SelectAlbum durably records the choice before queue writes. Selection is
// immutable; a caller resolving an uncertain update must reread Get/Lookup.
func (s *Store) SelectAlbum(ctx context.Context, id string, revision int64, album string) (Operation, error) {
	if !textWithin(album, 1024) || revision < 1 {
		return Operation{}, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tx, q, control, err := s.beginLocked(ctx)
	if err != nil {
		return Operation{}, err
	}
	defer rollback(tx)
	if err := s.active(control); err != nil {
		return Operation{}, err
	}
	row, err := q.SelectAlbum(ctx, dbgen.SelectAlbumParams{ID: id, SelectedAlbum: pgtype.Text{String: album, Valid: true},
		UpdatedAt: timestamp(s.now()), Revision: revision, Epoch: s.epoch})
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrRevision
	}
	if err != nil {
		return Operation{}, fmt.Errorf("select album: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Operation{}, fmt.Errorf("%w: select album: %w", ErrCommitUncertain, err)
	}
	return operation(row), nil
}

// Recover activates this process epoch and marks old unfinished work uncertain
// in bounded batches. Call only when the previous device writer is known stopped.
// SQL epoch checks fence journal writes, not a partitioned writer's device I/O.
// Retrying this method never resumes playback or reactivates an initialized epoch.
func (s *Store) Recover(ctx context.Context) error {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if s.initialized.Load() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	for {
		done, err := s.recoverBatch(ctx)
		if err != nil {
			return err
		}
		if done {
			s.initialized.Store(true)
			return nil
		}
	}
}

func (s *Store) recoverBatch(ctx context.Context) (bool, error) {
	tx, q, control, err := s.beginLocked(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(tx)
	if s.previousEpoch == nil {
		previous := control.Epoch
		s.previousEpoch = &previous
	}
	if control.Epoch != *s.previousEpoch && control.Epoch != s.epoch {
		return false, ErrStaleEpoch
	}
	if err := q.SetJournalEpoch(ctx, dbgen.SetJournalEpochParams{Epoch: s.epoch}); err != nil {
		return false, err
	}
	now := s.now().UTC()
	ids, err := q.InterruptBatch(ctx, dbgen.InterruptBatchParams{Epoch: s.epoch, UpdatedAt: timestamp(now), Limit: MaxBatch})
	if err != nil {
		return false, fmt.Errorf("interrupt old operations: %w", err)
	}
	if err := finish(ctx, q, ids, now); err != nil {
		return false, err
	}
	done := len(ids) < MaxBatch
	if done {
		if err := q.SetJournalEpoch(ctx, dbgen.SetJournalEpochParams{Epoch: s.epoch, Ready: true}); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("%w: recovery: %w", ErrCommitUncertain, err)
	}
	return done, nil
}

// Prune removes only fully expired terminal work. Saturation rejects admission;
// it never evicts active or unexpired deduplication data to make room.
func (s *Store) Prune(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > MaxBatch {
		return 0, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tx, q, control, err := s.beginLocked(ctx)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	if err := s.active(control); err != nil {
		return 0, err
	}
	now := s.now().UTC()
	ids, err := q.PruneOperations(ctx, dbgen.PruneOperationsParams{FinishedAt: timestamp(now.Add(-Retention)), ExpiresAt: timestamp(now), Limit: int32(limit)})
	if err != nil {
		return 0, fmt.Errorf("prune journal: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: retention: %w", ErrCommitUncertain, err)
	}
	return len(ids), nil
}
