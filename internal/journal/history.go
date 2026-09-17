package journal

import (
	"context"
	"fmt"
	"time"

	"github.com/dreylark/heos-control/internal/journal/dbgen"
	"github.com/jackc/pgx/v5/pgtype"
)

// HistoryQuery identity and player scope must come from authenticated server
// configuration, never from a client cursor. Optional filters only narrow scope.
type HistoryQuery struct {
	Principal     string
	Operator      bool
	Players       []string
	Player        string
	State         State
	Kind          string
	Delivery      string
	CreatedFrom   *time.Time
	CreatedBefore *time.Time
	Before        *HistoryPosition
	Limit         int
}

// HistoryPosition is the immutable ordering tuple, independent of whether its
// anchor operation still exists. Keep PostgreSQL's persisted time precision.
type HistoryPosition struct {
	CreatedAt time.Time
	ID        string
}

type HistoryPage struct {
	Items []Operation
	Next  *HistoryPosition
}

func (q HistoryQuery) validate() error {
	if !textWithin(q.Principal, 256) || len(q.Players) > 16 || q.Limit < 1 || q.Limit > 100 {
		return ErrInvalid
	}
	for _, player := range q.Players {
		if !textWithin(player, 256) {
			return ErrInvalid
		}
	}
	if q.Player != "" && !textWithin(q.Player, 256) {
		return ErrInvalid
	}
	switch q.State {
	case "", Accepted, Running, Succeeded, Failed, Cancelled, Released, Interrupted, Uncertain:
	default:
		return ErrInvalid
	}
	switch q.Kind {
	case "", "alarm", "playback", "volume", "mute", "transport", "stop", "cancel":
	default:
		return ErrInvalid
	}
	switch q.Delivery {
	case "", "confirmed", "rejected", "not_sent", "unknown":
	default:
		return ErrInvalid
	}
	for _, date := range []*time.Time{q.CreatedFrom, q.CreatedBefore} {
		if date != nil && !historyTimeValid(*date) {
			return ErrInvalid
		}
	}
	if q.CreatedFrom != nil && q.CreatedBefore != nil && !q.CreatedBefore.After(*q.CreatedFrom) {
		return ErrInvalid
	}
	if q.Before != nil && (!textWithin(q.Before.ID, 256) || !historyTimeValid(q.Before.CreatedAt) || q.Before.CreatedAt.Nanosecond()%1000 != 0) {
		return ErrInvalid
	}
	return nil
}

func historyTimeValid(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999
}

func historyTimestamp(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	// PostgreSQL stores microseconds; pgx truncates finer precision on encoding.
	// Both >= lower and < upper bounds need a ceiling to preserve the requested
	// comparison against that discrete grid (including sub-microsecond windows).
	bound := t.Truncate(time.Microsecond)
	if !bound.Equal(*t) {
		bound = bound.Add(time.Microsecond)
	}
	return timestamp(bound)
}

// List applies authorization in SQL before ordering and limiting. It uses one
// bounded read with no journal lock; pages are a live view, not an export snapshot.
// The returned operations contain only fields needed by the public projection.
func (s *Store) List(ctx context.Context, query HistoryQuery) (HistoryPage, error) {
	page := HistoryPage{Items: []Operation{}}
	if err := query.validate(); err != nil {
		return page, err
	}
	if len(query.Players) == 0 {
		return page, nil
	}
	params := dbgen.ListOperationHistoryParams{
		Players: query.Players, IsOperator: query.Operator, Principal: query.Principal,
		PlayerFilter: query.Player, StateFilter: string(query.State), KindFilter: query.Kind, DeliveryFilter: query.Delivery,
		CreatedFrom: historyTimestamp(query.CreatedFrom), CreatedBefore: historyTimestamp(query.CreatedBefore),
		PageLimit: int32(query.Limit + 1),
	}
	if query.Before != nil {
		params.BeforeTime, params.BeforeID = timestamp(query.Before.CreatedAt), query.Before.ID
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := dbgen.New(s.pool).ListOperationHistory(ctx, params)
	if err != nil {
		return page, fmt.Errorf("list operation history: %w", err)
	}
	if len(rows) > query.Limit {
		rows = rows[:query.Limit]
		last := rows[len(rows)-1]
		page.Next = &HistoryPosition{CreatedAt: last.CreatedAt.Time.UTC(), ID: last.ID}
	}
	for _, row := range rows {
		op := Operation{
			ID: row.ID, Kind: row.Kind, Player: row.Player, State: State(row.State), Revision: row.Revision,
			Phase: row.Phase, ConfigRevision: row.ConfigRevision, Progress: row.Progress, Outcome: row.Outcome,
			ErrorCode: row.ErrorCode, CreatedAt: row.CreatedAt.Time.UTC(), UpdatedAt: row.UpdatedAt.Time.UTC(),
		}
		if row.SelectedAlbum.Valid {
			op.SelectedAlbum = &row.SelectedAlbum.String
		}
		if row.StartedAt.Valid {
			started := row.StartedAt.Time.UTC()
			op.StartedAt = &started
		}
		if row.FinishedAt.Valid {
			finished := row.FinishedAt.Time.UTC()
			op.FinishedAt = &finished
		}
		page.Items = append(page.Items, op)
	}
	return page, nil
}
