//go:build integration

package journal

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedHistory(f *journalFixture, id, principal, player, kind string, state State, delivery string, at time.Time) {
	f.t.Helper()
	outcome := `{}`
	if delivery != "" {
		outcome = `{"delivery":"` + delivery + `"}`
	}
	var finished *time.Time
	if state.terminal() {
		finished = &at
	}
	f.exec(`INSERT INTO heos.operations
		(id, principal, player, kind, state, device_key, epoch, config_revision,
		effective_arguments, outcome, scheduled_for, not_after, created_at, updated_at, finished_at)
		VALUES ($1,$2,$3,$4,$5,'private-device',$6,'config',
		'{"private":"argument"}',$7,$8,$8::timestamptz + interval '1 minute',$8,$8,$9)`,
		id, principal, player, kind, string(state), f.store.epoch, outcome, at, finished)
}

func historyIDs(page HistoryPage) []string {
	ids := make([]string, 0, len(page.Items))
	for _, op := range page.Items {
		ids = append(ids, op.ID)
	}
	return ids
}

func checkHistory(t *testing.T, f *journalFixture, q HistoryQuery, ids ...string) HistoryPage {
	t.Helper()
	page, err := f.store.List(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(historyIDs(page), ids) {
		t.Fatalf("history IDs %v, want %v", historyIDs(page), ids)
	}
	return page
}

func TestHistoryVisibilityBeforeLimitAndSafeProjection(t *testing.T) {
	f := newJournalFixture(t)
	seedHistory(f, "old-own", "alice", "room", "playback", Running, "", f.now)
	seedHistory(f, "new-own", "alice", "room", "volume", Succeeded, "confirmed", f.now.Add(time.Second))
	seedHistory(f, "other-owner", "bob", "room", "volume", Succeeded, "confirmed", f.now.Add(2*time.Second))
	seedHistory(f, "other-player", "alice", "private", "volume", Succeeded, "confirmed", f.now.Add(3*time.Second))
	f.exec(`UPDATE heos.operations SET selected_album='album-ref', started_at=$1, phase='complete',
		progress='{"level":3}', error_code='historical_code' WHERE id='new-own'`, f.now)
	q := HistoryQuery{Principal: "alice", Players: []string{"room"}, Limit: 1}
	p := checkHistory(t, f, q, "new-own")
	if p.Next == nil || p.Next.ID != "new-own" || !p.Next.CreatedAt.Equal(f.now.Add(time.Second)) {
		t.Fatalf("wrong continuation: %+v", p.Next)
	}
	op := p.Items[0]
	if op.DeviceKey != "" || op.Principal != "" || op.Epoch != "" || len(op.EffectiveArguments) != 0 || !op.ScheduledFor.IsZero() || !op.NotAfter.IsZero() {
		t.Fatalf("history loaded private admission data: %+v", op)
	}
	if op.SelectedAlbum == nil || *op.SelectedAlbum != "album-ref" || op.StartedAt == nil || !op.StartedAt.Equal(f.now) || op.FinishedAt == nil ||
		op.Phase != "complete" || op.ConfigRevision != "config" || op.ErrorCode != "historical_code" || string(op.Progress) != `{"level": 3}` ||
		string(op.Outcome) != `{"delivery": "confirmed"}` {
		t.Fatalf("history omitted public operation fields: %+v", op)
	}
	q.Before = p.Next
	p = checkHistory(t, f, q, "old-own")
	if p.Next != nil {
		t.Fatal("last page has a continuation")
	}
	q.Before, q.Operator, q.Limit = nil, true, 10
	checkHistory(t, f, q, "other-owner", "new-own", "old-own")
	q.Player = "private"
	checkHistory(t, f, q, []string{}...)
	q.Player, q.Players = "", nil
	p = checkHistory(t, f, q, []string{}...)
	if p.Items == nil || p.Next != nil {
		t.Fatal("empty ACL must return an empty array without a cursor")
	}
}

func TestHistoryFiltersAndMissingDelivery(t *testing.T) {
	f := newJournalFixture(t)
	seedHistory(f, "absent", "alice", "room", "alarm", Interrupted, "", f.now)
	seedHistory(f, "unknown", "alice", "room", "playback", Uncertain, "unknown", f.now.Add(time.Second))
	seedHistory(f, "confirmed", "alice", "room", "volume", Succeeded, "confirmed", f.now.Add(2*time.Second))
	seedHistory(f, "not-sent", "alice", "kitchen", "mute", Failed, "not_sent", f.now.Add(3*time.Second))
	seedHistory(f, "rejected", "alice", "room", "transport", Failed, "rejected", f.now.Add(4*time.Second))
	q := HistoryQuery{Principal: "alice", Players: []string{"room", "kitchen"}, Limit: 100}
	for _, tc := range []struct {
		name string
		edit func(*HistoryQuery)
		ids  []string
	}{
		{"legacy kind", func(q *HistoryQuery) { q.Kind = "alarm" }, []string{"absent"}},
		{"state", func(q *HistoryQuery) { q.State = Failed }, []string{"rejected", "not-sent"}},
		{"player", func(q *HistoryQuery) { q.Player = "kitchen" }, []string{"not-sent"}},
		{"delivery unknown excludes absence", func(q *HistoryQuery) { q.Delivery = "unknown" }, []string{"unknown"}},
		{"delivery confirmed", func(q *HistoryQuery) { q.Delivery = "confirmed" }, []string{"confirmed"}},
		{"delivery rejected", func(q *HistoryQuery) { q.Delivery = "rejected" }, []string{"rejected"}},
		{"delivery not sent", func(q *HistoryQuery) { q.Delivery = "not_sent" }, []string{"not-sent"}},
		{"half-open creation interval", func(q *HistoryQuery) {
			start, end := f.now.Add(time.Second), f.now.Add(3*time.Second)
			q.CreatedFrom, q.CreatedBefore = &start, &end
		}, []string{"confirmed", "unknown"}},
		{"submicro lower bound excludes earlier persisted instant", func(q *HistoryQuery) {
			start := f.now.Add(time.Nanosecond)
			q.CreatedFrom = &start
		}, []string{"rejected", "not-sent", "confirmed", "unknown"}},
		{"submicro upper bound includes earlier persisted instant", func(q *HistoryQuery) {
			end := f.now.Add(time.Nanosecond)
			q.CreatedBefore = &end
		}, []string{"absent"}},
		{"submicro half-open interval keeps exact persisted instant", func(q *HistoryQuery) {
			start, end := f.now, f.now.Add(time.Nanosecond)
			q.CreatedFrom, q.CreatedBefore = &start, &end
		}, []string{"absent"}},
		{"combined filters", func(q *HistoryQuery) {
			q.State, q.Player, q.Kind, q.Delivery = Failed, "room", "transport", "rejected"
		}, []string{"rejected"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := q
			tc.edit(&query)
			checkHistory(t, f, query, tc.ids...)
		})
	}
}

func TestHistoryContinuationIsStableAcrossTiesDeletionAndChanges(t *testing.T) {
	f := newJournalFixture(t)
	at := f.now.Add(123456 * time.Microsecond).In(time.FixedZone("offset", 3600))
	for _, id := range []string{"a", "b", "c", "d"} {
		seedHistory(f, id, "alice", "room", "playback", Running, "", at)
	}
	q := HistoryQuery{Principal: "alice", Players: []string{"room"}, Limit: 2}
	p := checkHistory(t, f, q, "d", "c")
	if p.Next == nil || !p.Next.CreatedAt.Equal(at) {
		t.Fatalf("lost PostgreSQL timestamp precision: %+v", p.Next)
	}
	f.exec(`DELETE FROM heos.operations WHERE id='c'`)
	seedHistory(f, "new", "alice", "room", "playback", Running, "", at.Add(time.Second))
	f.exec(`UPDATE heos.operations SET revision=revision+1, updated_at=$1, state='succeeded', finished_at=$1 WHERE id='d'`, at.Add(2*time.Second))
	q.Before = p.Next
	p = checkHistory(t, f, q, "b", "a")
	if p.Next != nil {
		t.Fatal("unexpected continuation after final exact-sized page")
	}
	q.Before = &HistoryPosition{ID: "a", CreatedAt: at}
	checkHistory(t, f, q, []string{}...)
	q.Before = nil
	checkHistory(t, f, q, "new", "d")
	q.State = Running
	checkHistory(t, f, q, "new", "b")
}

func TestHistoryBoundsAndDatabaseFailure(t *testing.T) {
	f := newJournalFixture(t)
	valid := HistoryQuery{Principal: "alice", Players: []string{"room"}, Limit: 50}
	for _, tc := range []struct {
		name string
		edit func(*HistoryQuery)
	}{
		{"empty principal", func(q *HistoryQuery) { q.Principal = "" }},
		{"long principal", func(q *HistoryQuery) { q.Principal = strings.Repeat("x", 257) }},
		{"invalid player", func(q *HistoryQuery) { q.Players = []string{""} }},
		{"too many players", func(q *HistoryQuery) { q.Players = make([]string, 17) }},
		{"invalid explicit player", func(q *HistoryQuery) { q.Player = "room\x00" }},
		{"zero limit", func(q *HistoryQuery) { q.Limit = 0 }},
		{"large limit", func(q *HistoryQuery) { q.Limit = 101 }},
		{"state", func(q *HistoryQuery) { q.State = "bogus" }},
		{"kind", func(q *HistoryQuery) { q.Kind = "bogus" }},
		{"delivery", func(q *HistoryQuery) { q.Delivery = "bogus" }},
		{"zero date", func(q *HistoryQuery) { q.CreatedFrom = new(time.Time) }},
		{"empty interval", func(q *HistoryQuery) { q.CreatedFrom, q.CreatedBefore = &f.now, &f.now }},
		{"cursor without id", func(q *HistoryQuery) { q.Before = &HistoryPosition{CreatedAt: f.now} }},
		{"cursor without timestamp", func(q *HistoryQuery) { q.Before = &HistoryPosition{ID: "x"} }},
		{"cursor must retain persisted precision", func(q *HistoryQuery) {
			q.Before = &HistoryPosition{ID: "x", CreatedAt: f.now.Add(time.Nanosecond)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := valid
			tc.edit(&q)
			if _, err := f.store.List(f.ctx, q); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid query accepted: %v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, err := f.store.List(ctx, valid); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled query: %v", err)
	}
	f.store.Close()
	if _, err := f.store.List(f.ctx, valid); err == nil {
		t.Fatal("closed database reported an empty successful page")
	}
}

func TestHistoryTimestampPrecision(t *testing.T) {
	base := time.Date(2026, 9, 16, 12, 0, 0, 123456000, time.UTC)
	for _, tc := range []struct {
		name        string
		input, want time.Time
	}{
		{"persisted microsecond unchanged", base, base},
		{"fraction rounds upward", base.Add(time.Nanosecond), base.Add(time.Microsecond)},
		{"second rollover", base.Truncate(time.Second).Add(time.Second - time.Nanosecond), base.Truncate(time.Second).Add(time.Second)},
		{"before Unix epoch", time.Unix(-1, 999999999), time.Unix(0, 0)},
		{"upper year rollover", time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := historyTimestamp(&tc.input)
			if !got.Valid || !got.Time.Equal(tc.want) || got.Time.Location() != time.UTC {
				t.Fatalf("filter timestamp %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHistoryDoesNotAcquireJournalOrOperationWriteLocks(t *testing.T) {
	f := newJournalFixture(t)
	seedHistory(f, "visible", "alice", "room", "playback", Running, "", f.now)
	tx, err := f.owner.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	if _, err := tx.Exec(f.ctx, `SELECT * FROM heos.journal_control FOR UPDATE; SELECT * FROM heos.operations FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	checkHistory(t, f, HistoryQuery{Principal: "alice", Players: []string{"room"}, Limit: 50}, "visible")
}

// Capture the actual sqlc query for EXPLAIN instead of maintaining a second SQL
// implementation in the test. Only the serial measurement pool uses this tracer.
type historyQueryTrace struct {
	sql  string
	args []any
}

func (trace *historyQueryTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "-- name: ListOperationHistory") {
		trace.sql, trace.args = data.SQL, data.Args
	}
	return ctx
}

func (*historyQueryTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// Keep this reproducible at the journal's actual capacity. Timings are evidence
// for schema decisions, not CI thresholds; correctness does not depend on speed.
func TestHistoryAtCapacityQueryPlansAndConcurrentUpdates(t *testing.T) {
	f := newJournalFixture(t)
	_, active := f.admit("measurement", "measurement-device")
	f.exec(`INSERT INTO heos.operations
		(id, principal, player, kind, state, device_key, epoch, phase, config_revision,
		effective_arguments, progress, outcome, scheduled_for, not_after, created_at, updated_at, finished_at)
		SELECT 'history-' || lpad(n::text, 5, '0'), 'owner-' || (n % 11), 'room-' || (n % 16),
		CASE n % 3 WHEN 0 THEN 'playback' WHEN 1 THEN 'volume' ELSE 'transport' END,
		CASE n % 4 WHEN 0 THEN 'succeeded' WHEN 1 THEN 'released' WHEN 2 THEN 'failed' ELSE 'uncertain' END,
		'private-device-' || (n % 16), $1, 'complete', 'configuration',
		jsonb_build_object('albums', repeat('private album,', 200)),
		jsonb_build_object('playback_started_at', $2::timestamptz::text, 'level', n % 50),
		CASE n % 7 WHEN 0 THEN '{}'::jsonb ELSE jsonb_build_object(
			'delivery', CASE n % 4 WHEN 0 THEN 'confirmed' WHEN 1 THEN 'not_sent' WHEN 2 THEN 'rejected' ELSE 'unknown' END,
			'commands_confirmed', n % 50, 'playback_may_continue', n % 4 <> 0,
			'diagnostics', jsonb_build_object('reason', 'unexpected_event', 'phase', 'ramping',
			'source', 'event', 'changed_fields', jsonb_build_array('volume'),
			'expected', jsonb_build_object('volume', 10, 'muted', false),
			'observed', jsonb_build_object('volume', 13, 'muted', false))) END,
		$2::timestamptz - n * interval '1 second', $2::timestamptz + interval '1 minute',
		$2::timestamptz - n * interval '1 second', $2::timestamptz, $2::timestamptz
		FROM generate_series(1,9999) AS series(n)`, f.store.epoch, f.now)
	f.exec(`ANALYZE heos.operations`)
	var version, size string
	if err := f.owner.QueryRow(f.ctx, `SELECT current_setting('server_version'), pg_size_pretty(pg_total_relation_size('heos.operations'))`).Scan(&version, &size); err != nil {
		t.Fatal(err)
	}
	t.Logf("PostgreSQL %s; operations table including indexes: %s", version, size)
	var count int
	if err := f.owner.QueryRow(f.ctx, `SELECT count(*) FROM heos.operations`).Scan(&count); err != nil || count != 10000 {
		t.Fatalf("measurement capacity: %d: %v", count, err)
	}
	pc, err := poolConfig(f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	trace := new(historyQueryTrace)
	pc.ConnConfig.Tracer = trace
	pool, err := pgxpool.NewWithConfig(f.ctx, pc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := &Store{pool: pool, timeout: time.Duration(f.runtime.TimeoutSeconds) * time.Second}
	players := make([]string, 16)
	for i := range players {
		players[i] = fmt.Sprintf("room-%d", i)
	}
	base := HistoryQuery{Principal: "owner-1", Operator: true, Players: players, Limit: 50}
	cases := []struct {
		name string
		q    HistoryQuery
	}{
		{name: "operator-all", q: base},
		{name: "owner-all", q: base},
		{name: "owner-one-player", q: base},
		{name: "state-kind-delivery", q: base},
		{name: "deep-cursor-time-window", q: base},
		{name: "no-matches", q: base},
	}
	cases[1].q.Operator = false
	cases[2].q.Operator, cases[2].q.Players = false, []string{"room-1"}
	cases[3].q.State, cases[3].q.Kind, cases[3].q.Delivery = Released, "playback", "not_sent"
	start, end := f.now.Add(-9999*time.Second), f.now.Add(-8000*time.Second)
	cases[4].q.Before = &HistoryPosition{CreatedAt: f.now.Add(-9000 * time.Second), ID: "history-09000"}
	cases[4].q.CreatedFrom, cases[4].q.CreatedBefore = &start, &end
	cases[5].q.Principal, cases[5].q.Operator = "nobody", false
	measure := func(name string, q HistoryQuery) {
		t.Helper()
		times := make([]time.Duration, 30)
		for i := range times {
			start := time.Now()
			page, err := s.List(f.ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Items) > q.Limit {
				t.Fatal("history exceeded page limit")
			}
			times[i] = time.Since(start)
		}
		slices.Sort(times)
		t.Logf("%s: 30 calls including TLS/scan; median=%s p95=%s max=%s", name, times[15], times[28], times[29])
	}
	for _, tc := range cases {
		measure(tc.name, tc.q)
		rows, err := f.owner.Query(f.ctx, "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) "+trace.sql, trace.args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			plan = append(plan, line)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s plan:\n%s", tc.name, strings.Join(plan, "\n"))
	}
	// PostgreSQL may choose generic plans for a cached prepared query. Optional
	// filters must remain reasonable with that plan as well as custom plans.
	if _, err := pool.Exec(f.ctx, `SET plan_cache_mode = force_generic_plan`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		measure("generic-"+tc.name, tc.q)
	}
	// Use the ordinary journal transition path, including its control-row lock,
	// from a separate runtime connection while the list is repeatedly read.
	updates := make(chan error, 1)
	go func() {
		for i := range 100 {
			active, err = f.store.Transition(f.ctx, active.ID, active.Revision, Update{
				State: Running, Phase: "ramping", Progress: []byte(fmt.Sprintf(`{"level":%d}`, i)), Outcome: []byte(`{}`),
			})
			if err != nil {
				updates <- err
				return
			}
		}
		updates <- nil
	}()
	measure("generic-operator-with-journal-updates", base)
	if err := <-updates; err != nil {
		t.Fatal(err)
	}
}
