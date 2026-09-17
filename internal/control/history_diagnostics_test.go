package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func persistedDiagnostics(t *testing.T, op journal.Operation) map[string]any {
	t.Helper()
	var result struct {
		Diagnostics map[string]any `json:"diagnostics"`
	}
	if err := json.Unmarshal(op.Outcome, &result); err != nil || result.Diagnostics == nil {
		t.Fatalf("missing durable diagnostics: %s, %v", op.Outcome, err)
	}
	return result.Diagnostics
}

func TestHistoryRetainsFirstEventEvidence(t *testing.T) {
	c, j, d, req, cmd := automationFixture(t)
	d.before = func(m heos.Mutation) {
		if m.Kind == "queue" {
			d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"pause"}, "text": {"private-marker"}}})
			d.handler(heos.Event{Gap: true})
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	diag := persistedDiagnostics(t, o)
	if diag["reason"] != "unexpected_event" || diag["source"] != "event" || diag["phase"] != "sending_queue" || diag["detected_at"] == nil {
		t.Fatal(diag)
	}
	observed := diag["observed"].(map[string]any)
	if len(observed) != 1 || observed["state"] != "pause" {
		t.Fatal("event invented a full observation", observed)
	}
	if strings.Contains(string(o.Outcome), "private-marker") {
		t.Fatal("private event data persisted")
	}
}

func TestHistoryRecordsObservationRule(t *testing.T) {
	c, j, base, req, cmd := albumFixture(t)
	c.clock = &advancingClock{now: time.Now()}
	d := &pendingDevice{albumDevice: base, kind: "queue", change: "volume"}
	c.lanes["room"].writer, c.lanes["room"].device.Observer = d, d
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	diag := persistedDiagnostics(t, awaitOperation(t, j, a.ID))
	if diag["reason"] != "pending_readback_changed" || diag["rule"] != "queue_controls_changed" || diag["source"] != "observation" {
		t.Fatal(diag)
	}
	if diag["expected"].(map[string]any)["volume"] == diag["observed"].(map[string]any)["volume"] {
		t.Fatal("lost scalar comparison", diag)
	}
}

func TestHistoryEventEvidenceIsImmutableAndPartial(t *testing.T) {
	at := time.Date(2026, 9, 16, 7, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	volume, mute := 10, false
	r := &execution{ctx: ctx, cancel: cancel, phase: "holding", now: func() time.Time { return at },
		expected: heos.Snapshot{Player: heos.Player{ID: "1"}, State: "play", Volume: &volume, Muted: &mute}}
	event := heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"13"}, "mute": {"off"}}}
	r.event(event)
	event.Params.Set("level", "99")
	volume, mute, r.phase = 77, true, "complete"
	at = at.Add(time.Hour)
	c := &Coordinator{clock: &advancingClock{now: at}}
	d := c.completionDiagnostics(r, context.Cause(ctx), "ownership_lost")
	if d.Phase != "holding" || d.DetectedAt == nil || d.DetectedAt.Hour() != 7 || *d.Expected.Volume != 10 || *d.Observed.Volume != 13 || *d.Observed.Muted {
		t.Fatalf("evidence changed after detection: %+v", d)
	}
	if !reflect.DeepEqual(d.ChangedFields, []string{"volume"}) {
		t.Fatal("unchanged mute reported as intervention", d.ChangedFields)
	}
	if d.Observed.State != nil || d.Observed.Grouped != nil {
		t.Fatal("partial notification invented fields", d.Observed)
	}
}

func TestHistoryDeadlineKeepsRuleAndDoesNotMutateSentinel(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(&ownershipError{reason: "event_gap"})
	at := time.Now().UTC()
	r := &execution{ctx: ctx, phase: "holding"}
	err := r.captureDecision(errQueueTransitionTimeout, "transition_timeout", at)
	if !errors.Is(err, errQueueTransitionTimeout) || !errors.Is(err, ErrOwnership) {
		t.Fatal("error classification changed", err)
	}
	c := &Coordinator{clock: &advancingClock{now: at.Add(time.Hour)}}
	d := c.completionDiagnostics(r, err, "ownership_lost")
	if d.Reason != "queue_transition_timeout" || d.Rule == nil || *d.Rule != "transition_timeout" || !d.DetectedAt.Equal(at) || d.Phase != "holding" {
		t.Fatal(d)
	}
	if errQueueTransitionTimeout.evidence.DetectedAt != nil || errQueueTransitionTimeout.evidence.Rule != nil {
		t.Fatal("shared error mutated")
	}
}

type historyAckJournal struct {
	*memoryJournal
	corrupt bool
}

func (j historyAckJournal) Transition(ctx context.Context, id string, rev int64, u journal.Update) (journal.Operation, error) {
	if j.corrupt {
		u.Outcome = json.RawMessage(`{"delivery":"unknown","diagnostics":{"reason":"wrong_cause"}}`)
	}
	op, err := j.memoryJournal.Transition(ctx, id, rev, u)
	if err != nil {
		return op, err
	}
	return op, journal.ErrCommitUncertain
}

func TestHistoryLostAckRequiresExactIntendedDiagnostic(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(strconv.FormatBool(corrupt), func(t *testing.T) {
			c, j, _, _ := fixtureCoordinator(t)
			op := journal.Operation{ID: "ack", Revision: 1, State: journal.Running, Phase: "holding"}
			j.ops[op.ID] = op
			c.db = historyAckJournal{memoryJournal: j, corrupt: corrupt}
			r := &execution{ctx: context.Background(), op: op}
			u := journal.Update{State: journal.Released, Phase: "complete", ErrorCode: "ownership_lost", Outcome: json.RawMessage(`{"delivery":"unknown","diagnostics":{"reason":"unexpected_event"}}`)}
			err := c.transition(context.Background(), r, u)
			if (err != nil) != corrupt {
				t.Fatalf("lost ack resolution ignored intended outcome, corrupt=%t: %v", corrupt, err)
			}
		})
	}
}

func TestHistorySuccessDoesNotAcquireLaterEventCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(&ownershipError{reason: "event_gap"})
	c := &Coordinator{clock: &advancingClock{now: time.Now()}}
	d := c.completionDiagnostics(&execution{ctx: ctx, op: journal.Operation{Phase: "sending_volume"}}, nil, "")
	if d.Reason != "completed" || d.Source != "controller" {
		t.Fatal(d)
	}
}

func TestHistoryTargetOwnEvidenceSurvivesCancellationAndFailedHandoff(t *testing.T) {
	for _, phase := range []string{"cancelled", "handoff_failed"} {
		t.Run(phase, func(t *testing.T) {
			c, j, _, _ := fixtureCoordinator(t)
			at := c.clock.Now().UTC()
			prior, err := journal.WithDiagnostics(json.RawMessage(`{"commands_confirmed":9}`), journal.Diagnostics{Reason: "cancellation_requested", Source: "controller", Phase: "holding", DetectedAt: &at})
			if err != nil {
				t.Fatal(err)
			}
			j.ops["target"] = journal.Operation{ID: "target", Revision: 3, State: journal.Running, Phase: "cancelling", Outcome: prior}
			c.persistByID("target", journal.Update{State: journal.Cancelled, Phase: phase, Outcome: json.RawMessage(`{"delivery":"confirmed","playback_may_continue":false}`)}, nil)
			op, _ := j.Get(context.Background(), "target")
			d := persistedDiagnostics(t, op)
			if d["phase"] != "holding" || d["observed"] != nil || d["expected"] != nil || !strings.Contains(string(op.Outcome), `"commands_confirmed":9`) {
				t.Fatal(string(op.Outcome))
			}
		})
	}
}

func TestHistoryCompletionReasonsKeepDeliveryStagesSeparate(t *testing.T) {
	for _, tc := range []struct {
		name        string
		err         error
		code        string
		unconfirmed bool
		reason      string
	}{
		{"completed", nil, "", false, "completed"},
		{"rejected", heos.ErrRejected, "device_rejected", false, "device_rejected"},
		{"confirmation deadline", context.DeadlineExceeded, "device_unavailable", true, "confirmation_timeout"},
		{"wire deadline", context.DeadlineExceeded, "device_unavailable", false, "command_timeout"},
		{"database deadline", context.DeadlineExceeded, "journal_unavailable", false, "journal_unavailable"},
		{"shutdown", context.DeadlineExceeded, "service_stopping", false, "service_stopping"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Coordinator{clock: &advancingClock{now: time.Now()}}
			r := &execution{ctx: context.Background(), unconfirmed: tc.unconfirmed, op: journal.Operation{Phase: "sending_volume"}}
			d := c.completionDiagnostics(r, tc.err, tc.code)
			if d.Reason != tc.reason {
				t.Fatalf("reason = %q, want %q", d.Reason, tc.reason)
			}
		})
	}
}

type historyRejectedWriter struct {
	*fakeDevice
	err error
}

func (d historyRejectedWriter) Write(context.Context, heos.Mutation, heos.Guard) (heos.Response, error) {
	return heos.Response{}, d.err
}

func TestHistoryNamesDeviceRejectionWithoutChangingClassification(t *testing.T) {
	for _, tc := range []struct {
		code   int
		reason string
	}{{14, "cannot_play"}, {999, "unknown"}} {
		t.Run(tc.reason, func(t *testing.T) {
			c, j, d, req := fixtureCoordinator(t)
			rejected := &heos.DeviceError{Command: "player/set_volume", Code: tc.code, Text: "private-media-marker"}
			c.lanes["room"].writer = historyRejectedWriter{fakeDevice: d, err: fmt.Errorf("send control: %w", &heos.CommandError{Delivery: heos.Rejected, Cause: rejected})}
			a, err := c.Submit(context.Background(), req, Command{Kind: "volume", Level: 10})
			if err != nil {
				t.Fatal(err)
			}
			op := awaitOperation(t, j, a.ID)
			diagnostics := persistedDiagnostics(t, op)
			if op.State != journal.Failed || op.ErrorCode != "device_rejected" || diagnostics["reason"] != tc.reason {
				t.Fatalf("wrong rejection classification: state=%s, code=%s, diagnostics=%v", op.State, op.ErrorCode, diagnostics)
			}
			if strings.Contains(string(op.Outcome), "private-media-marker") {
				t.Fatal("device error leaked free-form text", string(op.Outcome))
			}
		})
	}
}

func TestHistoryDeviceRejectionDoesNotReplaceFirstOwnershipCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(&ownershipError{reason: "event_gap"})
	r := &execution{ctx: ctx, op: journal.Operation{Phase: "sending_volume"}}
	c := &Coordinator{clock: &advancingClock{now: time.Now()}}
	d := c.completionDiagnostics(r, &heos.DeviceError{Code: 14}, "device_rejected")
	if d.Reason != "event_gap" {
		t.Fatal("later device reply replaced the first ownership cause", d)
	}
}
