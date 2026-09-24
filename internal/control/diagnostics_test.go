package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func TestOwnershipEventLogsFirstCauseAtInfo(t *testing.T) {
	c, j, d, req, cmd := automationFixture(t)
	var output bytes.Buffer
	c.logger = slog.New(slog.NewJSONHandler(&output, nil))
	d.before = func(m heos.Mutation) {
		if m.Kind == heos.MutationKindQueue {
			// Denon 5.4: pause while loading a stopped player is unexpected.
			d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"pause"}, "text": {"private-marker"}}})
			d.handler(heos.Event{Gap: true}) // A later disconnect must not replace the cause.
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.ErrorCode != "ownership_lost" || o.State != journal.Uncertain {
		t.Fatal(o)
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatal(err, output.String())
	}
	if record["level"] != "INFO" || record["reason"] != "unexpected_event" || record["phase"] != "sending_queue" || record["operation_id"] != a.ID || record["commands_confirmed"] != float64(3) {
		t.Fatal(record)
	}
	if record["event"].(map[string]any)["command"] != "event/player_state_changed" {
		t.Fatal(record)
	}
	if strings.Contains(output.String(), "private-marker") {
		t.Fatal("private event text leaked")
	}
}

func TestPendingReadbackLogsChangedField(t *testing.T) {
	c, j, base, req, cmd := albumFixture(t)
	c.clock = &advancingClock{now: time.Now()}
	var output bytes.Buffer
	c.logger = slog.New(slog.NewJSONHandler(&output, nil))
	d := &pendingDevice{albumDevice: base, kind: "queue", change: "volume"}
	c.lanes["room"].writer, c.lanes["room"].device.Observer = d, d
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if o := awaitOperation(t, j, a.ID); o.ErrorCode != "ownership_lost" {
		t.Fatal(o)
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatal(err, output.String())
	}
	if record["reason"] != "pending_readback_changed" || record["changed_fields"].([]any)[0] != "volume" || record["rule"] != "queue_controls_changed" {
		t.Fatal(record)
	}
}

func TestOwnershipMismatchKeepsClassificationAndPrivateDataOut(t *testing.T) {
	v, muted := 10, false
	a := heos.Snapshot{State: heos.PlayStatePlay, Volume: &v, Muted: &muted, Media: &heos.Media{Source: "1024", ID: "private-media", Song: "private-song"}}
	b := a
	v2 := 11
	b.Volume = &v2
	err := ownershipMismatch("before_write_changed", stateChanges(a, b), a, b)
	if !errors.Is(err, ErrOwnership) {
		t.Fatal(err)
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	logger.Info("test", "detail", err)
	if strings.Contains(output.String(), "private-") || !strings.Contains(output.String(), "volume") {
		t.Fatal(output.String())
	}
}

func TestQueueDeadlineLogKeepsCauseBeforeSocketCleanupGap(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(&ownershipError{reason: "event_gap"})
	var output bytes.Buffer
	c := &Coordinator{logger: slog.New(slog.NewJSONHandler(&output, nil))}
	c.logOwnershipLoss(&execution{ctx: ctx}, errQueueTransitionTimeout)
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatal(err)
	}
	if record["reason"] != "queue_transition_timeout" || record["level"] != "INFO" {
		t.Fatal(record)
	}
}
