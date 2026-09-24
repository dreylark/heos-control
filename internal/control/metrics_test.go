package control

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
	"github.com/dreylark/heos-control/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func metricSamples(t *testing.T, player *telemetry.Player, name string) []*dto.Metric {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(player)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family.GetMetric()
		}
	}
	return nil
}

func metricLabel(sample *dto.Metric, name string) string {
	for _, label := range sample.GetLabel() {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}

func TestConfirmationMetricsMeasureApplicationAndExcludeNotSent(t *testing.T) {
	for _, outcome := range []string{"event", "event_before_reply", "fallback", "timeout", "rejected", "not_sent", "intervention", "cancelled", "uncertain"} {
		t.Run(outcome, func(t *testing.T) {
			c, r, device, clock := scalarFixture(t)
			metrics := telemetry.NewPlayer("room")
			c.lanes["room"].device.Metrics = metrics
			began := clock.Now()
			expectedResult, expectedDuration := "confirmed", 350*time.Millisecond
			device.onWrite = func(heos.Mutation) error {
				switch outcome {
				case "rejected":
					return &heos.CommandError{Delivery: heos.Rejected, Cause: &heos.DeviceError{Code: 14}}
				case "not_sent":
					return &heos.CommandError{Delivery: heos.NotSent, Cause: heos.ErrStale}
				case "uncertain":
					return &heos.CommandError{Delivery: heos.Uncertain, Cause: heos.ErrProtocol}
				case "event_before_reply":
					return clock.Wait(r.ctx, time.Second, nil)
				}
				return nil
			}
			switch outcome {
			case "event":
				clock.events = []modeClockEvent{{began.Add(expectedDuration), func() { r.event(volumeEvent("21", "off")) }}}
			case "event_before_reply":
				clock.events = []modeClockEvent{{began.Add(350 * time.Millisecond), func() { r.event(volumeEvent("21", "off")) }}}
				expectedDuration = time.Second
			case "fallback":
				expectedDuration = 11 * time.Second
				device.onScalarRead = func() error { v := 21; device.s.Volume = &v; return nil }
			case "timeout":
				expectedResult, expectedDuration = "timeout", 11*time.Second
			case "rejected":
				expectedResult, expectedDuration = "rejected", 0
			case "intervention":
				expectedResult = "released"
				clock.events = []modeClockEvent{{began.Add(expectedDuration), func() { r.event(volumeEvent("50", "off")) }}}
			case "cancelled":
				expectedResult = "cancelled"
				clock.events = []modeClockEvent{{began.Add(expectedDuration), func() { r.cancel(context.Canceled) }}}
			case "uncertain":
				expectedResult, expectedDuration = "uncertain", 0
			}
			err := c.write(c.lanes["room"], r, heos.Mutation{Kind: heos.MutationKindVolume, Level: 21})
			if (err == nil) != (outcome == "event" || outcome == "event_before_reply" || outcome == "fallback") {
				t.Fatal("unexpected command result", err)
			}
			samples := metricSamples(t, metrics, "heos_command_confirmation_duration_seconds")
			if outcome == "not_sent" {
				if len(samples) != 0 {
					t.Fatal("unsent command counted as confirmation", samples)
				}
				return
			}
			if len(samples) != 1 || samples[0].GetHistogram().GetSampleCount() != 1 || metricLabel(samples[0], "result") != expectedResult || samples[0].GetHistogram().GetSampleSum() != expectedDuration.Seconds() {
				t.Fatalf("confirmation metrics: %v; want %s after %s", samples, expectedResult, expectedDuration)
			}
			fallbacks := metricSamples(t, metrics, "heos_confirmation_fallbacks_total")
			if outcome == "fallback" || outcome == "timeout" {
				if len(fallbacks) != 1 || fallbacks[0].GetCounter().GetValue() != 1 || len(device.scalarReads) != 1 {
					t.Fatal("fallback count differs from attempted verification", fallbacks, device.scalarReads)
				}
			} else if len(fallbacks) != 0 || len(device.scalarReads) != 0 {
				t.Fatal("metrics created a fallback", fallbacks, device.scalarReads)
			}
		})
	}
}

func TestOperationMetricsDoNotCountIdempotentReplay(t *testing.T) {
	c, journalStore, _, request := fixtureCoordinator(t)
	metrics := telemetry.NewPlayer("room")
	c.lanes["room"].device.Metrics = metrics
	first, err := c.Submit(context.Background(), request, Command{Kind: CommandKindVolume, Level: 10})
	if err != nil {
		t.Fatal(err)
	}
	awaitOperation(t, journalStore, first.ID)
	second, err := c.Submit(context.Background(), request, Command{Kind: CommandKindVolume, Level: 10})
	if err != nil || second.ID != first.ID {
		t.Fatal(second, err)
	}
	c.Close()
	samples := metricSamples(t, metrics, "heos_operations_finished_total")
	if len(samples) != 1 || samples[0].GetCounter().GetValue() != 1 || metricLabel(samples[0], "state") != "succeeded" || metricLabel(samples[0], "reason") != "completed" {
		t.Fatal("operation completion counted incorrectly", samples)
	}
	for _, sample := range metricSamples(t, metrics, "heos_active_operations") {
		if sample.GetGauge().GetValue() != 0 {
			t.Fatal("worker exit left an active phase", sample)
		}
	}
}

func TestCompletionReceiptOnlyCountsConfirmedTerminalStateOnce(t *testing.T) {
	metrics := telemetry.NewPlayer("room")
	receipt := &operationMetrics{player: metrics, kind: "playback"}
	op := journal.Operation{State: journal.Running}
	receipt.finish(op)
	if len(metricSamples(t, metrics, "heos_operations_finished_total")) != 0 {
		t.Fatal("counted unfinished work")
	}
	now := time.Now()
	op.State, op.FinishedAt = journal.Released, &now
	op.Outcome, _ = journal.WithDiagnostics(nil, journal.Diagnostics{Reason: "event_gap", Source: "event", Phase: "playing"})
	var callers sync.WaitGroup
	for range 16 {
		callers.Go(func() { receipt.finish(op) })
	}
	callers.Wait()
	samples := metricSamples(t, metrics, "heos_operations_finished_total")
	if len(samples) != 1 || samples[0].GetCounter().GetValue() != 1 || metricLabel(samples[0], "reason") != "event_gap" {
		t.Fatal("duplicate completion or reason lost", samples)
	}
}

func TestOperationMetricsResolveLostJournalAcknowledgements(t *testing.T) {
	for _, path := range []string{"worker", "orphan"} {
		t.Run(path, func(t *testing.T) {
			c, store, _, _ := fixtureCoordinator(t)
			c.clock = &advancingClock{now: time.Now()}
			c.db = historyAckJournal{memoryJournal: store}
			metrics := telemetry.NewPlayer("room")
			receipt := &operationMetrics{player: metrics, kind: "playback"}
			op := journal.Operation{ID: "lost-ack", Kind: "playback", State: journal.Running, Revision: 1}
			store.ops[op.ID] = op
			update := journal.Update{State: journal.Released, Phase: "complete", Outcome: json.RawMessage(`{"diagnostics":{"reason":"event_gap"}}`)}
			if path == "worker" {
				run := &execution{ctx: context.Background(), op: op, metrics: receipt}
				if err := c.transition(context.Background(), run, update); err != nil {
					t.Fatal(err)
				}
			} else {
				c.persistByID(op.ID, update, receipt)
			}
			// A later reconciler sees the durable terminal result, sharing the
			// receipt rather than reconstructing one from journal history.
			c.persistByID(op.ID, update, receipt)
			samples := metricSamples(t, metrics, "heos_operations_finished_total")
			if len(samples) != 1 || samples[0].GetCounter().GetValue() != 1 || metricLabel(samples[0], "state") != "released" {
				t.Fatal("lost commit acknowledgement duplicated or lost completion", samples)
			}
			actual, err := store.Get(context.Background(), op.ID)
			if err != nil || metricLabel(samples[0], "reason") != persistedDiagnostics(t, actual)["reason"] {
				t.Fatal("counter reason differs from durable result", samples, actual, err)
			}
		})
	}
}

type metricsTerminalJournal struct {
	*memoryJournal
	beforeTerminal func(journal.Update)
}

func (j metricsTerminalJournal) Transition(ctx context.Context, id string, revision int64, update journal.Update) (journal.Operation, error) {
	if update.State == journal.Cancelled {
		j.beforeTerminal(update)
	}
	return j.memoryJournal.Transition(ctx, id, revision, update)
}

func TestOperationMetricsCancellationAndSupersession(t *testing.T) {
	for _, mode := range []string{"release", "stop_owned", "operator_stop"} {
		t.Run(mode, func(t *testing.T) {
			c, store, _, request := fixtureCoordinator(t)
			metrics := telemetry.NewPlayer("room")
			c.lanes["room"].device.Metrics = metrics
			clock := &heldClock{entered: make(chan struct{})}
			c.clock = clock
			persisting := make(chan bool, 1)
			c.db = metricsTerminalJournal{memoryJournal: store, beforeTerminal: func(journal.Update) {
				active := false
				for _, sample := range metricSamples(t, metrics, "heos_active_operations") {
					active = active || (metricLabel(sample, "phase") == "persisting" && sample.GetGauge().GetValue() == 1)
				}
				persisting <- active
			}}
			request.Method, request.IfMatch = "POST", ""
			original, err := c.Submit(context.Background(), request, Command{Kind: CommandKindStop, FadeSeconds: 30})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-clock.entered:
			case <-time.After(time.Second):
				t.Fatal("fade did not start")
			}
			active := 0.0
			for _, sample := range metricSamples(t, metrics, "heos_active_operations") {
				active += sample.GetGauge().GetValue()
			}
			if active != 1 || len(metricSamples(t, metrics, "heos_operations_finished_total")) != 0 {
				t.Fatal("active work was not reported as unfinished")
			}
			request.Key = "replacement"
			command := Command{Kind: CommandKindCancel, Mode: mode, Target: original.ID}
			if mode == "operator_stop" {
				command = Command{Kind: CommandKindStop}
			}
			replacement, err := c.Submit(context.Background(), request, command)
			if err != nil {
				t.Fatal(err)
			}
			awaitOperation(t, store, replacement.ID)
			c.Close()
			if mode != "operator_stop" {
				select {
				case active := <-persisting:
					if !active {
						t.Error("target journal persistence kept reporting device activity")
					}
				case <-time.After(time.Second):
					t.Fatal("target was not finalized")
				}
			}
			samples := metricSamples(t, metrics, "heos_operations_finished_total")
			total := 0.0
			for _, sample := range samples {
				total += sample.GetCounter().GetValue()
				if metricLabel(sample, "state") != "succeeded" {
					wantReason, wantState := "cancelled", "cancelled"
					if mode == "operator_stop" {
						wantReason, wantState = "superseded", "released"
					}
					if metricLabel(sample, "reason") != wantReason || metricLabel(sample, "state") != wantState {
						t.Fatal("wrong target result", sample)
					}
				}
			}
			if len(samples) != 2 || total != 2 {
				t.Fatal("handoff did not count each operation exactly once", samples)
			}
			for _, sample := range metricSamples(t, metrics, "heos_active_operations") {
				if sample.GetGauge().GetValue() != 0 {
					t.Fatal("handoff left active work", sample)
				}
			}
		})
	}
}
