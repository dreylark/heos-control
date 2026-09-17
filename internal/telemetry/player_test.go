package telemetry_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/dreylark/heos-control/internal/telemetry"
)

func gather(t *testing.T, p *telemetry.Player) map[string]*dto.MetricFamily {
	t.Helper()
	r := prometheus.NewPedanticRegistry()
	r.MustRegister(p)
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]*dto.MetricFamily)
	for _, f := range families {
		out[f.GetName()] = f
	}
	return out
}

func metric(t *testing.T, families map[string]*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	f := families[name]
	if f != nil {
		for _, m := range f.GetMetric() {
			values := make(map[string]string)
			for _, l := range m.GetLabel() {
				values[l.GetName()] = l.GetValue()
			}
			match := len(values) == len(labels)
			for k, v := range labels {
				match = match && values[k] == v
			}
			if match {
				return m
			}
		}
	}
	t.Fatalf("missing %s with labels %v", name, labels)
	return nil
}

func TestPlayerRecordsActualObservations(t *testing.T) {
	p := telemetry.NewPlayer("room")
	p.WireCommand("player/set_volume", "reply_success", 250*time.Millisecond)
	p.WireCommand("player/set_volume", "uncertain", 30*time.Second)
	p.Reconnected()
	p.EventGap("event_buffer_overflow")
	p.Observation("scalars", "fallback")
	p.Confirmation("volume", "confirmed", 12*time.Second)
	p.Fallback("volume")
	p.OperationFinished("playback", "released", "unexpected_event")
	p.Active("ramping")
	f := gather(t, p)
	for _, tc := range []struct {
		name   string
		labels map[string]string
	}{
		{"heos_wire_commands_total", map[string]string{"player": "room", "command": "player/set_volume", "result": "reply_success"}},
		{"heos_reconnects_total", map[string]string{"player": "room"}},
		{"heos_event_gaps_total", map[string]string{"player": "room", "reason": "event_buffer_overflow"}},
		{"heos_observation_refreshes_total", map[string]string{"player": "room", "scope": "scalars", "trigger": "fallback"}},
		{"heos_confirmation_fallbacks_total", map[string]string{"player": "room", "kind": "volume"}},
		{"heos_operations_finished_total", map[string]string{"player": "room", "kind": "playback", "state": "released", "reason": "unexpected_event"}},
	} {
		if got := metric(t, f, tc.name, tc.labels).GetCounter().GetValue(); got != 1 {
			t.Errorf("%s = %g, want 1", tc.name, got)
		}
	}
	wire := metric(t, f, "heos_wire_command_duration_seconds", map[string]string{"player": "room", "command": "player/set_volume"}).GetHistogram()
	if wire.GetSampleCount() != 2 || wire.GetSampleSum() != 30.25 {
		t.Fatal("wrong command duration", wire)
	}
	confirmation := metric(t, f, "heos_command_confirmation_duration_seconds", map[string]string{"player": "room", "kind": "volume", "result": "confirmed"}).GetHistogram()
	if confirmation.GetSampleCount() != 1 || confirmation.GetSampleSum() != 12 {
		t.Fatal("wrong confirmation duration", confirmation)
	}
	for _, h := range []*dto.Histogram{wire, confirmation} {
		buckets := h.GetBucket()
		if len(buckets) > 16 || len(buckets) == 0 || buckets[len(buckets)-1].GetUpperBound() < 30 {
			t.Fatal("histogram lacks a bounded useful timeout range", buckets)
		}
	}
	if metric(t, f, "heos_active_operations", map[string]string{"player": "room", "phase": "ramping"}).GetGauge().GetValue() != 1 {
		t.Fatal("missing active phase")
	}
}

func TestConcurrentEventsAndScrapesPreserveCountsAndOneActivePhase(t *testing.T) {
	p := telemetry.NewPlayer("room")
	r := prometheus.NewPedanticRegistry()
	r.MustRegister(p)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				p.WireCommand("player/get_volume", "reply_success", time.Millisecond)
				p.Active("ramping")
				p.Active("playing")
			}
		})
	}
	for range 2 {
		wg.Go(func() {
			for range 50 {
				families, err := r.Gather()
				if err != nil {
					t.Error(err)
					return
				}
				for _, f := range families {
					if f.GetName() != "heos_active_operations" {
						continue
					}
					total := 0.0
					for _, m := range f.GetMetric() {
						total += m.GetGauge().GetValue()
					}
					if total > 1 {
						t.Error("scrape saw more than one active phase", total)
					}
				}
			}
		})
	}
	wg.Wait()
	p.Active("")
	f := gather(t, p)
	if got := metric(t, f, "heos_wire_commands_total", map[string]string{"player": "room", "command": "player/get_volume", "result": "reply_success"}).GetCounter().GetValue(); got != 400 {
		t.Fatal("lost concurrent observations", got)
	}
	for _, m := range f["heos_active_operations"].GetMetric() {
		if m.GetGauge().GetValue() != 0 {
			t.Fatal("clearing operation retained active phase", m)
		}
	}
}

func TestUnknownLabelsCollapseWithoutLeakingPayloads(t *testing.T) {
	p := telemetry.NewPlayer("room")
	for i := range 100 {
		secret := fmt.Sprintf("private-operation-%d", i)
		p.WireCommand(secret, secret, -time.Second)
		p.EventGap(secret)
		p.Observation(secret, secret)
		p.Confirmation(secret, secret, -time.Second)
		p.Fallback(secret)
		p.OperationFinished(secret, secret, secret)
		p.Active(secret)
	}
	f := gather(t, p)
	for name, family := range f {
		for _, m := range family.GetMetric() {
			for _, label := range m.GetLabel() {
				value := label.GetValue()
				if label.GetName() == "player" && value != "room" {
					t.Error("changed player label", value)
				}
				if label.GetName() != "player" && label.GetName() != "phase" && value != "unknown" {
					t.Error("unbounded label survived normalization", name, label)
				}
			}
		}
		if name != "heos_active_operations" && len(family.GetMetric()) != 1 {
			t.Error("unknown inputs increased series cardinality", name, len(family.GetMetric()))
		}
	}
	wire := metric(t, f, "heos_wire_commands_total", map[string]string{"player": "room", "command": "unknown", "result": "unknown"})
	if wire.GetCounter().GetValue() != 100 {
		t.Fatal("unknown commands did not share a series", wire)
	}
	for _, name := range []string{"heos_wire_command_duration_seconds", "heos_command_confirmation_duration_seconds"} {
		h := f[name].GetMetric()[0].GetHistogram()
		if h.GetSampleCount() != 100 || h.GetSampleSum() != 0 {
			t.Fatal("negative elapsed time corrupted histogram", name, h)
		}
	}
	if metric(t, f, "heos_active_operations", map[string]string{"player": "room", "phase": "unknown"}).GetGauge().GetValue() != 1 {
		t.Fatal("unknown phase missing")
	}
}

func TestPlayerNilIsNoopAndCollectorsAreIndependent(t *testing.T) {
	var absent *telemetry.Player
	absent.WireCommand("player/set_volume", "reply_success", time.Second)
	absent.Reconnected()
	absent.EventGap("connection_closed")
	absent.Observation("full", "startup")
	absent.Confirmation("volume", "confirmed", time.Second)
	absent.Fallback("volume")
	absent.OperationFinished("volume", "succeeded", "completed")
	absent.Active("ramping")
	absent.Describe(nil)
	absent.Collect(nil)

	// Identical names can be registered in independent registries without
	// accumulating global state; different players can share one scrape.
	a, b := telemetry.NewPlayer("a"), telemetry.NewPlayer("b")
	a.Reconnected()
	r := prometheus.NewPedanticRegistry()
	r.MustRegister(a, b)
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	f := make(map[string]*dto.MetricFamily)
	for _, family := range families {
		f[family.GetName()] = family
	}
	if metric(t, f, "heos_reconnects_total", map[string]string{"player": "a"}).GetCounter().GetValue() != 1 ||
		metric(t, f, "heos_reconnects_total", map[string]string{"player": "b"}).GetCounter().GetValue() != 0 {
		t.Fatal("per-player collectors shared counts")
	}
	fresh := gather(t, telemetry.NewPlayer("a"))
	if metric(t, fresh, "heos_reconnects_total", map[string]string{"player": "a"}).GetCounter().GetValue() != 0 {
		t.Fatal("metrics leaked between service instances")
	}
}
