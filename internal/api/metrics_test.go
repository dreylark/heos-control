package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/telemetry"
)

func TestMetricsAuthorizationAndBoundedLabels(t *testing.T) {
	for _, tc := range []struct {
		auth   bool
		scopes []string
		status int
	}{{false, nil, 401}, {true, []string{"operator"}, 403}, {true, []string{"read"}, 200}} {
		s := testAPI(t)
		s.credentials[0].Scopes = tc.scopes
		r := httptest.NewRequest("GET", "/metrics", nil)
		if tc.auth {
			r.Header.Set("Authorization", "Bearer "+testToken)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatal(w.Code, w.Body)
		}
		if tc.status == 200 {
			if w.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
				t.Fatal(w.Header())
			}
			for _, name := range []string{"heos_journal_ready 1", "heos_go_goroutines", `heos_player_connected{player="room"}`} {
				if !strings.Contains(w.Body.String(), name) {
					t.Fatal("missing metric", name, w.Body)
				}
			}
			for _, secret := range []string{testToken, "principal=", "operation=", "album=", "serial="} {
				if strings.Contains(w.Body.String(), secret) {
					t.Fatal("unsafe metric", secret)
				}
			}
			if ok, errs := s.validator.ValidateHttpResponse(r, w.Result()); !ok {
				t.Fatal(errs)
			}
		}
	}
}

type offlineMetricsJournal struct{ readJournal }

func (offlineMetricsJournal) Ready(context.Context) error { return errors.New("offline") }

func TestMetricsRemainObservableDuringDatabaseOutageAndRespectPlayerACL(t *testing.T) {
	s := testAPI(t)
	s.credentials[0].Players = []string{"other"}
	s.journal = offlineMetricsJournal{}
	r := httptest.NewRequest("GET", "/metrics", nil)
	r.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "heos_journal_ready 0") || strings.Contains(w.Body.String(), `player="room"`) {
		t.Fatal(w.Code, w.Body)
	}
}

type metricObservation struct {
	apiObservation
	value heos.Snapshot
	calls *atomic.Int64
}

func (o metricObservation) Snapshot() heos.Snapshot {
	if o.calls == nil {
		panic("scrape observed unauthorized player")
	}
	o.calls.Add(1)
	return o.value
}

func TestMetricsLastFullObservationUsesSnapshotAgeWithoutDeviceReads(t *testing.T) {
	for _, observed := range []bool{false, true} {
		s := testAPI(t)
		devices := s.reads.Devices()
		calls := new(atomic.Int64)
		value := heos.Snapshot{Connected: true, Stale: true, EventUpdated: true}
		if observed {
			value.ObservedAt = time.Unix(1234567890, 0)
		}
		devices[0].Observer = metricObservation{value: value, calls: calls}
		devices[1].Observer = metricObservation{}
		s.reads = control.NewReads("metrics", devices, nil)
		r := httptest.NewRequest("GET", "/metrics", nil)
		r.Header.Set("Authorization", "Bearer "+testToken)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 200 || calls.Load() != 1 {
			t.Fatalf("scrape failed or repeated snapshot: %d, calls=%d, %s", w.Code, calls.Load(), w.Body)
		}
		if strings.Contains(w.Body.String(), "heos_player_last_full_observation_timestamp_seconds") != observed {
			t.Fatal("missing full observation or invented timestamp", w.Body)
		}
		if observed && !strings.Contains(w.Body.String(), `heos_player_last_full_observation_timestamp_seconds{player="room"} 1.23456789e+09`) {
			t.Fatal("partial event or stale flag changed full observation time", w.Body)
		}
	}
}

func TestMetricsExposesOnlyAuthorizedCollectorsAndKeepsCountsAcrossScrapes(t *testing.T) {
	s := testAPI(t)
	devices := s.reads.Devices()
	for i := range devices {
		devices[i].Metrics = telemetry.NewPlayer(devices[i].Config.Key)
		for range i + 1 {
			devices[i].Metrics.WireCommand("player/get_volume", "reply_success", time.Millisecond)
		}
		devices[i].Metrics.Confirmation("volume", "confirmed", 250*time.Millisecond)
		devices[i].Metrics.OperationFinished("playback", "failed", "cannot_play")
		devices[i].Metrics.Active("ramping")
	}
	s.reads = control.NewReads("metrics", devices, nil)
	s.journal = offlineMetricsJournal{}
	for _, player := range []string{"room", "other", "room"} {
		s.credentials[0].Players = []string{player}
		r := httptest.NewRequest("GET", "/metrics", nil)
		r.Header.Set("Authorization", "Bearer "+testToken)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 200 || w.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
			t.Fatal(w.Code, w.Header(), w.Body)
		}
		parser := expfmt.NewTextParser(model.LegacyValidation)
		families, err := parser.TextToMetricFamilies(strings.NewReader(w.Body.String()))
		if err != nil {
			t.Fatal("invalid Prometheus exposition", err)
		}
		for name, f := range families {
			for _, m := range f.GetMetric() {
				for _, label := range m.GetLabel() {
					if label.GetName() == "player" && label.GetValue() != player {
						t.Fatal("unauthorized collector leaked", name, label)
					}
				}
			}
		}
		for _, name := range []string{"heos_journal_ready", "heos_shutting_down", "heos_go_goroutines", "heos_go_heap_objects_bytes",
			"heos_player_connected", "heos_player_stale", "heos_player_writes_enabled"} {
			if families[name] == nil || len(families[name].GetMetric()) != 1 || families[name].GetMetric()[0].Gauge == nil {
				t.Fatal("legacy gauge contract changed", name)
			}
		}
		if families["heos_journal_ready"].GetMetric()[0].GetGauge().GetValue() != 0 {
			t.Fatal("outage incorrectly reported ready")
		}
		want := 1.0
		if player == "other" {
			want = 2
		}
		wire := families["heos_wire_commands_total"].GetMetric()
		if len(wire) != 1 || wire[0].GetCounter().GetValue() != want {
			t.Fatal("scrape reset or misattributed counters", wire)
		}
		confirmation := families["heos_command_confirmation_duration_seconds"].GetMetric()
		if len(confirmation) != 1 || confirmation[0].GetHistogram().GetSampleCount() != 1 || confirmation[0].GetHistogram().GetSampleSum() != .25 {
			t.Fatal("histogram was not correctly encoded", confirmation)
		}
		if ok, errs := s.validator.ValidateHttpResponse(r, w.Result()); !ok {
			t.Fatal(errs)
		}
	}
}

func TestConcurrentMetricsScrapesUseIndependentRegistries(t *testing.T) {
	s := testAPI(t)
	devices := s.reads.Devices()
	p := telemetry.NewPlayer("room")
	devices[0].Metrics = p
	s.reads = control.NewReads("metrics", devices, nil)
	handler := s.Handler()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 4 {
				p.EventGap("event_buffer_overflow")
				r := httptest.NewRequest("GET", "/metrics", nil)
				r.Header.Set("Authorization", "Bearer "+testToken)
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				if w.Code != 200 || !strings.Contains(w.Body.String(), "heos_event_gaps_total") {
					t.Errorf("concurrent scrape failed: %d %s", w.Code, w.Body)
				}
			}
		})
	}
	wg.Wait()
}
