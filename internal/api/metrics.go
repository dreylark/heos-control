package api

import (
	"context"
	"fmt"
	"net/http"
	"runtime/metrics"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

// GetMetrics returns scrape-time snapshots without a sampler goroutine, forced GC or unbounded labels.
func (s *Server) GetMetrics(ctx context.Context, _ GetMetricsRequestObject) (GetMetricsResponseObject, error) {
	registry := prometheus.NewRegistry()
	var snapshot metricSnapshot
	gauge := func(name, help string, value float64, labels prometheus.Labels) {
		desc := prometheus.NewDesc(name, help, nil, labels)
		snapshot = append(snapshot, prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value))
	}
	bit := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}
	check, cancel := context.WithTimeout(ctx, 2*time.Second)
	ready := s.journal.Ready(check) == nil
	cancel()
	gauge("heos_journal_ready", "Whether the operation journal is currently ready.", bit(ready), nil)
	gauge("heos_shutting_down", "Whether the service is shutting down.", bit(s.stopping.Load()), nil)
	samples := []metrics.Sample{{Name: "/sched/goroutines:goroutines"}, {Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(samples)
	gauge("heos_go_goroutines", "Current Go runtime goroutines.", float64(samples[0].Value.Uint64()), nil)
	gauge("heos_go_heap_objects_bytes", "Current Go runtime heap object bytes.", float64(samples[1].Value.Uint64()), nil)
	for _, device := range s.reads.Devices() {
		if !identity(ctx).allows(device.Config.Key) {
			continue
		}
		observed := device.Observer.Snapshot()
		labels := prometheus.Labels{"player": device.Config.Key}
		for _, g := range []struct {
			name  string
			help  string
			value bool
		}{{"connected", "Whether the player's HEOS connection is established.", observed.Connected},
			{"stale", "Whether the player's observed state is stale.", observed.Stale},
			{"writes_enabled", "Whether writes are configured for the player.", device.Config.WritesEnabled}} {
			gauge("heos_player_"+g.name, g.help, bit(g.value), labels)
		}
		if !observed.ObservedAt.IsZero() {
			gauge("heos_player_last_full_observation_timestamp_seconds", "Unix time of the last full observation; event projections do not advance it.",
				float64(observed.ObservedAt.UnixNano())/float64(time.Second), labels)
		}
		if device.Metrics != nil {
			if err := registry.Register(device.Metrics); err != nil {
				return nil, fmt.Errorf("register player metrics: %w", err)
			}
		}
	}
	if err := registry.Register(snapshot); err != nil {
		return nil, fmt.Errorf("register snapshot metrics: %w", err)
	}
	families, err := registry.Gather()
	if err != nil {
		return nil, fmt.Errorf("collect metrics: %w", err)
	}
	var out strings.Builder
	encoder := expfmt.NewEncoder(&out, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		if err := encoder.Encode(family); err != nil {
			return nil, fmt.Errorf("encode metrics: %w", err)
		}
	}
	return metricsResponse(out.String()), nil
}

// The endpoint captures these constants once, after authorizing each player.
// Collection and encoding never call back into the device or journal.
type metricSnapshot []prometheus.Metric

func (m metricSnapshot) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range m {
		ch <- metric.Desc()
	}
}

func (m metricSnapshot) Collect(ch chan<- prometheus.Metric) {
	for _, metric := range m {
		ch <- metric
	}
}

type metricsResponse string

func (response metricsResponse) VisitGetMetricsResponse(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", string(expfmt.NewFormat(expfmt.TypeTextPlain)))
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte(response))
	return err
}
