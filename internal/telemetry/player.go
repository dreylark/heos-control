// Package telemetry records bounded controller measurements without device I/O
// or a background sampler. Each configured player owns an independent collector.
package telemetry

import (
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Player is registered only in scrapes authorized to see its configured key.
// Methods are nil-safe so optional instrumentation cannot affect device policy.
type Player struct {
	wireCommands  *prometheus.CounterVec
	wireDuration  *prometheus.HistogramVec
	confirmations *prometheus.HistogramVec
	fallbacks     *prometheus.CounterVec
	observations  *prometheus.CounterVec
	reconnects    prometheus.Counter
	gaps          *prometheus.CounterVec
	finished      *prometheus.CounterVec
	collectors    []prometheus.Collector
	activeDesc    *prometheus.Desc
	mu            sync.Mutex
	activePhase   string
}

// NewPlayer does not register in the process-global Prometheus registry. The key
// comes from bounded service configuration, never from request or HEOS payloads.
func NewPlayer(key string) *Player {
	labels := prometheus.Labels{"player": key}
	counter := func(name, help string, variables ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: "heos_" + name, Help: help, ConstLabels: labels}, variables)
	}
	histogram := func(name, help string, variables ...string) *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "heos_" + name, Help: help, ConstLabels: labels,
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 12, 20, 30, 60}}, variables)
	}
	p := &Player{
		wireCommands:  counter("wire_commands_total", "HEOS commands with bytes sent, by final wire result.", "command", "result"),
		wireDuration:  histogram("wire_command_duration_seconds", "Elapsed time of HEOS commands with bytes sent.", "command"),
		confirmations: histogram("command_confirmation_duration_seconds", "Elapsed time waiting for a delivered command's state confirmation.", "kind", "result"),
		fallbacks:     counter("confirmation_fallbacks_total", "Targeted fallback reads attempted after missing scalar confirmation events.", "kind"),
		observations:  counter("observation_refreshes_total", "Observation read sequences actually started, by scope and trigger.", "scope", "trigger"),
		reconnects:    prometheus.NewCounter(prometheus.CounterOpts{Name: "heos_reconnects_total", Help: "Connections established after the initial successful connection.", ConstLabels: labels}),
		gaps:          counter("event_gaps_total", "Observed breaks in the event stream.", "reason"),
		finished:      counter("operations_finished_total", "Operations finalized by this controller process, by kind, terminal state and bounded reason.", "kind", "state", "reason"),
		activeDesc:    prometheus.NewDesc("heos_active_operations", "Current locally owned operation phase; at most one phase is active per player.", []string{"phase"}, labels),
	}
	p.collectors = []prometheus.Collector{p.wireCommands, p.wireDuration, p.confirmations, p.fallbacks, p.observations, p.reconnects, p.gaps, p.finished}
	return p
}

// Describe implements prometheus.Collector without collecting device state.
func (p *Player) Describe(ch chan<- *prometheus.Desc) {
	if p == nil {
		return
	}
	for _, c := range p.collectors {
		c.Describe(ch)
	}
	ch <- p.activeDesc
}

// Collect snapshots the phase once so concurrent transitions never produce two
// active phases in one scrape. Counters and histograms provide their own locks.
func (p *Player) Collect(ch chan<- prometheus.Metric) {
	if p == nil {
		return
	}
	for _, c := range p.collectors {
		c.Collect(ch)
	}
	p.mu.Lock()
	active := p.activePhase
	p.mu.Unlock()
	for _, phase := range phases {
		value := 0.0
		if phase == active {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(p.activeDesc, prometheus.GaugeValue, value, phase)
	}
}

func (p *Player) WireCommand(command, result string, elapsed time.Duration) {
	if p != nil {
		command = bounded(command, commands)
		p.wireCommands.WithLabelValues(command, bounded(result, wireResults)).Inc()
		p.wireDuration.WithLabelValues(command).Observe(max(0, elapsed.Seconds()))
	}
}

func (p *Player) Reconnected() {
	if p != nil {
		p.reconnects.Inc()
	}
}

func (p *Player) EventGap(reason string) {
	if p != nil {
		p.gaps.WithLabelValues(bounded(reason, gapReasons)).Inc()
	}
}

func (p *Player) Observation(scope, trigger string) {
	if p != nil {
		p.observations.WithLabelValues(bounded(scope, scopes), bounded(trigger, triggers)).Inc()
	}
}

func (p *Player) Confirmation(kind, result string, elapsed time.Duration) {
	if p != nil {
		p.confirmations.WithLabelValues(bounded(kind, mutationKinds), bounded(result, confirmationResults)).Observe(max(0, elapsed.Seconds()))
	}
}

func (p *Player) Fallback(kind string) {
	if p != nil {
		p.fallbacks.WithLabelValues(bounded(kind, scalarKinds)).Inc()
	}
}

func (p *Player) OperationFinished(kind, state, reason string) {
	if p != nil {
		p.finished.WithLabelValues(bounded(kind, operationKinds), bounded(state, terminalStates), bounded(reason, operationReasons)).Inc()
	}
}

func (p *Player) Active(phase string) {
	if p == nil {
		return
	}
	if phase != "" {
		phase = bounded(phase, phases)
	}
	p.mu.Lock()
	p.activePhase = phase
	p.mu.Unlock()
}

func bounded(value string, allowed []string) string {
	if slices.Contains(allowed, value) {
		return value
	}
	return "unknown"
}

var (
	commands = []string{
		"system/heart_beat", "system/register_for_change_events",
		"player/get_players", "player/get_player_info", "player/get_play_state", "player/get_volume", "player/get_mute",
		"player/get_play_mode", "player/get_now_playing_media", "player/get_queue", "group/get_groups", "group/get_group_info",
		"browse/get_music_sources", "browse/get_source_info", "browse/browse", "browse/add_to_queue",
		"player/set_volume", "player/set_mute", "player/set_play_state", "player/play_next", "player/play_previous", "player/set_play_mode", "player/remove_from_queue",
	}
	wireResults         = []string{"reply_success", "reply_rejected", "uncertain"}
	confirmationResults = []string{"confirmed", "timeout", "released", "rejected", "uncertain", "cancelled"}
	gapReasons          = []string{"connection_closed", "event_buffer_overflow"}
	scopes              = []string{"full", "media", "scalars", "playback"}
	triggers            = []string{"startup", "event", "audit", "recovery", "admission", "preflight", "prewrite", "confirmation", "fallback", "manual"}
	mutationKinds       = []string{"volume", "mute", "mode", "transport", "skip", "queue", "remove"}
	scalarKinds         = []string{"volume", "mute", "mode"}
	operationKinds      = []string{"volume", "mute", "transport", "skip", "playback", "stop", "cancel"}
	terminalStates      = []string{"succeeded", "failed", "cancelled", "released", "interrupted", "uncertain"}
	phases              = []string{"accepted", "preparing", "sending_volume", "sending_mute", "sending_mode", "sending_queue", "sending_remove", "queue_loading", "sending_transport", "sending_skip", "ramping", "playing", "fading", "stopping", "cancelling", "persisting", "unknown"}
	operationReasons    = []string{
		"completed", "command_timeout", "confirmation_timeout", "device_unavailable", "device_rejected", "not_skippable", "journal_unavailable", "queue_loading_incomplete", "session_expired", "buffer_capacity",
		"service_stopping", "ownership_lost", "ownership_revoked", "unexpected_event", "event_gap", "queue_transition_timeout", "grouped_target",
		"before_write_changed", "readback_changed", "active_state_changed", "pending_readback_changed", "queue_transition_changed",
		"queue_media_not_selected", "queue_contains_unselected_media", "queue_incomplete_or_invalid", "queue_media_not_found",
		"superseded", "cancelled", "cancellation_failed", "handoff_failed", "process_interrupted",
		"unrecognized_command", "invalid_id", "invalid_arguments", "data_unavailable", "resource_unavailable", "invalid_credentials",
		"command_not_executed", "user_not_logged_in", "parameter_out_of_range", "user_not_found", "internal_error", "system_error",
		"device_busy", "cannot_play", "option_not_supported", "device_queue_full", "skip_limit_reached",
	}
)
