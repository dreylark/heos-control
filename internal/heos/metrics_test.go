package heos

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordedWire struct {
	command, result string
	elapsed         time.Duration
}

type recordedObservation struct{ scope, trigger string }

type recordingMetrics struct {
	mu           sync.Mutex
	wires        []recordedWire
	observations []recordedObservation
	gaps         []string
	reconnects   int
}

func (m *recordingMetrics) WireCommand(command, result string, elapsed time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wires = append(m.wires, recordedWire{command, result, elapsed})
}
func (m *recordingMetrics) Reconnected() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnects++
}
func (m *recordingMetrics) EventGap(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gaps = append(m.gaps, reason)
}
func (m *recordingMetrics) Observation(scope, trigger string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observations = append(m.observations, recordedObservation{scope, trigger})
}
func (m *recordingMetrics) snapshot() ([]recordedWire, []recordedObservation, []string, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.wires), slices.Clone(m.observations), slices.Clone(m.gaps), m.reconnects
}

type metricConn struct {
	net.Conn
	write func([]byte) (int, error)
}

func (c metricConn) Write(p []byte) (int, error)    { return c.write(p) }
func (metricConn) SetWriteDeadline(time.Time) error { return nil }
func (metricConn) Close() error                     { return nil }

// These cases exercise the exchange's final classification, including a context
// cancelled during a write and a partial write that cannot be safely replayed.
func TestWireMetricsCountOnlyTransmittedExchanges(t *testing.T) {
	for _, tc := range []struct {
		name, result string
	}{
		{"success", "reply_success"}, {"pending-success", "reply_success"},
		{"rejection", "reply_rejected"}, {"partial", "uncertain"},
		{"cancel-during-write", "uncertain"}, {"protocol", "uncertain"},
		{"zero-write", ""}, {"cancel-before-write", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics := &recordingMetrics{}
			client := &Client{cfg: Config{Metrics: metrics}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			response := Response{Command: "system/heart_beat", Result: "success"}
			if tc.name == "rejection" {
				response.Result, response.Params = "fail", url.Values{"eid": {"13"}}
			}
			if tc.name == "protocol" {
				response.Command = "player/get_players"
			}
			frames := make(chan incoming, 2)
			if tc.name == "pending-success" {
				frames <- incoming{response: Response{Command: response.Command, Result: "success", Pending: true}}
			}
			frames <- incoming{response: response}
			writes := 0
			conn := metricConn{write: func(p []byte) (int, error) {
				writes++
				switch tc.name {
				case "zero-write":
					return 0, io.ErrClosedPipe
				case "partial":
					if writes == 1 {
						return 3, nil
					}
					return 0, io.ErrClosedPipe
				case "cancel-during-write":
					cancel()
				}
				return len(p), nil
			}}
			if tc.name == "cancel-before-write" {
				cancel()
			}
			_, err := client.exchange(ctx, &generation{conn: conn, frames: frames, stop: make(chan struct{})}, "system/heart_beat", nil)
			wire, _, _, _ := metrics.snapshot()
			if tc.result == "" {
				if len(wire) != 0 || err == nil {
					t.Fatalf("unsent command counted: records=%+v err=%v", wire, err)
				}
				return
			}
			if len(wire) != 1 || wire[0].command != "system/heart_beat" || wire[0].result != tc.result || wire[0].elapsed < 0 {
				t.Fatalf("wrong wire record: %+v", wire)
			}
			if tc.name == "cancel-during-write" && !errors.Is(err, context.Canceled) {
				t.Fatal("metrics ran before final cancellation classification", err)
			}
		})
	}
}

func metricClient(t *testing.T, server *fakeHEOS, metrics *recordingMetrics) *Client {
	t.Helper()
	c, err := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin,
		CommandTimeout: time.Second, ReconnectDelay: time.Nanosecond, EventBuffer: 8, Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestReconnectMetricsRequirePriorUsableSubscription(t *testing.T) {
	var registrations atomic.Int32
	server := newFakeHEOSWithRegistration(t, func(c net.Conn, u *url.URL, _ int64) {
		sendReply(c, u, nil, nil)
	}, func(c net.Conn, u *url.URL) {
		if n := registrations.Add(1); n == 1 || n == 3 {
			sendFailure(c, u, "eid=7&text=Cannot-subscribe")
			return
		}
		sendReply(c, u, nil, nil)
	})
	metrics := &recordingMetrics{}
	client := metricClient(t, server, metrics)
	if _, err := client.Read(context.Background(), "system/heart_beat", nil); err == nil {
		t.Fatal("fixture registration succeeded")
	}
	wire, _, _, reconnects := metrics.snapshot()
	if len(wire) != 1 || wire[0].command != "system/register_for_change_events" || wire[0].result != "reply_rejected" || reconnects != 0 {
		t.Fatalf("failed bootstrap counted user command or reconnect: %+v %d", wire, reconnects)
	}
	if _, err := client.Read(context.Background(), "system/heart_beat", nil); err != nil {
		t.Fatal(err)
	}
	_, _, _, reconnects = metrics.snapshot()
	if reconnects != 0 {
		t.Fatal("first usable connection counted as reconnect")
	}
	client.Interrupt()
	eventually(t, func() bool {
		_, err := client.Read(context.Background(), "system/heart_beat", nil)
		return registrations.Load() == 3 && err != nil
	})
	_, _, _, reconnects = metrics.snapshot()
	if reconnects != 0 {
		t.Fatal("failed resubscription counted as reconnect", reconnects)
	}
	eventually(t, func() bool {
		_, err := client.Read(context.Background(), "system/heart_beat", nil)
		return err == nil
	})
	_, _, _, reconnects = metrics.snapshot()
	if reconnects != 1 {
		t.Fatal("usable reconnect not counted exactly once", reconnects)
	}
}

func TestGapMetricsCountAtCreationAndIgnoreProgressDrops(t *testing.T) {
	metrics := &recordingMetrics{}
	client := &Client{cfg: Config{Metrics: metrics}, events: make(chan Event, 1)}
	callbacks := 0
	client.SetEventHandler(func(e Event) {
		if e.Gap {
			callbacks++
			_, _, gaps, _ := metrics.snapshot()
			if len(gaps) != callbacks {
				t.Fatal("gap recorded after callback or more than once", gaps, callbacks)
			}
		}
	})
	client.publish(Event{})
	client.event(Response{Command: "event/player_now_playing_progress", Params: url.Values{"pid": {"1"}, "cur_pos": {"0"}, "duration": {"1"}}})
	_, _, gaps, _ := metrics.snapshot()
	if len(gaps) != 0 {
		t.Fatal("dropped progress counted as a gap", gaps)
	}
	client.publish(Event{})
	if event := <-client.Events(); !event.Gap {
		t.Fatal("missing overflow gap")
	}
	done := make(chan struct{})
	close(done)
	client.drop(&generation{conn: metricConn{}, stop: make(chan struct{}), done: done})
	if event := <-client.Events(); !event.Gap {
		t.Fatal("missing connection gap")
	}
	_, _, gaps, _ = metrics.snapshot()
	if !slices.Equal(gaps, []string{"event_buffer_overflow", "connection_closed"}) {
		t.Fatal("wrong gap counts", gaps)
	}
}

func TestObservationMetricsCountReadsNotCacheOrRejectedBaselines(t *testing.T) {
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) { observationReply(c, u, "serial-A") })
	metrics := &recordingMetrics{}
	client := metricClient(t, server, metrics)
	observer, err := NewObserver(client, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	observer.now = func() time.Time { return now }
	if err := observer.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	initial := server.commands.Load()
	if err := observer.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = observer.Snapshot()
	if server.commands.Load() != initial {
		t.Fatal("metrics or cache reuse added wire commands")
	}
	if err := observer.RefreshScalars(WithObservationTrigger(context.Background(), "fallback"), MutationKindVolume); err != nil {
		t.Fatal(err)
	}
	if err := observer.RefreshPlayback(WithObservationTrigger(context.Background(), "confirmation")); err != nil {
		t.Fatal(err)
	}
	client.event(Response{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {string(observer.Snapshot().Player.ID)}}})
	observer.Notify(<-client.Events())
	if err := observer.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(IdleObservationInterval)
	if err := observer.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(IdleObservationInterval)
	if err := observer.Reconcile(WithObservationTrigger(context.Background(), "admission")); err != nil {
		t.Fatal(err)
	}
	if err := observer.Refresh(WithObservationTrigger(context.Background(), "preflight")); err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	observer.invalid = true
	observer.mu.Unlock()
	before := server.commands.Load()
	if err := observer.RefreshScalars(WithObservationTrigger(context.Background(), "fallback"), MutationKindVolume); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	if err := observer.RefreshPlayback(WithObservationTrigger(context.Background(), "confirmation")); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	wire, observations, _, _ := metrics.snapshot()
	want := []recordedObservation{{"full", "manual"}, {"scalars", "fallback"}, {"playback", "confirmation"}, {"media", "event"}, {"full", "audit"}, {"full", "admission"}, {"full", "preflight"}}
	if !slices.Equal(observations, want) || server.commands.Load() != before || len(wire) != int(before) {
		t.Fatalf("wrong observation/wire accounting: observations=%+v wire=%d commands=%d before=%d", observations, len(wire), server.commands.Load(), before)
	}
}

func TestObservationMetricsIncludeFailedReadAttempts(t *testing.T) {
	for _, tc := range []struct{ scope, trigger, command string }{
		{"full", "preflight", "player/get_players"},
		{"media", "confirmation", "player/get_now_playing_media"},
		{"scalars", "fallback", "player/get_volume"},
		{"playback", "confirmation", "player/get_play_state"},
	} {
		t.Run(tc.scope, func(t *testing.T) {
			var failure atomic.Value
			failure.Store("")
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				if commandName(u) == failure.Load().(string) {
					sendFailure(c, u, "eid=13&text=Busy")
					return
				}
				observationReply(c, u, "serial-A")
			})
			metrics := &recordingMetrics{}
			client := metricClient(t, server, metrics)
			observer, err := NewObserver(client, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			if err != nil {
				t.Fatal(err)
			}
			if err := observer.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			before := server.commands.Load()
			failure.Store(tc.command)
			ctx := WithObservationTrigger(context.Background(), tc.trigger)
			switch tc.scope {
			case "full":
				err = observer.Refresh(ctx)
			case "media":
				client.event(Response{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {string(observer.Snapshot().Player.ID)}}})
				observer.Notify(<-client.Events())
				err = observer.Reconcile(ctx)
			case "scalars":
				err = observer.RefreshScalars(ctx, MutationKindVolume)
			case "playback":
				err = observer.RefreshPlayback(ctx)
			}
			if !errors.Is(err, ErrRejected) {
				t.Fatal("fixture did not reject observation", err)
			}
			wire, observations, _, _ := metrics.snapshot()
			if len(observations) != 2 || observations[1] != (recordedObservation{tc.scope, tc.trigger}) ||
				server.commands.Load() != before+1 || len(wire) != int(before+1) || wire[len(wire)-1].result != "reply_rejected" {
				t.Fatalf("failed observation was omitted or retried: observations=%+v wire=%+v", observations, wire)
			}
		})
	}
}

func TestObservationRunMetricsDistinguishTriggers(t *testing.T) {
	var failed atomic.Bool
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/get_players" && failed.CompareAndSwap(false, true) {
			sendFailure(c, u, "eid=13&text=Busy")
			return
		}
		if commandName(u) == "system/heart_beat" {
			sendReply(c, u, nil, nil)
			return
		}
		observationReply(c, u, "serial-A")
	})
	metrics := &recordingMetrics{}
	client := metricClient(t, server, metrics)
	observer, err := NewObserver(client, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	if err != nil {
		t.Fatal(err)
	}
	clock := &observationTestClock{now: time.Now()}
	observer.now, observer.newTimer = clock.Now, clock.NewTimer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- observer.Run(ctx, IdleObservationInterval, nil) }()
	eventually(t, func() bool { return clock.Scheduled(time.Second) })
	clock.Advance(time.Second)
	eventually(t, func() bool { return observer.Snapshot().Verified && clock.Scheduled(IdleObservationInterval) })
	client.event(Response{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {string(observer.Snapshot().Player.ID)}}})
	observer.Notify(<-client.Events())
	eventually(t, func() bool { return clock.Scheduled(observationDebounce) })
	clock.Advance(observationDebounce)
	eventually(t, func() bool {
		return observer.Snapshot().Verified && clock.Scheduled(IdleObservationInterval-observationDebounce)
	})
	clock.Advance(IdleObservationInterval - observationDebounce)
	eventually(t, func() bool { return clock.Scheduled(IdleObservationInterval) })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	_, observations, _, _ := metrics.snapshot()
	want := []recordedObservation{{"full", "startup"}, {"full", "recovery"}, {"media", "event"}, {"full", "audit"}}
	if !slices.Equal(observations, want) {
		t.Fatalf("observer trigger classification: got %+v, want %+v", observations, want)
	}
}
