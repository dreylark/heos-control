package control

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
	"github.com/dreylark/heos-control/internal/telemetry"
)

func TestReplayTLS(t *testing.T) {
	selected := 0
	for _, f := range loadReplayFixtures(t) {
		if !slices.Contains([]string{"delayed-mode", "duplicate-volume", "loading-unknown"}, f.Name) {
			continue
		}
		selected++
		t.Run(f.Name, func(t *testing.T) {
			result, err := runTLSReplay(t, f)
			if err != nil {
				t.Fatal(err)
			}
			var outcome struct{ Diagnostics struct{ Reason string } }
			if err := json.Unmarshal(result.operation.Outcome, &outcome); err != nil {
				t.Fatal(err)
			}
			if string(result.operation.State) != f.Expect.State || outcome.Diagnostics.Reason != f.Expect.Reason {
				t.Fatalf("operation: %+v; commands: %v", result.operation, result.commands)
			}
			var gets, writes, queues int
			for _, name := range result.commands {
				if name == "browse/add_to_queue" {
					queues++
				}
				if replayWireMutation(name) {
					writes++
				} else {
					gets++
				}
			}
			if gets > f.Expect.MaxWireGets || writes > f.Expect.MaxWireWrites || queues != f.Expect.QueueWrites || !slices.Equal(result.levels, f.Expect.VolumeLevels) {
				t.Fatalf("wire budget: GET/read=%d writes=%d levels=%v; commands=%v", gets, writes, result.levels, result.commands)
			}
			// This count includes startup, catalog and background observation, not
			// merely calls made directly by the operation under test.
			var measured int
			for _, sample := range metricSamples(t, result.metrics, "heos_wire_commands_total") {
				measured += int(sample.GetCounter().GetValue())
			}
			if measured != len(result.commands) {
				t.Fatalf("wire metrics=%d, actual commands=%d", measured, len(result.commands))
			}
			var full, scalar int
			for _, sample := range metricSamples(t, result.metrics, "heos_observation_refreshes_total") {
				switch metricLabel(sample, "scope") {
				case "full":
					// The shared full-read budget starts from an established
					// baseline. The separate wire budget includes TLS startup.
					if metricLabel(sample, "trigger") != "startup" {
						full += int(sample.GetCounter().GetValue())
					}
				case "scalars":
					scalar += int(sample.GetCounter().GetValue())
				}
			}
			if full > f.Expect.MaxFullReads || scalar != f.Expect.MaxScalarReads {
				t.Fatalf("observation budgets: full=%d scalar=%d", full, scalar)
			}
			t.Logf("wire reads/control=%d writes=%d queue=%d; full observations after startup=%d scalars=%d", gets, writes, queues, full, scalar)
			if f.Name == "loading-unknown" {
				assertQueueLoadingWireBudget(t, result.commands)
			} else {
				assertPlaybackWireRequestBudget(t, result.commands)
			}
		})
	}
	if selected != 3 {
		t.Fatalf("required TLS corpus: got %d fixtures, want 3", selected)
	}
}

// An invalid command expectation must abort the outstanding real socket read,
// release the coordinator and join the listener/consumer. A test failure must
// not strand a goroutine until the package-wide go test timeout.
func TestReplayTLSMismatchWhileAwaitingReply(t *testing.T) {
	f := loadReplayFixtures(t)[0]
	f.Scripts = []replayScript{{On: "player/set_volume", Occurrence: 1, Args: map[string]string{"level": "999"}, Steps: []replayStep{{Action: "reply", Result: "success"}}}}
	started := time.Now()
	_, err := runTLSReplay(t, f)
	if err == nil || !strings.Contains(err.Error(), "expected level=999") {
		t.Fatalf("mismatched pending command: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("fixture mismatch waited for the transport timeout")
	}
}

type tlsReplayResult struct {
	operation journal.Operation
	commands  []string
	levels    []int
	metrics   *telemetry.Player
}

// Only the coordinator's operation clock is virtual. Socket deadlines,
// Observer.Run's coalescing/audit timers and the outer watchdog remain real.
type tlsReplayClock struct {
	mu       sync.Mutex
	now      time.Time
	events   []modeClockEvent
	delivery *wireEventDelivery
}

func (c *tlsReplayClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = maxTime(c.now, time.Now())
	return c.now
}

func (c *tlsReplayClock) schedule(at time.Time, fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, modeClockEvent{at: at, fn: fn})
	slices.SortStableFunc(c.events, func(a, b modeClockEvent) int { return a.at.Compare(b.at) })
}

func (c *tlsReplayClock) advance(ctx context.Context, at time.Time) error {
	if err := c.delivery.wait(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	c.now = maxTime(c.now, at)
	c.mu.Unlock()
	return nil
}

func (c *tlsReplayClock) Wait(ctx context.Context, d time.Duration, wake <-chan struct{}) error {
	end := c.Now().Add(d)
	for {
		if err := c.delivery.wait(ctx); err != nil {
			return err
		}
		select {
		case <-wake:
			return nil
		default:
		}
		c.mu.Lock()
		if len(c.events) == 0 || c.events[0].at.After(end) {
			c.now = maxTime(c.now, end)
			c.mu.Unlock()
			return context.Cause(ctx)
		}
		at := c.events[0].at
		n := 0
		for n < len(c.events) && c.events[n].at.Equal(at) {
			n++
		}
		batch := slices.Clone(c.events[:n])
		c.events = c.events[n:]
		c.now = maxTime(c.now, at)
		c.mu.Unlock()
		for _, e := range batch {
			e.fn()
		}
	}
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

type tlsReplayDevice struct {
	mu         sync.Mutex
	writeMu    sync.Mutex
	state      heos.Snapshot
	fixture    replayFixture
	clock      *tlsReplayClock
	ctx        context.Context
	cancel     context.CancelCauseFunc
	commands   []string
	levels     []int
	occurrence map[string]int
	conns      []net.Conn
	wg         sync.WaitGroup
}

func replayWireMutation(name string) bool {
	return strings.HasPrefix(name, "player/set_") || name == "browse/add_to_queue"
}

func (d *tlsReplayDevice) frame(conn net.Conn, name, result string, params url.Values, payload any) {
	b, err := json.Marshal(map[string]any{"heos": map[string]any{"command": name, "result": result, "message": params.Encode()}, "payload": payload})
	if err == nil {
		d.writeMu.Lock()
		_, err = conn.Write(append(b, '\r', '\n'))
		d.writeMu.Unlock()
	}
	if err != nil {
		d.cancel(fmt.Errorf("replay frame: %w", err))
	}
}

func (d *tlsReplayDevice) event(conn net.Conn, e replayEvent) {
	if e.Gap {
		d.cancel(fmt.Errorf("synthetic gaps require disconnect, not an invented wire event"))
		return
	}
	p := url.Values{}
	for k, v := range e.Params {
		p.Set(k, v)
	}
	d.frame(conn, e.Command, "", p, nil)
}

func (d *tlsReplayDevice) batch(conn net.Conn, name string, params url.Values, steps []replayStep) {
	count := 0
	for _, step := range steps {
		if step.Action == "event" {
			count++
		}
	}
	// Register the complete same-time batch before its first frame. In
	// particular a reply preceding an event cannot let logical time overtake it.
	d.clock.delivery.sending(count)
	for _, step := range steps {
		d.step(conn, name, params, step)
	}
}

func (d *tlsReplayDevice) step(conn net.Conn, name string, params url.Values, step replayStep) {
	switch step.Action {
	case "apply":
		d.mu.Lock()
		applyReplayPatch(&d.state, *step.Patch)
		d.mu.Unlock()
	case "event":
		d.event(conn, *step.Event)
	case "reply":
		result := "success"
		if step.Result == "rejected" {
			result = "fail"
			params.Set("eid", "14")
		}
		d.frame(conn, name, result, params, nil)
	case "disconnect":
		_ = conn.Close()
	}
}

func (d *tlsReplayDevice) command(conn net.Conn, name string, params url.Values) {
	d.mu.Lock()
	d.commands = append(d.commands, name)
	d.occurrence[name]++
	occurrence := d.occurrence[name]
	if name == "player/set_volume" {
		level, _ := strconv.Atoi(params.Get("level"))
		d.levels = append(d.levels, level)
	}
	d.mu.Unlock()
	for _, script := range d.fixture.Scripts {
		if script.On != name || script.Occurrence != occurrence {
			continue
		}
		for k, v := range script.Args {
			if params.Get(k) != v {
				d.cancel(fmt.Errorf("%s occurrence %d: expected %s=%s; got %s", name, occurrence, k, v, params.Get(k)))
				_ = conn.Close()
				return
			}
		}
		at, terminalMS := d.clock.Now(), 0
		for _, step := range script.Steps {
			if step.Action == "reply" || step.Action == "disconnect" {
				terminalMS = step.AtMS
				break
			}
		}
		// Register future state changes before exposing the reply: the waiting
		// coordinator may immediately advance logical time once it receives it.
		var immediate [][]replayStep
		for i := 0; i < len(script.Steps); {
			j := i + 1
			for j < len(script.Steps) && script.Steps[j].AtMS == script.Steps[i].AtMS {
				j++
			}
			batch := script.Steps[i:j]
			when := at.Add(time.Duration(batch[0].AtMS) * time.Millisecond)
			if batch[0].AtMS > terminalMS {
				d.clock.schedule(when, func() { d.batch(conn, name, params, batch) })
			} else {
				immediate = append(immediate, batch)
			}
			i = j
		}
		for _, batch := range immediate {
			if batch[0].AtMS > 0 {
				when := at.Add(time.Duration(batch[0].AtMS) * time.Millisecond)
				if err := d.clock.advance(d.ctx, when); err != nil {
					return
				}
			}
			d.batch(conn, name, params, batch)
		}
		return
	}
	d.mu.Lock()
	payload, events, err := d.baseline(name, params)
	d.mu.Unlock()
	if err != nil {
		d.cancel(err)
		_ = conn.Close()
		return
	}
	d.clock.delivery.sending(len(events))
	for _, event := range events {
		d.event(conn, event)
	}
	d.frame(conn, name, "success", params, payload)
}

// The uninteresting baseline implements documented Denon commands. Reads only
// serialize current state: neither a GET nor its count advances the scenario.
func (d *tlsReplayDevice) baseline(name string, params url.Values) (any, []replayEvent, error) {
	s := &d.state
	event := func(name string, fields map[string]string) replayEvent {
		fields["pid"] = "1"
		return replayEvent{Command: "event/" + name, Params: fields}
	}
	volume := func() replayEvent {
		return event("player_volume_changed", map[string]string{"level": strconv.Itoa(*s.Volume), "mute": replayOnOff(*s.Muted)})
	}
	var events []replayEvent
	switch name {
	case "system/register_for_change_events", "system/heart_beat":
	case "player/get_players":
		return []map[string]any{{"pid": 1, "serial": "synthetic", "name": "Room", "model": "test"}}, nil, nil
	case "group/get_groups":
		return []any{}, nil, nil
	case "player/get_play_state":
		params.Set("state", s.State)
	case "player/get_volume":
		params.Set("level", strconv.Itoa(*s.Volume))
	case "player/get_mute":
		params.Set("state", replayOnOff(*s.Muted))
	case "player/get_play_mode":
		params.Set("repeat", s.Repeat)
		params.Set("shuffle", replayOnOff(s.Shuffle))
	case "player/get_now_playing_media":
		return s.Media, nil, nil
	case "player/get_queue":
		params.Set("count", strconv.Itoa(len(s.Queue.Items)))
		params.Set("returned", strconv.Itoa(len(s.Queue.Items)))
		return s.Queue.Items, nil, nil
	case "browse/browse":
		var items []map[string]any
		switch {
		case params.Get("sid") == "1024":
			items = []map[string]any{{"sid": 900, "name": "Synthetic server", "type": "heos_server"}}
		case params.Get("cid") == "":
			items = []map[string]any{{"cid": "green", "name": "Synthetic album", "container": "yes", "playable": "yes"}}
		default:
			for _, track := range replaySelectedQueue() {
				items = append(items, map[string]any{"mid": track.ID, "name": "Synthetic song", "type": "song", "container": "no", "playable": "yes"})
			}
		}
		params.Set("count", strconv.Itoa(len(items)))
		params.Set("returned", strconv.Itoa(len(items)))
		return items, nil, nil
	case "player/set_volume":
		level, err := strconv.Atoi(params.Get("level"))
		if err != nil {
			return nil, nil, err
		}
		s.Volume = &level
		events = append(events, volume())
	case "player/set_mute":
		muted := params.Get("state") == "on"
		s.Muted = &muted
		events = append(events, volume())
	case "player/set_play_mode":
		s.Repeat, s.Shuffle = params.Get("repeat"), params.Get("shuffle") == "on"
		events = append(events, event("repeat_mode_changed", map[string]string{"repeat": s.Repeat}), event("shuffle_mode_changed", map[string]string{"shuffle": replayOnOff(s.Shuffle)}))
	case "player/set_play_state":
		s.State = params.Get("state")
		events = append(events, event("player_state_changed", map[string]string{"state": s.State}))
	case "browse/add_to_queue":
		if params.Get("sid") != "900" || params.Get("cid") != "green" || params.Get("aid") != "4" {
			return nil, nil, fmt.Errorf("unexpected queue request %s", params.Encode())
		}
		applyReplayPatch(s, replayPatch{Queue: "selected"})
		media := s.Queue.Items[0]
		s.Media, s.State = &media, "play"
		events = append(events, event("player_queue_changed", map[string]string{}), event("player_now_playing_changed", map[string]string{}), event("player_state_changed", map[string]string{"state": "play"}))
	default:
		return nil, nil, fmt.Errorf("unsupported replay request %s", name)
	}
	return nil, events, nil
}

func replayOnOff(on bool) string {
	if on {
		return "on"
	}
	return "off"
}

func runTLSReplay(t *testing.T, f replayFixture) (tlsReplayResult, error) {
	t.Helper()
	var result tlsReplayResult
	ctx, cancel := context.WithCancelCause(context.Background())
	watchdog := time.AfterFunc(5*time.Second, func() { cancel(fmt.Errorf("TLS replay watchdog")) })
	defer watchdog.Stop()
	defer cancel(nil)
	clock := &tlsReplayClock{now: time.Now(), delivery: newWireEventDelivery()}
	device := &tlsReplayDevice{fixture: f, clock: clock, ctx: ctx, cancel: cancel, occurrence: map[string]int{}, state: heos.Snapshot{State: f.Initial.State, Volume: &f.Initial.Volume, Muted: &f.Initial.Muted, Repeat: f.Initial.Repeat, Shuffle: f.Initial.Shuffle}}
	applyReplayPatch(&device.state, replayPatch{Queue: "old", Media: &replayMedia{MID: "previous-track", QID: "1", Source: "1024"}})
	seed := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := seed.TLS.Certificates[0]
	seed.Close()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}})
	if err != nil {
		return result, err
	}
	defer func() {
		cancel(nil)
		_ = listener.Close()
		device.mu.Lock()
		for _, conn := range device.conns {
			_ = conn.Close()
		}
		device.mu.Unlock()
		device.wg.Wait()
	}()
	device.wg.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			device.mu.Lock()
			device.conns = append(device.conns, conn)
			device.mu.Unlock()
			device.wg.Go(func() {
				defer func() { _ = conn.Close() }()
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					u, err := url.Parse(strings.TrimSpace(line))
					if err != nil {
						cancel(err)
						return
					}
					device.command(conn, u.Host+u.Path, u.Query())
				}
			})
		}
	})
	metrics := telemetry.NewPlayer("room")
	client, err := heos.New(ctx, heos.Config{Address: listener.Addr().String(), Fingerprint: fmt.Sprintf("%x", sha256.Sum256(certificate.Certificate[0])), EnableWrites: true, Metrics: metrics})
	if err != nil {
		return result, err
	}
	defer client.Close()
	observer, err := heos.NewObserver(client, heos.Identity{Key: "room", Serial: "synthetic"}, heos.ObservationCacheTTL)
	if err != nil {
		return result, err
	}
	observationCtx, stopObservation := context.WithCancel(ctx)
	var observationWG sync.WaitGroup
	defer func() { stopObservation(); observationWG.Wait() }()
	observationWG.Go(func() {
		for {
			select {
			case <-observationCtx.Done():
				return
			case event, ok := <-client.Events():
				if !ok {
					return
				}
				observer.Notify(event)
				clock.delivery.received()
			}
		}
	})
	ready := make(chan error, 1)
	observationWG.Go(func() {
		_ = observer.Run(observationCtx, heos.IdleObservationInterval, func(err error) {
			select {
			case ready <- err:
			default:
			}
		})
	})
	select {
	case err = <-ready:
		if err != nil {
			return result, err
		}
	case <-ctx.Done():
		return result, context.Cause(ctx)
	}
	catalog, err := heos.NewCatalog(client, 32, time.Minute)
	if err != nil {
		return result, err
	}
	page, err := catalog.Browse(ctx, "900", "", "", 50)
	if err != nil {
		return result, err
	}
	ceiling := 40
	reads := NewReads("replay", []Device{{Config: config.Player{Key: "room", Serial: "synthetic", WritesEnabled: true, VolumeCeiling: &ceiling}, Observer: observer, Client: client, Metrics: metrics, Catalogs: map[string]Browser{"music": catalog}}}, []config.Source{{Key: "music", Player: "room", Name: "Synthetic server"}})
	db := &memoryJournal{ops: map[string]journal.Operation{}, keys: map[string]string{}}
	coordinator := NewCoordinator(ctx, reads, db, nil, nil)
	coordinator.clock = clock
	defer coordinator.Close()
	player, err := reads.Player("room")
	if err != nil {
		return result, err
	}
	request := journal.Request{Principal: "operator", Player: "room", Key: "replay", Method: "POST", Endpoint: "/v1/players/room/playback", IfMatch: fmt.Sprintf("%q", player.Revision), Body: json.RawMessage(`{"initial_volume":{"unit":"heos","level":10}}`)}
	admission, err := coordinator.Submit(ctx, request, Command{Kind: "playback", Level: 10, ItemRef: page.Items[0].Ref, Shuffle: true, Repeat: "off", Automation: &f.Automation})
	if err != nil {
		return result, err
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return result, context.Cause(ctx)
		case <-ticker.C:
			operation, err := db.Get(ctx, admission.ID)
			if err != nil {
				return result, err
			}
			if operation.FinishedAt != nil {
				if err := context.Cause(ctx); err != nil {
					return result, err
				}
				coordinator.Close()
				stopObservation()
				observationWG.Wait()
				clock.mu.Lock()
				pending := len(clock.events)
				clock.mu.Unlock()
				if pending > 0 {
					return result, fmt.Errorf("%d required replay steps unconsumed", pending)
				}
				device.mu.Lock()
				for _, script := range f.Scripts {
					if device.occurrence[script.On] < script.Occurrence {
						device.mu.Unlock()
						return result, fmt.Errorf("required replay script %s/%d unconsumed", script.On, script.Occurrence)
					}
				}
				result = tlsReplayResult{operation: operation, commands: slices.Clone(device.commands), levels: slices.Clone(device.levels), metrics: metrics}
				device.mu.Unlock()
				return result, nil
			}
		}
	}
}
