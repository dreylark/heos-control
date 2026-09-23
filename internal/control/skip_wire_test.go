package control

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

// Synthetic Denon 4.2.21/4.2.22 replies contain no selected QID. The real
// transport and observer must confirm membership after the reply, including a
// MID/QID transition whose completion is driven by time rather than GET count.
func TestSkipTLSConfirmsNativeNavigationWithoutReplay(t *testing.T) {
	for _, direction := range []string{"next", "previous"} {
		for _, hybrid := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/hybrid=%t", direction, hybrid), func(t *testing.T) {
				c, db, device, req := skipWireFixture(t, direction, hybrid)
				cmd := Command{Kind: "skip", Direction: direction}
				admission, err := c.Submit(context.Background(), req, cmd)
				if err != nil {
					t.Fatal(err)
				}
				operation := awaitOperation(t, db, admission.ID)
				if operation.State != journal.Succeeded {
					t.Fatalf("skip was not confirmed: %+v", operation)
				}
				again, err := c.Submit(context.Background(), req, cmd)
				if err != nil || again.ID != admission.ID {
					t.Fatal("same-key retry did not reuse admission", again, err)
				}
				c.Close()
				device.mu.Lock()
				defer device.mu.Unlock()
				writes := 0
				for _, name := range device.commands {
					if name == "player/play_"+direction {
						writes++
					} else if replayWireMutation(name) || name == "player/play_next" || name == "player/play_previous" {
						t.Fatalf("unexpected mutation %s", name)
					}
				}
				if writes != 1 {
					t.Fatalf("native skip sent %d times: %v", writes, device.commands)
				}
				device.clock.mu.Lock()
				pending := len(device.clock.events)
				device.clock.mu.Unlock()
				if pending != 0 {
					t.Fatalf("confirmed before %d required convergence steps", pending)
				}
			})
		}
	}
}

func TestSkipTLSRejectionLeavesDirectControlsAdmissible(t *testing.T) {
	for _, direction := range []string{"next", "previous"} {
		for _, kind := range []string{"volume", "transport"} {
			t.Run(direction+"/"+kind, func(t *testing.T) {
				c, db, device, req := skipWireFixture(t, direction, false, func(d *tlsReplayDevice) {
					d.rejectionCodes = map[string]int{"player/play_" + direction: 17}
					d.fixture.Scripts[0].Steps = []replayStep{{Action: "reply", Result: "rejected"}}
				})
				l := c.lanes["room"]
				before := l.device.Observer.Snapshot()
				cmd := Command{Kind: "skip", Direction: direction}
				admission, err := c.Submit(context.Background(), req, cmd)
				if err != nil {
					t.Fatal(err)
				}
				operation := awaitOperation(t, db, admission.ID)
				assertSkipTLSRejected(t, operation)
				view := l.device.Client.PlayerView(before.Player.ID)
				if !view.Connected || view.Token.Generation != before.Token.Generation || view.Token.Write != before.Token.Write+1 {
					t.Fatalf("native rejection did not consume its guard on the existing connection: before=%+v after=%+v", before.Token, view)
				}
				again, err := c.Submit(context.Background(), req, cmd)
				if err != nil || again.ID != admission.ID {
					t.Fatal("rejected skip retry did not reuse admission", again, err)
				}
				device.mu.Lock()
				fullReads := device.occurrence["player/get_players"]
				device.mu.Unlock()
				if fullReads != 3 { // Startup, prewrite, and one recovery observation.
					t.Fatalf("rejection recovery performed %d full observations, want 3", fullReads)
				}
				next := playerRequest(c, req, "after-rejected-skip", "PUT", "/v1/players/room/"+kind)
				admission, err = c.Submit(context.Background(), next, Command{Kind: kind, Level: 10, State: "stop"})
				if err != nil {
					t.Fatalf("%s after native eid 17 must remain admissible: %v", kind, err)
				}
				if operation := awaitOperation(t, db, admission.ID); operation.State != journal.Succeeded {
					t.Fatalf("%s after native rejection failed: %+v", kind, operation)
				}
				c.Close()
				want := "player/set_volume"
				if kind == "transport" {
					want = "player/set_play_state"
				}
				assertSkipTLSWrites(t, device, []string{"player/play_" + direction, want})
			})
		}
	}
}

func TestSkipTLSRejectedRecoveryFailureKeepsStateUnavailable(t *testing.T) {
	for _, failure := range []string{"rejected", "disconnect"} {
		t.Run(failure, func(t *testing.T) {
			c, db, device, req := skipWireFixture(t, "next", false, func(d *tlsReplayDevice) {
				d.rejectionCodes = map[string]int{"player/play_next": 17}
				d.fixture.Scripts[0].Steps = []replayStep{{Action: "reply", Result: "rejected"}}
				step := replayStep{Action: "reply", Result: "rejected"}
				if failure == "disconnect" {
					step = replayStep{Action: "disconnect"}
				}
				// Initial observation and prewrite observation succeed. Recovery
				// cannot establish a new baseline after the rejected command.
				d.fixture.Scripts = append(d.fixture.Scripts, replayScript{On: "player/get_players", Occurrence: 3, Steps: []replayStep{step}})
			})
			admission, err := c.Submit(context.Background(), req, Command{Kind: "skip", Direction: "next"})
			if err != nil {
				t.Fatal(err)
			}
			assertSkipTLSRejected(t, awaitOperation(t, db, admission.ID))
			if snapshot := c.lanes["room"].device.Observer.Snapshot(); !snapshot.Stale || snapshot.Verified {
				t.Fatalf("failed recovery fabricated usable state: %+v", snapshot)
			}
			next := playerRequest(c, req, "after-failed-recovery", "PUT", "/v1/players/room/volume")
			if _, err := c.Submit(context.Background(), next, Command{Kind: "volume", Level: 10}); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("failed recovery must refuse later writes: %v", err)
			}
			c.Close()
			assertSkipTLSWrites(t, device, []string{"player/play_next"})
			device.mu.Lock()
			defer device.mu.Unlock()
			if device.occurrence["player/get_players"] != 3 {
				t.Fatalf("missing bounded recovery attempt: %v", device.commands)
			}
		})
	}
}

func assertSkipTLSRejected(t *testing.T, operation journal.Operation) {
	t.Helper()
	var outcome struct {
		Delivery    string `json:"delivery"`
		Diagnostics struct {
			Reason string `json:"reason"`
		} `json:"diagnostics"`
	}
	if err := json.Unmarshal(operation.Outcome, &outcome); err != nil {
		t.Fatal(err)
	}
	if operation.State != journal.Failed || operation.ErrorCode != "device_rejected" || outcome.Delivery != "rejected" || outcome.Diagnostics.Reason != "skip_limit_reached" {
		t.Fatalf("native rejection classification changed: %+v", operation)
	}
}

func assertSkipTLSWrites(t *testing.T, device *tlsReplayDevice, want []string) {
	t.Helper()
	device.mu.Lock()
	defer device.mu.Unlock()
	var writes []string
	for _, name := range device.commands {
		if replayWireMutation(name) || name == "player/play_next" || name == "player/play_previous" {
			writes = append(writes, name)
		}
	}
	if !slices.Equal(writes, want) {
		t.Fatalf("unexpected wire mutations: got %v, want %v", writes, want)
	}
}

func skipWireFixture(t *testing.T, direction string, hybrid bool, configure ...func(*tlsReplayDevice)) (*Coordinator, *memoryJournal, *tlsReplayDevice, journal.Request) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	watchdog := time.AfterFunc(5*time.Second, func() { cancel(fmt.Errorf("skip TLS watchdog")) })
	clock := &tlsReplayClock{now: time.Now(), delivery: newWireEventDelivery()}
	volume, muted := 20, false
	device := &tlsReplayDevice{clock: clock, ctx: ctx, cancel: cancel, occurrence: map[string]int{},
		state: heos.Snapshot{State: "play", Volume: &volume, Muted: &muted, Repeat: "off"}}
	from, target := 1, 2
	if direction == "previous" {
		from, target, device.state.State = 2, 1, "pause"
	}
	applyReplayPatch(&device.state, replayPatch{Queue: "selected", Media: &replayMedia{MID: fmt.Sprintf("track-%d", from), QID: fmt.Sprint(from), Source: "1024"}})
	selected := replayMedia{MID: fmt.Sprintf("track-%d", target), QID: fmt.Sprint(target), Source: "1024"}
	initial := selected
	if hybrid {
		initial.QID = fmt.Sprint(from)
	}
	mediaEvent := &replayEvent{Command: "event/player_now_playing_changed", Params: map[string]string{"pid": "1"}}
	steps := []replayStep{
		{Action: "apply", Patch: &replayPatch{Media: &initial}},
		{Action: "reply", Result: "success"},
		{Action: "event", Event: mediaEvent},
	}
	if hybrid {
		steps = append(steps,
			replayStep{AtMS: 250, Action: "apply", Patch: &replayPatch{Media: &selected}},
			replayStep{AtMS: 250, Action: "event", Event: mediaEvent})
	}
	device.fixture.Scripts = []replayScript{{On: "player/play_" + direction, Occurrence: 1, Args: map[string]string{"pid": "1"}, Steps: steps}}
	for _, configure := range configure {
		configure(device)
	}
	seed := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := seed.TLS.Certificates[0]
	seed.Close()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}})
	if err != nil {
		watchdog.Stop()
		cancel(nil)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		watchdog.Stop()
		cancel(nil)
		_ = listener.Close()
		device.mu.Lock()
		for _, conn := range device.conns {
			_ = conn.Close()
		}
		device.mu.Unlock()
		device.wg.Wait()
	})
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
	client, err := heos.New(ctx, heos.Config{Address: listener.Addr().String(), Fingerprint: fmt.Sprintf("%x", sha256.Sum256(certificate.Certificate[0])), EnableWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	observer, err := heos.NewObserver(client, heos.Identity{Key: "room", Serial: "synthetic"}, heos.ObservationCacheTTL)
	if err != nil {
		t.Fatal(err)
	}
	observationCtx, stopObservation := context.WithCancel(ctx)
	var observations sync.WaitGroup
	t.Cleanup(func() { stopObservation(); observations.Wait() })
	observations.Go(func() {
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
	if err := observer.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	ceiling := 40
	reads := NewReads("skip-wire", []Device{{Config: config.Player{Key: "room", Serial: "synthetic", WritesEnabled: true, VolumeCeiling: &ceiling}, Observer: observer, Client: client}}, nil)
	db := &memoryJournal{ops: map[string]journal.Operation{}, keys: map[string]string{}}
	c := NewCoordinator(ctx, reads, db, nil, nil)
	c.clock = clock
	t.Cleanup(c.Close)
	player, err := reads.Player("room")
	if err != nil {
		t.Fatal(err)
	}
	req := journal.Request{Principal: "operator", Player: "room", Key: "skip-wire", Method: "POST", Endpoint: "/v1/players/room/skip", IfMatch: fmt.Sprintf("%q", player.Revision), Body: json.RawMessage(fmt.Sprintf(`{"direction":%q}`, direction))}
	return c, db, device, req
}
