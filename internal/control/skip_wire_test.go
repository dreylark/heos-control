package control

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func skipWireFixture(t *testing.T, direction string, hybrid bool) (*Coordinator, *memoryJournal, *tlsReplayDevice, journal.Request) {
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
