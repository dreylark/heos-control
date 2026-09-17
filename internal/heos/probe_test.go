package heos

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func probeConfig(s *fakeHEOS) Config {
	return Config{Address: s.listener.Addr().String(), Fingerprint: s.pin, ConnectTimeout: time.Second, CommandTimeout: time.Second}
}

func probeIdentity() Identity { return Identity{Key: "room", Serial: "serial-A", Model: "Home"} }
func probePlayers() []Player {
	return []Player{{ID: "1", Serial: "serial-A", Model: "Home", Name: "private-room", IP: "private-address"}}
}

type forbiddenProbeMetrics struct{}

func (forbiddenProbeMetrics) WireCommand(string, string, time.Duration) {
	panic("probe must disable metrics")
}
func (forbiddenProbeMetrics) Reconnected()               { panic("probe must disable metrics") }
func (forbiddenProbeMetrics) EventGap(string)            { panic("probe must disable metrics") }
func (forbiddenProbeMetrics) Observation(string, string) { panic("probe must disable metrics") }

// Denon 3.1/3.2 framing, 4.1.1 registration, 4.2.1 identity and 4.2.3 state.
// The existing client subscribes first; this probe never starts an observer.
func TestProbeUsesOnlyRegistrationIdentityAndState(t *testing.T) {
	for _, state := range []string{"play", "pause", "stop", "unknown"} {
		t.Run(state, func(t *testing.T) {
			var mu sync.Mutex
			var commands []string
			record := func(u *url.URL) { mu.Lock(); commands = append(commands, commandName(u)); mu.Unlock() }
			closed := make(chan struct{})
			s := newFakeHEOSWithRegistration(t, func(c net.Conn, u *url.URL, _ int64) {
				record(u)
				switch commandName(u) {
				case "player/get_players":
					sendReply(c, u, nil, probePlayers())
				case "player/get_play_state":
					if u.Query().Get("pid") != "1" {
						t.Error("state did not address resolved identity")
					}
					sendReply(c, u, url.Values{"state": {state}}, nil)
					// The probe closes its client before returning, including its reader.
					_, _ = c.Read(make([]byte, 1))
					close(closed)
				default:
					t.Errorf("unexpected probe command %s", commandName(u))
				}
			}, func(c net.Conn, u *url.URL) { record(u); sendReply(c, u, nil, nil) })
			cfg := probeConfig(s)
			var logs bytes.Buffer
			cfg.EnableWrites, cfg.Logger, cfg.Metrics = true, slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})), forbiddenProbeMetrics{}
			result := Probe(context.Background(), cfg, probeIdentity())
			if !result.OK || result.Code != "ok" {
				t.Fatal(result)
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("probe connection not closed")
			}
			mu.Lock()
			got := append([]string(nil), commands...)
			mu.Unlock()
			want := []string{"system/register_for_change_events", "player/get_players", "player/get_play_state"}
			if !reflect.DeepEqual(got, want) || s.connections.Load() != 1 || logs.Len() != 0 {
				t.Fatalf("probe effects: commands=%v connections=%d logs=%q", got, s.connections.Load(), logs.String())
			}
		})
	}
}

func TestProbeIdentityFailuresStopBeforeStateRead(t *testing.T) {
	for _, scenario := range []string{"missing", "duplicate-serial", "model", "duplicate-pid", "missing-pid"} {
		t.Run(scenario, func(t *testing.T) {
			players := probePlayers()
			switch scenario {
			case "missing":
				players[0].Serial = "unrelated"
			case "duplicate-serial":
				players = append(players, Player{ID: "2", Serial: "serial-A", Model: "Home"})
			case "model":
				players[0].Model = "different"
			case "duplicate-pid":
				players = append(players, Player{ID: "1", Serial: "other", Model: "Home"})
			case "missing-pid":
				players[0].ID = ""
			}
			s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				if commandName(u) != "player/get_players" {
					t.Error("identity failure issued another command")
				}
				if scenario == "missing-pid" {
					sendReply(c, u, nil, []any{map[string]string{"serial": "serial-A", "model": "Home"}})
				} else {
					sendReply(c, u, nil, players)
				}
			})
			result := Probe(context.Background(), probeConfig(s), probeIdentity())
			if result.OK || result.Code != "identity" || s.commands.Load() != 2 {
				t.Fatal(result, s.commands.Load())
			}
		})
	}
}

func TestProbeRejectsMalformedOrRejectedProtocolWithoutLeakingData(t *testing.T) {
	const private = "private-account-and-media-secret"
	for _, scenario := range []string{"registration-rejected", "discovery-rejected", "state-rejected", "payload-null", "payload-object", "bad-frame", "invalid-state", "duplicate-state", "missing-state", "wrong-pid", "missing-pid", "duplicate-pid"} {
		t.Run(scenario, func(t *testing.T) {
			reject := func(c net.Conn, u *url.URL) {
				_, _ = io.WriteString(c, `{"heos":{"command":"`+commandName(u)+`","result":"fail","message":"eid=12&text=`+private+`"}}`+"\r\n")
			}
			s := newFakeHEOSWithRegistration(t, func(c net.Conn, u *url.URL, _ int64) {
				if commandName(u) == "player/get_players" {
					switch scenario {
					case "discovery-rejected":
						reject(c, u)
					case "payload-null":
						sendReply(c, u, nil, nil)
					case "payload-object":
						sendReply(c, u, nil, map[string]string{"private": private})
					case "bad-frame":
						_, _ = io.WriteString(c, private+"\r\n")
					default:
						sendReply(c, u, nil, probePlayers())
					}
					return
				}
				if scenario == "state-rejected" {
					reject(c, u)
					return
				}
				params := url.Values{"pid": {"1"}, "state": {"stop"}}
				switch scenario {
				case "invalid-state":
					params.Set("state", private)
				case "duplicate-state":
					params["state"] = []string{"stop", private}
				case "missing-state":
					params.Del("state")
				case "wrong-pid":
					params.Set("pid", private)
				case "missing-pid":
					params.Del("pid")
				case "duplicate-pid":
					params["pid"] = []string{"1", private}
				}
				_, _ = io.WriteString(c, `{"heos":{"command":"player/get_play_state","result":"success","message":"`+params.Encode()+`"}}`+"\r\n")
			}, func(c net.Conn, u *url.URL) {
				if scenario == "registration-rejected" {
					reject(c, u)
				} else {
					sendReply(c, u, nil, nil)
				}
			})
			cfg := probeConfig(s)
			var logs bytes.Buffer
			cfg.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			result := Probe(context.Background(), cfg, probeIdentity())
			if result.OK || result.Code != "protocol" || result.Message == "" {
				t.Fatal(result)
			}
			for _, sensitive := range []string{private, cfg.Address, cfg.Fingerprint, "serial-A", "private-room", "private-address"} {
				if strings.Contains(result.Code+result.Message+logs.String(), sensitive) {
					t.Fatalf("diagnostic leaked private value: %+v %q", result, logs.String())
				}
			}
			if scenario == "registration-rejected" && s.commands.Load() != 1 {
				t.Fatal("continued after rejected subscription")
			}
		})
	}
}

func TestProbePinMismatchSendsNoHEOSCommands(t *testing.T) {
	s := newFakeHEOS(t, func(net.Conn, *url.URL, int64) { t.Error("wrong pin reached HEOS") })
	cfg := probeConfig(s)
	cfg.Fingerprint = strings.Repeat("00", 32)
	result := Probe(context.Background(), cfg, probeIdentity())
	if result.OK || result.Code != "pin_mismatch" || s.commands.Load() != 0 || s.connections.Load() != 1 {
		t.Fatal(result, s.commands.Load(), s.connections.Load())
	}
}

func TestProbeKeepsPinnedTrustWithoutPublicCAOrHostname(t *testing.T) {
	// The shared fixture is self-signed and has no matching DNS SAN. Explicit
	// pinning is the service's trust contract; doctor must not add CA validation.
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/get_players" {
			sendReply(c, u, nil, probePlayers())
		} else {
			sendReply(c, u, url.Values{"state": {"stop"}}, nil)
		}
	})
	if result := Probe(context.Background(), probeConfig(s), probeIdentity()); !result.OK {
		t.Fatal(result)
	}
}

func TestProbeClassifiesTCPAndTLSFailures(t *testing.T) {
	for _, scenario := range []string{"refused", "not-tls", "handshake-timeout"} {
		t.Run(scenario, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			if scenario == "refused" {
				_ = listener.Close()
				// A released ephemeral port can be reused before dialing it.
				// Port zero cannot be assigned to a listening TCP socket.
				address = "127.0.0.1:0"
			} else {
				done := make(chan struct{})
				go func() {
					defer close(done)
					conn, e := listener.Accept()
					if e != nil {
						return
					}
					defer func() { _ = conn.Close() }()
					if scenario == "not-tls" {
						_, _ = io.WriteString(conn, "HTTP/1.0 400 Bad Request\r\n\r\n")
					} else {
						_, _ = io.Copy(io.Discard, conn)
					}
				}()
				t.Cleanup(func() {
					_ = listener.Close()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Error("TLS probe did not close connection")
					}
				})
			}
			cfg := Config{Address: address, Fingerprint: strings.Repeat("00", 32), ConnectTimeout: 75 * time.Millisecond, CommandTimeout: time.Second}
			began := time.Now()
			result := Probe(context.Background(), cfg, probeIdentity())
			want := "tls"
			if scenario == "refused" {
				want = "unreachable"
			}
			if result.OK || result.Code != want || time.Since(began) > time.Second {
				t.Fatal(result, time.Since(began))
			}
		})
	}
}

func TestProbeTimeoutAndCancellationAreBounded(t *testing.T) {
	t.Run("before", func(t *testing.T) {
		s := newFakeHEOS(t, func(net.Conn, *url.URL, int64) {
			t.Error("cancelled probe sent a command")
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result := Probe(ctx, probeConfig(s), probeIdentity())
		if result.OK || result.Code != "cancelled" || s.connections.Load() != 0 {
			t.Fatal(result, s.connections.Load())
		}
	})
	for _, stage := range []string{"registration", "discovery", "state"} {
		for _, terminal := range []struct {
			code  string
			cause error
		}{{"timeout", context.DeadlineExceeded}, {"cancelled", context.Canceled}} {
			t.Run(stage+"/"+terminal.code, func(t *testing.T) {
				reached, release := make(chan struct{}), make(chan struct{})
				defer close(release)
				stall := func() { close(reached); <-release }
				s := newFakeHEOSWithRegistration(t, func(c net.Conn, u *url.URL, _ int64) {
					if stage == "discovery" || commandName(u) == "player/get_play_state" {
						stall()
						return
					}
					sendReply(c, u, nil, probePlayers())
				}, func(c net.Conn, u *url.URL) {
					if stage == "registration" {
						stall()
						return
					}
					sendReply(c, u, nil, nil)
				})
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(context.Canceled)
				result := make(chan ProbeResult, 1)
				go func() { result <- Probe(ctx, probeConfig(s), probeIdentity()) }()
				select {
				case <-reached:
				case got := <-result:
					t.Fatalf("probe ended before reaching %s: %+v", stage, got)
				case <-time.After(5 * time.Second):
					t.Fatal("probe did not reach its target command")
				}
				// End the caller's budget only after the command is on the wire.
				// A startup-relative 100ms timer instead races unrelated TCP/TLS
				// setup when the test machine is compiling instrumented packages.
				cancel(terminal.cause)
				select {
				case got := <-result:
					if got.OK || got.Code != terminal.code {
						t.Fatal(got)
					}
				case <-time.After(time.Second):
					t.Fatal("probe did not release its client after cancellation")
				}
				wantCommands := map[string]int64{"registration": 1, "discovery": 2, "state": 3}[stage]
				if s.commands.Load() != wantCommands {
					t.Fatal("probe continued after cancellation", s.commands.Load())
				}
			})
		}
	}
}

func TestProbeEventsCannotCreateFalseStableIdentity(t *testing.T) {
	for _, scenario := range []string{"progress", "ordinary-player-event", "identity-change"} {
		t.Run(scenario, func(t *testing.T) {
			s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				if commandName(u) == "player/get_players" {
					sendReply(c, u, nil, probePlayers())
					return
				}
				switch scenario {
				case "progress":
					for range 100 {
						sendEvent(c, "event/player_now_playing_progress", "pid=1&cur_pos=1&duration=3")
					}
				case "ordinary-player-event":
					sendEvent(c, "event/player_state_changed", "pid=1&state=pause")
				case "identity-change":
					sendEvent(c, "event/players_changed", "")
				}
				sendReply(c, u, url.Values{"state": {"stop"}}, nil)
			})
			result := Probe(context.Background(), probeConfig(s), probeIdentity())
			wantOK := scenario == "progress" || scenario == "ordinary-player-event"
			if result.OK != wantOK || (!wantOK && result.Code != "changed") || s.commands.Load() != 3 {
				t.Fatal(result, s.commands.Load())
			}
		})
	}
}

func TestProbeConsumesProgressBeforeOrdinaryNotification(t *testing.T) {
	stateRequested, releaseState := make(chan struct{}), make(chan struct{})
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/get_players" {
			for range 32 {
				sendEvent(c, "event/player_now_playing_progress", "pid=1&cur_pos=1&duration=3")
			}
			sendReply(c, u, nil, probePlayers())
			return
		}
		close(stateRequested)
		<-releaseState
		sendEvent(c, "event/player_state_changed", "pid=1&state=pause")
		sendReply(c, u, url.Values{"state": {"stop"}}, nil)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := New(ctx, probeConfig(s))
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan ProbeResult, 1)
	go func() { result <- probeClient(ctx, client, probeIdentity()) }()
	defer func() {
		close(releaseState)
		client.Close()
	}()
	select {
	case <-stateRequested:
	case <-ctx.Done():
		t.Fatal("state request not sent")
	}
	// Receiving the later state request proves all progress frames preceding
	// discovery's reply have been processed. Wait for the consumer, not a timer,
	// before delivering the control notification to avoid scheduler-dependent
	// overflow assertions.
	eventually(t, func() bool { return len(client.events) == 0 })
	releaseState <- struct{}{}
	select {
	case got := <-result:
		if !got.OK || s.commands.Load() != 3 {
			t.Fatal(got, s.commands.Load())
		}
	case <-ctx.Done():
		t.Fatal("probe did not finish")
	}
}

func TestProbeRejectsTransportGap(t *testing.T) {
	for _, reason := range []string{"event_buffer_overflow", "connection_closed"} {
		t.Run(reason, func(t *testing.T) {
			var client *Client
			clientReady := make(chan struct{})
			s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				if commandName(u) == "player/get_players" {
					sendReply(c, u, nil, probePlayers())
					return
				}
				// Inject at the client's publication boundary. Transport overflow
				// has its own deterministic tests with a blocked consumer; a wire
				// burst cannot guarantee overflow while this consumer is running.
				<-clientReady
				client.publish(Event{Token: client.View().Token, Gap: true, GapReason: reason})
				sendReply(c, u, url.Values{"state": {"stop"}}, nil)
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var err error
			client, err = New(ctx, probeConfig(s))
			if err != nil {
				t.Fatal(err)
			}
			close(clientReady)
			result := probeClient(ctx, client, probeIdentity())
			if result.OK || result.Code != "changed" || s.commands.Load() != 3 {
				t.Fatal(result, s.commands.Load())
			}
		})
	}
}

func TestProbeInvalidConfigurationDoesNotConnect(t *testing.T) {
	s := newFakeHEOS(t, func(net.Conn, *url.URL, int64) { t.Error("invalid configuration contacted device") })
	for _, field := range []string{"address", "fingerprint", "timeout", "identity"} {
		cfg, id := probeConfig(s), probeIdentity()
		switch field {
		case "address":
			cfg.Address = "private-invalid-address"
		case "fingerprint":
			cfg.Fingerprint = "private-invalid-pin"
		case "timeout":
			cfg.CommandTimeout = -time.Second
		case "identity":
			id.Serial = ""
		}
		result := Probe(context.Background(), cfg, id)
		if result.OK || result.Code != "configuration" || s.connections.Load() != 0 {
			t.Fatal(field, result, s.connections.Load())
		}
	}
}

func TestProbeConnectionClassificationPreservesCauses(t *testing.T) {
	for _, phase := range []error{ErrConnect, ErrTLS} {
		err := &connectionError{phase: phase, cause: context.DeadlineExceeded}
		if !errors.Is(err, phase) || !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "deadline") {
			t.Fatal(err)
		}
	}
}

func TestProbeCallerDeadlineDoesNotHideIndependentFailures(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(context.DeadlineExceeded)
	for _, tc := range []struct {
		name string
		err  error
		code string
	}{
		{"owned client shutdown", &CommandError{Uncertain, ErrClosed}, "timeout"},
		{"peer closed", io.EOF, "unreachable"},
		{"pin mismatch", ErrPinMismatch, "pin_mismatch"},
		{"TLS failure", &connectionError{phase: ErrTLS, cause: io.EOF}, "tls"},
		{"TCP failure", &connectionError{phase: ErrConnect, cause: io.EOF}, "unreachable"},
		{"rejected command", ErrRejected, "protocol"},
		{"invalid response", ErrProtocol, "protocol"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := probeError(ctx, tc.err); got.OK || got.Code != tc.code {
				t.Fatal(got)
			}
		})
	}
	if got := probeError(context.Background(), ErrClosed); got.OK || got.Code != "unreachable" {
		t.Fatal("unexplained client closure must not claim caller timeout", got)
	}
}
