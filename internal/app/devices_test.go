package app

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contract "github.com/dreylark/heos-control/api"
	"github.com/dreylark/heos-control/internal/api"
	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
	"github.com/pb33f/libopenapi"
	validator "github.com/pb33f/libopenapi-validator"
	validationconfig "github.com/pb33f/libopenapi-validator/config"
)

type syntheticJournal struct{}

func (syntheticJournal) Ready(context.Context) error { return nil }
func (syntheticJournal) List(context.Context, journal.HistoryQuery) (journal.HistoryPage, error) {
	return journal.HistoryPage{Items: []journal.Operation{}}, nil
}
func (syntheticJournal) Get(context.Context, string) (journal.Operation, error) {
	return journal.Operation{ID: "op", Kind: "alarm", Player: "room", Principal: "reader", State: journal.Uncertain, Revision: 2, CreatedAt: time.Now(), UpdatedAt: time.Now()}, nil
}

func TestLogicalSourceReferencesRemainIsolated(t *testing.T) {
	address, pin, writes := syntheticHEOS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Deliberately alias a SID below the config-validation boundary to model
	// firmware reassigning a previously observed source ID to another view.
	cfg := config.Config{Players: []config.Player{{Key: "room", Address: address, FingerprintSHA256: pin, Serial: "synthetic"}}, Sources: []config.Source{{Key: "first", Player: "room", Name: "Gerbera"}, {Key: "second", Player: "room", Name: "Gerbera"}}}
	ds, e := openDevices(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if e != nil {
		t.Fatal(e)
	}
	defer ds.Close()
	// Wait for the app-owned initial refresh; launching a competing refresh
	// would legitimately invalidate a snapshot midway through this assertion.
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		p, _ := ds.reads.Player("room")
		if !p.Stale {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("initial observation unavailable")
		case <-ticker.C:
		}
	}
	page, e := ds.reads.Items(ctx, "first", "", "", 50)
	if e != nil || len(page.Items) != 1 {
		t.Fatal(page, e)
	}
	ref := page.Items[0].Ref
	if _, e = ds.reads.Items(ctx, "second", ref, "", 50); !errors.Is(e, heos.ErrStaleReference) {
		t.Fatal("cross-source reference accepted", e)
	}
	if _, e = ds.reads.Items(ctx, "first", ref, "", 50); e != nil {
		t.Fatal("own-source reference rejected", e)
	}
	if writes.Load() != 0 {
		t.Fatal("read sent unexpected commands")
	}
}
func (syntheticJournal) Active(context.Context, string) (journal.Operation, error) {
	return journal.Operation{}, journal.ErrNotFound
}

// Fixtures follow Denon 3.1/3.2 (wire envelope), 4.2 (player reads),
// 4.4.3/4.4.4 (Local Music, sources and paged containers). No writes are accepted.
func syntheticHEOS(t *testing.T) (string, string, *atomic.Int64) {
	t.Helper()
	return syntheticHEOSWithHook(t, nil)
}

func syntheticHEOSWithHook(t *testing.T, afterReply func(net.Conn, string)) (string, string, *atomic.Int64) {
	t.Helper()
	seed := httptest.NewTLSServer(http.NotFoundHandler())
	cert := seed.TLS.Certificates[0]
	seed.Close()
	listener, e := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}})
	if e != nil {
		t.Fatal(e)
	}
	var writes atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	connections := []net.Conn{}
	wg.Go(func() {
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			connections = append(connections, c)
			mu.Unlock()
			wg.Go(func() {
				defer func() { _ = c.Close() }()
				reader := bufio.NewReader(c)
				for {
					line, e := reader.ReadString('\n')
					if e != nil {
						return
					}
					u, e := url.Parse(strings.TrimSpace(line))
					if e != nil {
						return
					}
					command := u.Host + u.Path
					params := u.Query()
					payload := any([]any{})
					result := "success"
					switch command {
					case "system/heart_beat":
					case "system/register_for_change_events":
						if params.Get("enable") != "on" {
							writes.Add(1)
						}
					case "player/get_players":
						payload = []map[string]any{{"pid": -42, "name": "Room", "model": "test", "serial": "synthetic"}}
					case "group/get_groups":
						payload = []any{}
					case "player/get_play_state":
						params.Set("state", "stop")
					case "player/get_volume":
						params.Set("level", "15")
					case "player/get_play_mode":
						params.Set("repeat", "off")
						params.Set("shuffle", "off")
					case "player/get_mute":
						params.Set("state", "off")
					case "player/get_now_playing_media":
						payload = map[string]any{"song": "", "album": "", "artist": ""}
					case "player/get_queue":
						params.Set("count", "0")
						params.Set("returned", "0")
					case "browse/browse":
						switch {
						case params.Get("sid") == "1024":
							payload = []map[string]any{{"sid": "900", "name": "Gerbera", "type": "heos_server"}}
						case params.Get("cid") == "":
							payload = []map[string]any{{"cid": "audio", "name": "Audio", "container": "yes", "playable": "no"}}
						case params.Get("cid") == "audio":
							payload = []map[string]any{{"cid": "albums", "name": "Albums", "container": "yes", "playable": "no"}}
						case params.Get("cid") == "albums":
							payload = []map[string]any{{"cid": "green", "name": "Green", "container": "yes", "playable": "yes"}}
						default:
							result = "fail"
						}
						if result == "success" {
							params.Set("count", "1")
							params.Set("returned", "1")
						}
					default:
						writes.Add(1)
						result = "fail"
					}
					b, _ := json.Marshal(map[string]any{"heos": map[string]string{"command": command, "result": result, "message": params.Encode()}, "payload": payload})
					if _, e = c.Write(append(b, '\r', '\n')); e != nil {
						return
					}
					if afterReply != nil {
						afterReply(c, command)
					}
				}
			})
		}
	})
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for _, c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return listener.Addr().String(), fmt.Sprintf("%x", sha256.Sum256(cert.Certificate[0])), &writes
}

func TestDeviceEventsUpdateIdleAPICacheWithoutFullRead(t *testing.T) {
	var full atomic.Int32
	address, pin, writes := syntheticHEOSWithHook(t, func(c net.Conn, command string) {
		if command == "player/get_players" {
			full.Add(1)
		}
		if command == "system/heart_beat" {
			_, _ = fmt.Fprint(c, "{\"heos\":{\"command\":\"event/player_volume_changed\",\"message\":\"pid=-42&level=17&mute=off\"}}\r\n")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ds, err := openDevices(ctx, config.Config{Players: []config.Player{{Key: "room", Address: address, FingerprintSHA256: pin, Serial: "synthetic"}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	waitFresh := func(level int) control.Player {
		t.Helper()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			player, err := ds.reads.Player("room")
			if err == nil && !player.Stale && player.ObservedAt != nil && (level < 0 || player.Volume.Level != nil && *player.Volume.Level == level) {
				return player
			}
			select {
			case <-ctx.Done():
				t.Fatal("HEOS event did not refresh the API cache within the event debounce budget")
			case <-tick.C:
			}
		}
	}
	initial := waitFresh(-1)
	if _, err := ds.clients[0].Read(ctx, "system/heart_beat", nil); err != nil {
		t.Fatal(err)
	}
	updated := waitFresh(17)
	if updated.Revision == initial.Revision || !updated.ObservedAt.Equal(*initial.ObservedAt) || full.Load() != 1 {
		t.Fatal("event failed to update revision, renewed full observation age or sent GETs", full.Load())
	}
	if writes.Load() != 0 {
		t.Fatal("observation sent a device write")
	}
}

func TestReadOnlyWiringContractSSEAndShutdown(t *testing.T) {
	address, pin, writes := syntheticHEOS(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ceiling := 45
	cfg := config.Config{Players: []config.Player{{Key: "room", Address: address, FingerprintSHA256: pin, Serial: "synthetic", Model: "test", VolumeCeiling: &ceiling}}, Sources: []config.Source{{Key: "gerbera-music", Player: "room", Name: "Gerbera"}}}
	ds, e := openDevices(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if e != nil {
		t.Fatal(e)
	}
	defer ds.Close()
	ds.watchOperations(syntheticJournal{})
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		p, _ := ds.reads.Player("room")
		if !p.Stale {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("observation never became fresh: %+v", p)
		case <-tick.C:
		}
	}
	token := strings.Repeat("t", 48)
	credentials := []config.Credential{{Principal: "reader", TokenSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(token))), Scopes: []string{"read"}, Players: []string{"room"}}}
	server, e := api.NewServer(ds.reads, syntheticJournal{}, credentials, new(atomic.Bool), ds.events, false)
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	httpServer := httptest.NewTLSServer(server.Handler())
	defer httpServer.Close()
	client := httpServer.Client()
	doc, e := libopenapi.NewDocument(contract.OpenAPI)
	if e != nil {
		t.Fatal(e)
	}
	v, errs := validator.NewValidator(doc, validationconfig.WithoutSecurityValidation())
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	defer v.Release()
	for _, tc := range []struct{ method, path, body string }{
		{"GET", "/v1/players", ""}, {"GET", "/v1/players/room", ""}, {"GET", "/v1/groups", ""}, {"GET", "/v1/sources", ""}, {"GET", "/v1/sources/gerbera-music/items", ""}, {"GET", "/v1/players/room/queue", ""}, {"GET", "/v1/operations/op", ""}, {"POST", "/v1/players/room/preflight", `{"item_ref":"unused-while-writes-disabled","queue_mode":"replace","shuffle":true,"repeat":"off","initial_volume":{"unit":"heos","level":10},"takeover":false}`},
	} {
		t.Run(tc.path, func(t *testing.T) {
			r, _ := http.NewRequest(tc.method, httpServer.URL+tc.path, strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer "+token)
			if tc.body != "" {
				r.Header.Set("Content-Type", "application/json")
			}
			response, e := client.Do(r)
			if e != nil {
				t.Fatal(e)
			}
			defer func() { _ = response.Body.Close() }()
			b, e := io.ReadAll(response.Body)
			if e != nil {
				t.Fatal(e)
			}
			if response.StatusCode != 200 {
				t.Fatalf("%d: %s", response.StatusCode, b)
			}
			response.Body = io.NopCloser(strings.NewReader(string(b)))
			if ok, errs := v.ValidateHttpResponse(r, response); !ok {
				t.Fatalf("response contract: %v body=%s", errs, b)
			}
			if tc.method == "POST" {
				var p struct {
					Ready bool `json:"ready"`
				}
				if e := json.Unmarshal(b, &p); e != nil || p.Ready {
					t.Fatalf("writes-disabled preflight should not be ready: %s", b)
				}
			}
		})
	}
	r, _ := http.NewRequest("GET", httpServer.URL+"/v1/events", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Last-Event-ID", "previous:1")
	stream, e := client.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = stream.Body.Close() }()
	reader := bufio.NewReader(stream.Body)
	var first strings.Builder
	for i := 0; i < 4; i++ {
		line, e := reader.ReadString('\n')
		if e != nil {
			t.Fatal(e)
		}
		first.WriteString(line)
	}
	if !strings.Contains(first.String(), "snapshot_required") {
		t.Fatal(first.String())
	}
	done := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, stream.Body); close(done) }()
	ds.events.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SSE shutdown blocked")
	}
	_ = stream.Body.Close()
	ds.Close()
	cancel()
	if writes.Load() != 0 {
		t.Fatalf("read-only wiring sent %d unexpected commands", writes.Load())
	}
}
