package app

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/api"
	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
)

func TestProgressReachesHTTPWithoutReadsOrSSEInvalidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var mu sync.Mutex
	commands := map[string]int{}
	var event atomic.Value
	event.Store("")
	address, pin, writes := syntheticHEOSWithReplies(t, func(command string, params url.Values, payload any) any {
		mu.Lock()
		commands[command]++
		mu.Unlock()
		switch command {
		case "player/get_play_state":
			params.Set("state", "play")
		case "player/get_now_playing_media":
			return heos.Media{Source: "900", ID: "track", QueueID: "1", Song: "Track"}
		}
		return payload
	}, func(c net.Conn, command string) {
		if sample := event.Load().(string); command == "system/heart_beat" && sample != "" {
			_, _ = fmt.Fprintf(c, "%s\r\n", sample)
		}
	})
	ds, err := openDevices(ctx, config.Config{Players: []config.Player{{Key: "room", Address: address, FingerprintSHA256: pin, Serial: "synthetic"}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for ds.reads.Devices()[0].Observer.Snapshot().Stale {
		select {
		case <-ctx.Done():
			t.Fatal("initial observation unavailable")
		case <-tick.C:
		}
	}
	token := strings.Repeat("p", 48)
	credentials := []config.Credential{{Principal: "reader", TokenSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(token))), Scopes: []string{"read"}, Players: []string{"room"}}}
	server, err := api.NewServer(ds.reads, syntheticJournal{}, credentials, new(atomic.Bool), ds.events, false)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	httpServer := httptest.NewTLSServer(server.Handler())
	defer httpServer.Close()
	get := func(path string) *http.Response {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := httpServer.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			_ = response.Body.Close()
			t.Fatalf("%s: HTTP %d", path, response.StatusCode)
		}
		return response
	}
	detail := func() mediaPlayerResponse {
		t.Helper()
		response := get("/v1/players/room")
		defer func() { _ = response.Body.Close() }()
		var player mediaPlayerResponse
		if err := json.NewDecoder(response.Body).Decode(&player); err != nil {
			t.Fatal(err)
		}
		if response.Header.Get("ETag") != strconv.Quote(player.Revision) {
			t.Fatalf("ETag does not match player revision: %q, %q", response.Header.Get("ETag"), player.Revision)
		}
		return player
	}
	stream := get("/v1/events")
	events := make(chan string, 8)
	readerDone := make(chan struct{})
	defer func() {
		cancel()
		_ = stream.Body.Close()
		<-readerDone
	}()
	go func() {
		defer close(readerDone)
		defer close(events)
		scanner := bufio.NewScanner(stream.Body)
		for scanner.Scan() {
			if line := scanner.Text(); strings.HasPrefix(line, "event: ") {
				select {
				case events <- strings.TrimPrefix(line, "event: "):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	select {
	case got := <-events:
		if got != "snapshot_required" {
			t.Fatalf("initial SSE handshake: %s", got)
		}
	case <-ctx.Done():
		t.Fatal("missing SSE handshake")
	}
	// Drain initial revision invalidations before observing progress. The real
	// watcher ticks every 500 ms; a quiet interval also covers a late startup tick.
	quiet := time.NewTimer(600 * time.Millisecond)
	defer quiet.Stop()
settled:
	for {
		select {
		case got := <-events:
			if got != "player_changed" {
				t.Fatalf("startup SSE invalidation: %s", got)
			}
			quiet.Reset(600 * time.Millisecond)
		case <-quiet.C:
			break settled
		case <-ctx.Done():
			t.Fatal("initial revision watcher did not settle")
		}
	}
	before := detail()
	if before.NowPlaying == nil || before.NowPlaying.Stale || before.NowPlaying.Progress != nil {
		t.Fatalf("initial current media: %+v", before.NowPlaying)
	}
	mu.Lock()
	wantCommands := maps.Clone(commands)
	mu.Unlock()
	var previousSample time.Time
	for _, sample := range []struct{ position, duration int64 }{{0, 0}, {1500, 2000}} {
		sentAt := time.Now()
		event.Store(fmt.Sprintf(`{"heos":{"command":"event/player_now_playing_progress","message":"pid=-42&cur_pos=%d&duration=%d"}}`, sample.position, sample.duration))
		if _, err := ds.clients[0].Read(ctx, "system/heart_beat", nil); err != nil {
			t.Fatal(err)
		}
		wantCommands["system/heart_beat"]++
		for {
			player := detail()
			if player.Revision != before.Revision || !player.ObservedAt.Equal(*before.ObservedAt) {
				t.Fatal("progress changed revision, ETag or full-observation age")
			}
			if player.NowPlaying == nil || player.NowPlaying.Stale {
				t.Fatal("progress lost fresh current media")
			}
			progress := player.NowPlaying.Progress
			if progress != nil && progress.PositionMS != nil && *progress.PositionMS == sample.position {
				if progress.SampledAt.Before(sentAt) || progress.SampledAt.After(time.Now()) || !progress.SampledAt.After(previousSample) {
					t.Fatalf("sample acceptance time: %s", progress.SampledAt)
				}
				if sample.duration == 0 && progress.DurationMS != nil || sample.duration > 0 && (progress.DurationMS == nil || *progress.DurationMS != sample.duration) {
					t.Fatalf("sample duration: %+v", progress)
				}
				previousSample = progress.SampledAt
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("wire progress did not reach the cached HTTP response")
			case <-tick.C:
			}
		}
	}
	// Allow two complete watcher intervals to expose a progress invalidation.
	quiet.Reset(1100 * time.Millisecond)
	select {
	case got := <-events:
		t.Fatalf("progress emitted an SSE event or closed the stream: %s", got)
	case <-ctx.Done():
		t.Fatal("deadline before checking progress-only SSE behavior")
	case <-quiet.C:
	}
	last := detail()
	if last.Revision != before.Revision || last.NowPlaying == nil || last.NowPlaying.Progress == nil || !last.NowPlaying.Progress.SampledAt.Equal(previousSample) || last.NowPlaying.Progress.PositionMS == nil || *last.NowPlaying.Progress.PositionMS != 1500 {
		t.Fatal("cached progress changed without another device sample")
	}
	mu.Lock()
	gotCommands := maps.Clone(commands)
	mu.Unlock()
	if !maps.Equal(gotCommands, wantCommands) || writes.Load() != 0 {
		t.Fatalf("progress or HTTP polling sent native commands: got=%v want=%v writes=%d", gotCommands, wantCommands, writes.Load())
	}
}
