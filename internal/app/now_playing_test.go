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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/api"
	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/heos"
)

// Decode the public wire shape independently of control.Player so this test
// detects an absent projection as well as media lost by the application wiring.
type mediaPlayerResponse struct {
	control.Player
	NowPlaying *struct {
		Song     string `json:"song"`
		Album    string `json:"album"`
		Artist   string `json:"artist"`
		QueueID  string `json:"queue_id"`
		MediaID  string `json:"media_id"`
		Stale    bool   `json:"stale"`
		Progress *struct {
			PositionMS *int64    `json:"position_ms"`
			DurationMS *int64    `json:"duration_ms"`
			SampledAt  time.Time `json:"sampled_at"`
		} `json:"progress"`
	} `json:"now_playing"`
}

func TestNowPlayingChangesReachHTTPThroughRevisionWatcher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var mu sync.Mutex
	commands := map[string]int{}
	media := heos.Media{Source: "900", ID: "https://media.invalid/a", QueueID: "1", Song: "Track A", Album: "Album", Artist: "Artist"}
	mediaStarted := make(chan struct{})
	releaseMedia := make(chan struct{})
	resumeMedia := sync.OnceFunc(func() { close(releaseMedia) })
	address, pin, writes := syntheticHEOSWithReplies(t, func(command string, params url.Values, payload any) any {
		mu.Lock()
		commands[command]++
		count, current := commands[command], media
		mu.Unlock()
		switch command {
		case "player/get_play_state":
			params.Set("state", "play")
		case "player/get_now_playing_media":
			if count == 2 {
				close(mediaStarted)
				select {
				case <-releaseMedia:
				case <-ctx.Done():
				}
			}
			return current
		}
		return payload
	}, func(c net.Conn, command string) {
		if command == "system/heart_beat" {
			// Denon 5.5 carries only a PID; no scalar state or metadata is in
			// this notification, so the existing targeted read must supply B.
			_, _ = fmt.Fprint(c, "{\"heos\":{\"command\":\"event/player_now_playing_changed\",\"message\":\"pid=-42\"}}\r\n")
		}
	})
	ds, err := openDevices(ctx, config.Config{Players: []config.Player{{Key: "room", Address: address, FingerprintSHA256: pin, Serial: "synthetic"}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	defer resumeMedia()
	observer := ds.reads.Devices()[0].Observer.(*heos.Observer)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for observer.Snapshot().Stale {
		select {
		case <-ctx.Done():
			t.Fatal("initial observation unavailable")
		case <-tick.C:
		}
	}
	token := strings.Repeat("m", 48)
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
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer "+token)
		response, err := httpServer.Client().Do(r)
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
	assertList := func(want mediaPlayerResponse) {
		t.Helper()
		response := get("/v1/players")
		defer func() { _ = response.Body.Close() }()
		var players []mediaPlayerResponse
		if err := json.NewDecoder(response.Body).Decode(&players); err != nil {
			t.Fatal(err)
		}
		if len(players) != 1 || !reflect.DeepEqual(players[0], want) {
			t.Fatalf("list did not converge to detail: list=%+v detail=%+v", players, want)
		}
	}
	initial := detail()
	if initial.NowPlaying == nil || initial.NowPlaying.Song != "Track A" || initial.NowPlaying.Stale || initial.NowPlaying.MediaID == "" || initial.NowPlaying.MediaID == string(media.ID) {
		t.Fatalf("initial media projection missing, stale or discloses native MID: %+v", initial.NowPlaying)
	}
	assertList(initial)
	baseline := observer.Snapshot()
	mu.Lock()
	initialCommands := maps.Clone(commands)
	mu.Unlock()
	stream := get("/v1/events")
	defer func() { _ = stream.Body.Close() }()
	reader := bufio.NewReader(stream.Body)
	var lastSequence uint64
	var epoch string
	nextEvent := func() (string, string) {
		t.Helper()
		var id, kind, data string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal("SSE ended before the expected media invalidation:", err)
			}
			line = strings.TrimSuffix(line, "\n")
			if line == "" {
				if kind == "" {
					continue
				}
				break
			}
			switch {
			case strings.HasPrefix(line, "id: "):
				id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				kind = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				data = strings.TrimPrefix(line, "data: ")
			}
		}
		gotEpoch, rawSequence, ok := strings.Cut(id, ":")
		sequence, err := strconv.ParseUint(rawSequence, 10, 64)
		if !ok || err != nil || gotEpoch == "" || epoch != "" && (gotEpoch != epoch || sequence <= lastSequence) {
			t.Fatalf("invalid SSE revision sequence: %q after %s:%d", id, epoch, lastSequence)
		}
		epoch, lastSequence = gotEpoch, sequence
		return kind, data
	}
	if kind, data := nextEvent(); kind != "snapshot_required" || data != "{}" {
		t.Fatalf("initial SSE handshake: %s %s", kind, data)
	}
	nextMedia := func(want func(mediaPlayerResponse) bool) mediaPlayerResponse {
		t.Helper()
		for {
			kind, data := nextEvent()
			if kind != "player_changed" || data != `{"player":"room"}` {
				t.Fatalf("unexpected or unauthorized SSE payload: %s %s", kind, data)
			}
			player := detail()
			if want(player) {
				assertList(player)
				return player
			}
		}
	}
	assertCommands := func(mediaReads, stateReads, heartbeats int) {
		t.Helper()
		want := maps.Clone(initialCommands)
		want["player/get_now_playing_media"] += mediaReads
		want["player/get_play_state"] += stateReads
		if heartbeats > 0 {
			want["system/heart_beat"] += heartbeats
		}
		mu.Lock()
		got := maps.Clone(commands)
		mu.Unlock()
		if !maps.Equal(got, want) || writes.Load() != 0 {
			t.Fatalf("unexpected native commands: got=%v want=%v writes=%d", got, want, writes.Load())
		}
	}
	assertUnchangedBaseline := func(player mediaPlayerResponse) {
		t.Helper()
		if player.PlaybackState != initial.PlaybackState || !reflect.DeepEqual(player.Volume, initial.Volume) || !reflect.DeepEqual(player.Muted, initial.Muted) || !reflect.DeepEqual(player.Grouped, initial.Grouped) || !player.ObservedAt.Equal(*initial.ObservedAt) {
			t.Fatalf("media update changed scalar state or full-observation age: %+v", player)
		}
	}

	mu.Lock()
	media.ID, media.QueueID, media.Song = "https://media.invalid/b", "2", "Track B"
	mu.Unlock()
	// An unauthorized identifier must be filtered before reaching this stream.
	ds.events.Publish(api.Event{Kind: "player_changed", Player: "hidden"})
	if _, err := ds.clients[0].Read(ctx, "system/heart_beat", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-mediaStarted:
	case <-ctx.Done():
		t.Fatal("metadata hint did not start a targeted native read")
	}
	stale := nextMedia(func(player mediaPlayerResponse) bool {
		return player.NowPlaying != nil && player.NowPlaying.Song == "Track A" && player.NowPlaying.Stale
	})
	if stale.Revision == initial.Revision || stale.NowPlaying.MediaID != initial.NowPlaying.MediaID {
		t.Fatal("pending verification must retain A and invalidate its revision")
	}
	assertUnchangedBaseline(stale)
	assertCommands(1, 0, 1)
	resumeMedia()
	updated := nextMedia(func(player mediaPlayerResponse) bool {
		return player.NowPlaying != nil && player.NowPlaying.Song == "Track B" && !player.NowPlaying.Stale
	})
	if updated.Revision == stale.Revision || updated.NowPlaying.MediaID == initial.NowPlaying.MediaID || updated.NowPlaying.QueueID != "2" || updated.Stale {
		t.Fatal("metadata completion failed to invalidate the stale projection:", updated.NowPlaying)
	}
	assertUnchangedBaseline(updated)
	assertCommands(1, 0, 1)
	if observer.Snapshot().Token == baseline.Token {
		t.Fatal("PID-only now-playing event did not advance the HEOS token")
	}

	for i, change := range []string{"identity only", "title only"} {
		before := observer.Snapshot()
		mu.Lock()
		if change == "identity only" {
			media.ID = "https://media.invalid/b-new-identity"
		} else {
			media.Song = "Track B corrected title"
		}
		mu.Unlock()
		// No event is sent. RefreshPlayback is an existing transition path
		// whose accepted media read retains the token and baseline time.
		if err := observer.RefreshPlayback(ctx); err != nil {
			t.Fatal(err)
		}
		after := observer.Snapshot()
		if after.Token != before.Token || !after.ObservedAt.Equal(before.ObservedAt) {
			t.Fatal("same-token regression changed unrelated revision inputs")
		}
		previous := updated
		if detail().Revision == previous.Revision {
			t.Fatalf("%s media update did not change the revision at the same HEOS token and baseline time", change)
		}
		updated = nextMedia(func(player mediaPlayerResponse) bool {
			return player.NowPlaying != nil && !player.NowPlaying.Stale && player.Revision != previous.Revision
		})
		if change == "identity only" && (updated.NowPlaying.MediaID == previous.NowPlaying.MediaID || updated.NowPlaying.Song != previous.NowPlaying.Song) {
			t.Fatal("identity-only update did not preserve title and change media identity")
		}
		if change == "title only" && (updated.NowPlaying.MediaID != previous.NowPlaying.MediaID || updated.NowPlaying.Song != "Track B corrected title") {
			t.Fatal("title-only update changed media identity or lost the new title")
		}
		assertUnchangedBaseline(updated)
		assertCommands(2+i, 1+i, 1)
	}
}
