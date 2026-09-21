package api

import (
	"encoding/json"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/heos"
)

type nowPlayingDevice struct {
	preflightDevice
	snapshot      heos.Snapshot
	snapshotCalls int
}

func (d *nowPlayingDevice) Snapshot() heos.Snapshot {
	d.snapshotCalls++
	return d.snapshot
}

func nowPlayingAPI(t *testing.T) (*Server, *nowPlayingDevice, *nowPlayingDevice) {
	t.Helper()
	s := testAPI(t)
	v, muted, ceiling := 10, false, 40
	d := &nowPlayingDevice{snapshot: heos.Snapshot{
		Player: heos.Player{ID: "1", Serial: "serial"}, State: "play", Volume: &v, Muted: &muted,
		Connected: true, Verified: true, ObservedAt: time.Unix(100, 0),
		Media: &heos.Media{Source: "private-source", ID: "https://private.invalid/song.mp3?secret=token", QueueID: "7", Song: "Song", Album: "Album", Artist: "Artist"},
	}}
	foreign := &nowPlayingDevice{snapshot: d.snapshot}
	foreign.snapshot.Media = &heos.Media{ID: "private-foreign-id", Song: "Private foreign title"}
	s.reads = control.NewReads("epoch", []control.Device{
		{Config: config.Player{Key: "room", Serial: "serial", WritesEnabled: true, VolumeCeiling: &ceiling}, Observer: d, Client: d, Catalogs: map[string]control.Browser{"music": d}},
		{Config: config.Player{Key: "other", Serial: "serial"}, Observer: foreign},
	}, []config.Source{{Key: "music", Player: "room", Name: "Gerbera"}})
	return s, d, foreign
}

func nowPlayingRequest(t *testing.T, s *Server, path string, status int) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	method, body := "GET", ""
	if strings.HasSuffix(path, "/preflight") {
		method, body = "POST", preflightBody
	}
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+testToken)
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != status {
		t.Fatalf("%s: status=%d want=%d: %s", path, w.Code, status, w.Body)
	}
	response := w.Result()
	defer func() { _ = response.Body.Close() }()
	if ok, errs := s.validator.ValidateHttpResponse(r, response); !ok {
		t.Fatalf("%s response contract: %v; body=%s", path, errs, w.Body)
	}
	if status != 200 {
		return w, nil
	}
	var player map[string]any
	if path == "/v1/players" {
		var players []map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &players); err != nil {
			t.Fatal(err)
		}
		if len(players) != 1 || players[0]["key"] != "room" {
			t.Fatalf("authorized player list: %s", w.Body)
		}
		player = players[0]
	} else {
		if err := json.Unmarshal(w.Body.Bytes(), &player); err != nil {
			t.Fatal(err)
		}
		if body != "" {
			player = player["player"].(map[string]any)
		}
	}
	if path == "/v1/players/room" && w.Header().Get("ETag") != `"`+player["revision"].(string)+`"` {
		t.Fatalf("ETag must quote the body revision: %s %s", w.Header().Get("ETag"), w.Body)
	}
	return w, player
}

func TestNowPlayingResponseContractForListDetailAndPreflight(t *testing.T) {
	opaqueMediaID := regexp.MustCompile(`^[a-f0-9]{64}$`)
	for _, name := range []string{"play", "pause", "stale", "metadata pending", "empty metadata", "stop", "unknown", "offline", "startup", "missing media", "reconnect"} {
		t.Run(name, func(t *testing.T) {
			s, d, foreign := nowPlayingAPI(t)
			wantNull := false
			switch name {
			case "pause", "stop", "unknown":
				d.snapshot.State = name
				wantNull = name != "pause"
			case "stale":
				d.snapshot.Stale, d.snapshot.Verified = true, false
			case "metadata pending":
				d.snapshot.MediaStale = true
			case "empty metadata":
				d.snapshot.Media = &heos.Media{}
			case "offline":
				d.snapshot.Connected, d.snapshot.Stale, d.snapshot.Verified = false, true, false
				wantNull = true
			case "startup":
				d.snapshot = heos.Snapshot{State: "unknown", Stale: true}
				wantNull = true
			case "missing media":
				d.snapshot.Media = nil
				wantNull = true
			case "reconnect":
				d.snapshot.MediaUnavailable = true
				wantNull = true
			}
			var revision any
			for _, path := range []string{"/v1/players", "/v1/players/room", "/v1/players/room/preflight"} {
				w, player := nowPlayingRequest(t, s, path, 200)
				if path != "/v1/players/room/preflight" && d.calls != 0 {
					t.Fatal("cached player read reached the device")
				}
				media, present := player["now_playing"]
				if !present {
					t.Fatalf("%s omitted required now_playing: %s", path, w.Body)
				}
				if revision != nil && revision != player["revision"] {
					t.Fatalf("unchanged evidence has different revisions: %v and %v", revision, player["revision"])
				}
				revision = player["revision"]
				if wantNull {
					if media != nil {
						t.Fatalf("%s must explicitly report null media: %s", name, w.Body)
					}
					continue
				}
				now, ok := media.(map[string]any)
				if !ok {
					t.Fatalf("%s must report an object: %s", name, w.Body)
				}
				if now["song"] != string(d.snapshot.Media.Song) || now["album"] != string(d.snapshot.Media.Album) || now["artist"] != string(d.snapshot.Media.Artist) || now["stale"] != (d.snapshot.Stale || d.snapshot.MediaStale) {
					t.Fatalf("incorrect current media: %s", w.Body)
				}
				if name == "empty metadata" {
					if len(now) != 4 {
						t.Fatalf("missing native IDs must be omitted: %v", now)
					}
				} else {
					id, _ := now["media_id"].(string)
					if len(now) != 6 || now["queue_id"] != "7" || !opaqueMediaID.MatchString(id) {
						t.Fatalf("incorrect opaque media identity: %v", now)
					}
				}
				for _, private := range []string{"private-source", "private.invalid", "secret=token", "Private foreign title", "private-foreign-id"} {
					if strings.Contains(w.Body.String(), private) {
						t.Fatalf("private native identity or unauthorized metadata leaked: %s", w.Body)
					}
				}
			}
			if foreign.snapshotCalls != 0 {
				t.Fatal("response accessed an unauthorized player's snapshot")
			}
			wantCalls := 1 // Existing preflight full refresh; REST snapshot reads add none.
			if name == "pause" || name == "stop" {
				wantCalls++ // An otherwise ready preflight resolves the configured source.
			}
			if d.calls != wantCalls {
				t.Fatalf("device reads=%d, want existing preflight budget=%d", d.calls, wantCalls)
			}
		})
	}
}

func TestNowPlayingMetadataChangeUpdatesDetailETag(t *testing.T) {
	s, d, _ := nowPlayingAPI(t)
	beforeResponse, before := nowPlayingRequest(t, s, "/v1/players/room", 200)
	d.snapshot.Media = &heos.Media{Source: d.snapshot.Media.Source, ID: d.snapshot.Media.ID, QueueID: "7", Song: "Corrected song", Album: "Album", Artist: "Artist"}
	afterResponse, after := nowPlayingRequest(t, s, "/v1/players/room", 200)
	if beforeResponse.Header().Get("ETag") == afterResponse.Header().Get("ETag") || before["revision"] == after["revision"] {
		t.Fatal("same-token metadata change did not change the public revision and ETag")
	}
	beforeMedia, beforeOK := before["now_playing"].(map[string]any)
	afterMedia, afterOK := after["now_playing"].(map[string]any)
	if !beforeOK || !afterOK || beforeMedia["media_id"] != afterMedia["media_id"] || afterMedia["song"] != "Corrected song" {
		t.Fatalf("display correction must retain opaque identity: before=%v after=%v", before, after)
	}
	if d.calls != 0 {
		t.Fatal("detail reads reached the device")
	}
}

func TestNowPlayingAuthorizationPrecedesSnapshotAccess(t *testing.T) {
	s, d, foreign := nowPlayingAPI(t)
	for _, path := range []string{"/v1/players/other", "/v1/players/other/preflight"} {
		nowPlayingRequest(t, s, path, 403)
	}
	s.credentials[0].Scopes = []string{"operator", "control"}
	for _, path := range []string{"/v1/players", "/v1/players/room", "/v1/players/room/preflight"} {
		nowPlayingRequest(t, s, path, 403)
	}
	if d.snapshotCalls != 0 || foreign.snapshotCalls != 0 || d.calls != 0 || foreign.calls != 0 {
		t.Fatal("unauthorized current-media request reached cached observations or devices")
	}
}

func TestNowPlayingContractRejectsMissingFieldsAndPrivateExtensions(t *testing.T) {
	s, _, _ := nowPlayingAPI(t)
	_, player := nowPlayingRequest(t, s, "/v1/players/room", 200)
	r := httptest.NewRequest("GET", "/v1/players/room", nil)
	for _, name := range []string{"omitted", "wrong type", "missing song", "missing album", "missing artist", "missing stale", "null song", "null queue id", "raw media id", "private source", "artwork", "progress", "nested timestamp"} {
		t.Run(name, func(t *testing.T) {
			media := map[string]any{"song": "Song", "album": "Album", "artist": "Artist", "stale": false}
			player["now_playing"] = media
			switch name {
			case "omitted":
				delete(player, "now_playing")
			case "wrong type":
				player["now_playing"] = "Song"
			case "missing song", "missing album", "missing artist", "missing stale":
				delete(media, strings.TrimPrefix(name, "missing "))
			case "null song":
				media["song"] = nil
			case "null queue id":
				media["queue_id"] = nil
			case "raw media id":
				media["media_id"] = "https://private.invalid/song.mp3"
			case "private source":
				media["source"] = "private-source"
			case "artwork":
				media["image_url"] = "https://private.invalid/image.jpg"
			case "progress":
				media["position"] = 10
			case "nested timestamp":
				media["observed_at"] = "2026-01-01T00:00:00Z"
			}
			body, err := json.Marshal(player)
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
			response := w.Result()
			defer func() { _ = response.Body.Close() }()
			if ok, _ := s.validator.ValidateHttpResponse(r, response); ok {
				t.Fatalf("schema accepted invalid current media: %s", body)
			}
		})
	}
}
