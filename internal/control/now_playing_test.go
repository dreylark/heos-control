package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func mediaSnapshot() heos.Snapshot {
	return heos.Snapshot{
		Player: heos.Player{ID: "7", Serial: "synthetic", Model: "test"},
		State:  "play", Connected: true, Verified: true,
		ObservedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Token:      heos.Token{Generation: 1, Player: 4},
		Media:      &heos.Media{Source: "900", ID: "https://media.example.invalid/private/track?token=synthetic", QueueID: "9007199254740993", Song: "Track %20 + one", Album: "Album", Artist: "Artist"},
	}
}

func mediaReads(epoch, key string, snapshot heos.Snapshot) (*Reads, *observationStub) {
	o := &observationStub{value: snapshot}
	return NewReads(epoch, []Device{{Config: config.Player{Key: key, Serial: "synthetic", Model: "test"}, Observer: o}}, nil), o
}

func playerMediaJSON(t *testing.T, reads *Reads, key string) (Player, map[string]any) {
	t.Helper()
	p, err := reads.Player(key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(b, &body); err != nil {
		t.Fatal(err)
	}
	media, present := body["now_playing"]
	if !present {
		t.Fatal("Player must include now_playing, including explicit null")
	}
	if media == nil {
		return p, nil
	}
	object, ok := media.(map[string]any)
	if !ok {
		t.Fatalf("now_playing is not an object: %v", media)
	}
	return p, object
}

func TestPlayerNowPlayingProjection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*heos.Snapshot)
		null   bool
		stale  bool
	}{
		{name: "playing"},
		{name: "paused", change: func(s *heos.Snapshot) { s.State = heos.PlayStatePause }},
		{name: "stopped", change: func(s *heos.Snapshot) { s.State = heos.PlayStateStop }, null: true},
		{name: "unknown", change: func(s *heos.Snapshot) { s.State = heos.PlayStateUnknown }, null: true},
		{name: "offline", change: func(s *heos.Snapshot) { s.Connected = false; s.Stale = true }, null: true},
		{name: "unobserved", change: func(s *heos.Snapshot) { s.ObservedAt = time.Time{} }, null: true},
		{name: "no media", change: func(s *heos.Snapshot) { s.Media = nil }, null: true},
		{name: "wrong identity", change: func(s *heos.Snapshot) { s.Player.Serial = "other" }, null: true},
		{name: "missing player identity", change: func(s *heos.Snapshot) { s.Player.ID = "" }, null: true},
		{name: "wrong model", change: func(s *heos.Snapshot) { s.Player.Model = "other" }, null: true},
		{name: "pending verification", change: func(s *heos.Snapshot) { s.Stale = true; s.Verified = false }, stale: true},
		{name: "media unconfirmed", change: func(s *heos.Snapshot) { s.MediaStale = true }, stale: true},
		{name: "new connection", change: func(s *heos.Snapshot) { s.MediaUnavailable = true }, null: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := mediaSnapshot()
			if tc.change != nil {
				tc.change(&snapshot)
			}
			reads, _ := mediaReads("epoch", "room", snapshot)
			_, media := playerMediaJSON(t, reads, "room")
			if (media == nil) != tc.null {
				t.Fatalf("now_playing=%v; want null=%v", media, tc.null)
			}
			if tc.null {
				return
			}
			if len(media) != 6 || media["song"] != "Track %20 + one" || media["album"] != "Album" || media["artist"] != "Artist" || media["queue_id"] != "9007199254740993" || media["stale"] != tc.stale {
				t.Fatalf("incorrect media projection: %v", media)
			}
			id, ok := media["media_id"].(string)
			if !ok || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(id) {
				t.Fatalf("media_id must be a bounded opaque digest, got %q", id)
			}
		})
	}
}

func TestPlayerNowPlayingIncompleteMetadata(t *testing.T) {
	snapshot := mediaSnapshot()
	snapshot.Media = &heos.Media{}
	reads, _ := mediaReads("epoch", "room", snapshot)
	_, media := playerMediaJSON(t, reads, "room")
	if len(media) != 4 || media["song"] != "" || media["album"] != "" || media["artist"] != "" || media["stale"] != false {
		t.Fatalf("missing labels must remain an object without invented IDs: %v", media)
	}
}

func TestPlayerNowPlayingRevisionIncludesMedia(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*heos.Snapshot)
	}{
		{"song", func(s *heos.Snapshot) { s.Media.Song = "different" }},
		{"album", func(s *heos.Snapshot) { s.Media.Album = "different" }},
		{"artist", func(s *heos.Snapshot) { s.Media.Artist = "different" }},
		{"queue ID", func(s *heos.Snapshot) { s.Media.QueueID = "2" }},
		{"native media ID", func(s *heos.Snapshot) { s.Media.ID = "different" }},
		{"source ID", func(s *heos.Snapshot) { s.Media.Source = "different" }},
		{"absent", func(s *heos.Snapshot) { s.Media = nil }},
		{"stop", func(s *heos.Snapshot) { s.State = heos.PlayStateStop }},
		{"media pending", func(s *heos.Snapshot) { s.MediaStale = true }},
		{"media unavailable", func(s *heos.Snapshot) { s.MediaUnavailable = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads, observation := mediaReads("epoch", "room", mediaSnapshot())
			before, _ := reads.Player("room")
			tc.change(&observation.value)
			after, _ := reads.Player("room")
			if before.Revision == after.Revision {
				t.Fatal("media changed at the same native token and baseline time without changing revision")
			}
			again, _ := reads.Player("room")
			if after.Revision != again.Revision {
				t.Fatal("unchanged snapshot changed revision")
			}
		})
	}
}

func TestPlayerMediaIDScopeAndStability(t *testing.T) {
	reads, observation := mediaReads("epoch", "room", mediaSnapshot())
	_, initial := playerMediaJSON(t, reads, "room")
	id := initial["media_id"]
	observation.value.Media.Song = "new label"
	observation.value.Media.QueueID = "2"
	_, changed := playerMediaJSON(t, reads, "room")
	if changed["media_id"] != id {
		t.Fatal("display metadata or a different occurrence changed media identity")
	}
	for _, tc := range []struct{ epoch, key, source string }{
		{"new-epoch", "room", "900"}, {"epoch", "other-room", "900"}, {"epoch", "room", "other-source"},
	} {
		snapshot := mediaSnapshot()
		snapshot.Media.Source = heos.ID(tc.source)
		other, _ := mediaReads(tc.epoch, tc.key, snapshot)
		_, media := playerMediaJSON(t, other, tc.key)
		if media["media_id"] == id {
			t.Fatalf("media identity crossed its scope: %+v", tc)
		}
	}
	// A tuple must remain unambiguous even when native IDs contain separators.
	a, b := mediaSnapshot(), mediaSnapshot()
	a.Media.Source, a.Media.ID = "source:part", "track"
	b.Media.Source, b.Media.ID = "source", "part:track"
	ra, _ := mediaReads("epoch", "room", a)
	rb, _ := mediaReads("epoch", "room", b)
	_, ma := playerMediaJSON(t, ra, "room")
	_, mb := playerMediaJSON(t, rb, "room")
	if ma["media_id"] == mb["media_id"] {
		t.Fatal("ambiguous native identity tuple")
	}
}

func TestPlayerPlayheadIsLastSampleAndOutsideRevision(t *testing.T) {
	at := time.Unix(100, 0).UTC()
	base := mediaSnapshot()
	base.Playhead = &heos.Playhead{
		PositionMS: 0, DurationMS: 0, At: at,
		Source: base.Media.Source, Media: base.Media.ID, Queue: base.Media.QueueID,
	}
	reads, observation := mediaReads("epoch", "room", base)
	player, media := playerMediaJSON(t, reads, "room")
	progress, _ := media["progress"].(map[string]any)
	if progress["position_ms"] != float64(0) || progress["sampled_at"] != "1970-01-01T00:01:40Z" {
		t.Fatalf("zero position: %v", progress)
	}
	if _, ok := progress["duration_ms"]; ok {
		t.Fatalf("unknown duration was projected: %v", progress)
	}
	bare, _ := mediaReads("epoch", "room", mediaSnapshot())
	without, _ := bare.Player("room")
	if without.Revision != player.Revision {
		t.Fatal("adding a playhead changed revision")
	}
	observation.value.Playhead.PositionMS = 1500
	observation.value.Playhead.DurationMS = 2000
	next, media := playerMediaJSON(t, reads, "room")
	progress, _ = media["progress"].(map[string]any)
	if next.Revision != player.Revision || progress["position_ms"] != float64(1500) || progress["duration_ms"] != float64(2000) {
		t.Fatalf("playhead movement changed revision or dropped duration: rev %s %s progress %v", player.Revision, next.Revision, progress)
	}
	observation.value.Stale = true
	_, media = playerMediaJSON(t, reads, "room")
	if _, ok := media["progress"]; ok || media["stale"] != true {
		t.Fatalf("stale baseline kept a playhead: %v", media)
	}
	observation.value.Stale = false
	observation.value.MediaStale = true
	_, media = playerMediaJSON(t, reads, "room")
	if _, ok := media["progress"]; ok || media["stale"] != true {
		t.Fatalf("unverified media kept a playhead: %v", media)
	}
	observation.value.MediaStale = false
	observation.value.Playhead.Media = "other"
	_, media = playerMediaJSON(t, reads, "room")
	if _, ok := media["progress"]; ok {
		t.Fatalf("sample bound to other media was projected: %v", media)
	}
	observation.value.State = heos.PlayStateStop
	if _, media = playerMediaJSON(t, reads, "room"); media != nil {
		t.Fatalf("stop kept now_playing: %v", media)
	}
}

func TestMediaChangeFencesNewAdmissionButNotAcceptedRetry(t *testing.T) {
	c, db, device, request := fixtureCoordinator(t)
	device.mu.Lock()
	device.s.State, device.s.Media = "pause", mediaSnapshot().Media
	device.mu.Unlock()
	before, _ := c.reads.Player("room")
	request.IfMatch = fmt.Sprintf("%q", before.Revision)
	device.mu.Lock()
	device.s.Media = &heos.Media{Source: "900", ID: "track-B", Song: "Track B"}
	device.mu.Unlock()
	if _, err := c.Submit(context.Background(), request, Command{Kind: CommandKindVolume, Level: 10}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("new request using pre-media-change revision: %v", err)
	}
	after, _ := c.reads.Player("room")
	request.IfMatch = fmt.Sprintf("%q", after.Revision)
	accepted, err := c.Submit(context.Background(), request, Command{Kind: CommandKindVolume, Level: 10})
	if err != nil {
		t.Fatal(err)
	}
	if operation := awaitOperation(t, db, accepted.ID); operation.State != journal.Succeeded {
		t.Fatal(operation)
	}
	device.mu.Lock()
	device.s.Media = &heos.Media{Source: "900", ID: "track-C", Song: "Track C"}
	device.mu.Unlock()
	retry, err := c.Submit(context.Background(), request, Command{Kind: CommandKindVolume, Level: 10})
	if err != nil || retry.ID != accepted.ID {
		t.Fatalf("original accepted retry changed after media update: %+v %v", retry, err)
	}
	device.mu.Lock()
	defer device.mu.Unlock()
	if len(device.writes) != 1 {
		t.Fatalf("rejection or idempotent retry sent an extra command: %+v", device.writes)
	}
}

func TestMediaRevisionFencesQueueContinuation(t *testing.T) {
	reads, observation := mediaReads("epoch", "room", mediaSnapshot())
	before, _ := reads.Player("room")
	observation.value.Media.ID = "different-track"
	// A stale continuation is rejected before the nil native reader can be used.
	if _, err := reads.Queue(context.Background(), "room", before.Revision, 1, 1); !errors.Is(err, heos.ErrStaleReference) {
		t.Fatalf("queue traversal accepted pre-media-change revision: %v", err)
	}
}
