package heos

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func observationReply(c net.Conn, u *url.URL, serial string) {
	switch commandName(u) {
	case "player/get_players":
		sendReply(c, u, nil, []any{map[string]any{"pid": 9007199254740993, "serial": serial, "name": "renamed player", "model": "Home 150"}})
	case "group/get_groups":
		sendReply(c, u, nil, []any{})
	case "player/get_play_state":
		sendReply(c, u, url.Values{"state": {"pause"}}, nil)
	case "player/get_volume":
		sendReply(c, u, url.Values{"level": {"12"}}, nil)
	case "player/get_play_mode":
		sendReply(c, u, url.Values{"repeat": {"off"}, "shuffle": {"on"}}, nil)
	case "player/get_mute":
		sendReply(c, u, url.Values{"state": {"off"}}, nil)
	case "player/get_now_playing_media":
		sendReply(c, u, nil, map[string]any{"sid": 1024, "mid": "track-1", "qid": 1, "album": "Green"})
	case "player/get_queue":
		sendReply(c, u, url.Values{"count": {"1"}, "returned": {"1"}}, []any{map[string]any{"qid": 1, "mid": "track-1", "sid": 1024, "song": "song"}})
	default:
		sendReply(c, u, nil, []any{})
	}
}

func TestIdentityAndSnapshotFreshness(t *testing.T) {
	var wrong atomic.Bool
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		serial := "serial-A"
		if wrong.Load() {
			serial = "another"
		}
		observationReply(c, u, serial)
	})
	c := fakeClient(t, s)
	o, err := NewObserver(c, Identity{Key: "living-room", Serial: "serial-A", Model: "Home 150"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	o.now = func() time.Time { return now }
	if snapshot := o.Snapshot(); !snapshot.Stale || snapshot.State != PlayStateUnknown || snapshot.Volume != nil {
		t.Fatal(snapshot)
	}
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := o.Snapshot()
	if snapshot.Stale || !snapshot.Verified || snapshot.Player.ID != "9007199254740993" || snapshot.State != PlayStatePause || snapshot.Repeat != RepeatOff || !snapshot.Shuffle || snapshot.Grouped || snapshot.Volume == nil || *snapshot.Volume != 12 || len(snapshot.Queue.Items) != 1 {
		t.Fatalf("snapshot %+v", snapshot)
	}
	*snapshot.Volume = 90
	snapshot.Queue.Items[0].Song = "changed by caller"
	if *o.Snapshot().Volume != 12 || o.Snapshot().Queue.Items[0].Song == "changed by caller" {
		t.Fatal("snapshot aliases cache")
	}
	now = now.Add(time.Second)
	if !o.Snapshot().Stale {
		t.Fatal("expired snapshot is fresh")
	}
	wrong.Store(true)
	if err := o.Refresh(context.Background()); !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
	if !o.Snapshot().Stale || o.Snapshot().Verified {
		t.Fatal("wrong physical identity is usable")
	}
}

func TestGroupsAndEventsDuringObservation(t *testing.T) {
	var change atomic.Bool
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "group/get_groups" {
			sendReply(c, u, nil, []any{map[string]any{"gid": 1, "players": []any{map[string]any{"pid": 9007199254740993, "role": "leader"}}}})
			return
		}
		if change.Load() && commandName(u) == "player/get_volume" {
			sendEvent(c, "event/player_state_changed", "pid=9007199254740993&state=play")
		}
		observationReply(c, u, "serial-A")
	})
	c := fakeClient(t, s)
	o, err := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Refresh(context.Background()); err != nil || !o.Snapshot().Grouped {
		t.Fatal(o.Snapshot(), err)
	}
	change.Store(true)
	if err := o.Refresh(context.Background()); !errors.Is(err, ErrStale) {
		t.Fatal("mixed observation accepted", err)
	}
	if !o.Snapshot().Stale {
		t.Fatal("event did not invalidate state")
	}
	c.Interrupt()
	if snapshot := o.Snapshot(); snapshot.Connected || !snapshot.Stale {
		t.Fatal("offline snapshot trusted")
	}
}

func TestDuplicateIdentityAndUnknownState(t *testing.T) {
	_, err := ResolveIdentity([]Player{{ID: "1", Serial: "same"}, {ID: "2", Serial: "same"}}, Identity{Key: "room", Serial: "same"})
	if !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/get_play_state" {
			sendReply(c, u, url.Values{"state": {"unknown"}}, nil)
			return
		}
		observationReply(c, u, "same")
	})
	o, err := NewObserver(fakeClient(t, s), Identity{Key: "room", Serial: "same"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Refresh(context.Background()); err != nil || o.Snapshot().State != PlayStateUnknown {
		t.Fatal(o.Snapshot(), err)
	}
}

func TestReconnectReconcilesIdentityAndState(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, connection int64) {
		if connection > 1 && commandName(u) == "player/get_players" {
			sendReply(c, u, nil, []any{map[string]any{"pid": "new-pid", "serial": "same", "name": "renamed again"}})
			return
		}
		if connection > 1 && u.Query().Get("pid") != "" && u.Query().Get("pid") != "new-pid" {
			t.Error("reused the previous generation's player ID")
		}
		observationReply(c, u, "same")
	})
	c := fakeClient(t, s)
	o, err := NewObserver(c, Identity{Key: "room", Serial: "same"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	old := o.Snapshot()
	c.Interrupt()
	if state := o.Snapshot(); !state.Stale || state.Verified {
		t.Fatal("disconnect preserved trusted state")
	}
	eventually(t, func() bool { return o.Refresh(context.Background()) == nil })
	current := o.Snapshot()
	if current.Stale || !current.Verified || current.Player.ID != "new-pid" || current.Token.Generation <= old.Token.Generation {
		t.Fatal(current)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx, time.Second, nil) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("observation loop ignored cancellation")
	}
}

func TestObservationEventScope(t *testing.T) {
	var mode atomic.Int32
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/get_volume" || commandName(u) == "system/heart_beat" {
			switch mode.Load() {
			case 1:
				for range 100 {
					sendEvent(c, "event/player_now_playing_progress", "pid=9007199254740993&cur_pos=1000&duration=300000")
				}
			case 2:
				sendEvent(c, "event/player_volume_changed", "pid=another-player&level=20&mute=off")
			case 3:
				sendEvent(c, "event/player_queue_changed", "pid=9007199254740993")
			case 4:
				sendEvent(c, "event/player_volume_changed", "level=20") // Unattributable event.
			}
		}
		observationReply(c, u, "serial-A")
	})
	c := fakeClient(t, s)
	o, err := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []int32{1, 2} {
		mode.Store(m)
		if err := o.Refresh(context.Background()); err != nil {
			t.Fatalf("mode %d: %v", m, err)
		}
		// Drain notifications between scenarios; gaps must still invalidate state.
		for len(c.Events()) > 0 {
			<-c.Events()
		}
		if _, err := c.Read(context.Background(), "system/heart_beat", nil); err != nil {
			t.Fatal(err)
		}
		if o.Snapshot().Stale {
			t.Fatalf("non-state event %d invalidated snapshot", m)
		}
		for len(c.Events()) > 0 {
			<-c.Events()
		}
	}
	for _, m := range []int32{3, 4} {
		mode.Store(m)
		if err := o.Refresh(context.Background()); !errors.Is(err, ErrStale) {
			t.Fatal(m, err)
		}
		if !o.Snapshot().Stale {
			t.Fatal("unsafe snapshot remained fresh")
		}
	}
}

func TestObservationWithLiteralQueueRange(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		// Inspect the wire, not Query(): URL decoding hid this firmware rejection
		// in earlier fixtures. Denon 4.2.15 shows range=0,10.
		if commandName(u) == "player/get_queue" && strings.Contains(u.RawQuery, "range=0%2C99") {
			_, _ = fmt.Fprint(c, "{\"heos\":{\"command\":\"player/get_queue\",\"result\":\"fail\",\"message\":\"eid=3&text=Missing%20Command%20arguments\"}}\r\n")
			return
		}
		observationReply(c, u, "serial-A")
	})
	o, err := NewObserver(fakeClient(t, s), Identity{Key: "room", Serial: "serial-A"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := o.Snapshot()
	if !snapshot.Verified || snapshot.Stale || len(snapshot.Queue.Items) != 1 {
		t.Fatalf("incomplete observation: %+v", snapshot)
	}
}
