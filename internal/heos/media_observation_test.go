package heos

import (
	"context"
	"errors"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestObservationMediaFreshnessRequiresPlayingBaseline(t *testing.T) {
	for _, state := range []string{"play", "pause", "stop", "unknown"} {
		t.Run(state, func(t *testing.T) {
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				if commandName(u) == "player/get_play_state" {
					sendReply(c, u, url.Values{"state": {state}}, nil)
					return
				}
				observationReply(c, u, "serial-A")
			})
			c := fakeClient(t, server)
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			before, count := o.Snapshot(), server.commands.Load()
			wantStale := state == "stop" || state == "unknown"
			if before.Stale || !before.Verified || before.MediaStale != wantStale || before.MediaUnavailable {
				t.Fatalf("baseline media freshness for %s: %+v", state, before)
			}
			deliverObservationEvent(c, o, "event/player_state_changed", "pid=9007199254740993&state=play")
			after := o.Snapshot()
			if after.MediaStale != wantStale || after.Stale || !after.Verified || after.ObservedAt != before.ObservedAt || server.commands.Load() != count {
				t.Fatalf("state event changed media evidence or safety: %+v", after)
			}
		})
	}
}

func TestObservationMediaFreshnessAcrossReadPaths(t *testing.T) {
	for _, path := range []string{"full", "media", "playback"} {
		for _, state := range []string{"play", "pause", "stop", "unknown"} {
			t.Run(path+"/"+state, func(t *testing.T) {
				var nativeState atomic.Value
				nativeState.Store("play")
				server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
					if commandName(u) == "player/get_play_state" {
						sendReply(c, u, url.Values{"state": {nativeState.Load().(string)}}, nil)
						return
					}
					observationReply(c, u, "serial-A")
				})
				c := fakeClient(t, server)
				o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
				now := time.Now()
				o.now = func() time.Time { return now }
				if err := o.Refresh(context.Background()); err != nil {
					t.Fatal(err)
				}
				initial := o.Snapshot()
				deliverObservationEvent(c, o, "event/player_state_changed", "pid=9007199254740993&state=stop")
				nativeState.Store(state)
				deliverObservationEvent(c, o, "event/player_state_changed", "pid=9007199254740993&state="+state)
				if s := o.Snapshot(); !s.MediaStale || s.Stale || !s.Verified {
					t.Fatalf("state transition repaired media or invalidated control state: %+v", s)
				}
				deliverObservationEvent(c, o, "event/player_now_playing_changed", "pid=9007199254740993")
				now = now.Add(time.Second)
				count := server.commands.Load()
				if err := refreshMediaPath(o, path); err != nil {
					t.Fatal(err)
				}
				wantReads := map[string]int64{"full": 8, "media": 1, "playback": 2}[path]
				wantStale := state == "stop" || state == "unknown"
				s := o.Snapshot()
				if s.MediaStale != wantStale || s.Stale || !s.Verified || s.MediaUnavailable || server.commands.Load()-count != wantReads {
					t.Fatalf("media read changed freshness or read budget: %+v, reads=%d", s, server.commands.Load()-count)
				}
				if path != "full" && (s.ObservedAt != initial.ObservedAt || s.Queue.Items[0] != initial.Queue.Items[0]) {
					t.Fatal("targeted media read renewed full baseline or queue")
				}
				// Repeated Play, progress and scalar publication cannot make media
				// retained from Stop/unknown fresh or add a background read.
				count, at := server.commands.Load(), s.ObservedAt
				for _, event := range []struct{ command, params string }{
					{"player_state_changed", "state=play"},
					{"player_state_changed", "state=play"},
					{"player_now_playing_progress", "cur_pos=100&duration=200"},
					{"player_volume_changed", "level=21&mute=off"},
					{"repeat_mode_changed", "repeat=on_one"},
					{"shuffle_mode_changed", "shuffle=off"},
				} {
					deliverObservationEvent(c, o, "event/"+event.command, "pid=9007199254740993&"+event.params)
				}
				if err := o.ConfirmScalars(o.Snapshot()); err != nil {
					t.Fatal(err)
				}
				s = o.Snapshot()
				if s.MediaStale != wantStale || s.Stale || !s.Verified || s.ObservedAt != at || server.commands.Load() != count {
					t.Fatalf("scalar/progress publication renewed media: %+v", s)
				}
				if err := o.RefreshScalars(context.Background(), MutationKindVolume); err != nil {
					t.Fatal(err)
				}
				if s = o.Snapshot(); s.MediaStale != wantStale || s.ObservedAt != at || server.commands.Load() != count+2 {
					t.Fatalf("scalar read renewed media evidence: %+v", s)
				}
				// The existing media paths can repair presentation after a native
				// observation associated with resumed playback.
				nativeState.Store("play")
				deliverObservationEvent(c, o, "event/player_now_playing_changed", "pid=9007199254740993")
				if err := refreshMediaPath(o, path); err != nil {
					t.Fatal(err)
				}
				if s = o.Snapshot(); s.MediaStale || s.Stale || !s.Verified {
					t.Fatalf("confirmed playing media stayed stale: %+v", s)
				}
			})
		}
	}
}

func refreshMediaPath(o *Observer, path string) error {
	switch path {
	case "full":
		return o.Refresh(context.Background())
	case "playback":
		return o.RefreshPlayback(context.Background())
	default:
		return o.refresh(context.Background(), false)
	}
}

func TestObservationMediaHintRetainsStaleDataUntilReadCompletes(t *testing.T) {
	var delay atomic.Bool
	started, release := make(chan struct{}, 1), make(chan struct{}, 1)
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if delay.Load() && commandName(u) == "player/get_now_playing_media" {
			started <- struct{}{}
			<-release
			sendReply(c, u, nil, map[string]any{"sid": 1024, "mid": "track-2", "qid": 2})
			return
		}
		observationReply(c, u, "serial-A")
	})
	t.Cleanup(func() { release <- struct{}{} })
	c := fakeClient(t, server)
	o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	initial := o.Snapshot()
	deliverObservationEvent(c, o, "event/player_now_playing_changed", "pid=9007199254740993")
	if s := o.Snapshot(); !s.MediaStale || !s.Stale || s.Media.ID != initial.Media.ID {
		t.Fatalf("hint promoted unverified media: %+v", s)
	}
	delay.Store(true)
	done := make(chan error, 1)
	go func() { done <- o.refresh(context.Background(), false) }()
	<-started
	if s := o.Snapshot(); !s.MediaStale || s.Media.ID != initial.Media.ID || s.ObservedAt != initial.ObservedAt {
		t.Fatalf("pending read changed media: %+v", s)
	}
	release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s := o.Snapshot(); s.MediaStale || s.Stale || s.Media.ID != "track-2" || s.ObservedAt != initial.ObservedAt {
		t.Fatalf("successful metadata read did not publish fresh media: %+v", s)
	}
}

func TestObservationMediaReadCannotCommitAcrossNewerEvent(t *testing.T) {
	for _, path := range []string{"full", "media", "playback"} {
		for _, event := range []string{"player_state_changed", "player_now_playing_changed"} {
			t.Run(path+"/"+event, func(t *testing.T) {
				var race atomic.Bool
				server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
					if race.Load() && commandName(u) == "player/get_now_playing_media" {
						sendEvent(c, "event/"+event, "pid=9007199254740993&state=stop")
						sendReply(c, u, nil, map[string]any{"sid": 1024, "mid": "unconfirmed", "qid": 2})
						return
					}
					observationReply(c, u, "serial-A")
				})
				c := fakeClient(t, server)
				o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
				if err := o.Refresh(context.Background()); err != nil {
					t.Fatal(err)
				}
				initial := o.Snapshot()
				deliverObservationEvent(c, o, "event/player_now_playing_changed", "pid=9007199254740993")
				c.SetEventHandler(o.Notify)
				race.Store(true)
				if err := refreshMediaPath(o, path); !errors.Is(err, ErrStale) {
					t.Fatalf("read raced a newer event: %v", err)
				}
				if s := o.Snapshot(); !s.MediaStale || !s.Stale || s.Media.ID != initial.Media.ID || s.ObservedAt != initial.ObservedAt {
					t.Fatalf("racing read published media evidence: %+v", s)
				}
			})
		}
	}
}

func TestObservationReconnectionWithholdsRetainedMediaUntilBaseline(t *testing.T) {
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) { observationReply(c, u, "serial-A") })
	c := fakeClient(t, server)
	o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	initial := o.Snapshot()
	if initial.MediaUnavailable {
		t.Fatal("current connection baseline media is unavailable")
	}
	c.Interrupt()
	eventually(t, func() bool {
		_, err := c.Read(context.Background(), "system/heart_beat", nil)
		return err == nil
	})
	retained := o.Snapshot()
	if !retained.Connected || !retained.Stale || !retained.MediaUnavailable || retained.Media.ID != initial.Media.ID {
		t.Fatalf("reconnection promoted old-connection media: %+v", retained)
	}
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if current := o.Snapshot(); current.MediaUnavailable || current.MediaStale || current.Stale || current.Token.Generation <= initial.Token.Generation {
		t.Fatalf("new connection baseline did not restore media availability: %+v", current)
	}
}

func TestObservationFullMediaCommitRejectsNewerEvent(t *testing.T) {
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) { observationReply(c, u, "serial-A") })
	c := fakeClient(t, server)
	o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	var atCommit atomic.Bool
	now := time.Now()
	o.now = func() time.Time {
		// The full read captures its timestamp after its final queue read. A
		// newer event at that boundary must win before the snapshot commits.
		if atCommit.CompareAndSwap(true, false) {
			deliverObservationEvent(c, o, "event/player_state_changed", "pid=9007199254740993&state=stop")
		}
		return now
	}
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	initial := o.Snapshot()
	deliverObservationEvent(c, o, "event/player_now_playing_changed", "pid=9007199254740993")
	now = now.Add(time.Second)
	atCommit.Store(true)
	if err := o.Refresh(context.Background()); !errors.Is(err, ErrStale) {
		t.Fatalf("full observation overwrote a newer event: %v", err)
	}
	if s := o.Snapshot(); !s.MediaStale || !s.Stale || s.ObservedAt != initial.ObservedAt {
		t.Fatalf("racing full commit repaired old media: %+v", s)
	}
}
