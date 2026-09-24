package heos

import (
	"context"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestDelayedProgressCannotCrossObservationBoundary(t *testing.T) {
	for _, path := range []string{"full with event", "full without event", "full same media", "playback", "metadata", "reconnect"} {
		t.Run(path, func(t *testing.T) {
			var media atomic.Value
			media.Store(Media{Source: "1024", ID: "track-A", QueueID: "1"})
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				if commandName(u) == "player/get_now_playing_media" {
					sendReply(c, u, nil, media.Load())
					return
				}
				observationReply(c, u, "serial-A")
			})
			c := fakeClient(t, server)
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			now := time.Unix(1_700_000_000, 0).UTC()
			o.now = func() time.Time { return now }
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			before := o.Snapshot()
			if path == "metadata" {
				deliverObservationEvent(c, o, "event/player_now_playing_changed", "pid=9007199254740993")
			}
			// The native client has received the sample, but the ordinary consumer
			// has not yet delivered it to the observer.
			c.event(Response{Command: string(EventProgress), Params: url.Values{
				"pid": {string(before.Player.ID)}, "cur_pos": {"90000"}, "duration": {"100000"},
			}})
			if path == "full with event" {
				c.event(Response{Command: string(EventNowPlaying), Params: url.Values{"pid": {string(before.Player.ID)}}})
			}
			if path != "full same media" && path != "reconnect" {
				media.Store(Media{Source: "1024", ID: "track-B", QueueID: "2"})
			}
			now = now.Add(time.Second)
			var err error
			switch path {
			case "playback":
				err = o.RefreshPlayback(context.Background())
			case "metadata":
				err = o.refresh(context.Background(), false)
			case "reconnect":
				c.Interrupt()
				eventually(t, func() bool { return o.Refresh(context.Background()) == nil })
			default:
				err = o.Refresh(context.Background())
			}
			if err != nil {
				t.Fatal(err)
			}
			baseline := o.Snapshot()
			if baseline.Stale || baseline.Playhead != nil || *baseline.Media != media.Load().(Media) {
				t.Fatalf("invalid new baseline: %+v", baseline)
			}
			if path == "reconnect" && baseline.Token.Generation <= before.Token.Generation {
				t.Fatal("fixture did not reconnect")
			}
			commands := server.commands.Load()
			wake := len(o.wake)
			// Deliver just the old progress first. A reconnect gap, if queued,
			// must not conceal acceptance of a sample from the old connection.
			o.Notify(<-c.Events())
			if s := o.Snapshot(); s.Playhead != nil || s.Stale || s.Token != baseline.Token || s.ObservedAt != baseline.ObservedAt {
				t.Fatalf("buffered sample crossed %s: media=%+v playhead=%+v stale=%t", path, s.Media, s.Playhead, s.Stale)
			}
			if server.commands.Load() != commands || len(o.wake) != wake {
				t.Fatal("discarding old progress scheduled observation work")
			}
			// The covered media hint cannot repair an incorrectly rebound sample.
			if path == "full with event" {
				o.Notify(<-c.Events())
				if o.Snapshot().Playhead != nil || o.Snapshot().Stale {
					t.Fatal("covered media hint restored old progress")
				}
			}
			// Reconnection gaps use the ordinary reconciliation path separately.
			for len(c.Events()) > 0 {
				<-c.Events()
			}
			deliverObservationEvent(c, o, "event/player_now_playing_progress", "pid=9007199254740993&cur_pos=1&duration=200")
			if s := o.Snapshot(); s.Playhead == nil || s.Playhead.PositionMS != 1 || s.Playhead.Media != s.Media.ID || !s.Playhead.At.Equal(now) || s.Stale || s.Token != baseline.Token || s.ObservedAt != baseline.ObservedAt {
				t.Fatalf("new sample was not accepted after %s: %+v", path, s)
			}
		})
	}
}

func TestProgressRequiresCurrentEventHistoryButAllowsNewWriteStamp(t *testing.T) {
	for _, field := range []string{"generation", "revision", "catalog", "player", "write"} {
		t.Run(field, func(t *testing.T) {
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) { observationReply(c, u, "serial-A") })
			c := fakeClient(t, server)
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			before := o.Snapshot()
			c.event(Response{Command: string(EventProgress), Params: url.Values{
				"pid": {string(before.Player.ID)}, "cur_pos": {"5"}, "duration": {"10"},
			}})
			e := <-c.Events()
			switch field {
			case "generation":
				e.Token.Generation--
			case "revision":
				e.Token.Revision--
			case "catalog":
				e.Token.Catalog--
			case "player":
				e.Token.Player++
			case "write":
				e.Token.Write++
			}
			o.Notify(e)
			s := o.Snapshot()
			if (s.Playhead != nil) != (field == "write") || s.Stale || s.Token != before.Token || s.ObservedAt != before.ObservedAt || len(o.wake) != 0 {
				t.Fatalf("progress with different %s changed observation incorrectly: %+v", field, s)
			}
		})
	}
}
