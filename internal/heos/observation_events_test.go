package heos

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func deliverObservationEvent(c *Client, o *Observer, command, message string) Event {
	params, _ := url.ParseQuery(message)
	c.event(Response{Command: command, Params: params})
	e := <-c.Events()
	o.Notify(e)
	return e
}

func TestObservationAppliesCompleteEventsWithoutReads(t *testing.T) {
	var reads, callbacks atomic.Int32
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		reads.Add(1)
		observationReply(c, u, "serial-A")
	})
	c := fakeClient(t, server)
	o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	now := time.Now()
	o.now = func() time.Time { return now }
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.SetEventHandler(func(Event) { callbacks.Add(1) })
	initial, count := o.Snapshot(), reads.Load()
	now = now.Add(time.Minute)
	// Denon 5.4 and 5.9–5.11 include the complete changed values.
	for _, e := range []struct{ command, params string }{
		{"player_volume_changed", "level=20&mute=on"},
		{"player_volume_changed", "level=20&mute=on"},
		{"player_state_changed", "state=play"},
		{"repeat_mode_changed", "repeat=on_one"},
		{"shuffle_mode_changed", "shuffle=off"},
	} {
		deliverObservationEvent(c, o, "event/"+e.command, "pid=9007199254740993&"+e.params)
	}
	s := o.Snapshot()
	if s.Stale || !s.Verified || !s.EventUpdated || *s.Volume != 20 || !*s.Muted || s.State != "play" || s.Repeat != "on_one" || s.Shuffle {
		t.Fatalf("event projection: %+v", s)
	}
	if reads.Load() != count || callbacks.Load() != 5 || len(o.wake) != 0 {
		t.Fatal("events caused reads, lost ownership callback or scheduled a full read", reads.Load(), callbacks.Load())
	}
	if s.ObservedAt != initial.ObservedAt || s.Media.ID != initial.Media.ID || s.Queue.Items[0] != initial.Queue.Items[0] {
		t.Fatal("events renewed full observation or changed unrelated fields")
	}
	*s.Volume = 99
	if *o.Snapshot().Volume != 20 {
		t.Fatal("projection aliases caller memory")
	}
	// Explicit Refresh must still read everything, even with current event data.
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != count+8 || o.Snapshot().EventUpdated || *o.Snapshot().Volume != 12 {
		t.Fatal("forced read was replaced by event projection")
	}
}

func TestProgressCannotHideGapsOrMalformedTelemetry(t *testing.T) {
	for _, fault := range []string{"valid", "unknown-duration", "gap", "missing-pid", "duplicate-pid", "missing-position", "duplicate-position", "negative-position", "overflow-position", "invalid-duration", "position-after-duration"} {
		t.Run(fault, func(t *testing.T) {
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) { observationReply(c, u, "serial-A") })
			c := fakeClient(t, server)
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			before := o.Snapshot()
			params := url.Values{"pid": {string(before.Player.ID)}, "cur_pos": {"100"}, "duration": {"200"}}
			switch fault {
			case "unknown-duration":
				params.Set("duration", "0")
			case "missing-pid":
				params.Del("pid")
			case "duplicate-pid":
				params.Add("pid", "another")
			case "missing-position":
				params.Del("cur_pos")
			case "duplicate-position":
				params.Add("cur_pos", "110")
			case "negative-position":
				params.Set("cur_pos", "-1")
			case "overflow-position":
				params.Set("cur_pos", "18446744073709551616")
			case "invalid-duration":
				params.Set("duration", "unknown")
			case "position-after-duration":
				params.Set("cur_pos", "201")
			}
			c.event(Response{Command: "event/player_now_playing_progress", Params: params})
			e := <-c.Events()
			e.Gap = fault == "gap"
			o.Notify(e)
			invalid := fault != "valid" && fault != "unknown-duration"
			if o.Snapshot().Stale != invalid || (len(o.wake) != 0) != invalid || o.Snapshot().ObservedAt != before.ObservedAt {
				t.Fatal("wrong progress gap/validation behavior", fault, o.Snapshot(), len(o.wake))
			}
		})
	}
}

func TestObservationMediaEventOnlyReadsMetadataWithoutPostponingBackup(t *testing.T) {
	var full, media, queue atomic.Int32
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		switch commandName(u) {
		case "player/get_players":
			full.Add(1)
		case "player/get_queue":
			queue.Add(1)
		case "player/get_now_playing_media":
			media.Add(1)
			sendReply(c, u, nil, map[string]any{"sid": 1024, "mid": "current-track", "qid": 2})
			return
		}
		observationReply(c, u, "serial-A")
	})
	o, clock, c := runClockedObserver(t, server)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log("reads", full.Load(), media.Load(), queue.Load(), "snapshot", o.Snapshot(), "retry", clock.Scheduled(time.Second))
		}
	})
	eventually(t, func() bool { return o.Snapshot().Verified && clock.Scheduled(IdleObservationInterval) })
	initial := o.Snapshot().ObservedAt
	clock.Advance(10 * time.Second)
	// Denon 5.5 carries PID only. Its burst needs one bounded metadata read.
	for range 20 {
		c.event(Response{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"9007199254740993"}}})
		// The shared fake has a two-event buffer. Exercise coalescing without
		// deliberately overflowing it (overflow requires full reconciliation).
		eventually(t, func() bool {
			o.mu.Lock()
			defer o.mu.Unlock()
			return o.last.Token == c.PlayerView(o.last.Player.ID).Token
		})
	}
	eventually(t, func() bool {
		o.mu.Lock()
		caughtUp := o.last.Token == c.PlayerView(o.last.Player.ID).Token
		o.mu.Unlock()
		return caughtUp && clock.Scheduled(observationDebounce)
	})
	clock.Advance(observationDebounce)
	remaining := IdleObservationInterval - 10*time.Second - observationDebounce
	eventually(t, func() bool { return !o.Snapshot().Stale && clock.Scheduled(remaining) })
	if full.Load() != 1 || media.Load() != 2 || queue.Load() != 1 || !o.Snapshot().EventUpdated || o.Snapshot().ObservedAt != initial {
		t.Fatal("metadata triggered a full read or renewed its age", full.Load(), media.Load(), queue.Load(), o.Snapshot())
	}
	clock.Advance(remaining)
	eventually(t, func() bool {
		return full.Load() == 2 && !o.Snapshot().EventUpdated && clock.Scheduled(IdleObservationInterval)
	})
}

func TestObservationEventsCannotRepairMissingHistoryOrExpiredState(t *testing.T) {
	for _, scenario := range []string{"missing-event", "gap", "expired", "failed-refresh", "duplicate-level", "missing-mute", "invalid-level", "invalid-state", "duplicate-pid", "queue", "group", "unknown-event"} {
		t.Run(scenario, func(t *testing.T) {
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) { observationReply(c, u, "serial-A") })
			c := fakeClient(t, server)
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			now := time.Now()
			o.now = func() time.Time { return now }
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			command, params := "player_volume_changed", "pid=9007199254740993&level=20&mute=off"
			switch scenario {
			case "missing-event":
				c.event(Response{Command: "event/player_state_changed", Params: url.Values{"pid": {"9007199254740993"}, "state": {"stop"}}})
				<-c.Events() // The next event cannot bridge an unobserved control revision.
			case "gap":
				o.Notify(Event{Gap: true, Token: c.PlayerView("9007199254740993").Token})
			case "expired":
				now = now.Add(ObservationCacheTTL)
			case "failed-refresh":
				o.invalid = true
			case "duplicate-level":
				params += "&level=30"
			case "missing-mute":
				params = "pid=9007199254740993&level=20"
			case "invalid-level":
				params = "pid=9007199254740993&level=101&mute=off"
			case "invalid-state":
				command, params = "player_state_changed", "pid=9007199254740993&state=invalid"
			case "duplicate-pid":
				params += "&pid=other"
			case "queue":
				command, params = "player_queue_changed", "pid=9007199254740993"
			case "group":
				command, params = "groups_changed", ""
			case "unknown-event":
				command = "future_event"
			}
			deliverObservationEvent(c, o, "event/"+command, params)
			if !o.Snapshot().Stale || len(o.wake) != 1 || *o.Snapshot().Volume != 12 {
				t.Fatal("event fabricated a trusted observation", o.Snapshot())
			}
			// A subsequent well-formed event cannot clear the missing-history flag.
			deliverObservationEvent(c, o, "event/player_volume_changed", "pid=9007199254740993&level=21&mute=off")
			if !o.Snapshot().Stale {
				t.Fatal("later event repaired missing history")
			}
		})
	}
}

func TestObservationOlderEventsCannotOverwriteNewerReadback(t *testing.T) {
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) { observationReply(c, u, "serial-A") })
	c := fakeClient(t, server)
	o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.event(Response{Command: "event/player_volume_changed", Params: url.Values{"pid": {"9007199254740993"}, "level": {"20"}, "mute": {"on"}}})
	e := <-c.Events()
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	o.Notify(e)
	if *o.Snapshot().Volume != 12 || o.Snapshot().Stale || o.Snapshot().EventUpdated || len(o.wake) != 0 {
		t.Fatal("old buffered event replaced a newer full read", o.Snapshot())
	}
}

func TestObservationPendingMediaKeepsScalarUpdates(t *testing.T) {
	var reads atomic.Int32
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		reads.Add(1)
		observationReply(c, u, "serial-A")
	})
	c := fakeClient(t, server)
	o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	count, at := reads.Load(), o.Snapshot().ObservedAt
	deliverObservationEvent(c, o, "event/player_now_playing_changed", "pid=9007199254740993")
	old := deliverObservationEvent(c, o, "event/player_volume_changed", "pid=9007199254740993&level=20&mute=on")
	deliverObservationEvent(c, o, "event/player_volume_changed", "pid=9007199254740993&level=21&mute=off")
	o.Notify(old) // A delayed duplicate must not undo the later value.
	if !o.Snapshot().Stale || *o.Snapshot().Volume != 21 {
		t.Fatal("pending metadata lost scalar update")
	}
	if err := o.refresh(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	s := o.Snapshot()
	if s.Stale || !s.EventUpdated || *s.Volume != 21 || *s.Muted || s.ObservedAt != at || reads.Load() != count+1 {
		t.Fatal("metadata read replaced scalar state or read other fields", s, reads.Load())
	}
}

func TestObservationIncompleteMetadataRequiresFullRecovery(t *testing.T) {
	for _, fault := range []string{"rejected", "malformed", "concurrent-volume", "concurrent-queue"} {
		t.Run(fault, func(t *testing.T) {
			var fail atomic.Bool
			var full atomic.Int32
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				if commandName(u) == "player/get_players" {
					full.Add(1)
				}
				if fail.Load() && commandName(u) == "player/get_now_playing_media" {
					switch fault {
					case "rejected":
						_, _ = fmt.Fprint(c, "{\"heos\":{\"command\":\"player/get_now_playing_media\",\"result\":\"fail\",\"message\":\"eid=1&text=temporary\"}}\r\n")
						return
					case "malformed":
						sendReply(c, u, nil, []any{})
						return
					case "concurrent-volume":
						sendEvent(c, "event/player_volume_changed", "pid=9007199254740993&level=21&mute=off")
					case "concurrent-queue":
						sendEvent(c, "event/player_queue_changed", "pid=9007199254740993")
					}
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
			fail.Store(true)
			if err := o.refresh(context.Background(), false); err == nil || !o.Snapshot().Stale || full.Load() != 1 {
				t.Fatal("incomplete metadata was accepted", err, o.Snapshot(), full.Load())
			}
			if s := o.Snapshot(); !s.MediaStale || s.Media.ID != initial.Media.ID || s.ObservedAt != initial.ObservedAt {
				t.Fatal("failed metadata read changed presentation evidence", s)
			}
			fail.Store(false)
			if err := o.refresh(context.Background(), false); err != nil || o.Snapshot().Stale || o.Snapshot().MediaStale || o.Snapshot().EventUpdated || full.Load() != 2 {
				t.Fatal("failed partial read did not require full recovery", err, full.Load())
			}
		})
	}
}
