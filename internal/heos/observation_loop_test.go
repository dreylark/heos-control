package heos

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type observationTestClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*observationTestTimer
}
type observationTestTimer struct {
	clock   *observationTestClock
	ch      chan time.Time
	due     time.Time
	active  bool
	waiting bool
}

func (c *observationTestClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *observationTestClock) NewTimer(d time.Duration) observationTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &observationTestTimer{clock: c, ch: make(chan time.Time, 1), due: c.now.Add(d), active: true}
	c.timers = append(c.timers, t)
	return t
}
func (t *observationTestTimer) Chan() <-chan time.Time {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	// Run evaluates timer channels in select only after scheduling is complete.
	t.waiting = true
	return t.ch
}
func (t *observationTestTimer) Reset(d time.Duration) {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	select {
	case <-t.ch:
	default:
	}
	t.due = t.clock.now.Add(d)
	t.active = true
	t.waiting = false
}
func (t *observationTestTimer) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.active = false
	select {
	case <-t.ch:
	default:
	}
}
func (c *observationTestClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.timers {
		if t.active && !t.due.After(c.now) {
			t.active = false
			t.ch <- c.now
		}
	}
}

// Scheduled reports a timer only after Run reaches select following its setup.
// A published snapshot alone does not prove that both timer resets have finished;
// advancing the clock early could let a later Reset discard the pending tick.
func (c *observationTestClock) Scheduled(d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.timers {
		if t.active && t.waiting && t.due.Equal(c.now.Add(d)) {
			return true
		}
	}
	return false
}

func TestIdleObservationUsesEventsAndFiveMinuteReconciliation(t *testing.T) {
	var full, beats atomic.Int32
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/get_players" {
			full.Add(1)
		}
		if commandName(u) == "system/heart_beat" {
			beats.Add(1)
			sendReply(c, u, nil, nil)
			return
		}
		observationReply(c, u, "serial-A")
	})
	client := fakeClient(t, server)
	observer, err := NewObserver(client, Identity{Key: "room", Serial: "serial-A"}, 310*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clock := &observationTestClock{now: time.Now()}
	observer.now = clock.Now
	observer.newTimer = clock.NewTimer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- observer.Run(ctx, 300*time.Second, nil) }()
	eventually(t, func() bool { return clock.Scheduled(300*time.Second) && observer.Snapshot().Verified })
	initial := observer.Snapshot().ObservedAt
	for i := int32(1); i <= 4; i++ {
		clock.Advance(time.Minute)
		eventually(t, func() bool { return beats.Load() == i && clock.Scheduled(time.Minute) })
	}
	if full.Load() != 1 || beats.Load() != 4 || observer.Snapshot().Stale || observer.Snapshot().ObservedAt != initial {
		t.Fatalf("idle full=%d heartbeat=%d snapshot=%+v", full.Load(), beats.Load(), observer.Snapshot())
	}
	// Denon 5.6 progress and another player's 5.9 event are not target changes.
	for range 100 {
		client.event(Response{Command: "event/player_now_playing_progress", Params: url.Values{"pid": {"9007199254740993"}, "cur_pos": {"1"}, "duration": {"10"}}})
		observer.Notify(<-client.Events())
	}
	client.event(Response{Command: "event/player_volume_changed", Params: url.Values{"pid": {"another"}, "level": {"20"}, "mute": {"off"}}})
	observer.Notify(<-client.Events())
	if len(observer.wake) != 0 {
		t.Fatal("irrelevant event scheduled refresh")
	}
	clock.Advance(time.Minute)
	eventually(t, func() bool { return full.Load() == 2 && clock.Scheduled(300*time.Second) })
	// A burst invalidates immediately and causes one bounded, delayed refresh.
	for range 100 {
		client.event(Response{Command: "event/player_queue_changed", Params: url.Values{"pid": {"9007199254740993"}}})
		observer.Notify(<-client.Events())
	}
	if !observer.Snapshot().Stale {
		t.Fatal("event did not invalidate cached state")
	}
	eventually(t, func() bool { return clock.Scheduled(250 * time.Millisecond) })
	clock.Advance(250 * time.Millisecond)
	eventually(t, func() bool {
		return full.Load() == 3 && observer.Snapshot().Verified && clock.Scheduled(300*time.Second)
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func runClockedObserver(t *testing.T, server *fakeHEOS) (*Observer, *observationTestClock, *Client) {
	t.Helper()
	client := fakeClient(t, server)
	observer, err := NewObserver(client, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	if err != nil {
		t.Fatal(err)
	}
	clock := &observationTestClock{now: time.Now()}
	observer.now, observer.newTimer = clock.Now, clock.NewTimer
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { _ = observer.Run(ctx, IdleObservationInterval, nil) })
	wg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-client.Events():
				if !ok {
					return
				}
				observer.Notify(e)
			}
		}
	})
	t.Cleanup(func() { cancel(); wg.Wait() })
	return observer, clock, client
}

func TestObservationHeartbeatFailureInvalidatesUntilReconciled(t *testing.T) {
	var full atomic.Int32
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "system/heart_beat" {
			// A definite rejection keeps the socket open, but does not prove
			// that the idle observation is still usable (Denon 3.2, 4.1.5).
			_, _ = fmt.Fprint(c, "{\"heos\":{\"command\":\"system/heart_beat\",\"result\":\"fail\",\"message\":\"eid=1&text=temporary\"}}\r\n")
			return
		}
		if commandName(u) == "player/get_players" {
			full.Add(1)
		}
		observationReply(c, u, "serial-A")
	})
	observer, clock, client := runClockedObserver(t, server)
	eventually(t, func() bool { return observer.Snapshot().Verified && clock.Scheduled(IdleObservationInterval) })
	clock.Advance(time.Minute)
	eventually(t, func() bool { return clock.Scheduled(time.Second) && observer.Snapshot().Stale })
	if !client.View().Connected || observer.Snapshot().Verified || full.Load() != 1 {
		t.Fatal("heartbeat rejection was not distinguished from a complete observation")
	}
	clock.Advance(time.Second)
	eventually(t, func() bool {
		return full.Load() == 2 && observer.Snapshot().Verified && clock.Scheduled(IdleObservationInterval)
	})
}

func TestForegroundObservationFailureSchedulesRecovery(t *testing.T) {
	var wrong atomic.Bool
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		serial := "serial-A"
		if wrong.Load() {
			serial = "wrong"
		}
		observationReply(c, u, serial)
	})
	observer, clock, _ := runClockedObserver(t, server)
	eventually(t, func() bool { return observer.Snapshot().Verified && clock.Scheduled(IdleObservationInterval) })
	wrong.Store(true)
	if err := observer.Refresh(context.Background()); !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
	wrong.Store(false)
	// Failed foreground reads need recovery even without a transport event.
	eventually(t, func() bool { return clock.Scheduled(observationDebounce) })
	clock.Advance(observationDebounce)
	eventually(t, func() bool { return observer.Snapshot().Verified && clock.Scheduled(IdleObservationInterval) })
}

func TestObservationFailureBackoffIsBoundedAndIgnoresEventStorm(t *testing.T) {
	var full atomic.Int32
	var wrong atomic.Bool
	wrong.Store(true)
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/get_players" {
			full.Add(1)
		}
		serial := "serial-A"
		if wrong.Load() {
			serial = "wrong"
		}
		observationReply(c, u, serial)
	})
	observer, clock, client := runClockedObserver(t, server)
	for i, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		eventually(t, func() bool { return clock.Scheduled(delay) })
		if full.Load() != int32(i+1) || !observer.Snapshot().Stale {
			t.Fatalf("attempts=%d, snapshot=%+v", full.Load(), observer.Snapshot())
		}
		client.event(Response{Command: "event/groups_changed"})
		clock.Advance(delay)
	}
	eventually(t, func() bool { return full.Load() == 8 && clock.Scheduled(30*time.Second) })
	wrong.Store(false)
	clock.Advance(30 * time.Second)
	eventually(t, func() bool { return observer.Snapshot().Verified && clock.Scheduled(IdleObservationInterval) })
	// A later failure starts again at one second after successful recovery.
	wrong.Store(true)
	clock.Advance(IdleObservationInterval)
	eventually(t, func() bool { return clock.Scheduled(time.Second) && observer.Snapshot().Stale })
}

func TestObservationEventsReuseForegroundRefreshAndDetectDisconnect(t *testing.T) {
	var full, beats atomic.Int32
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/get_players" {
			full.Add(1)
		}
		if commandName(u) == "system/heart_beat" {
			beats.Add(1)
		}
		observationReply(c, u, "serial-A")
	})
	observer, clock, client := runClockedObserver(t, server)
	eventually(t, func() bool { return observer.Snapshot().Verified && clock.Scheduled(IdleObservationInterval) })
	clock.Advance(30 * time.Second)
	client.event(Response{Command: "event/player_queue_changed", Params: url.Values{"pid": {"9007199254740993"}}})
	eventually(t, func() bool { return clock.Scheduled(observationDebounce) })
	if err := observer.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(observationDebounce)
	eventually(t, func() bool { return clock.Scheduled(IdleObservationInterval - observationDebounce) })
	if full.Load() != 2 {
		t.Fatal("event redundantly polled after a foreground refresh", full.Load())
	}
	clock.Advance(30 * time.Second)
	if err := observer.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(30 * time.Second)
	eventually(t, func() bool { return clock.Scheduled(30 * time.Second) })
	if beats.Load() != 0 {
		t.Fatal("heartbeat ignored recent foreground observation")
	}
	// EOF invalidates immediately; reconnection does not wait five minutes.
	server.mu.Lock()
	for _, conn := range server.conns {
		_ = conn.Close()
	}
	server.mu.Unlock()
	eventually(t, func() bool {
		return !observer.Snapshot().Connected && clock.Scheduled(observationDebounce)
	})
	if !observer.Snapshot().Stale {
		t.Fatal("disconnected cache remained fresh")
	}
	clock.Advance(observationDebounce)
	// The transport retains its own real-time reconnect cooldown. If it has
	// not elapsed, the observer retries with backoff instead of spinning.
	eventually(t, func() bool { return observer.Snapshot().Verified || clock.Scheduled(time.Second) })
	if !observer.Snapshot().Verified {
		clock.Advance(time.Second)
	}
	eventually(t, func() bool { return observer.Snapshot().Verified && clock.Scheduled(IdleObservationInterval) })
}
