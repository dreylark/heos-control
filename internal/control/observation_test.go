package control

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

type reconciliationDevice struct {
	*fakeDevice
	view       heos.View
	refreshes  int
	refreshErr error
	onRefresh  func()
	queueReads int
	queuePage  heos.QueuePage
}

func (d *reconciliationDevice) PlayerView(heos.ID) heos.View { return d.view }
func (d *reconciliationDevice) Refresh(ctx context.Context) error {
	d.refreshes++
	if d.onRefresh != nil {
		d.onRefresh()
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return d.refreshErr
}

func (d *reconciliationDevice) Queue(context.Context, heos.ID, int, int) (heos.QueuePage, error) {
	d.queueReads++
	return d.queuePage, nil
}

func TestObservationCacheRequiresNewCurrentSnapshot(t *testing.T) {
	for _, name := range []string{"new", "one-second-old", "event-projection", "same", "older", "expired", "future", "stale", "disconnected", "view-disconnected", "token-changed", "forced"} {
		t.Run(name, func(t *testing.T) {
			c, _, base, _ := fixtureCoordinator(t)
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			base.s.ObservedAt = clock.Now().Add(-500 * time.Millisecond)
			base.s.Token = heos.Token{Generation: 1, Player: 2}
			d := &reconciliationDevice{fakeDevice: base, view: heos.View{Connected: true, Token: base.s.Token}}
			l := c.lanes["room"]
			l.device.Observer, l.device.Client = d, d
			r := &execution{observedAt: clock.Now().Add(-2 * time.Second), wake: make(chan struct{}, 1)}
			reuse := true
			switch name {
			case "event-projection":
				d.s.EventUpdated = true
				d.onRefresh = func() { d.s.EventUpdated = false }
			case "one-second-old":
				d.s.ObservedAt = clock.Now().Add(-time.Second)
			case "same":
				r.observedAt = d.s.ObservedAt
			case "older":
				r.observedAt = d.s.ObservedAt.Add(time.Millisecond)
			case "expired":
				d.s.ObservedAt = clock.Now().Add(-time.Second - time.Nanosecond)
			case "future":
				d.s.ObservedAt = clock.Now().Add(time.Nanosecond)
			case "stale":
				d.s.Stale = true
			case "disconnected":
				d.s.Connected = false
			case "view-disconnected":
				d.view.Connected = false
			case "token-changed":
				d.view.Token.Player++
			case "forced":
				reuse = false
			}
			snapshot, err := c.observeRun(context.Background(), l, r, reuse)
			if err != nil {
				t.Fatal(err)
			}
			wantReads := 1
			if name == "new" || name == "one-second-old" {
				wantReads = 0
			}
			if d.refreshes != wantReads {
				t.Fatalf("full reads = %d, want %d", d.refreshes, wantReads)
			}
			if snapshot.ObservedAt != d.s.ObservedAt || r.observedAt != d.s.ObservedAt {
				t.Fatal("reconciliation renewed snapshot age")
			}
		})
	}
}

func TestObservationCachedQueueStillRequiresAllPagesAtCurrentToken(t *testing.T) {
	for _, changed := range []bool{false, true} {
		name := "same-token"
		if changed {
			name = "changed-token"
		}
		t.Run(name, func(t *testing.T) {
			c, _, base, _ := fixtureCoordinator(t)
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			base.s.ObservedAt = clock.Now().Add(-500 * time.Millisecond)
			base.s.Token = heos.Token{Generation: 1, Player: 2}
			total, next := 2, 1
			base.s.Queue = heos.QueuePage{Total: &total, Next: &next, Token: base.s.Token,
				Items: []heos.Media{{ID: "first", QueueID: "1"}}}
			d := &reconciliationDevice{fakeDevice: base, view: heos.View{Connected: true, Token: base.s.Token},
				queuePage: heos.QueuePage{Total: &total, Token: base.s.Token, Items: []heos.Media{{ID: "second", QueueID: "2"}}}}
			if changed {
				d.queuePage.Token.Player++
			}
			l := c.lanes["room"]
			l.device.Observer, l.device.Client = d, d
			previous := clock.Now().Add(-2 * time.Second)
			r := &execution{bounded: true, observedAt: previous}
			snapshot, err := c.observeRun(context.Background(), l, r, true)
			if d.refreshes != 0 || d.queueReads != 1 {
				t.Fatalf("complete snapshot reuse did full reads=%d, queue page reads=%d", d.refreshes, d.queueReads)
			}
			if changed {
				if !errors.Is(err, heos.ErrStale) || r.observedAt != previous {
					t.Fatalf("changed queue token accepted: err=%v, observedAt=%s", err, r.observedAt)
				}
			} else if err != nil || len(snapshot.Queue.Items) != total || snapshot.Queue.Next != nil {
				t.Fatalf("reused queue was not completed: err=%v, queue=%+v", err, snapshot.Queue)
			}
		})
	}
}

func TestObservationCanceledContextCannotReuseSnapshot(t *testing.T) {
	c, _, base, _ := fixtureCoordinator(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	base.s.ObservedAt = clock.Now().Add(-500 * time.Millisecond)
	d := &reconciliationDevice{fakeDevice: base, view: heos.View{Connected: true}}
	l := c.lanes["room"]
	l.device.Observer, l.device.Client = d, d
	r := &execution{observedAt: clock.Now().Add(-2 * time.Second)}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("confirmation deadline expired")
	cancel(cause)
	if _, err := c.observeRun(ctx, l, r, true); !errors.Is(err, cause) {
		t.Fatalf("canceled reconciliation accepted cached state: %v", err)
	}
	if d.refreshes != 0 {
		t.Fatal("canceled reconciliation attempted a device read")
	}
}

func TestObservationEventsAfterRefreshCannotConfirmState(t *testing.T) {
	c, _, base, _ := fixtureCoordinator(t)
	base.s.EventUpdated = true
	d := &reconciliationDevice{fakeDevice: base}
	l := c.lanes["room"]
	l.device.Observer = d
	// Each read is followed by an event before Snapshot. Preserve the existing
	// three bounded read retries; never accept that projection as full readback.
	if _, err := observe(context.Background(), l, false); !errors.Is(err, heos.ErrStale) || d.refreshes != 3 {
		t.Fatal("event projection confirmed a command", err, d.refreshes)
	}
	if len(d.writes) != 0 {
		t.Fatal("read retry sent a setter")
	}
}

func TestObservationStopAfterPlayReadKeepsOriginalTransitionDeadline(t *testing.T) {
	c, _, base, _ := fixtureCoordinator(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	base.s.State = heos.PlayStatePlay
	base.s.ObservedAt = clock.Now()
	base.s.Media = &heos.Media{Source: "1024", ID: "track", QueueID: "1"}
	total := 1
	base.s.Queue = heos.QueuePage{Total: &total, Items: []heos.Media{*base.s.Media}}
	d := &reconciliationDevice{fakeDevice: base, view: heos.View{Connected: true}}
	l := c.lanes["room"]
	l.device.Observer, l.device.Client = d, d
	r := observationExecution(t, clock)
	r.bounded = true
	r.playbackDeadline = clock.Now().Add(20 * time.Second)
	fresh, err := c.observeRun(r.ctx, l, r, false)
	if err != nil {
		t.Fatal(err)
	}
	r.expected = fresh
	// Stop arrives after the completed Play snapshot and before reconciliation.
	// That earlier snapshot cannot prove Play after this new transition began.
	d.s.State = heos.PlayStateStop
	r.event(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
	deadline := r.queueWait
	d.onRefresh = func() {
		if r.queueWait != deadline {
			t.Error("an older Play snapshot cleared or renewed the first Stop deadline")
		}
		d.s.ObservedAt = clock.Now()
	}
	_, waited, err := c.awaitOwnedPlayback(l, r, fresh, fresh)
	if !errors.Is(err, errQueueTransitionTimeout) || !waited {
		t.Fatalf("older Play confirmed a subsequent Stop: waited=%t, err=%v", waited, err)
	}
	if r.queueWait != deadline || clock.Now() != deadline || d.refreshes < 2 {
		t.Fatalf("transition did not retain/read through its original deadline: now=%s, original=%s, current=%s, reads=%d",
			clock.Now(), deadline, r.queueWait, d.refreshes)
	}
}

func observationExecution(t *testing.T, clock controlClock) *execution {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(context.Canceled) })
	volume, muted := 20, false
	return &execution{ctx: ctx, cancel: cancel, now: clock.Now, wake: make(chan struct{}, 1),
		events: map[string][]map[string]string{}, queueOwned: true, automating: true,
		expected: heos.Snapshot{Player: heos.Player{ID: "1"}, State: heos.PlayStatePlay, Volume: &volume, Muted: &muted},
		lastRead: clock.Now(), lastReadAttempt: clock.Now()}
}

func mediaObservationEvent() heos.Event {
	// Denon 5.5 identifies the player only; it requests readback of MID/QID.
	return heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}}
}

func TestObservationEventDuringReadRemainsPending(t *testing.T) {
	c, _, base, _ := fixtureCoordinator(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	r := observationExecution(t, clock)
	d := &reconciliationDevice{fakeDevice: base, view: heos.View{Connected: true}, onRefresh: func() { r.event(mediaObservationEvent()) }}
	l := c.lanes["room"]
	l.device.Observer, l.device.Client = d, d
	if _, err := c.observeRun(r.ctx, l, r, false); err != nil {
		t.Fatal(err)
	}
	if !r.observationPending() {
		t.Fatal("read consumed an event that arrived after its initial revision")
	}
	began := clock.Now()
	if err := c.waitForObservation(r.ctx, r, began.Add(12*time.Second)); err != nil {
		t.Fatal(err)
	}
	if elapsed := clock.Now().Sub(began); elapsed != observationEventSpacing {
		t.Fatalf("event wait = %s, want burst spacing %s instead of the fallback", elapsed, observationEventSpacing)
	}
	d.onRefresh = nil
	if _, err := c.observeRun(r.ctx, l, r, false); err != nil {
		t.Fatal(err)
	}
	if r.observationPending() {
		t.Fatal("successful subsequent read did not reconcile the event")
	}
}

type signalledObservationClock struct {
	realClock
	entered chan struct{}
}

func (c signalledObservationClock) Wait(ctx context.Context, d time.Duration, wake <-chan struct{}) error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	return c.realClock.Wait(ctx, d, wake)
}

func TestObservationWaitHandlesIncomingEventAndCancellation(t *testing.T) {
	for _, name := range []string{"media", "pause", "volume", "gap"} {
		t.Run(name, func(t *testing.T) {
			clock := signalledObservationClock{entered: make(chan struct{}, 1)}
			c := &Coordinator{clock: clock}
			r := observationExecution(t, clock)
			// The event-spacing window has elapsed, but fallback is still 750 ms away.
			r.lastReadAttempt = clock.Now().Add(-observationEventSpacing)
			ctx, cancel := context.WithTimeout(r.ctx, 500*time.Millisecond)
			result, done := make(chan error, 1), make(chan struct{})
			go func() {
				defer close(done)
				result <- c.waitForObservation(ctx, r, clock.Now().Add(12*time.Second))
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("observation waiter did not stop")
				}
			})
			select {
			case <-clock.entered:
			case <-ctx.Done():
				t.Fatal("observation wait did not start")
			}
			event := mediaObservationEvent()
			switch name {
			case "pause":
				event = heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"pause"}}}
			case "volume":
				event = heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"19"}, "mute": {"off"}}}
			case "gap":
				event = heos.Event{Gap: true}
			}
			r.event(event)
			err := <-result
			if name == "media" && err != nil {
				t.Fatalf("event did not wake before fallback: %v", err)
			}
			if name != "media" && !errors.Is(err, ErrOwnership) {
				t.Fatalf("intervention did not cancel wait: %v", err)
			}
		})
	}
}

func TestObservationIgnoresProgressAndConfirmedVolume(t *testing.T) {
	clock := &advancingClock{now: time.Now()}
	r := observationExecution(t, clock)
	for range 100 {
		r.event(heos.Event{Command: "event/player_now_playing_progress", Params: url.Values{"pid": {"1"}, "cur_pos": {"1000"}, "duration": {"300000"}}})
		r.event(heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"20"}, "mute": {"off"}}})
	}
	if r.observationPending() || len(r.wake) != 0 || context.Cause(r.ctx) != nil {
		t.Fatal("telemetry or unchanged controls requested reconciliation/cancellation")
	}
}

func TestObservationBurstsCoalesceWithoutExtendingDeadline(t *testing.T) {
	clock := &advancingClock{now: time.Now()}
	c := &Coordinator{clock: clock}
	r := observationExecution(t, clock)
	deadline := clock.Now().Add(600 * time.Millisecond)
	for _, want := range []time.Duration{250 * time.Millisecond, 250 * time.Millisecond, 100 * time.Millisecond} {
		began := clock.Now()
		r.lastReadAttempt = began
		for range 100 {
			r.event(mediaObservationEvent())
		}
		if len(r.wake) != 1 {
			t.Fatal("event backlog was not coalesced")
		}
		if err := c.waitForObservation(r.ctx, r, deadline); err != nil {
			t.Fatal(err)
		}
		if elapsed := clock.Now().Sub(began); elapsed != want {
			t.Fatalf("burst spacing = %s, want %s", elapsed, want)
		}
	}
	if clock.Now() != deadline {
		t.Fatal("event bursts extended the original deadline")
	}
}

func TestObservationFailedReadsKeepFallbackSpacing(t *testing.T) {
	c, _, base, _ := fixtureCoordinator(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	r := observationExecution(t, clock)
	d := &reconciliationDevice{fakeDevice: base, view: heos.View{Connected: true}, refreshErr: heos.ErrStale}
	l := c.lanes["room"]
	l.device.Observer, l.device.Client = d, d
	began := clock.Now()
	for range 3 {
		if _, err := c.observeRun(r.ctx, l, r, false); !errors.Is(err, heos.ErrStale) {
			t.Fatalf("stale read error = %v", err)
		}
		if err := c.waitForObservation(r.ctx, r, began.Add(12*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := clock.Now().Sub(began); elapsed != 3*time.Second {
		t.Fatalf("failed read attempts took %s, want three one-second fallback waits", elapsed)
	}
	if r.lastRead != began {
		t.Fatal("failed reads renewed successful observation age")
	}
}
