package control

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

// A deterministic event timeline: unrelated/partial events do not finish Wait
// unless the implementation signals its wake channel. No wall-clock sleeps.
type modeClockEvent struct {
	at time.Time
	fn func()
}

type modeEventClock struct {
	now    time.Time
	events []modeClockEvent
}

func (f *modeEventClock) Now() time.Time { return f.now }
func (f *modeEventClock) Wait(ctx context.Context, d time.Duration, wake <-chan struct{}) error {
	end := f.now.Add(d)
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		select {
		case <-wake:
			return nil
		default:
		}
		if len(f.events) == 0 || f.events[0].at.After(end) {
			f.now = end
			return nil
		}
		e := f.events[0]
		f.events = f.events[1:]
		f.now = e.at
		e.fn()
	}
}

type modeEventDevice struct {
	*fakeDevice
	clock        *modeEventClock
	reads        []time.Time
	scalarReads  []time.Time
	onScalarRead func(string) error
	onWrite      func(heos.Mutation) error
	onRead       func() error
	lastGuard    heos.Guard
}

func (d *modeEventDevice) Refresh(ctx context.Context) error {
	d.reads = append(d.reads, d.clock.Now())
	if d.onRead != nil {
		if err := d.onRead(); err != nil {
			return err
		}
	}
	d.s.ObservedAt = d.clock.Now()
	return context.Cause(ctx)
}

// A mode fallback reads only mode fields and does not renew the full baseline.
func (d *modeEventDevice) RefreshScalars(ctx context.Context, kind string) error {
	d.scalarReads = append(d.scalarReads, d.clock.Now())
	if d.onScalarRead != nil {
		if err := d.onScalarRead(kind); err != nil {
			return err
		}
	}
	return context.Cause(ctx)
}
func (d *modeEventDevice) PlayerView(heos.ID) heos.View {
	return heos.View{Connected: d.s.Connected, Token: d.s.Token}
}
func (d *modeEventDevice) Write(ctx context.Context, m heos.Mutation, g heos.Guard) (heos.Response, error) {
	d.writes = append(d.writes, m)
	d.lastGuard = g
	if d.onWrite != nil {
		if err := d.onWrite(m); err != nil {
			return heos.Response{}, err
		}
	}
	// Mode application is controlled separately from the successful reply.
	if m.Kind == "mute" {
		muted := m.Muted
		d.s.Muted = &muted
	}
	return heos.Response{}, context.Cause(ctx)
}

func modeEventFixture(t *testing.T) (*Coordinator, *execution, *modeEventDevice, *modeEventClock) {
	t.Helper()
	c, j, base, _ := fixtureCoordinator(t)
	clock := &modeEventClock{now: time.Now()}
	c.clock = clock
	base.s.State, base.s.Repeat = "pause", "off"
	base.s.ObservedAt = clock.Now()
	d := &modeEventDevice{fakeDevice: base, clock: clock}
	l := c.lanes["room"]
	l.device.Observer, l.device.Client, l.writer = d, d, d
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	r := &execution{ctx: ctx, cancel: cancel, expected: base.s, now: clock.Now,
		events: map[string][]map[string]string{}, wake: make(chan struct{}, 1),
		op: journal.Operation{ID: "mode-test", Player: "room", State: journal.Running, Revision: 1}}
	j.ops[r.op.ID] = r.op
	d.handler = r.event
	return c, r, d, clock
}

func modeEvent(name, value string) heos.Event {
	return heos.Event{Command: "event/" + name + "_mode_changed", Params: url.Values{"pid": {"1"}, name: {value}}}
}

func TestModeWaitsForEventsWithoutFullReadback(t *testing.T) {
	for _, scenario := range []string{"event", "before-reply", "both-fields", "reverse-order", "missing-event", "partial-missing", "already-target", "repeat-only", "disable-shuffle", "unchanged-repeat", "unchanged-volume", "event-before-get-catches-up", "duplicate-burst", "wrong-player", "progress", "deadline"} {
		t.Run(scenario, func(t *testing.T) {
			c, r, d, clock := modeEventFixture(t)
			began := clock.Now()
			m := heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: true}
			at := func(ms int, fn func()) {
				clock.events = append(clock.events, modeClockEvent{began.Add(time.Duration(ms) * time.Millisecond), fn})
			}
			shuffle := func() { d.s.Shuffle = true; r.event(modeEvent("shuffle", "on")) }
			wantElapsed, wantFallback := 350*time.Millisecond, 0
			switch scenario {
			case "event":
				at(350, shuffle)
			case "before-reply":
				d.onWrite = func(heos.Mutation) error { shuffle(); return clock.Wait(r.ctx, 100*time.Millisecond, nil) }
				wantElapsed = 100 * time.Millisecond
			case "both-fields", "reverse-order", "partial-missing":
				d.s.Repeat, r.expected.Repeat = "on_all", "on_all"
				repeat := func() { d.s.Repeat = "off"; r.event(modeEvent("repeat", "off")) }
				if scenario == "reverse-order" {
					at(100, shuffle)
					at(400, repeat)
				} else {
					at(100, repeat)
					at(400, func() {
						d.s.Shuffle = true
						if scenario != "partial-missing" {
							r.event(modeEvent("shuffle", "on"))
						}
					})
				}
				wantElapsed = 400 * time.Millisecond
				if scenario == "partial-missing" {
					wantElapsed, wantFallback = 11*time.Second, 1
				}
			case "missing-event":
				at(350, func() { d.s.Shuffle = true })
				wantElapsed, wantFallback = 11*time.Second, 1
			case "already-target":
				d.s.Shuffle, r.expected.Shuffle = true, true
				wantElapsed = 0
			case "repeat-only":
				d.s.Repeat, r.expected.Repeat, m.Shuffle = "on_all", "on_all", false
				at(350, func() { d.s.Repeat = "off"; r.event(modeEvent("repeat", "off")) })
			case "disable-shuffle":
				d.s.Shuffle, r.expected.Shuffle, m.Shuffle = true, true, false
				at(350, func() { d.s.Shuffle = false; r.event(modeEvent("shuffle", "off")) })
			case "unchanged-repeat", "unchanged-volume":
				at(100, func() {
					e := modeEvent("repeat", "off")
					if scenario == "unchanged-volume" {
						e = heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"20"}, "mute": {"off"}}}
					}
					r.event(e)
				})
				at(350, shuffle)
			case "event-before-get-catches-up", "duplicate-burst":
				at(350, func() {
					r.event(modeEvent("shuffle", "on"))
					if scenario == "duplicate-burst" {
						for range 100 {
							r.event(modeEvent("shuffle", "on"))
						}
					}
				})
				// A hypothetical get_play_mode remains old. The complete event is
				// the observation; querying it again would introduce the original race.
			case "wrong-player", "progress":
				at(100, func() {
					e := modeEvent("shuffle", "on")
					if scenario == "wrong-player" {
						e.Params.Set("pid", "2")
					} else {
						e.Command = "event/player_now_playing_progress"
						// Denon 5.6 requires both position and duration.
						// Incomplete progress is an invalidation, not telemetry.
						e.Params = url.Values{"pid": {"1"}, "cur_pos": {"100"}, "duration": {"1000"}}
					}
					r.event(e)
				})
				at(350, shuffle)
			case "deadline":
				wantElapsed, wantFallback = 11*time.Second, 1
			}
			err := c.write(c.lanes["room"], r, m)
			if scenario == "deadline" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if len(d.reads) != 1 || len(d.scalarReads) != wantFallback || len(d.writes) != 1 || clock.Now().Sub(began) != wantElapsed {
				t.Fatalf("traffic/timing: err=%v full=%d fallback=%v writes=%d elapsed=%s want=%s", err, len(d.reads), d.scalarReads, len(d.writes), clock.Now().Sub(began), wantElapsed)
			}
			if wantFallback != 0 && d.scalarReads[0] != began.Add(11*time.Second) {
				t.Fatal("fallback began before final second", d.scalarReads)
			}
			if err == nil && (r.expected.Repeat != m.Repeat || r.expected.Shuffle != m.Shuffle || r.confirmed != 1) {
				t.Fatal("mode was not confirmed", r.expected)
			}
		})
	}
}

func TestModeWaitCancelsWithoutAnotherReadOrWrite(t *testing.T) {
	for _, scenario := range []string{"pause", "volume", "mute", "queue", "group", "gap", "gap-progress", "gap-other-player", "foreign-mode", "malformed-mode", "missing-pid", "duplicate-pid", "context", "parent-deadline", "database-down"} {
		t.Run(scenario, func(t *testing.T) {
			c, r, d, clock := modeEventFixture(t)
			began := clock.Now()
			clock.events = []modeClockEvent{{began.Add(100 * time.Millisecond), func() {
				e := modeEvent("shuffle", "on")
				switch scenario {
				case "pause":
					e = heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"pause"}}}
				case "volume", "mute":
					e = heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"21"}, "mute": {"off"}}}
					if scenario == "mute" {
						e.Params.Set("level", "20")
						e.Params.Set("mute", "on")
					}
				case "queue":
					e.Command = "event/player_queue_changed"
				case "group":
					e.Command = "event/groups_changed"
				case "gap":
					e.Gap = true
				case "gap-progress":
					e.Gap, e.Command = true, "event/player_now_playing_progress"
				case "gap-other-player":
					e.Gap = true
					e.Params.Set("pid", "2")
				case "foreign-mode":
					e = modeEvent("repeat", "on_one")
				case "malformed-mode":
					e.Params["shuffle"] = []string{"on", "off"}
				case "missing-pid":
					e.Params.Del("pid")
				case "duplicate-pid":
					e.Params["pid"] = []string{"1", "2"}
				case "context":
					r.cancel(context.Canceled)
					return
				case "parent-deadline":
					r.cancel(context.DeadlineExceeded)
					return
				case "database-down":
					j := c.db.(*memoryJournal)
					j.mu.Lock()
					j.down = true
					j.mu.Unlock()
					return
				}
				r.event(e)
			}}}
			err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: true})
			want, wantElapsed := ErrOwnership, 100*time.Millisecond
			switch scenario {
			case "context":
				want = context.Canceled
			case "parent-deadline":
				want = context.DeadlineExceeded
			case "database-down":
				want, wantElapsed = errJournalUnavailable, time.Second
			}
			if !errors.Is(err, want) || len(d.reads) != 1 || len(d.scalarReads) != 0 || len(d.writes) != 1 || clock.Now().Sub(began) != wantElapsed {
				t.Fatalf("intervention did not preempt wait: err=%v full=%d fallback=%d writes=%d elapsed=%s", err, len(d.reads), len(d.scalarReads), len(d.writes), clock.Now().Sub(began))
			}
		})
	}
}

func TestModeConfirmedChainGuardsFollowingScalarWrites(t *testing.T) {
	for _, scenario := range []string{"current", "one-second", "250ms", "audit-due", "future", "new-token", "new-generation", "disconnected", "stale", "changed-volume", "different-command", "canceled", "database-down"} {
		t.Run(scenario, func(t *testing.T) {
			c, r, d, clock := modeEventFixture(t)
			d.onWrite = func(m heos.Mutation) error {
				if m.Kind == "mode" {
					d.s.Shuffle = true
					r.event(modeEvent("shuffle", "on"))
				}
				if m.Kind == "mute" {
					muted := m.Muted
					d.s.Muted = &muted
					value := "off"
					if muted {
						value = "on"
					}
					r.event(heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"20"}, "mute": {value}}, Token: d.s.Token})
				}
				return nil
			}
			l := c.lanes["room"]
			if err := c.write(l, r, heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: true}); err != nil {
				t.Fatal(err)
			}
			if len(d.reads) != 1 {
				t.Fatal("mode performed full readback", len(d.reads))
			}
			forceRead, reject := false, false
			switch scenario {
			case "one-second":
				clock.now = clock.now.Add(time.Second)
			case "250ms":
				clock.now = clock.now.Add(250 * time.Millisecond)
			case "audit-due":
				clock.now = clock.now.Add(300 * time.Second)
				forceRead = true
			case "future":
				clock.now = clock.now.Add(-time.Millisecond)
				forceRead = true
			case "new-token":
				d.s.Token.Player++
				forceRead = true
			case "new-generation":
				d.s.Token.Generation++
				forceRead, reject = true, true
			case "disconnected":
				d.s.Connected = false
				forceRead, reject = true, true
			case "stale":
				d.s.Stale, r.expected.Stale = true, true
				forceRead, reject = true, true
			case "changed-volume":
				level := 21
				d.s.Volume = &level
				d.s.Token.Player++
				forceRead, reject = true, true
			case "canceled":
				r.cancel(context.Canceled)
				reject = true
			case "database-down":
				j := c.db.(*memoryJournal)
				j.mu.Lock()
				j.down = true
				j.mu.Unlock()
				reject = true
			}
			err := c.write(l, r, heos.Mutation{Kind: "mute", Muted: scenario == "different-command"})
			if reject {
				if err == nil || len(d.writes) != 1 {
					t.Fatalf("unsafe chain reused: %v %+v", err, d.writes)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wantReads := 1
			if forceRead {
				wantReads++
			}
			if len(d.reads) != wantReads || len(d.scalarReads) != 0 {
				t.Fatalf("full=%d fallback=%d want full=%d", len(d.reads), len(d.scalarReads), wantReads)
			}
			if remaining := time.Until(d.lastGuard.ExpiresAt); remaining <= 4*time.Second || remaining > 5*time.Second {
				t.Fatal("guard is not bounded from current validation time", d.lastGuard)
			}
			if err := c.write(l, r, heos.Mutation{Kind: "mute", Muted: false}); err != nil {
				t.Fatal(err)
			}
			if len(d.reads) != wantReads {
				t.Fatal("confirmed chain unexpectedly consumed after one scalar write", d.reads)
			}
		})
	}
}

func TestModePartialDuplicateEventsCannotRenewConfirmationDeadline(t *testing.T) {
	c, r, d, clock := modeEventFixture(t)
	d.s.Repeat, r.expected.Repeat = "on_all", "on_all"
	began := clock.Now()
	for ms := 100; ms <= 12000; ms += 100 {
		clock.events = append(clock.events, modeClockEvent{began.Add(time.Duration(ms) * time.Millisecond), func() { d.s.Repeat = "off"; r.event(modeEvent("repeat", "off")) }})
	}
	err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: true})
	if !errors.Is(err, context.DeadlineExceeded) || len(d.reads) != 1 || len(d.scalarReads) != 1 || len(d.writes) != 1 || r.confirmed != 0 || clock.Now().Sub(began) != 11*time.Second {
		t.Fatalf("partial events completed mode or extended budget: %v full=%d fallback=%d writes=%d confirmed=%d elapsed=%s", err, len(d.reads), len(d.scalarReads), len(d.writes), r.confirmed, clock.Now().Sub(began))
	}
}

func TestModeFallbackErrorsNeverReplayOrAdvancePreparation(t *testing.T) {
	for _, scenario := range []string{"read-error", "canceled", "gap", "volume-event", "generation", "disconnected", "stale", "old-repeat", "old-shuffle"} {
		t.Run(scenario, func(t *testing.T) {
			c, r, d, clock := modeEventFixture(t)
			began := clock.Now()
			readErr := errors.New("targeted read failed")
			d.onScalarRead = func(kind string) error {
				if kind != "mode" {
					t.Fatal("unexpected fallback", kind)
				}
				d.s.Repeat, d.s.Shuffle = "off", true
				switch scenario {
				case "read-error":
					return readErr
				case "canceled":
					r.cancel(context.Canceled)
				case "gap":
					r.event(heos.Event{Gap: true})
				case "volume-event":
					r.event(heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"21"}, "mute": {"off"}}})
				case "generation":
					d.s.Token.Generation++
				case "disconnected":
					d.s.Connected = false
				case "stale":
					d.s.Stale = true
				case "old-repeat":
					d.s.Repeat = "on_all"
				case "old-shuffle":
					d.s.Shuffle = false
				}
				return nil
			}
			err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: true})
			if err == nil || r.confirmed != 0 || len(d.writes) != 1 || len(d.reads) != 1 || len(d.scalarReads) != 1 || clock.Now().Sub(began) != 11*time.Second {
				t.Fatalf("bad fallback outcome: %v full=%d fallback=%d writes=%d", err, len(d.reads), len(d.scalarReads), len(d.writes))
			}
			if scenario == "read-error" && !errors.Is(err, readErr) {
				t.Fatal("read error hidden", err)
			}
		})
	}
}

func TestModeConfirmedChainAuditDetectsSilentIntervention(t *testing.T) {
	for _, field := range []string{"volume", "mute", "state", "media", "queue", "group", "generation", "identity"} {
		t.Run(field, func(t *testing.T) {
			c, r, d, clock := modeEventFixture(t)
			d.onWrite = func(heos.Mutation) error { d.s.Shuffle = true; r.event(modeEvent("shuffle", "on")); return nil }
			if err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: true}); err != nil {
				t.Fatal(err)
			}
			clock.now = clock.now.Add(300 * time.Second)
			switch field {
			case "volume":
				v := 21
				d.s.Volume = &v
			case "mute":
				v := true
				d.s.Muted = &v
			case "state":
				d.s.State = "stop"
			case "media":
				d.s.Media = &heos.Media{Source: "foreign", ID: "foreign"}
			case "queue":
				d.s.Queue.Items = []heos.Media{{ID: "foreign"}}
			case "group":
				d.s.Grouped = true
			case "generation":
				d.s.Token.Generation++
			case "identity":
				d.s.Player.Serial = "foreign"
			}
			if err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "mute", Muted: false}); err == nil || len(d.writes) != 1 || len(d.reads) != 2 {
				t.Fatalf("audit adopted intervention: %v writes=%d reads=%d", err, len(d.writes), len(d.reads))
			}
		})
	}
}

func TestModeFailedSetterDoesNotEnterConfirmation(t *testing.T) {
	for _, delivery := range []heos.Delivery{heos.NotSent, heos.Rejected, heos.Uncertain} {
		t.Run(string(delivery), func(t *testing.T) {
			c, r, d, clock := modeEventFixture(t)
			began := clock.Now()
			cause := &heos.CommandError{Delivery: delivery, Cause: heos.ErrProtocol}
			d.onWrite = func(heos.Mutation) error { return cause }
			err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: true})
			if !errors.Is(err, cause) || len(d.reads) != 1 || len(d.scalarReads) != 0 || len(d.writes) != 1 || clock.Now() != began {
				t.Fatalf("failed setter waited/replayed: %v full=%d fallback=%d writes=%d", err, len(d.reads), len(d.scalarReads), len(d.writes))
			}
		})
	}
}

func TestModeLateConfirmedEventsDoNotBorrowEarlierTargets(t *testing.T) {
	for _, reversal := range []bool{false, true} {
		t.Run(fmt.Sprint(reversal), func(t *testing.T) {
			c, r, d, _ := modeEventFixture(t)
			d.onWrite = func(heos.Mutation) error { d.s.Shuffle = true; r.event(modeEvent("shuffle", "on")); return nil }
			if err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: true}); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				r.event(modeEvent("repeat", "off"))
				r.event(modeEvent("shuffle", "on"))
			}
			if r.ctx.Err() != nil || r.observationPending() {
				t.Fatal("unchanged duplicates canceled or requested reads")
			}
			d.onWrite = func(heos.Mutation) error {
				// The old value may arrive before this step reaches its target.
				r.event(modeEvent("shuffle", "on"))
				d.s.Shuffle = false
				r.event(modeEvent("shuffle", "off"))
				if reversal {
					r.event(modeEvent("shuffle", "on"))
				}
				return nil
			}
			err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: false})
			if reversal {
				if !errors.Is(err, ErrOwnership) {
					t.Fatal("confirmed field reversal accepted", err)
				}
			} else if err != nil {
				t.Fatal("unchanged old field was not allowed while pending", err)
			}
			if len(d.reads) != 1 || len(d.scalarReads) != 0 || len(d.writes) != 2 {
				t.Fatalf("duplicate traffic full=%d fallback=%d writes=%d", len(d.reads), len(d.scalarReads), len(d.writes))
			}
		})
	}
}
