package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func TestQueueTransitionHybridMediaPreservesOtherControls(t *testing.T) {
	for _, state := range []heos.PlayState{heos.PlayStateStop, heos.PlayStateUnknown, heos.PlayStatePlay} {
		for _, field := range []string{"queue_items", "volume", "mute", "grouped", "connection_generation"} {
			t.Run(string(state)+"/"+field, func(t *testing.T) {
				volume, muted, total := 10, false, 2
				before := heos.Snapshot{State: heos.PlayStatePlay, Volume: &volume, Muted: &muted,
					Token: heos.Token{Generation: 1}, Media: &heos.Media{Source: "1024", ID: "first", QueueID: "1"},
					Queue: heos.QueuePage{Total: &total, Items: []heos.Media{{ID: "first", QueueID: "1"}, {ID: "second", QueueID: "2"}}}}
				after := before
				after.State = state
				after.Media = &heos.Media{Source: "1024", ID: "first", QueueID: "2"}
				switch field {
				case "queue_items":
					after.Queue.Items = slices.Clone(before.Queue.Items)
					after.Queue.Items[1].ID = "foreign"
				case "volume":
					changed := 11
					after.Volume = &changed
				case "mute":
					changed := true
					after.Muted = &changed
				case "grouped":
					after.Grouped = true
				case "connection_generation":
					after.Token.Generation++
				}
				if changes := transitionChanges(before, after); !slices.Contains(changes, field) {
					t.Fatalf("hybrid MID/QID hid changed %s: %v", field, changes)
				}
			})
		}
	}
}

func TestQueueTransitionPersistentHybridMediaKeepsDeadlines(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		duration    int
	}{
		{"stop-timeout", "stop", 20}, {"unknown-timeout", "unknown", 20},
		{"stop-playback-deadline", "stop", 6}, {"unknown-playback-deadline", "unknown", 6},
		{"play-timeout", "play", 20}, {"play-playback-deadline", "play", 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, j, d, req, cmd := albumFixture(t)
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			cmd.Automation = &Automation{TargetLevel: 20, RampSeconds: 5, DurationSeconds: tc.duration}
			began := clock.Now()
			started, writesAtStop := false, 0
			clock.hook = func() {
				if clock.Now().Sub(began) < 4*time.Second {
					return
				}
				d.mu.Lock()
				if !started {
					started, writesAtStop = true, len(d.writes)
					d.s.State = heos.PlayState(tc.state)
					media := d.tracks[0]
					media.QueueID = d.tracks[1].QueueID
					d.s.Media = &media
				}
				d.mu.Unlock()
				// Duplicate notifications cannot confirm a hybrid pair or renew
				// either deadline. A natural transition need not emit Stop (5.5).
				if tc.state == "play" {
					d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
				} else {
					d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
				}
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if !started || o.State != journal.Released || o.ErrorCode != "ownership_lost" {
				t.Fatalf("persistent transitional media was accepted: started=%t, operation=%+v", started, o)
			}
			if elapsed := clock.Now().Sub(began); elapsed != time.Duration(min(16, tc.duration))*time.Second {
				t.Fatalf("transition extended or skipped its deadline: elapsed=%s", elapsed)
			}
			if len(d.writes) != writesAtStop {
				t.Fatal("persistent transitional media allowed automation or cleanup writes")
			}
		})
	}
}

// Home 150 can report a new queue position before the corresponding MID while
// state remains Play. Denon 4.2.5/4.2.15 define the pair; 5.5 only supplies PID.
func TestActivePlayTransitionPreservesEnvelope(t *testing.T) {
	for _, phase := range []struct {
		name string
		at   time.Duration
	}{{"ramp", 4 * time.Second}, {"hold", 12 * time.Second}, {"fade", 18 * time.Second}, {"full-window", 268 * time.Second}} {
		for _, order := range []string{"qid-first", "mid-first", "missing-final-event"} {
			t.Run(phase.name+"/"+order, func(t *testing.T) {
				c, j, d, req, cmd := albumFixture(t)
				clock := &advancingClock{now: time.Now()}
				c.clock = clock
				cmd.Automation = &Automation{TargetLevel: 20, RampSeconds: 10, DurationSeconds: 20, FadeSeconds: 3}
				if phase.name == "full-window" {
					cmd.Automation = &Automation{TargetLevel: 40, RampSeconds: 300, DurationSeconds: 1200, FadeSeconds: 30}
				}
				duration := time.Duration(cmd.Automation.DurationSeconds) * time.Second
				began := clock.Now()
				started, settled := false, false
				writesAtTransition := 0
				clock.hook = func() {
					elapsed := clock.Now().Sub(began)
					if elapsed < phase.at {
						return
					}
					d.mu.Lock()
					notify := false
					if !started {
						started, writesAtTransition, notify = true, len(d.writes), true
						m := d.tracks[0]
						m.QueueID = d.tracks[1].QueueID
						if order == "mid-first" {
							m = d.tracks[1]
							m.QueueID = d.tracks[0].QueueID
						}
						d.s.Media = &m // No Stop/unknown or queue-change notification.
					}
					if !settled && len(d.writes) != writesAtTransition {
						t.Error("write before media pair settled")
					}
					if !settled && elapsed >= phase.at+time.Second {
						settled = true
						m := d.tracks[1]
						d.s.Media = &m
						notify = order != "missing-final-event"
					}
					d.mu.Unlock()
					if notify {
						d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
					}
				}
				a, err := c.Submit(context.Background(), req, cmd)
				if err != nil {
					t.Fatal(err)
				}
				o := awaitOperation(t, j, a.ID)
				if o.State != journal.Succeeded || !started || !settled {
					t.Fatalf("Play-only transition lost automation: started=%t settled=%t operation=%+v", started, settled, o)
				}
				var progress PlaybackProgress
				if err := json.Unmarshal(o.Progress, &progress); err != nil {
					t.Fatal(err)
				}
				if !progress.StopAt.Equal(began.Add(duration)) || clock.Now().Sub(began) != duration {
					t.Fatal("transition shifted the original deadline", progress)
				}
				if *d.s.Volume != 0 || d.s.State != heos.PlayStateStop {
					t.Fatal("transition prevented fade/Stop", d.s)
				}
			})
		}
	}
}

func TestActivePlayTransitionRejectsIntervention(t *testing.T) {
	for _, scenario := range []string{"pause", "volume", "mute", "queue", "group", "gap", "foreign-mid", "foreign-qid", "foreign-source"} {
		t.Run(scenario, func(t *testing.T) {
			c, j, d, req, cmd := albumFixture(t)
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			began := clock.Now()
			started, intervened, writesAtTransition := false, false, 0
			clock.hook = func() {
				elapsed := clock.Now().Sub(began)
				if elapsed < time.Second || intervened {
					return
				}
				d.mu.Lock()
				event := heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}}
				if !started {
					started, writesAtTransition = true, len(d.writes)
					m := d.tracks[0]
					m.QueueID = d.tracks[1].QueueID
					d.s.Media = &m
				}
				if elapsed >= 2*time.Second {
					intervened = true
					switch scenario {
					case "pause":
						d.s.State = heos.PlayStatePause
						event.Command, event.Params = "event/player_state_changed", url.Values{"pid": {"1"}, "state": {"pause"}}
					case "volume", "mute":
						event.Command, event.Params = "event/player_volume_changed", url.Values{"pid": {"1"}, "level": {"11"}, "mute": {"off"}}
						if scenario == "mute" {
							event.Params.Set("level", "10")
							event.Params.Set("mute", "on")
						}
					case "queue":
						event.Command = "event/player_queue_changed"
					case "group":
						event.Command = "event/groups_changed"
					case "gap":
						event.Gap = true
					default:
						m := *d.s.Media
						switch scenario {
						case "foreign-mid":
							m.ID = "foreign"
						case "foreign-qid":
							m.QueueID = "100"
						case "foreign-source":
							m.Source = "foreign"
						}
						d.s.Media = &m
					}
				}
				d.mu.Unlock()
				d.handler(event)
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if !intervened || o.State != journal.Released || o.ErrorCode != "ownership_lost" || clock.Now().Sub(began) != 2*time.Second {
				t.Fatalf("intervention not exercised or delayed: %t %+v elapsed=%s", intervened, o, clock.Now().Sub(began))
			}
			if len(d.writes) != writesAtTransition {
				t.Fatal("write during transition or cleanup after intervention", d.writes)
			}
		})
	}
}

func TestPlayTransitionEventWakeAndFallback(t *testing.T) {
	for _, scenario := range []string{"event", "missing-event", "pause", "gap", "database"} {
		t.Run(scenario, func(t *testing.T) {
			c, r, d, clock := modeEventFixture(t)
			var output bytes.Buffer
			c.logger = slog.New(slog.NewJSONHandler(&output, nil))
			began := clock.Now()
			total := 2
			first := heos.Media{Source: "1024", ID: "first", QueueID: "1"}
			second := heos.Media{Source: "1024", ID: "second", QueueID: "2"}
			d.s.State, d.s.Media = "play", &first
			d.s.Queue = heos.QueuePage{Total: &total, Items: []heos.Media{first, second}}
			r.expected = d.s
			r.queueOwned, r.automating = true, true
			r.playbackDeadline, r.lastReadAttempt = began.Add(20*time.Second), began
			r.observedAt = began
			hybrid := first
			hybrid.QueueID = second.QueueID
			d.s.Media = &hybrid
			clock.events = []modeClockEvent{{began.Add(500 * time.Millisecond), func() {
				d.s.Media = &second
				e := heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}}
				switch scenario {
				case "missing-event":
					return
				case "pause":
					e.Command, e.Params = "event/player_state_changed", url.Values{"pid": {"1"}, "state": {"pause"}}
				case "gap":
					e.Gap = true
				case "database":
					j := c.db.(*memoryJournal)
					j.mu.Lock()
					j.down = true
					j.mu.Unlock()
				}
				r.event(e)
			}}}
			s, waited, err := c.awaitOwnedPlayback(c.lanes["room"], r, r.expected, d.s)
			wantElapsed, wantReads := 500*time.Millisecond, 0
			wantRules := []string{"media_pending"}
			switch scenario {
			case "event", "missing-event":
				wantRules = append(wantRules, "play_confirmed")
				wantReads = 1
				if scenario == "missing-event" {
					wantElapsed = time.Second
				}
				if err != nil || !queuedMedia(r.expected.Queue, s.Media) || !r.queueWait.IsZero() {
					t.Fatal("settled pair not confirmed", s, err)
				}
			case "database":
				if !errors.Is(err, errJournalUnavailable) {
					t.Fatal("database failure ignored", err)
				}
			default:
				if !errors.Is(err, ErrOwnership) {
					t.Fatal("intervention ignored", err)
				}
			}
			if !waited || clock.Now().Sub(began) != wantElapsed || len(d.reads) != wantReads || len(d.writes) != 0 {
				t.Fatalf("wait timing/traffic: waited=%t elapsed=%s reads=%d writes=%d", waited, clock.Now().Sub(began), len(d.reads), len(d.writes))
			}
			records := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n"))
			if len(records) != len(wantRules) {
				t.Fatal("missing or repeated transition logs", output.String())
			}
			for i, record := range records {
				var fields map[string]any
				if err := json.Unmarshal(record, &fields); err != nil {
					t.Fatal(err)
				}
				if fields["level"] != "INFO" || fields["rule"] != wantRules[i] {
					t.Fatal("transition rule missing from INFO log", fields)
				}
			}
		})
	}
}

func TestActiveQueueTransition(t *testing.T) {
	for _, scenario := range []string{"next", "missing-play-event", "unknown", "hybrid-stop", "hybrid-unknown", "hybrid-play", "stop-timeout", "duplicate-stop-timeout", "pause", "volume", "queue", "foreign-media", "foreign-mid", "generation", "deadline"} {
		t.Run(scenario, func(t *testing.T) {
			c, j, d, req, cmd := albumFixture(t)
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			cmd.Automation = &Automation{TargetLevel: 20, RampSeconds: 10, DurationSeconds: 20, FadeSeconds: 3}
			began := clock.Now()
			started, resumed := false, false
			writesAtStop := 0
			clock.hook = func() {
				elapsed := clock.Now().Sub(began)
				if elapsed < 4*time.Second {
					return
				}
				d.mu.Lock()
				handler := d.handler
				var event string
				if !started {
					started = true
					writesAtStop = len(d.writes)
					d.s.State = heos.PlayStateStop
					if scenario == "unknown" || scenario == "hybrid-unknown" {
						d.s.State = heos.PlayStateUnknown
					}
					if scenario == "hybrid-stop" || scenario == "hybrid-unknown" || scenario == "hybrid-play" {
						// Home 150 can expose the old MID with the next QID during
						// Next. Denon 5.4/5.5 do not promise atomic state/media updates.
						m := d.tracks[0]
						m.QueueID = d.tracks[1].QueueID
						d.s.Media = &m
					}
					event = "stop"
					if scenario == "unknown" {
						event = "unknown"
					}
				}
				if !resumed && len(d.writes) != writesAtStop {
					t.Error("write while awaiting play")
				}
				if scenario == "duplicate-stop-timeout" {
					event = "stop"
				}
				if elapsed >= 5*time.Second {
					switch scenario {
					case "pause":
						d.s.State, event = "pause", "pause"
					case "volume":
						v := 1
						d.s.Volume = &v
					case "queue":
						d.s.Queue.Items = nil
					case "foreign-media":
						m := d.tracks[0]
						m.Source = "foreign"
						d.s.Media = &m
					case "foreign-mid":
						m := d.tracks[0]
						m.ID = "outside-owned-queue"
						d.s.Media = &m
					case "hybrid-play":
						d.s.State = heos.PlayStatePlay
					case "generation":
						d.s.Token.Generation++
					}
				}
				if !resumed && elapsed >= 7*time.Second && (scenario == "next" || scenario == "missing-play-event" || scenario == "unknown" || scenario == "hybrid-stop" || scenario == "hybrid-unknown") {
					resumed = true
					d.s.State = heos.PlayStatePlay
					m := d.tracks[1]
					d.s.Media = &m
					if scenario == "next" {
						event = "play"
					}
				}
				d.mu.Unlock()
				if event != "" {
					handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {event}}})
				}
			}
			if scenario == "deadline" {
				cmd.Automation = &Automation{TargetLevel: 20, RampSeconds: 5, DurationSeconds: 6}
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if !started {
				t.Fatal("transition not exercised")
			}
			if scenario == "next" || scenario == "missing-play-event" || scenario == "unknown" || scenario == "hybrid-stop" || scenario == "hybrid-unknown" {
				if !resumed {
					t.Fatal("transition cancelled before play", o)
				}
				if o.State != journal.Succeeded {
					t.Fatal(o)
				}
				var p PlaybackProgress
				if err := json.Unmarshal(o.Progress, &p); err != nil {
					t.Fatal(err)
				}
				if !p.StopAt.Equal(began.Add(20*time.Second)) || clock.Now().Sub(began) != 20*time.Second {
					t.Fatal("shifted playback deadline", p)
				}
				return
			}
			if o.State != journal.Released || o.ErrorCode != "ownership_lost" {
				t.Fatal(o)
			}
			if len(d.writes) != writesAtStop {
				t.Fatal("cleanup after ownership release")
			}
			elapsed := clock.Now().Sub(began)
			if (scenario == "stop-timeout" || scenario == "duplicate-stop-timeout") && elapsed != 16*time.Second {
				t.Fatal("transition deadline", elapsed)
			}
			if scenario == "deadline" && elapsed != 6*time.Second {
				t.Fatal("past original deadline", elapsed)
			}
		})
	}
}

func TestTransportRetriesOnlyStaleReadback(t *testing.T) {
	c, j, d, req := fixtureCoordinator(t)
	written, reads := false, 0
	d.before = func(heos.Mutation) { written = true }
	c.lanes["room"].device.Observer = changingObservation{d, func() error {
		if written {
			reads++
			if reads == 1 {
				return heos.ErrStale
			}
		}
		return nil
	}}
	a, err := c.Submit(context.Background(), req, Command{Kind: CommandKindTransport, State: heos.PlayStateStop})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded || len(d.writes) != 1 || reads != 2 {
		t.Fatal(o, d.writes, reads)
	}
}

func TestQueueTransitionOverlapsVolumeCommand(t *testing.T) {
	for _, stage := range []string{"before-write", "after-write"} {
		t.Run(stage, func(t *testing.T) {
			c, j, d, req, cmd := albumFixture(t)
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			cmd.Automation = &Automation{TargetLevel: 20, RampSeconds: 10, DurationSeconds: 20, FadeSeconds: 3}
			began := clock.Now()
			started, resumed := false, false
			interrupt := func() {
				started = true
				d.mu.Lock()
				d.s.State = heos.PlayStateUnknown
				m := d.tracks[0]
				m.QueueID = d.tracks[1].QueueID
				d.s.Media = &m
				d.mu.Unlock()
				d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
			}

			d.before = func(m heos.Mutation) {
				if m.Kind != heos.MutationKindVolume || clock.Now().Sub(began) >= 10*time.Second {
					return
				}
				if started && !resumed {
					t.Error("volume write during transition", m.Level)
				}
				if m.Level == 15 || m.Level == 16 {
					t.Error("replayed missed level", m.Level)
				}
				if stage == "before-write" && m.Level == 14 {
					t.Error("issued obsolete pre-wait level")
				}
				if stage == "after-write" && m.Level == 14 {
					interrupt()
				}
			}
			clock.hook = func() {
				if stage == "before-write" && !started && clock.Now().Sub(began) >= 4*time.Second {
					interrupt()
				}
				if started && !resumed && clock.Now().Sub(began) >= 7*time.Second {
					resumed = true
					d.mu.Lock()
					d.s.State = heos.PlayStatePlay
					m := d.tracks[1]
					d.s.Media = &m
					d.mu.Unlock()
				}
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if o.State != journal.Succeeded || !resumed || clock.Now().Sub(began) != 20*time.Second {
				t.Fatal(o, resumed, clock.Now().Sub(began))
			}
		})
	}
}

type transitionHeldClock struct {
	*advancingClock
	entered chan struct{}
}

func (c *transitionHeldClock) Wait(ctx context.Context, d time.Duration, wake <-chan struct{}) error {
	if d == pendingObservationInterval {
		close(c.entered)
		<-ctx.Done()
		return context.Cause(ctx)
	}
	return c.advancingClock.Wait(ctx, d, wake)
}

func TestOperatorStopPreemptsQueueTransitionWait(t *testing.T) {
	for _, state := range []heos.PlayState{heos.PlayStateStop, heos.PlayStatePlay} {
		t.Run(string(state), func(t *testing.T) { operatorStopPreemptsQueueTransitionWait(t, state) })
	}
}

func operatorStopPreemptsQueueTransitionWait(t *testing.T, state heos.PlayState) {
	t.Helper()
	c, j, d, req, cmd := albumFixture(t)
	clock := &transitionHeldClock{advancingClock: &advancingClock{now: time.Now()}, entered: make(chan struct{})}
	c.clock = clock
	changed := false
	clock.hook = func() {
		if changed {
			return
		}
		changed = true
		d.mu.Lock()
		d.s.State = state
		if state == heos.PlayStatePlay {
			m := d.tracks[0]
			m.QueueID = d.tracks[1].QueueID
			d.s.Media = &m
		}
		d.mu.Unlock()
		if state == heos.PlayStatePlay {
			d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
		} else {
			d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-clock.entered:
	case <-time.After(time.Second):
		t.Fatal("transition wait not reached")
	}
	req.Key, req.IfMatch, req.Endpoint, req.Body = "stop", "", "/v1/players/room/stop", json.RawMessage(`{"fade_seconds":0}`)
	b, err := c.Submit(context.Background(), req, Command{Kind: CommandKindStop})
	if err != nil {
		t.Fatal(err)
	}
	if o := awaitOperation(t, j, b.ID); o.State != journal.Succeeded {
		t.Fatal(o)
	}
	if o := awaitOperation(t, j, a.ID); o.State != journal.Released {
		t.Fatal(o)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != 5 || d.writes[4].State != heos.PlayStateStop {
		t.Fatal("unexpected writes", d.writes)
	}
}
