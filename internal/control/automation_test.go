package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type playbackDevice struct{ *fakeDevice }

func (d playbackDevice) BrowseAll(context.Context, heos.ID, heos.ID) ([]heos.Item, error) {
	return []heos.Item{{Source: "900", Name: "Gerbera", Type: "heos_server"}}, nil
}

type playbackCatalog struct{}

func (playbackCatalog) Browse(context.Context, heos.ID, string, string, int) (heos.CatalogPage, error) {
	return heos.CatalogPage{}, nil
}
func (playbackCatalog) Resolve(_ heos.ID, ref string) (heos.Item, error) {
	if ref != "track-ref" {
		return heos.Item{}, heos.ErrStaleReference
	}
	return heos.Item{Source: "900", ContainerID: "green", MediaID: "track", Name: "Green", Playable: "yes", Container: "no", Type: "song"}, nil
}

func automationFixture(t *testing.T) (*Coordinator, *memoryJournal, *fakeDevice, journal.Request, Command) {
	t.Helper()
	c, j, d, req := fixtureCoordinator(t)
	l := c.lanes["room"]
	l.device.Client = playbackDevice{d}
	l.device.Catalogs = map[string]Browser{"music": playbackCatalog{}}
	c.reads.devices[0] = l.device
	c.reads.sources = []config.Source{{Key: "music", Player: "room", Name: "Gerbera"}}
	req.Method, req.Endpoint = "POST", "/v1/players/room/playback"
	cmd := Command{Kind: "playback", Level: 10, ItemRef: "track-ref", Repeat: "off", Shuffle: true, Automation: &Automation{TargetLevel: 40, RampSeconds: 300, DurationSeconds: 1200, FadeSeconds: 30}}
	req.Body, _ = json.Marshal(cmd)
	return c, j, d, req, cmd
}

func TestAutomationFullWindowAndReplay(t *testing.T) {
	for _, state := range []string{"stop", "pause"} {
		t.Run(state, func(t *testing.T) { automationFullWindowAndReplay(t, state) })
	}
}

func automationFullWindowAndReplay(t *testing.T, state string) {
	t.Helper()
	c, j, d, req, cmd := automationFixture(t)
	d.s.State = state
	p, _ := c.reads.Player("room")
	req.IfMatch = fmt.Sprintf("%q", p.Revision)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	// Preparation does not consume the playback window.
	d.before = func(m heos.Mutation) {
		if m.Kind == "queue" {
			clock.mu.Lock()
			clock.now = clock.now.Add(7 * time.Second)
			clock.mu.Unlock()
		}
	}
	began := clock.Now().Add(7 * time.Second)
	type point struct {
		at    time.Duration
		level int
	}
	var levels []point
	original := d.before
	d.before = func(m heos.Mutation) {
		original(m)
		if m.Kind == "volume" {
			levels = append(levels, point{clock.Now().Sub(began), m.Level})
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded {
		t.Fatal(o)
	}
	if got := clock.Now().Sub(began); got != 1200*time.Second {
		t.Fatal("window", got)
	}
	if len(levels) != 71 {
		t.Fatalf("expected initial + 30 ramp + 40 fade setters, got %d: %v", len(levels), levels)
	}
	for i, p := range levels {
		switch {
		case i == 0:
			if p.level != 10 {
				t.Fatal(p)
			}
		case i <= 30:
			if p.level != 10+i || p.at != time.Duration(i*10)*time.Second {
				t.Fatal(i, p)
			}
		default:
			if p.level != 70-i || p.at < 1170*time.Second || p.at > 1200*time.Second {
				t.Fatal(i, p)
			}
		}
	}
	var progress PlaybackProgress
	if err := json.Unmarshal(o.Progress, &progress); err != nil {
		t.Fatal(err)
	}
	if !progress.PlaybackStartedAt.Equal(began) || !progress.StopAt.Equal(began.Add(1200*time.Second)) || progress.Level != 0 {
		t.Fatal(progress)
	}
	if ProjectOperation(o).Playback == nil {
		t.Fatal("missing public progress")
	}
	d.mu.Lock()
	count := len(d.writes)
	last := d.writes[count-1]
	d.mu.Unlock()
	if last.Kind != "transport" || last.State != "stop" {
		t.Fatal(last)
	}
	// Replay the exact original request, whose revision is now stale.
	if replay, err := c.Submit(context.Background(), req, cmd); err != nil || replay.ID != a.ID {
		t.Fatal(replay, err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != count {
		t.Fatal("replayed playback")
	}
}

func TestVolumeEventsMatchPendingOrConfirmedState(t *testing.T) {
	for _, scenario := range []string{"duplicate", "different-level", "different-mute", "missing-mute", "after-confirmation", "after-confirmation-changed-level"} {
		t.Run(scenario, func(t *testing.T) {
			c, j, d, req, cmd := automationFixture(t)
			c.clock = &advancingClock{now: time.Now()}
			d.before = func(m heos.Mutation) {
				if m.Kind == "mode" && strings.HasPrefix(scenario, "after-confirmation") {
					// A delayed notification of the confirmed value is harmless;
					// a different value after confirmation is intervention.
					level := "10"
					if scenario == "after-confirmation-changed-level" {
						level = "11"
					}
					d.handler(heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {level}, "mute": {"off"}}})
				}
				if m.Kind != "volume" {
					return
				}
				params := url.Values{"pid": {"1"}, "level": {fmt.Sprint(m.Level)}, "mute": {"off"}}
				for range 2 {
					d.handler(heos.Event{Command: "event/player_volume_changed", Params: params})
				}
				switch scenario {
				case "different-level":
					params.Set("level", fmt.Sprint(m.Level+1))
				case "different-mute":
					params.Set("mute", "on")
				case "missing-mute":
					params.Del("mute")
				default:
					return
				}
				d.handler(heos.Event{Command: "event/player_volume_changed", Params: params})
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if scenario == "duplicate" || scenario == "after-confirmation" {
				if o.State != journal.Succeeded {
					t.Fatal("duplicate acknowledgement prevented playback", o)
				}
				return
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			wantWrites := 0
			if scenario == "after-confirmation-changed-level" {
				wantWrites = 1
			}
			if o.ErrorCode != "ownership_lost" || o.State == journal.Succeeded || len(d.writes) != wantWrites {
				t.Fatal("unexpected event retained control", o, d.writes)
			}
		})
	}
}

func TestAutomationRejectsInvalidAndBusyBeforeWrites(t *testing.T) {
	for _, kind := range []string{"duration", "fade", "overlap", "target", "ceiling", "repeat", "playing", "stale-ref"} {
		t.Run(kind, func(t *testing.T) {
			c, j, d, req, cmd := automationFixture(t)
			want := heos.ErrBounds
			switch kind {
			case "duration":
				cmd.Automation.DurationSeconds = 7201
			case "fade":
				cmd.Automation.FadeSeconds = 61
			case "overlap":
				cmd.Automation.RampSeconds = 1171
			case "target":
				cmd.Automation.TargetLevel = 9
			case "ceiling":
				cmd.Automation.TargetLevel = 41
			case "repeat":
				cmd.Repeat = "on_all"
			case "playing":
				d.s.State = "play"
				p, _ := c.reads.Player("room")
				req.IfMatch = fmt.Sprintf("%q", p.Revision)
				want = journal.ErrBusy
			case "stale-ref":
				cmd.ItemRef = "expired"
				want = heos.ErrStaleReference
			}
			if _, err := c.Submit(context.Background(), req, cmd); !errors.Is(err, want) {
				t.Fatal(err, want)
			}
			if len(d.writes) != 0 || len(j.ops) != 0 {
				t.Fatal("rejected request had side effects")
			}
		})
	}
}

func TestPausedPlaybackStillRejectsAnExistingReservation(t *testing.T) {
	c, j, d, req, cmd := automationFixture(t)
	d.s.State = "pause"
	j.ops["existing"] = journal.Operation{ID: "existing", Player: "room", State: journal.Running}
	p, _ := c.reads.Player("room")
	req.IfMatch = fmt.Sprintf("%q", p.Revision)
	if _, err := c.Submit(context.Background(), req, cmd); !errors.Is(err, journal.ErrBusy) {
		t.Fatal("paused device reservation was not respected", err)
	}
	if len(d.writes) != 0 || len(j.ops) != 1 || len(j.keys) != 0 {
		t.Fatal("busy rejection mutated device or journal")
	}
}

func TestAutomationReleasesOnInterventionEvenWithoutEvents(t *testing.T) {
	for _, kind := range []string{"volume", "mute", "pause", "stop", "unknown-qid", "foreign-source", "queue", "group", "generation", "event", "disconnect"} {
		t.Run(kind, func(t *testing.T) {
			c, j, d, req, cmd := automationFixture(t)
			clock := &advancingClock{now: time.Now()}
			start := clock.Now()
			c.clock = clock
			changed, count := false, 0
			clock.hook = func() {
				if changed || clock.Now().Sub(start) < 400*time.Second {
					return
				}
				changed = true
				d.mu.Lock()
				count = len(d.writes)
				switch kind {
				case "volume":
					v := 39
					d.s.Volume = &v
				case "mute":
					v := true
					d.s.Muted = &v
				case "pause":
					d.s.State = "pause"
				case "stop":
					d.s.State = "stop"
				case "unknown-qid":
					m := *d.s.Media
					m.QueueID = "2"
					d.s.Media = &m
				case "foreign-source":
					m := *d.s.Media
					m.Source = "999"
					d.s.Media = &m
				case "queue":
					d.s.Queue.Items = nil
				case "group":
					d.s.Grouped = true
				case "generation":
					d.s.Token.Generation++
				case "disconnect":
					d.s.Connected = false
				}
				handler := d.handler
				d.mu.Unlock()
				if kind == "event" {
					handler(heos.Event{Command: "event/player_queue_changed", Params: url.Values{"pid": {"1"}}})
				}
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if o.State == journal.Succeeded || !changed {
				t.Fatal(o, changed)
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			if len(d.writes) != count {
				t.Fatal("write after intervention", d.writes[count:])
			}
		})
	}
}

func TestAutomationDatabaseOutageDoesNotResumeOnRecovery(t *testing.T) {
	c, j, d, req, cmd := automationFixture(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	began, count, outage := clock.Now(), 0, false
	clock.hook = func() {
		if clock.Now().Sub(began) < 400*time.Second {
			return
		}
		j.mu.Lock()
		defer j.mu.Unlock()
		if !outage {
			outage, j.down = true, true
			d.mu.Lock()
			count = len(d.writes)
			d.mu.Unlock()
		} else {
			// The next wait belongs to journal-only reconciliation.
			j.down = false
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State == journal.Succeeded || o.ErrorCode != "journal_unavailable" || !outage {
		t.Fatal(o)
	}
	diagnostic := persistedDiagnostics(t, o)
	if diagnostic["reason"] != "journal_unavailable" || diagnostic["source"] != "controller" {
		t.Fatal(diagnostic)
	}
	detected, err := time.Parse(time.RFC3339Nano, diagnostic["detected_at"].(string))
	if err != nil || !detected.Before(clock.Now()) {
		t.Fatal("journal retry replaced the original detection time", diagnostic, err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != count {
		t.Fatal("device writes resumed after database recovery")
	}
}

func TestAutomationCancellationAndShutdownJoinWorker(t *testing.T) {
	for _, mode := range []string{"release", "stop_owned", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			c, j, d, req, cmd := automationFixture(t)
			clock := &heldClock{entered: make(chan struct{})}
			c.clock = clock
			requestContext, disconnect := context.WithCancel(context.Background())
			a, err := c.Submit(requestContext, req, cmd)
			disconnect()
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-clock.entered:
			case <-time.After(time.Second):
				t.Fatal("playback did not enter its owned window")
			}
			active, err := j.Active(context.Background(), req.Player)
			if err != nil || active.ID != a.ID {
				t.Fatal("reservation released early", active, err)
			}
			// An HTTP client disconnect is independent of the admitted window.
			if mode == "shutdown" {
				c.Close()
				o, _ := j.Get(context.Background(), a.ID)
				if o.State != journal.Interrupted {
					t.Fatal(o)
				}
				if diag := persistedDiagnostics(t, o); diag["reason"] != "service_stopping" || diag["phase"] != active.Phase {
					t.Fatal(diag)
				}
			} else {
				cancelRequest := req
				cancelRequest.Key, cancelRequest.IfMatch = "cancel", ""
				cancelRequest.Endpoint = "/v1/operations/" + a.ID + "/cancel"
				cancelCommand := Command{Kind: "cancel", Mode: mode, Target: a.ID}
				cancelRequest.Body, _ = json.Marshal(cancelCommand)
				b, err := c.Submit(context.Background(), cancelRequest, cancelCommand)
				if err != nil {
					t.Fatal(err)
				}
				if o := awaitOperation(t, j, b.ID); o.State != journal.Succeeded || persistedDiagnostics(t, o)["reason"] != "completed" {
					t.Fatal(o)
				}
				if o := awaitOperation(t, j, a.ID); o.State != journal.Cancelled || ProjectOperation(o).Playback == nil || persistedDiagnostics(t, o)["reason"] != "cancelled" {
					t.Fatal(o)
				}
				if retry, err := c.Submit(context.Background(), cancelRequest, cancelCommand); err != nil || retry.ID != b.ID {
					t.Fatal(retry, err)
				}
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			want := 4
			if mode == "stop_owned" {
				want++
			}
			if len(d.writes) != want {
				t.Fatal("unexpected cleanup/timer writes", d.writes)
			}
		})
	}
}

func TestAutomationZeroRampFadeAndLateTick(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprint(late), func(t *testing.T) {
			c, j, d, req, cmd := automationFixture(t)
			cmd.Automation = &Automation{TargetLevel: 40, DurationSeconds: 1}
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			if late {
				cmd.Automation.RampSeconds = 1
				clock.hook = func() { clock.mu.Lock(); clock.now = clock.now.Add(time.Hour); clock.mu.Unlock() }
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			if o := awaitOperation(t, j, a.ID); o.State != journal.Succeeded {
				t.Fatal(o)
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			want := 6 // prepare, immediate target, stop without a hidden fade
			if late {
				want = 5
			} // no catch-up ramp after the deadline
			if len(d.writes) != want || d.writes[want-1].State != "stop" {
				t.Fatal(d.writes)
			}
		})
	}
}

func TestDelayedConfirmedVolumeEventsDuringRampAndFade(t *testing.T) {
	c, j, d, req, cmd := automationFixture(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	count := 0
	clock.hook = func() {
		d.mu.Lock()
		state, level, handler := d.s.State, *d.s.Volume, d.handler
		d.mu.Unlock()
		if state == "play" {
			count++
			handler(heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {fmt.Sprint(level)}, "mute": {"off"}}})
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded || count == 0 {
		t.Fatal(o, count)
	}
}
