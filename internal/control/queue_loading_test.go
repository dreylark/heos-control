package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

// Keep the successful queue reply separate from the device's loading timeline.
// Denon 4.4.11 acknowledges replace-and-play; 5.5/5.8 only announce media/queue
// changes. The transient unknown state is observed Home 150 firmware behavior.
type loadingQueueDevice struct {
	*albumDevice
	clock        *modeEventClock
	intermediate heos.Media
	queueSentAt  time.Time
	playAt       time.Time
	onLoading    func()
	onPlay       func()
	onRefresh    func()
	reads        []time.Time
}

func (d *loadingQueueDevice) Refresh(ctx context.Context) error {
	err := d.albumDevice.Refresh(ctx)
	if !d.queueSentAt.IsZero() {
		d.reads = append(d.reads, d.clock.Now())
	}
	if d.onRefresh != nil {
		d.onRefresh()
	}
	return err
}

func (d *loadingQueueDevice) Write(ctx context.Context, m heos.Mutation, guard heos.Guard) (heos.Response, error) {
	before := d.Snapshot()
	response, err := d.albumDevice.Write(ctx, m, guard)
	if err != nil || m.Kind != "queue" {
		return response, err
	}
	d.queueSentAt = d.clock.Now()
	d.playAt = d.queueSentAt.Add(1250 * time.Millisecond)
	d.mu.Lock()
	d.s.State, d.s.Media, d.s.Queue = before.State, before.Media, before.Queue
	d.mu.Unlock()
	d.clock.events = append(d.clock.events,
		modeClockEvent{at: d.queueSentAt.Add(330 * time.Millisecond), fn: func() {
			d.setQueueState("unknown", d.intermediate)
			if d.onLoading != nil {
				d.onLoading()
			}
			d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
			d.handler(heos.Event{Command: "event/player_queue_changed", Params: url.Values{"pid": {"1"}}})
		}},
		modeClockEvent{at: d.playAt, fn: func() {
			if d.onPlay != nil {
				d.onPlay()
				return
			}
			d.setQueueState("play", d.tracks[1])
			d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play"}}})
			d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
		}},
	)
	return response, nil
}

func loadingQueueFixture(t *testing.T, initial string) (*Coordinator, *memoryJournal, *loadingQueueDevice, journal.Request, Command) {
	t.Helper()
	c, j, base, req, cmd := albumFixture(t)
	clock := &modeEventClock{now: time.Now()}
	c.clock = clock
	old := heos.Media{Source: "1024", ID: "previous-track", QueueID: "1"}
	total := 1
	base.s.State, base.s.Media = initial, &old
	base.s.Queue = heos.QueuePage{Items: []heos.Media{old}, Total: &total}
	base.s.ObservedAt = clock.Now()
	d := &loadingQueueDevice{albumDevice: base, clock: clock,
		intermediate: heos.Media{Source: "1024", ID: "loading-mid", QueueID: "2"}}
	l := c.lanes["room"]
	l.writer, l.device.Observer, l.device.Client = d, d, d
	c.reads.devices[0] = l.device
	p, err := c.reads.Player("room")
	if err != nil {
		t.Fatal(err)
	}
	req.IfMatch = fmt.Sprintf("%q", p.Revision)
	cmd.Automation = &Automation{TargetLevel: 12, RampSeconds: 2, DurationSeconds: 10, FadeSeconds: 2}
	req.Body, err = json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return c, j, d, req, cmd
}

func (d *loadingQueueDevice) setQueueState(state string, media heos.Media) {
	d.mu.Lock()
	defer d.mu.Unlock()
	total := len(d.tracks)
	d.s.State, d.s.Media = state, &media
	d.s.Queue = heos.QueuePage{Items: append([]heos.Media(nil), d.tracks...), Total: &total}
}

func TestQueueLoadingWaitsForSelectedMediaBeforeAutomation(t *testing.T) {
	for _, initial := range []string{"stop", "pause"} {
		for _, tc := range []struct {
			name string
			mid  heos.ID
		}{
			{name: "selected-mid", mid: "track-2"},
			{name: "unresolved-mid", mid: "loading-mid"},
		} {
			t.Run(initial+"/"+tc.name, func(t *testing.T) {
				c, j, d, req, cmd := loadingQueueFixture(t, initial)
				clock := d.clock
				d.intermediate.ID = tc.mid
				var output bytes.Buffer
				c.logger = slog.New(slog.NewJSONHandler(&output, nil))
				type write struct {
					at       time.Time
					mutation heos.Mutation
				}
				var writes []write
				d.before = func(m heos.Mutation) {
					writes = append(writes, write{at: clock.Now(), mutation: m})
				}

				// Reduced incident sequence: success reply -> old snapshot ->
				// unknown with the new queue and unresolved MID -> selected Play.
				// Logs contain MID hashes, so loading-mid does not assert the
				// origin or literal value of the intermediate device metadata.
				a, err := c.Submit(context.Background(), req, cmd)
				if err != nil {
					t.Fatal(err)
				}
				o := awaitOperation(t, j, a.ID)
				c.Close()
				queueWrites, peak := 0, 0
				for _, w := range writes {
					if w.mutation.Kind == "queue" {
						queueWrites++
						continue
					}
					if queueWrites > 0 && w.at.Before(d.playAt) {
						t.Errorf("write before selected Play confirmation: %+v", w)
					}
					if w.mutation.Kind == "volume" {
						peak = max(peak, w.mutation.Level)
					}
				}
				if queueWrites != 1 {
					t.Errorf("queue written %d times; want exactly once", queueWrites)
				}
				if o.State != journal.Succeeded {
					t.Fatalf("loading ended after %s: state=%s code=%s peak=%d; want confirmed Play and completed envelope\n%s",
						clock.Now().Sub(d.queueSentAt), o.State, o.ErrorCode, peak, output.String())
				}
				if peak != cmd.Automation.TargetLevel {
					t.Errorf("peak=%d; want %d", peak, cmd.Automation.TargetLevel)
				}
				last := writes[len(writes)-1].mutation
				if last.Kind != "transport" || last.State != "stop" || *d.Snapshot().Volume != 0 {
					t.Error("envelope did not finish with fade to zero and Stop", writes)
				}
				var progress PlaybackProgress
				if err := json.Unmarshal(o.Progress, &progress); err != nil {
					t.Fatal(err)
				}
				if !progress.PlaybackStartedAt.Equal(d.playAt) || !progress.StopAt.Equal(d.playAt.Add(10*time.Second)) || !clock.Now().Equal(progress.StopAt) {
					t.Error("loading consumed or extended the playback window", progress)
				}
			})
		}
	}
}

func TestQueueLoadingUnresolvedMediaBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		setup   func(*loadingQueueDevice)
		code    string
		elapsed time.Duration
	}{
		{"timeout", func(d *loadingQueueDevice) { d.onPlay = func() {} }, "device_unavailable", 12 * time.Second},
		{"duplicate-media-timeout", func(d *loadingQueueDevice) {
			d.onLoading = func() {
				d.clock.events = nil // No Play; metadata notifications continue past the deadline.
				for i := 1; i <= 140; i++ {
					d.clock.events = append(d.clock.events, modeClockEvent{at: d.clock.Now().Add(time.Duration(i) * 100 * time.Millisecond), fn: func() {
						d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
					}})
				}
			}
		}, "device_unavailable", 12 * time.Second},
		{"foreign-source", func(d *loadingQueueDevice) { d.intermediate.Source = "foreign" }, "ownership_lost", 330 * time.Millisecond},
		{"foreign-queue", func(d *loadingQueueDevice) {
			d.onLoading = func() { d.s.Queue.Items[0].ID = "foreign" }
		}, "ownership_lost", 330 * time.Millisecond},
		{"unknown-queue-total", func(d *loadingQueueDevice) {
			d.onLoading = func() { d.s.Queue.Total = nil }
		}, "ownership_lost", 330 * time.Millisecond},
		{"duplicate-queue-position", func(d *loadingQueueDevice) {
			d.onLoading = func() { d.s.Queue.Items[1].QueueID = d.s.Queue.Items[0].QueueID }
		}, "ownership_lost", 330 * time.Millisecond},
		{"stopped-unresolved-media", func(d *loadingQueueDevice) {
			d.onLoading = func() { d.s.State = "stop" }
		}, "ownership_lost", 330 * time.Millisecond},
		{"readback-volume-change", func(d *loadingQueueDevice) {
			d.onLoading = func() { v := 11; d.s.Volume = &v }
		}, "ownership_lost", 330 * time.Millisecond},
		{"manual-pause", func(d *loadingQueueDevice) {
			d.onPlay = func() {
				d.s.State = "pause"
				d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"pause"}}})
			}
		}, "ownership_lost", 1250 * time.Millisecond},
		{"event-gap", func(d *loadingQueueDevice) {
			d.onPlay = func() { d.handler(heos.Event{Gap: true}) }
		}, "ownership_lost", 1250 * time.Millisecond},
		{"play-with-unselected-mid", func(d *loadingQueueDevice) {
			d.onPlay = func() {
				d.setQueueState("play", d.intermediate)
				d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play"}}})
			}
		}, "ownership_lost", 1250 * time.Millisecond},
		{"unknown-after-play-event", func(d *loadingQueueDevice) {
			d.onPlay = func() {
				d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play"}}})
			}
		}, "ownership_lost", 1250 * time.Millisecond},
		{"control-change-with-selected-play", func(d *loadingQueueDevice) {
			d.onPlay = func() {
				d.setQueueState("play", d.tracks[1])
				v := 11
				d.s.Volume = &v
				d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play"}}})
			}
		}, "ownership_lost", 1250 * time.Millisecond},
	} {
		for _, initial := range []string{"stop", "pause"} {
			t.Run(initial+"/"+tc.name, func(t *testing.T) {
				c, j, d, req, cmd := loadingQueueFixture(t, initial)
				tc.setup(d)
				a, err := c.Submit(context.Background(), req, cmd)
				if err != nil {
					t.Fatal(err)
				}
				o := awaitOperation(t, j, a.ID)
				c.Close()
				if o.State != journal.Uncertain || o.ErrorCode != tc.code || d.clock.Now().Sub(d.queueSentAt) != tc.elapsed {
					t.Errorf("state=%s code=%s elapsed=%s; want uncertain/%s at %s", o.State, o.ErrorCode, d.clock.Now().Sub(d.queueSentAt), tc.code, tc.elapsed)
				}
				if len(d.writes) != 4 || d.writes[3].Kind != "queue" || *d.s.Volume > 11 {
					t.Error("unconfirmed queue caused another write or speculative cleanup", d.writes)
				}
				for i := 1; i < len(d.reads); i++ {
					if d.reads[i].Sub(d.reads[i-1]) < 250*time.Millisecond {
						t.Error("loading notifications caused excessive full reads", d.reads)
						break
					}
				}
			})
		}
	}
}

func TestQueueLoadingNewEventDuringReadCannotConfirmOlderPlay(t *testing.T) {
	c, j, d, req, cmd := loadingQueueFixture(t, "stop")
	d.intermediate = d.tracks[1]
	notified := false
	d.onRefresh = func() {
		if d.s.State == "play" && !notified {
			notified = true
			d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	c.Close()
	var progress PlaybackProgress
	if err := json.Unmarshal(o.Progress, &progress); err != nil {
		t.Fatal(err)
	}
	if !notified || o.State != journal.Succeeded || !progress.PlaybackStartedAt.Equal(d.playAt.Add(250*time.Millisecond)) {
		t.Fatal("queue confirmation ignored an event newer than its observation", o.State, progress)
	}
}
