package control

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type albumDevice struct {
	*fakeDevice
	tracks []heos.Media
}

func (d *albumDevice) Snapshot() heos.Snapshot {
	s := d.fakeDevice.Snapshot()
	if len(s.Queue.Items) > 100 {
		s.Queue.Items = s.Queue.Items[:100]
		next := 100
		s.Queue.Next = &next
	}
	return s
}

func (d *albumDevice) Queue(_ context.Context, _ heos.ID, start, limit int) (heos.QueuePage, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	total := len(d.s.Queue.Items)
	end := min(start+limit, total)
	page := heos.QueuePage{Items: append([]heos.Media(nil), d.s.Queue.Items[start:end]...), Total: &total, Token: d.s.Token}
	if end < total {
		page.Next = &end
	}
	return page, nil
}

func (d *albumDevice) BrowseAll(_ context.Context, sid, _ heos.ID) ([]heos.Item, error) {
	if sid == "1024" {
		return []heos.Item{{Source: "900", Name: "Gerbera", Type: "heos_server"}}, nil
	}
	var items []heos.Item
	for _, m := range d.tracks {
		items = append(items, heos.Item{Source: "900", MediaID: m.ID, ContainerID: "green", Type: "song", Playable: "yes"})
	}
	return items, nil
}
func (d *albumDevice) Write(ctx context.Context, m heos.Mutation, guard heos.Guard) (heos.Response, error) {
	response, err := d.fakeDevice.Write(ctx, m, guard)
	if err == nil && m.Kind == heos.MutationKindQueue {
		d.mu.Lock()
		total := len(d.tracks)
		d.s.Queue = heos.QueuePage{Items: append([]heos.Media(nil), d.tracks...), Total: &total}
		first := d.tracks[0]
		d.s.Media = &first
		d.mu.Unlock()
	}
	return response, err
}

type albumCatalog struct{ playbackCatalog }

func (albumCatalog) Resolve(heos.ID, string) (heos.Item, error) {
	return heos.Item{Source: "900", ContainerID: "green", Name: "Green", Container: "yes", Playable: "yes"}, nil
}

func albumFixture(t *testing.T) (*Coordinator, *memoryJournal, *albumDevice, journal.Request, Command) {
	t.Helper()
	c, j, fake, req, cmd := automationFixture(t)
	d := &albumDevice{fakeDevice: fake}
	for i := range 4 {
		d.tracks = append(d.tracks, heos.Media{Source: "1024", QueueID: heos.ID(fmt.Sprint(i + 1)), ID: heos.ID(fmt.Sprint("track-", i+1)), Album: "Green"})
	}
	l := c.lanes["room"]
	l.device.Client, l.writer = d, d
	l.device.Observer = d
	l.device.Catalogs["music"] = albumCatalog{}
	c.reads.devices[0] = l.device
	return c, j, d, req, cmd
}

func TestQueuePreparationMediaNotificationsRequireOwnershipReadback(t *testing.T) {
	for _, scenario := range []string{"duplicate", "foreign-queue", "pause", "stop-after-play", "paused-pause", "paused-stop-after-play", "paused-duplicate-stop", "duplicate-stop", "paused-duplicate-stop-after-play"} {
		t.Run(scenario, func(t *testing.T) {
			c, j, d, req, cmd := albumFixture(t)
			if strings.HasPrefix(scenario, "paused-") {
				d.s.State = heos.PlayStatePause
				p, _ := c.reads.Player("room")
				req.IfMatch = fmt.Sprintf("%q", p.Revision)
				scenario = strings.TrimPrefix(scenario, "paused-")
			}
			c.clock = &advancingClock{now: time.Now()}
			d.before = func(m heos.Mutation) {
				if m.Kind != heos.MutationKindQueue {
					return
				}
				for range 2 {
					d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
				}
				switch scenario {
				case "foreign-queue":
					// The requested membership was frozen before admission.
					// Media notifications alone cannot authorize this queue.
					d.tracks[0].ID = "foreign-track"
				case "pause":
					d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"pause"}}})
				case "stop-after-play":
					d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play"}}})
					d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
				case "duplicate-stop", "duplicate-stop-after-play":
					for range 2 {
						d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
					}
					if scenario == "duplicate-stop-after-play" {
						d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play"}}})
						d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
					}
				}
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if scenario == "duplicate" || scenario == "duplicate-stop" {
				if o.State != journal.Succeeded {
					t.Fatal(o)
				}
				return
			}
			if o.State == journal.Succeeded || o.ErrorCode != "ownership_lost" {
				t.Fatal("unowned queue retained control", o)
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			for _, m := range d.writes {
				if m.Kind == heos.MutationKindTransport || (m.Kind == heos.MutationKindVolume && m.Level != cmd.Level) {
					t.Fatal("automation continued after invalid preparation", d.writes)
				}
			}
		})
	}
}

func TestAlbumOwnershipChecksQueueBeyondFirstPage(t *testing.T) {
	for _, edit := range []bool{false, true} {
		t.Run(fmt.Sprint("queue_edit=", edit), func(t *testing.T) {
			c, j, d, req, cmd := albumFixture(t)
			for i := 4; i < 150; i++ {
				d.tracks = append(d.tracks, heos.Media{Source: "1024", QueueID: heos.ID(fmt.Sprint(i + 1)), ID: heos.ID(fmt.Sprint("track-", i+1)), Album: "Green"})
			}
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			cmd.Automation = &Automation{TargetLevel: 12, RampSeconds: 2, DurationSeconds: 10, FadeSeconds: 2}
			start, changed, count := clock.Now(), false, 0
			clock.hook = func() {
				if changed || clock.Now().Sub(start) < 3*time.Second {
					return
				}
				changed = true
				d.mu.Lock()
				count = len(d.writes)
				if edit {
					d.s.Queue.Items = append([]heos.Media(nil), d.s.Queue.Items...)
					d.s.Queue.Items[149].ID = "foreign-track"
				} else {
					m := d.tracks[149]
					d.s.Media = &m
				}
				d.mu.Unlock()
				name := "event/player_now_playing_changed"
				if edit {
					name = "event/player_queue_changed"
				}
				d.handler(heos.Event{Command: name, Params: url.Values{"pid": {"1"}}})
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if !changed {
				t.Fatal("did not reach transition", o)
			}
			if !edit && o.State != journal.Succeeded {
				t.Fatal(o)
			}
			if edit {
				d.mu.Lock()
				defer d.mu.Unlock()
				if o.State != journal.Released || len(d.writes) != count {
					t.Fatal("unseen queue edit retained control", o.State, len(d.writes), count)
				}
			}
		})
	}
}

type transitionRaceDevice struct {
	*albumDevice
	delivery heos.Delivery
	attempts int
}

func (d *transitionRaceDevice) Write(ctx context.Context, m heos.Mutation, g heos.Guard) (heos.Response, error) {
	if m.Kind == heos.MutationKindVolume && m.Level == 11 {
		d.attempts++
		if d.attempts == 1 {
			d.mu.Lock()
			media := d.tracks[1]
			d.s.Media = &media
			handler := d.handler
			d.mu.Unlock()
			handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
			if d.delivery == heos.NotSent {
				return heos.Response{}, &heos.CommandError{Delivery: heos.NotSent, Cause: heos.ErrStale}
			}
			// The device applies this setter, but its reply is lost.
			_, _ = d.albumDevice.Write(ctx, m, g)
			return heos.Response{}, &heos.CommandError{Delivery: heos.Uncertain, Cause: io.ErrUnexpectedEOF}
		}
	}
	return d.albumDevice.Write(ctx, m, g)
}

func TestTrackTransitionRetriesOnlyProvenNotSentGuard(t *testing.T) {
	for _, delivery := range []heos.Delivery{heos.NotSent, heos.Uncertain} {
		t.Run(string(delivery), func(t *testing.T) {
			c, j, device, req, cmd := albumFixture(t)
			d := &transitionRaceDevice{albumDevice: device, delivery: delivery}
			c.lanes["room"].writer = d
			c.clock = &advancingClock{now: time.Now()}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			want, state := 3, journal.Succeeded // two ramp attempts, then level 11 once in the fade
			if delivery == heos.Uncertain {
				want, state = 1, journal.Uncertain
			}
			if d.attempts != want || o.State != state {
				t.Fatal(d.attempts, o.State, o.ErrorCode)
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			if delivery == heos.NotSent && len(d.writes) != 75 {
				t.Fatal("not-sent retry duplicated a device setter", len(d.writes))
			}
			if delivery == heos.Uncertain && len(d.writes) != 5 {
				t.Fatal("replayed or cleaned up an uncertain write", d.writes)
			}
		})
	}
}

func TestQueueCompletionRemovesRemainingTimers(t *testing.T) {
	c, j, d, req, cmd := albumFixture(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	start, completed, count := clock.Now(), false, 0
	clock.hook = func() {
		if completed || clock.Now().Sub(start) < 600*time.Second {
			return
		}
		completed = true
		d.mu.Lock()
		d.s.State = heos.PlayStateStop
		d.s.Media = &heos.Media{}
		count = len(d.writes)
		handler := d.handler
		d.mu.Unlock()
		handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
		handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	d.mu.Lock()
	defer d.mu.Unlock()
	if !completed || o.State != journal.Released || len(d.writes) != count {
		t.Fatal("completed queue retained automation", o.State, len(d.writes), count)
	}
}

func TestOwnedCancellationAfterInQueueSkip(t *testing.T) {
	c, j, d, req, cmd := albumFixture(t)
	clock := &heldClock{entered: make(chan struct{})}
	c.clock = clock
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-clock.entered:
	case <-time.After(time.Second):
		t.Fatal("window did not start")
	}
	d.mu.Lock()
	media := d.tracks[2]
	d.s.Media = &media
	handler := d.handler
	d.mu.Unlock()
	handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
	req.Key, req.IfMatch = "cancel", ""
	req.Endpoint = "/v1/operations/" + a.ID + "/cancel"
	b, err := c.Submit(context.Background(), req, Command{Kind: CommandKindCancel, Target: a.ID, Mode: "stop_owned"})
	if err != nil {
		t.Fatal(err)
	}
	if o := awaitOperation(t, j, b.ID); o.State != journal.Succeeded {
		t.Fatal(o)
	}
	if o := awaitOperation(t, j, a.ID); o.State != journal.Cancelled {
		t.Fatal(o)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != 5 || d.writes[4].State != heos.PlayStateStop {
		t.Fatal(d.writes)
	}
}

func TestAlbumTransitionsPreserveFullAutomationWindow(t *testing.T) {
	for _, events := range []bool{false, true} {
		t.Run(fmt.Sprint("events=", events), func(t *testing.T) {
			c, j, d, req, cmd := albumFixture(t)
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			start, transitions := clock.Now(), 0
			// Shuffled order crosses ramp, hold, and fade. No queue edit occurs.
			at := []time.Duration{200 * time.Second, 600 * time.Second, 1190 * time.Second}
			order := []int{2, 1, 3}
			clock.hook = func() {
				if transitions == len(at) || clock.Now().Sub(start) < at[transitions] {
					return
				}
				d.mu.Lock()
				media := d.tracks[order[transitions]]
				d.s.Media = &media
				handler := d.handler
				d.mu.Unlock()
				transitions++
				if events {
					handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
				}
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if o.State != journal.Succeeded || transitions != 3 || clock.Now().Sub(start) != 1200*time.Second {
				t.Fatal("multi-track alarm did not finish its window", o.State, o.ErrorCode, transitions, clock.Now().Sub(start))
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			if d.s.State != heos.PlayStateStop || *d.s.Volume != 0 || len(d.writes) != 75 {
				t.Fatal("ramp/fade/stop changed across tracks", d.s.State, *d.s.Volume, len(d.writes))
			}
		})
	}
}
