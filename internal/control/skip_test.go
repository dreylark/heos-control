package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func TestSkipAdmissionRules(t *testing.T) {
	queue := heos.QueuePage{Items: []heos.Media{{Source: "1024", ID: "track-1", QueueID: "1"}}, Total: intPtr(1)}
	media := queue.Items[0]
	volume := 20
	ceiling := 40
	base := heos.Snapshot{State: "play", Volume: &volume, Media: &media, Queue: queue}
	if err := skipAdmissible(&ceiling, "next", base); err != nil {
		t.Fatal(err)
	}
	paused := base
	paused.State = "pause"
	if err := skipAdmissible(&ceiling, "previous", paused); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*heos.Snapshot)
		want error
	}{
		{name: "stopped", edit: func(s *heos.Snapshot) { s.State = "stop" }, want: ErrNotSkippable},
		{name: "unknown", edit: func(s *heos.Snapshot) { s.State = "unknown" }, want: ErrNotSkippable},
		{name: "missing media", edit: func(s *heos.Snapshot) { s.Media = nil }, want: ErrNotSkippable},
		{name: "foreign entry", edit: func(s *heos.Snapshot) { s.Media.QueueID = "9" }, want: ErrNotSkippable},
		{name: "incomplete queue", edit: func(s *heos.Snapshot) { next := 1; s.Queue.Next = &next }, want: ErrNotSkippable},
		{name: "above ceiling", edit: func(s *heos.Snapshot) { level := 41; s.Volume = &level }, want: heos.ErrBounds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := base
			s.Media = cloneMedia(base.Media)
			s.Queue.Items = append([]heos.Media(nil), base.Queue.Items...)
			tc.edit(&s)
			if err := skipAdmissible(&ceiling, "next", s); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
		})
	}
}

func TestSkipAdmissibleRejectsRepeatOffBoundaries(t *testing.T) {
	volume := 20
	ceiling := 40
	first := heos.Media{Source: "1024", ID: "track-1", QueueID: "1"}
	second := heos.Media{Source: "1024", ID: "track-2", QueueID: "2"}
	one := heos.Snapshot{State: "play", Volume: &volume, Repeat: "off", Media: &first, Queue: heos.QueuePage{Items: []heos.Media{first}, Total: intPtr(1)}}
	two := heos.Snapshot{State: "play", Volume: &volume, Repeat: "off", Media: &second, Queue: heos.QueuePage{Items: []heos.Media{first, second}, Total: intPtr(2)}}
	for _, tc := range []struct {
		name, direction string
		s               heos.Snapshot
		want            error
	}{
		{name: "single next", direction: "next", s: one, want: ErrNotSkippable},
		{name: "single previous", direction: "previous", s: one, want: ErrNotSkippable},
		{name: "single shuffled next", direction: "next", s: shuffled(one), want: ErrNotSkippable},
		{name: "last next", direction: "next", s: two, want: ErrNotSkippable},
		{name: "first previous", direction: "previous", s: atQueueStart(two, first), want: ErrNotSkippable},
		{name: "repeat all still sent", direction: "next", s: withRepeat(one, "on_all")},
		{name: "repeat one still sent", direction: "previous", s: withRepeat(one, "on_one")},
		{name: "shuffled last still sent", direction: "next", s: shuffled(two)},
		{name: "middle next", direction: "next", s: atQueueStart(two, first)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := skipAdmissible(&ceiling, tc.direction, tc.s)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func withRepeat(s heos.Snapshot, repeat string) heos.Snapshot {
	s.Repeat = repeat
	return s
}

func shuffled(s heos.Snapshot) heos.Snapshot {
	s.Shuffle = true
	return s
}

func atQueueStart(s heos.Snapshot, media heos.Media) heos.Snapshot {
	s.Media = &media
	return s
}

func TestSkipConfirmationRecognizesOnlyADifferentQueuedEntry(t *testing.T) {
	volume := 20
	muted := false
	items := []heos.Media{
		{Source: "1024", ID: "track-1", QueueID: "1", Song: "A"},
		{Source: "1024", ID: "track-2", QueueID: "2", Song: "B"},
	}
	before := heos.Snapshot{State: "play", Volume: &volume, Muted: &muted, Repeat: "off", Media: &items[0], Queue: heos.QueuePage{Items: items, Total: intPtr(2)}}
	next := before
	next.Media = &items[1]
	if !skipConfirmed(before, next) {
		t.Fatal("settled next entry was not confirmed")
	}
	paused := next
	paused.State = "pause"
	if !skipConfirmed(before, paused) {
		t.Fatal("paused settled entry was not confirmed")
	}
	for _, tc := range []struct {
		name string
		edit func(*heos.Snapshot)
	}{
		{name: "same entry", edit: func(s *heos.Snapshot) { s.Media = &items[0] }},
		{name: "stop", edit: func(s *heos.Snapshot) { s.State = "stop" }},
		{name: "unknown", edit: func(s *heos.Snapshot) { s.State = "unknown" }},
		{name: "cleared media", edit: func(s *heos.Snapshot) { s.Media = nil }},
		{name: "foreign entry", edit: func(s *heos.Snapshot) { foreign := items[1]; foreign.QueueID = "9"; s.Media = &foreign }},
		{name: "queue edit", edit: func(s *heos.Snapshot) {
			s.Queue.Items = append([]heos.Media(nil), items...)
			s.Queue.Items[1].ID = "foreign"
		}},
		{name: "volume", edit: func(s *heos.Snapshot) { level := 21; s.Volume = &level }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			after := next
			after.Queue.Items = append([]heos.Media(nil), items...)
			after.Media = &items[1]
			tc.edit(&after)
			if skipConfirmed(before, after) {
				t.Fatal("unsettled skip was confirmed")
			}
		})
	}
	prefix := heos.QueuePage{Items: items[:1], Total: intPtr(2)}
	full := heos.QueuePage{Items: items, Total: intPtr(2)}
	if !queueExtension(prefix, full) || !queueExtension(full, full) {
		t.Fatal("same queue pages were not an extension")
	}
	if queueExtension(heos.QueuePage{}, full) || queueExtension(heos.QueuePage{Items: []heos.Media{{QueueID: "other"}}, Total: intPtr(2)}, full) {
		t.Fatal("a different queue looked like a page extension")
	}
}

func TestSkipMovesWithinTheUnchangedQueue(t *testing.T) {
	for _, tc := range []struct {
		name, direction, state string
		from, target           int
	}{
		{name: "next", direction: "next", state: "play", from: 0, target: 1},
		{name: "previous", direction: "previous", state: "pause", from: 1, target: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, j, d, req := skipFixture(t, "", tc.from, tc.target)
			d.s.State = tc.state
			req = skipRequest(c, req)
			a, err := c.Submit(context.Background(), req, Command{Kind: "skip", Direction: tc.direction})
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if o.State != journal.Succeeded || len(d.writes) != 1 || d.writes[0].Kind != "skip" || d.writes[0].Direction != tc.direction {
				t.Fatal(o, d.writes)
			}
			if d.s.Media == nil || d.s.Media.QueueID != d.tracks[tc.target].QueueID || len(d.s.Queue.Items) != len(d.tracks) {
				t.Fatal(d.s.Media, len(d.s.Queue.Items))
			}
			again, err := c.Submit(context.Background(), req, Command{Kind: "skip", Direction: tc.direction})
			if err != nil || again.ID != a.ID || len(d.writes) != 1 {
				t.Fatal(err, again, d.writes)
			}
		})
	}
}

func TestSkipWaitsThroughStopBeforeConfirmingTheNewEntry(t *testing.T) {
	c, j, d, req := skipFixture(t, "late", 0, 1)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	// The stopping entry becomes the next queue member when virtual time
	// reaches the scheduled instant. A GET only observes that; it does not
	// advance the scenario.
	clock.hook = func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.settleAt.IsZero() || clock.Now().Before(d.settleAt) {
			return
		}
		media := d.tracks[d.target]
		d.s.Media = &media
		d.s.State = "play"
	}
	a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded || d.reads < 2 || len(d.writes) != 1 {
		t.Fatal(o, d.reads, d.writes)
	}
}

func TestSkipDoesNotConfirmOrReplayWhenTheEntryDoesNotChange(t *testing.T) {
	c, j, d, req := skipFixture(t, "stuck", 0, 1)
	c.clock = &advancingClock{now: time.Now()}
	a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Uncertain || len(d.writes) != 1 || o.ErrorCode == "not_skippable" {
		t.Fatal(o, d.writes)
	}
}

func TestSkipReleasesWhenTheQueueOrControlsChange(t *testing.T) {
	for _, mode := range []string{"queue", "volume"} {
		t.Run(mode, func(t *testing.T) {
			c, j, d, req := skipFixture(t, mode, 0, 1)
			a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if o.State != journal.Uncertain || o.ErrorCode != "ownership_lost" || len(d.writes) != 1 {
				t.Fatal(o, d.writes)
			}
		})
	}
}

func TestSkipRejectsQueueBoundaryBeforeSending(t *testing.T) {
	for _, tc := range []struct {
		name, direction string
		from, items     int
	}{
		{name: "only entry next", direction: "next", from: 0, items: 1},
		{name: "only entry previous", direction: "previous", from: 0, items: 1},
		{name: "last next", direction: "next", from: 3, items: 4},
		{name: "first previous", direction: "previous", from: 0, items: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, j, d, req := skipFixture(t, "", tc.from, 0)
			if tc.items == 1 {
				item := d.tracks[0]
				total := 1
				d.s.Queue = heos.QueuePage{Items: []heos.Media{item}, Total: &total}
				d.s.Media = &d.s.Queue.Items[0]
			}
			if _, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: tc.direction}); !errors.Is(err, ErrNotSkippable) {
				t.Fatal(err)
			}
			if len(d.writes) != 0 || len(j.ops) != 0 {
				t.Fatal("boundary skip had side effects", d.writes, j.ops)
			}
		})
	}
}

func TestSkipStillSendsWhenRepeatCanWrap(t *testing.T) {
	c, j, d, req := skipFixture(t, "", 3, 0)
	d.s.Repeat = "on_all"
	a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded || len(d.writes) != 1 || d.writes[0].Kind != "skip" {
		t.Fatal(o, d.writes)
	}
}

func TestSkipRejectsABoundaryDiscoveredBeforeSending(t *testing.T) {
	c, j, d, req := skipFixture(t, "boundary-before-send", 0, 1)
	a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Failed || o.ErrorCode != "not_skippable" || len(d.writes) != 0 {
		t.Fatal(o, d.writes)
	}
}

func TestSkipLimitReachedLeavesVolumeAndStopAdmissible(t *testing.T) {
	c, j, d, req := skipFixture(t, "limit", 0, 1)
	a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	var outcome struct {
		Delivery string `json:"delivery"`
	}
	if err := json.Unmarshal(o.Outcome, &outcome); err != nil {
		t.Fatal(err)
	}
	if o.State != journal.Failed || o.ErrorCode != "device_rejected" || outcome.Delivery != "rejected" {
		t.Fatalf("rejected skip: %+v", o)
	}
	volume := playerRequest(c, req, "volume-after-skip", "PUT", "/v1/players/room/volume")
	accepted, err := c.Submit(context.Background(), volume, Command{Kind: "volume", Level: 10})
	if err != nil {
		t.Fatal(err)
	}
	if vo := awaitOperation(t, j, accepted.ID); vo.State != journal.Succeeded {
		t.Fatal(vo)
	}
	stop := playerRequest(c, req, "stop-after-skip", "PUT", "/v1/players/room/transport")
	accepted, err = c.Submit(context.Background(), stop, Command{Kind: "transport", State: "stop"})
	if err != nil {
		t.Fatal(err)
	}
	if so := awaitOperation(t, j, accepted.ID); so.State != journal.Succeeded {
		t.Fatal(so)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	var skips, volumes, stops int
	for _, m := range d.writes {
		switch m.Kind {
		case "skip":
			skips++
		case "volume":
			volumes++
		case "transport":
			if m.State == "stop" {
				stops++
			}
		}
	}
	if skips != 1 || volumes != 1 || stops != 1 {
		t.Fatal(d.writes)
	}
}

func playerRequest(c *Coordinator, req journal.Request, key, method, endpoint string) journal.Request {
	req = skipRequest(c, req)
	req.Key = key
	req.Method = method
	req.Endpoint = endpoint
	return req
}

func TestSkipRejectsUnusableObservationsBeforeSending(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*skipDevice)
		want error
	}{
		{name: "stopped", edit: func(d *skipDevice) { d.s.State = "stop" }, want: ErrNotSkippable},
		{name: "no media", edit: func(d *skipDevice) { d.s.Media = nil }, want: ErrNotSkippable},
		{name: "above ceiling", edit: func(d *skipDevice) { level := 41; d.s.Volume = &level }, want: heos.ErrBounds},
		{name: "grouped", edit: func(d *skipDevice) { d.s.Grouped = true }, want: ErrGrouped},
		{name: "direction", edit: func(*skipDevice) {}, want: heos.ErrBounds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, j, d, req := skipFixture(t, "", 0, 1)
			tc.edit(d)
			cmd := Command{Kind: "skip", Direction: "next"}
			if tc.name == "direction" {
				cmd.Direction = "play"
			}
			if _, err := c.Submit(context.Background(), skipRequest(c, req), cmd); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
			if len(d.writes) != 0 || len(j.ops) != 0 {
				t.Fatal("rejected skip had side effects", d.writes, j.ops)
			}
		})
	}
}

func TestSkipRefusesAStateChangeDiscoveredBeforeTheCommand(t *testing.T) {
	c, j, d, req := skipFixture(t, "stopped-before-send", 0, 1)
	a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Failed || o.ErrorCode != "not_skippable" || len(d.writes) != 0 {
		t.Fatal(o, d.writes)
	}
}

func TestSkipConflictsWithAnActiveOperationUnlessTakeover(t *testing.T) {
	c, j, d, req := skipFixture(t, "", 0, 1)
	j.ops["existing"] = journal.Operation{ID: "existing", Player: "room", State: journal.Running, Revision: 1}
	req = skipRequest(c, req)
	if _, err := c.Submit(context.Background(), req, Command{Kind: "skip", Direction: "next"}); !errors.Is(err, journal.ErrBusy) {
		t.Fatal(err)
	}
	if len(d.writes) != 0 || j.ops["existing"].State != journal.Running {
		t.Fatal(d.writes, j.ops["existing"])
	}
	req.Key = "takeover"
	a, err := c.Submit(context.Background(), req, Command{Kind: "skip", Direction: "next", Takeover: true})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	replaced, _ := j.Get(context.Background(), "existing")
	if o.State != journal.Succeeded || replaced.State != journal.Released || len(d.writes) != 1 {
		t.Fatal(o, replaced, d.writes)
	}
}

func TestSkipTakeoverLeavesBoundedAutomationBeforeItsStop(t *testing.T) {
	c, j, d, req, cmd := albumFixture(t)
	clock := &holdClock{now: time.Now(), held: make(chan struct{}), resume: make(chan struct{})}
	c.clock = clock
	cmd.Automation = &Automation{TargetLevel: 40, RampSeconds: 0, DurationSeconds: 10, FadeSeconds: 1}
	d.before = func(m heos.Mutation) {
		if m.Kind != "skip" {
			return
		}
		d.mu.Lock()
		next := d.tracks[1]
		d.s.Media = &next
		d.s.State = "play"
		d.mu.Unlock()
	}
	playback, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-clock.held:
	case <-time.After(time.Second):
		t.Fatal("bounded automation never reached its envelope")
	}
	running, err := j.Get(context.Background(), playback.ID)
	if err != nil {
		t.Fatal(err)
	}
	var progress PlaybackProgress
	if err := json.Unmarshal(running.Progress, &progress); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	writesAtHold := len(d.writes)
	d.mu.Unlock()

	skip := skipRequest(c, req)
	skip.Key = "takeover"
	a, err := c.Submit(context.Background(), skip, Command{Kind: "skip", Direction: "next", Takeover: true})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	replaced, _ := j.Get(context.Background(), playback.ID)
	d.mu.Lock()
	writesAtSkip := append([]heos.Mutation(nil), d.writes...)
	d.mu.Unlock()
	if o.State != journal.Succeeded || replaced.State != journal.Released || len(writesAtSkip) != writesAtHold+1 || writesAtSkip[len(writesAtSkip)-1].Kind != "skip" {
		t.Fatal(o, replaced, writesAtSkip)
	}

	clock.advancePast(progress.StopAt)
	d.mu.Lock()
	defer d.mu.Unlock()
	if !clock.Now().After(progress.StopAt) || len(d.writes) != len(writesAtSkip) {
		t.Fatal("superseded automation sent commands after its old stop", clock.Now(), progress.StopAt, d.writes)
	}
}

func TestSkipBeyondTheFirstQueuePage(t *testing.T) {
	c, j, base, req, _ := albumFixture(t)
	for i := 4; i < 120; i++ {
		base.tracks = append(base.tracks, heos.Media{Source: "1024", QueueID: heos.ID(fmt.Sprint(i + 1)), ID: heos.ID(fmt.Sprint("track-", i+1))})
	}
	d := installSkip(c, base, "", 110, 111)
	a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded || len(d.writes) != 1 || d.s.Media == nil || d.s.Media.QueueID != d.tracks[111].QueueID {
		t.Fatal(o, d.writes, d.s.Media)
	}
}

func TestStalePlayerRevisionDoesNotSkip(t *testing.T) {
	c, j, d, req := skipFixture(t, "", 0, 1)
	req.IfMatch = `"stale"`
	if _, err := c.Submit(context.Background(), req, Command{Kind: "skip", Direction: "next"}); !errors.Is(err, ErrPrecondition) {
		t.Fatal(err)
	}
	if len(d.writes) != 0 || len(j.ops) != 0 {
		t.Fatal(d.writes, j.ops)
	}
}

type skipDevice struct {
	*albumDevice
	mode     string
	target   int
	sent     bool
	reads    int
	settleAt time.Time
}

func (d *skipDevice) Write(ctx context.Context, m heos.Mutation, guard heos.Guard) (heos.Response, error) {
	if d.mode == "limit" && m.Kind == "skip" {
		d.mu.Lock()
		d.sent = true
		d.writes = append(d.writes, m)
		d.mu.Unlock()
		command := "player/play_next"
		if m.Direction == "previous" {
			command = "player/play_previous"
		}
		return heos.Response{}, &heos.CommandError{Delivery: heos.Rejected, Cause: &heos.DeviceError{Command: command, Code: 17}}
	}
	return d.albumDevice.Write(ctx, m, guard)
}

func (d *skipDevice) Refresh(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.now != nil {
		d.s.ObservedAt = d.now()
	}
	if !d.sent {
		switch d.mode {
		case "stopped-before-send":
			d.s.State = "stop"
		case "boundary-before-send":
			if d.s.Media != nil {
				item := *d.s.Media
				total := 1
				d.s.Queue = heos.QueuePage{Items: []heos.Media{item}, Total: &total}
				current := item
				d.s.Media = &current
			}
		}
		return ctx.Err()
	}
	d.reads++
	switch d.mode {
	case "queue":
		items := append([]heos.Media(nil), d.s.Queue.Items...)
		items[0].ID = "foreign"
		d.s.Queue.Items = items
	case "volume":
		level := *d.s.Volume + 1
		d.s.Volume = &level
	}
	return ctx.Err()
}

func skipFixture(t *testing.T, mode string, from, target int) (*Coordinator, *memoryJournal, *skipDevice, journal.Request) {
	t.Helper()
	c, j, base, req, _ := albumFixture(t)
	return c, j, installSkip(c, base, mode, from, target), req
}

func installSkip(c *Coordinator, base *albumDevice, mode string, from, target int) *skipDevice {
	total := len(base.tracks)
	base.s.State = "play"
	base.s.Repeat = "off"
	base.s.Queue = heos.QueuePage{Items: append([]heos.Media(nil), base.tracks...), Total: &total}
	media := base.tracks[from]
	base.s.Media = &media
	d := &skipDevice{albumDevice: base, mode: mode, target: target}
	l := c.lanes["room"]
	l.device.Client, l.device.Observer, l.writer = d, d, d
	c.reads.devices[0] = l.device
	d.before = func(m heos.Mutation) {
		if m.Kind != "skip" {
			return
		}
		d.mu.Lock()
		d.sent = true
		switch d.mode {
		case "":
			next := d.tracks[d.target]
			d.s.Media = &next
			d.s.State = "play"
		case "late":
			d.s.State = "stop"
			d.s.Media = nil
			if d.now != nil {
				d.settleAt = d.now().Add(observationEventSpacing)
			}
		}
		d.mu.Unlock()
		if d.handler == nil {
			return
		}
		d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
		d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
		d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"unknown"}}})
		d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
		d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play"}}})
	}
	return d
}

func skipRequest(c *Coordinator, req journal.Request) journal.Request {
	p, _ := c.reads.Player("room")
	req.IfMatch = fmt.Sprintf("%q", p.Revision)
	req.Method = "POST"
	req.Endpoint = "/v1/players/room/skip"
	req.Key = "skip"
	return req
}

// holdClock advances virtual time, but blocks the first real wait so a test
// can supersede the worker. Cancellation leaves that wait without moving the
// clock; advancePast then steps beyond a deadline the old worker must ignore.
type holdClock struct {
	mu      sync.Mutex
	now     time.Time
	held    chan struct{}
	resume  chan struct{}
	didHold bool
}

func (c *holdClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *holdClock) Wait(ctx context.Context, d time.Duration, wake <-chan struct{}) error {
	select {
	case <-wake:
		return context.Cause(ctx)
	default:
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	hold := !c.didHold
	if hold {
		c.didHold = true
	}
	c.mu.Unlock()
	if hold {
		close(c.held)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-c.resume:
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
	return context.Cause(ctx)
}

func (c *holdClock) advancePast(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.now.After(at) {
		c.now = at.Add(time.Second)
	}
}

func intPtr(v int) *int { return &v }

func cloneMedia(m *heos.Media) *heos.Media {
	if m == nil {
		return nil
	}
	v := *m
	return &v
}
