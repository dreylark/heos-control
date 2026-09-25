package control

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type bufferedDevice struct {
	*multipartDevice
	afterRemove func()
}

func (d *bufferedDevice) Write(ctx context.Context, m heos.Mutation, g heos.Guard) (heos.Response, error) {
	if m.Kind != heos.MutationKindRemove {
		return d.multipartDevice.Write(ctx, m, g)
	}
	if err := ctx.Err(); err != nil {
		return heos.Response{}, err
	}
	d.mu.Lock()
	d.writes = append(d.writes, m)
	n := len(m.QueueIDs)
	position := -1
	for i, item := range d.s.Queue.Items {
		if item.QueueID == d.s.Media.QueueID {
			position = i
		}
	}
	items := append([]heos.Media(nil), d.s.Queue.Items[n:]...)
	for i := range items {
		items[i].QueueID = heos.ID(fmt.Sprint(i + 1))
	}
	total := len(items)
	d.s.Queue = heos.QueuePage{Items: items, Total: &total}
	current := items[position-n]
	d.s.Media = &current
	d.mu.Unlock()
	d.handler(heos.Event{Command: "event/player_queue_changed", Params: url.Values{"pid": {"1"}}})
	d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
	if d.afterRemove != nil {
		d.afterRemove()
	}
	return heos.Response{}, nil
}

func bufferedFixture(t *testing.T) (*Coordinator, *memoryJournal, *bufferedDevice, journal.Request, Command, *modeEventClock) {
	t.Helper()
	c, j, base, req, cmd := multipartFixture(t)
	d := &bufferedDevice{multipartDevice: base}
	l := c.lanes["room"]
	l.writer = d
	l.device.Client = d
	l.device.Observer = d
	c.reads.devices[0] = l.device
	cmd.Buffered = testBuffer()
	cmd.Buffered.MaxQueueTracks = 6
	clock := &modeEventClock{now: time.Now()}
	c.clock = clock
	req.Body, _ = json.Marshal(cmd)
	return c, j, d, req, cmd, clock
}

func bufferedMove(d *bufferedDevice, index int) {
	d.mu.Lock()
	m := d.s.Queue.Items[index]
	d.s.Media = &m
	d.mu.Unlock()
	d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
}

func TestBufferedWaitsRefillsAndPrunesPlayedPrefix(t *testing.T) {
	c, j, d, req, cmd, clock := bufferedFixture(t)
	began := clock.Now()
	clock.events = []modeClockEvent{
		{at: began.Add(time.Second), fn: func() {
			if n := len(d.s.Queue.Items); n != 3 {
				t.Errorf("loaded early: %d", n)
			}
			bufferedMove(d, 1)
		}},
		{at: began.Add(2 * time.Second), fn: func() {
			if n := len(d.s.Queue.Items); n != 5 {
				t.Errorf("first refill=%d", n)
			}
			bufferedMove(d, 3)
		}},
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	c.Close()
	if o.State != journal.Succeeded {
		t.Fatalf("%s %s %s", o.State, o.ErrorCode, o.Outcome)
	}
	var ids []heos.ID
	removes := 0
	for _, m := range d.s.Queue.Items {
		ids = append(ids, m.ID)
	}
	if !reflect.DeepEqual(ids, []heos.ID{"three", "two", "one", "three", "one", "three"}) {
		t.Fatal(ids)
	}
	for _, m := range d.writes {
		if m.Kind == heos.MutationKindRemove {
			removes++
			if !reflect.DeepEqual(m.QueueIDs, []heos.ID{"1", "2"}) {
				t.Fatal(m.QueueIDs)
			}
		}
	}
	progress := ProjectOperation(o).QueueLoading
	if removes != 1 || progress == nil || progress.ConfirmedTracks != 8 || progress.BufferedTracks == nil || *progress.BufferedTracks != 6 || progress.PrunedTracks == nil || *progress.PrunedTracks != 2 || progress.CurrentIndex == nil || *progress.CurrentIndex != 3 {
		t.Fatalf("removes=%d progress=%+v", removes, progress)
	}
	if clock.Now().Before(began.Add(2 * time.Second)) {
		t.Fatal("loaded all parts before threshold")
	}
}

func TestBufferedSessionExpiryReleasesWithoutStop(t *testing.T) {
	c, j, d, req, cmd, _ := bufferedFixture(t)
	cmd.Buffered.MaxSessionSeconds = 2
	req.Body, _ = json.Marshal(cmd)
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	c.Close()
	if o.State != journal.Released || o.ErrorCode != "session_expired" {
		t.Fatalf("%s %s", o.State, o.ErrorCode)
	}
	if len(d.s.Queue.Items) != 3 || d.s.State != heos.PlayStatePlay {
		t.Fatal("expiry changed playback", d.s.State, len(d.s.Queue.Items))
	}
	for _, m := range d.writes {
		if m.Append || m.Kind == heos.MutationKindRemove || m.Kind == heos.MutationKindTransport {
			t.Fatal("unexpected future write", m.Kind)
		}
	}
}

func TestBufferedTimedPlaybackMayLeaveUnusedParts(t *testing.T) {
	c, j, d, req, cmd, _ := bufferedFixture(t)
	cmd.Automation = &Automation{TargetLevel: cmd.Level, DurationSeconds: 2, FadeSeconds: 1}
	req.Body, _ = json.Marshal(cmd)
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	c.Close()
	if o.State != journal.Succeeded || len(d.s.Queue.Items) != 3 || d.s.State != heos.PlayStateStop {
		t.Fatalf("%s %s queue=%d", o.State, o.ErrorCode, len(d.s.Queue.Items))
	}
}
