package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type multipartCatalog struct {
	playbackCatalog
	items map[string]heos.Item
}

func (c multipartCatalog) Resolve(_ heos.ID, ref string) (heos.Item, error) {
	item, ok := c.items[ref]
	if !ok {
		return item, heos.ErrStaleReference
	}
	return item, nil
}

type multipartDevice struct {
	*albumDevice
	contents   map[heos.ID][]heos.Item
	afterQueue func(heos.Mutation)
	queueError error
	browsed    []heos.ID
	stalePages int
}

func (d *multipartDevice) BrowseAll(ctx context.Context, sid, cid heos.ID) ([]heos.Item, error) {
	if sid == "1024" {
		return d.albumDevice.BrowseAll(ctx, sid, cid)
	}
	d.browsed = append(d.browsed, cid)
	return append([]heos.Item(nil), d.contents[cid]...), nil
}
func (d *multipartDevice) Queue(ctx context.Context, pid heos.ID, start, limit int) (heos.QueuePage, error) {
	if d.stalePages > 0 {
		d.stalePages--
		return heos.QueuePage{}, heos.ErrStale
	}
	return d.albumDevice.Queue(ctx, pid, start, limit)
}
func (d *multipartDevice) Write(ctx context.Context, m heos.Mutation, g heos.Guard) (heos.Response, error) {
	if m.Kind != heos.MutationKindQueue {
		return d.fakeDevice.Write(ctx, m, g)
	}
	if d.before != nil {
		d.before(m)
	}
	if ctx.Err() != nil {
		return heos.Response{}, ctx.Err()
	}
	d.mu.Lock()
	d.writes = append(d.writes, m)
	items := append([]heos.Media(nil), d.s.Queue.Items...)
	if !m.Append {
		items = nil
	}
	tracks := d.contents[m.Item.ContainerID]
	if m.Item.Container != "yes" {
		tracks = []heos.Item{m.Item}
	}
	for _, track := range tracks {
		items = append(items, heos.Media{Source: "1024", ID: track.MediaID, QueueID: heos.ID(fmt.Sprint(len(items) + 1))})
	}
	total := len(items)
	d.s.Queue = heos.QueuePage{Items: items, Total: &total}
	if !m.Append {
		first := items[0]
		d.s.Media = &first
		d.s.State = heos.PlayStatePlay
	}
	handler := d.handler
	d.mu.Unlock()
	handler(heos.Event{Command: "event/player_queue_changed", Params: url.Values{"pid": {"1"}}})
	if d.afterQueue != nil {
		d.afterQueue(m)
	}
	if m.Append && d.queueError != nil {
		return heos.Response{}, d.queueError
	}
	return heos.Response{}, nil
}
func multipartFixture(t *testing.T) (*Coordinator, *memoryJournal, *multipartDevice, journal.Request, Command) {
	t.Helper()
	c, j, base, req, cmd := albumFixture(t)
	d := &multipartDevice{albumDevice: base, contents: map[heos.ID][]heos.Item{}}
	cat := multipartCatalog{items: map[string]heos.Item{}}
	for part, ids := range map[string][]heos.ID{"a": {"three", "one", "three"}, "b": {"two", "one"}} {
		cat.items[part] = heos.Item{Source: "900", ContainerID: heos.ID(part), Container: "yes", Playable: "no"}
		for _, id := range ids {
			d.contents[heos.ID(part)] = append(d.contents[heos.ID(part)], heos.Item{Source: "900", ContainerID: heos.ID(part), MediaID: id, Type: "song", Playable: "yes"})
		}
	}
	l := c.lanes["room"]
	l.device.Client = d
	l.device.Observer = d
	l.writer = d
	l.device.Catalogs["music"] = cat
	c.reads.devices[0] = l.device
	cmd.ItemRef = ""
	cmd.ItemRefs = []string{"a", "b", "a"}
	cmd.Shuffle = false
	cmd.Automation = nil
	req.Body, _ = json.Marshal(cmd)
	c.clock = &advancingClock{now: time.Now()}
	return c, j, d, req, cmd
}
func TestMultipartPreservesOrderRepeatsAndPrefix(t *testing.T) {
	c, j, d, req, cmd := multipartFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	admitted, err := c.Submit(ctx, req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	initial := ProjectOperation(admitted).QueueLoading
	if initial == nil || initial.TotalParts != 3 || initial.TotalTracks != 8 || initial.ConfirmedParts != 0 {
		t.Fatal(initial)
	}
	// Accepted work owns its input and survives caller mutation/disconnect.
	cmd.ItemRefs[1] = "missing"
	o := awaitOperation(t, j, admitted.ID)
	if o.State != journal.Succeeded {
		t.Fatalf("%s %s %s", o.State, o.ErrorCode, o.Outcome)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	var ids []heos.ID
	for _, m := range d.s.Queue.Items {
		ids = append(ids, m.ID)
	}
	if !reflect.DeepEqual(ids, []heos.ID{"three", "one", "three", "two", "one", "three", "one", "three"}) {
		t.Fatal(ids)
	}
	var appends []bool
	for _, m := range d.writes {
		if m.Kind == heos.MutationKindQueue {
			appends = append(appends, m.Append)
		}
	}
	if !reflect.DeepEqual(appends, []bool{false, true, true}) {
		t.Fatal(appends)
	}
	p := ProjectOperation(o)
	if p.QueueLoading == nil || p.QueueLoading.ConfirmedParts != 3 || p.QueueLoading.TotalTracks != 8 || p.QueueLoading.ConfirmedTracks != 8 || p.Playback != nil {
		t.Fatal(string(o.Progress))
	}
}
func TestMultipartRejectsEntirePlanBeforeWrites(t *testing.T) {
	for _, scenario := range []string{"missing", "both", "empty", "shuffle", "nested", "empty_part", "foreign_source", "too_many_parts"} {
		t.Run(scenario, func(t *testing.T) {
			c, _, d, req, cmd := multipartFixture(t)
			switch scenario {
			case "missing":
				cmd.ItemRefs[2] = "missing"
			case "both":
				cmd.ItemRef = "a"
			case "empty":
				cmd.ItemRefs = []string{}
			case "shuffle":
				cmd.Shuffle = true
			case "nested":
				d.contents["b"][0].Container = "yes"
			case "empty_part":
				d.contents["b"] = nil
			case "foreign_source":
				d.contents["b"][0].Source = "901"
			case "too_many_parts":
				cmd.ItemRefs = make([]string, 33)
				for i := range cmd.ItemRefs {
					cmd.ItemRefs[i] = "a"
				}
			}
			if _, err := c.Submit(context.Background(), req, cmd); err == nil {
				t.Fatal("invalid plan admitted")
			}
			if _, err := c.reads.Preflight(context.Background(), "room", cmd); err == nil {
				t.Fatal("invalid preflight accepted")
			}
			if len(d.writes) != 0 {
				t.Fatal(d.writes)
			}
		})
	}
}
func TestMultipartRejectsChangedAppendWithoutReplay(t *testing.T) {
	for _, scenario := range []string{"prefix_qid", "suffix_order", "duplicate_qid", "pause", "lost_reply"} {
		t.Run(scenario, func(t *testing.T) {
			c, j, d, req, cmd := multipartFixture(t)
			d.afterQueue = func(m heos.Mutation) {
				if !m.Append {
					return
				}
				d.mu.Lock()
				switch scenario {
				case "prefix_qid":
					d.s.Queue.Items[0].QueueID = "99"
				case "suffix_order":
					d.s.Queue.Items[3].ID, d.s.Queue.Items[4].ID = d.s.Queue.Items[4].ID, d.s.Queue.Items[3].ID
				case "duplicate_qid":
					d.s.Queue.Items[4].QueueID = "1"
				case "pause":
					d.s.State = heos.PlayStatePause
				}
				handler := d.handler
				d.mu.Unlock()
				if scenario == "pause" {
					handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"pause"}}})
				}
			}
			if scenario == "lost_reply" {
				d.queueError = &heos.CommandError{Delivery: heos.Uncertain, Cause: errors.New("lost reply")}
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if o.State == journal.Succeeded {
				t.Fatal("unconfirmed append succeeded")
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			queues := 0
			for _, m := range d.writes {
				if m.Kind == heos.MutationKindQueue {
					queues++
				}
			}
			if queues != 2 {
				t.Fatal("append replay or later part sent", queues)
			}
			p := ProjectOperation(o).QueueLoading
			if p == nil || p.ConfirmedParts != 1 || p.ConfirmedTracks != 3 {
				t.Fatal(string(o.Progress))
			}
		})
	}
}
func TestMultipartLoadingUsesOriginalAutomationTimeline(t *testing.T) {
	c, j, d, req, cmd := multipartFixture(t)
	clock := c.clock.(*advancingClock)
	cmd.Automation = &Automation{TargetLevel: 20, RampSeconds: 4, DurationSeconds: 10, FadeSeconds: 2}
	began := clock.Now()
	var stop time.Time
	d.before = func(m heos.Mutation) {
		if m.Append {
			clock.mu.Lock()
			clock.now = clock.now.Add(2 * time.Second)
			clock.mu.Unlock()
		}
		if m.Kind == heos.MutationKindTransport && m.State == heos.PlayStateStop {
			stop = clock.Now()
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded || !stop.Equal(began.Add(10*time.Second)) {
		t.Fatal(o.State, o.ErrorCode, string(o.Outcome), stop.Sub(began))
	}
	p := ProjectOperation(o)
	if p.Playback == nil || !p.Playback.PlaybackStartedAt.Equal(began) || p.QueueLoading.ConfirmedParts != 3 {
		t.Fatal(string(o.Progress))
	}
}

func TestMultipartAppendNavigationAndDelayedReadback(t *testing.T) {
	for _, scenario := range []string{"old_track", "new_track", "delayed", "hybrid", "stop_play", "no_op"} {
		t.Run(scenario, func(t *testing.T) {
			c, j, d, req, cmd := multipartFixture(t)
			cmd.ItemRefs = cmd.ItemRefs[:2]
			clock := c.clock.(*advancingClock)
			var full heos.Snapshot
			var restoreAt time.Time
			restored := false
			d.afterQueue = func(m heos.Mutation) {
				if !m.Append {
					return
				}
				d.mu.Lock()
				full = d.s
				switch scenario {
				case "old_track":
					media := d.s.Queue.Items[1]
					d.s.Media = &media
				case "new_track":
					media := d.s.Queue.Items[4]
					d.s.Media = &media
				case "delayed", "no_op":
					total := 3
					d.s.Queue = heos.QueuePage{Items: append([]heos.Media(nil), d.s.Queue.Items[:3]...), Total: &total}
				case "hybrid":
					media := d.s.Queue.Items[1]
					media.QueueID = "4"
					d.s.Media = &media
				case "stop_play":
					d.s.State = heos.PlayStateStop
				}
				handler := d.handler
				d.mu.Unlock()
				for range 3 {
					handler(heos.Event{Command: "event/player_queue_changed", Params: url.Values{"pid": {"1"}}})
				}
				if scenario == "stop_play" {
					handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
				}
				handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
				restoreAt = clock.Now().Add(2 * time.Second)
			}
			clock.hook = func() {
				if restored || restoreAt.IsZero() || clock.Now().Before(restoreAt) || scenario == "no_op" {
					return
				}
				restored = true
				d.mu.Lock()
				d.s = full
				handler := d.handler
				d.mu.Unlock()
				handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play"}}})
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if scenario == "no_op" {
				if o.State != journal.Uncertain || ProjectOperation(o).QueueLoading.ConfirmedParts != 1 {
					t.Fatal(o.State, string(o.Progress))
				}
			} else if o.State != journal.Succeeded {
				t.Fatal(o.State, o.ErrorCode, string(o.Outcome))
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			count := 0
			for _, m := range d.writes {
				if m.Kind == heos.MutationKindQueue {
					count++
				}
			}
			if count != 2 {
				t.Fatal("queue command replayed", count)
			}
		})
	}
}

func TestMultipartPartialWindowStopsWithoutStartingMoreParts(t *testing.T) {
	c, j, d, req, cmd := multipartFixture(t)
	clock := c.clock.(*advancingClock)
	began := clock.Now()
	cmd.Automation = &Automation{TargetLevel: 10, DurationSeconds: 4, FadeSeconds: 2}
	d.before = func(m heos.Mutation) {
		if m.Append {
			clock.mu.Lock()
			clock.now = clock.now.Add(2500 * time.Millisecond)
			clock.mu.Unlock()
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Failed || o.ErrorCode != "queue_loading_incomplete" || !clock.Now().Equal(began.Add(4*time.Second)) {
		t.Fatal(o.State, o.ErrorCode, clock.Now().Sub(began), string(o.Outcome))
	}
	p := ProjectOperation(o).QueueLoading
	if p.ConfirmedParts != 2 || p.ConfirmedTracks != 5 {
		t.Fatal(p)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.s.State != heos.PlayStateStop || *d.s.Volume != 0 {
		t.Fatal(d.s)
	}
	queues := 0
	for _, m := range d.writes {
		if m.Kind == heos.MutationKindQueue {
			queues++
		}
	}
	if queues != 2 {
		t.Fatal("append started during fade", queues)
	}
}

func TestMultipartPauseOrQueueEditBeforeNextPartCancels(t *testing.T) {
	for _, scenario := range []string{"pause", "queue_edit", "group", "gap"} {
		t.Run(scenario, func(t *testing.T) {
			c, j, d, req, cmd := multipartFixture(t)
			c.publish = func(o journal.Operation) {
				p := ProjectOperation(o).QueueLoading
				if o.Phase != "queue_loading" || p == nil || p.ConfirmedParts != 1 {
					return
				}
				event := heos.Event{Params: url.Values{"pid": {"1"}}}
				switch scenario {
				case "pause":
					event.Command = "event/player_state_changed"
					event.Params.Set("state", "pause")
				case "queue_edit":
					event.Command = "event/player_queue_changed"
				case "group":
					event.Command = "event/groups_changed"
				case "gap":
					event.Gap = true
				}
				d.handler(event)
			}
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if o.State != journal.Released {
				t.Fatal(o.State, string(o.Outcome))
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			if len(d.writes) != 4 {
				t.Fatal("continued after ownership loss", d.writes)
			}
		})
	}
}

func TestMultipartStopInterruptsPendingAppend(t *testing.T) {
	c, j, d, req, cmd := multipartFixture(t)
	clock := &heldClock{entered: make(chan struct{})}
	c.clock = clock
	d.afterQueue = func(m heos.Mutation) {
		if !m.Append {
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		total := 3
		d.s.Queue = heos.QueuePage{Items: append([]heos.Media(nil), d.s.Queue.Items[:3]...), Total: &total}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-clock.entered:
	case <-time.After(time.Second):
		t.Fatal("append did not reach confirmation")
	}
	req.Key = "priority-stop"
	req.IfMatch = ""
	req.Endpoint = "/v1/players/room/stop"
	b, err := c.Submit(context.Background(), req, Command{Kind: CommandKindStop})
	if err != nil {
		t.Fatal(err)
	}
	if o := awaitOperation(t, j, b.ID); o.State != journal.Succeeded {
		t.Fatal(o.State, string(o.Outcome))
	}
	old, err := j.Get(context.Background(), a.ID)
	if err != nil || old.State != journal.Uncertain || ProjectOperation(old).QueueLoading.ConfirmedParts != 1 {
		t.Fatal(old.State, err, string(old.Progress))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != 6 || d.writes[5].State != heos.PlayStateStop {
		t.Fatal(d.writes)
	}
}

func TestOrderedQueueInitialMismatchNeverStartsTail(t *testing.T) {
	c, j, d, req, cmd := multipartFixture(t)
	d.before = func(m heos.Mutation) {
		if m.Kind == heos.MutationKindQueue && !m.Append {
			d.contents["a"][0], d.contents["a"][1] = d.contents["a"][1], d.contents["a"][0]
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State == journal.Succeeded || ProjectOperation(o).QueueLoading.ConfirmedParts != 0 {
		t.Fatal(o.State, string(o.Progress))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != 4 {
		t.Fatal(d.writes)
	}
}

func TestOrderedFirstPartWaitsForCompleteSequence(t *testing.T) {
	c, j, d, req, cmd := multipartFixture(t)
	clock := c.clock.(*advancingClock)
	var complete heos.Snapshot
	var at time.Time
	d.afterQueue = func(m heos.Mutation) {
		if m.Append {
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		complete = d.s
		total := 1
		d.s.Queue = heos.QueuePage{Items: append([]heos.Media(nil), d.s.Queue.Items[:1]...), Total: &total}
		at = clock.Now().Add(time.Second)
	}
	clock.hook = func() {
		if !at.IsZero() && !clock.Now().Before(at) {
			d.mu.Lock()
			d.s = complete
			d.mu.Unlock()
			at = time.Time{}
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded {
		t.Fatal(o.State, o.ErrorCode, string(o.Outcome))
	}
}

func TestMultipartReconcilesStalePaginationWithoutReplay(t *testing.T) {
	c, j, d, req, cmd := multipartFixture(t)
	d.contents["a"] = nil
	for i := range 125 {
		d.contents["a"] = append(d.contents["a"], heos.Item{Source: "900", MediaID: heos.ID(fmt.Sprint("track-", i)), ContainerID: "a", Type: "song", Playable: "yes"})
	}
	d.afterQueue = func(m heos.Mutation) {
		if m.Append {
			d.stalePages = 2
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded || ProjectOperation(o).QueueLoading.ConfirmedTracks != 252 {
		t.Fatal(o.State, string(o.Outcome), string(o.Progress))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != 6 {
		t.Fatal(d.writes)
	}
}

func TestMultipartAcceptsSingleTracksAndBoundsTotalTracks(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(fmt.Sprint(large), func(t *testing.T) {
			c, j, d, req, cmd := multipartFixture(t)
			cat := c.lanes["room"].device.Catalogs["music"].(multipartCatalog)
			cat.items["track"] = heos.Item{Source: "900", ContainerID: "b", MediaID: "one", Playable: "yes", Type: "track"}
			cmd.ItemRefs = []string{"track", "a", "track"}
			if large {
				d.contents["a"] = make([]heos.Item, heos.MaxBrowseItems)
				for i := range d.contents["a"] {
					d.contents["a"][i] = cat.items["track"]
				}
			}
			preflight, err := c.reads.Preflight(context.Background(), "room", cmd)
			if large {
				if !errors.Is(err, heos.ErrBounds) {
					t.Fatal(err)
				}
			} else if err != nil || !preflight.Ready {
				t.Fatal(preflight, err)
			}
			player, _ := c.reads.Player("room")
			req.IfMatch = fmt.Sprintf("%q", player.Revision)
			a, err := c.Submit(context.Background(), req, cmd)
			if large {
				if !errors.Is(err, heos.ErrBounds) || len(d.writes) != 0 {
					t.Fatal(err, d.writes)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if o.State != journal.Succeeded || ProjectOperation(o).QueueLoading.ConfirmedTracks != 5 {
				t.Fatal(o.State, string(o.Progress))
			}
		})
	}
}

func TestMultipartOrdinaryTransitionBeforeAppendUsesBoundedWait(t *testing.T) {
	c, j, d, req, cmd := multipartFixture(t)
	clock := c.clock.(*advancingClock)
	began := clock.Now()
	waiting, resumed := false, false
	c.publish = func(o journal.Operation) {
		p := ProjectOperation(o).QueueLoading
		if waiting || o.Phase != "queue_loading" || p == nil || p.ConfirmedParts != 1 {
			return
		}
		waiting = true
		d.mu.Lock()
		d.s.State = heos.PlayStateStop
		handler := d.handler
		d.mu.Unlock()
		handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
	}
	clock.hook = func() {
		if !waiting || resumed || clock.Now().Sub(began) < 2*time.Second {
			return
		}
		resumed = true
		d.mu.Lock()
		d.s.State = heos.PlayStatePlay
		handler := d.handler
		d.mu.Unlock()
		handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play"}}})
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if !resumed || o.State != journal.Succeeded || clock.Now().Sub(began) < 2*time.Second {
		t.Fatal(resumed, o.State, string(o.Outcome))
	}
}

func TestAppendReadbackClosesQueueEventExpectationAtomically(t *testing.T) {
	c, _, d, _, _ := multipartFixture(t)
	before := d.Snapshot()
	before.State = heos.PlayStatePlay
	one := 1
	before.Queue = heos.QueuePage{Items: []heos.Media{{ID: "one", QueueID: "1"}}, Total: &one}
	before.Media = &heos.Media{Source: "1024", ID: "one", QueueID: "1"}
	after := before
	two := 2
	after.Queue = heos.QueuePage{Items: []heos.Media{{ID: "one", QueueID: "1"}, {ID: "two", QueueID: "2"}}, Total: &two}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	r := &execution{ctx: ctx, cancel: cancel, expected: before, queueOwned: true, loading: true, ordered: []heos.ID{"one", "two"}, events: map[string][]map[string]string{}, now: c.clock.Now}
	r.expect(heos.Mutation{Kind: heos.MutationKindQueue, Append: true})
	ok, err := c.appendObservation(ctx, c.lanes["room"], r, before, after)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	// A second queue notification after complete readback cannot borrow this
	// command's expectation while the worker is still finalizing its journal.
	r.event(heos.Event{Command: "event/player_queue_changed", Params: url.Values{"pid": {"1"}}})
	if !errors.Is(context.Cause(ctx), ErrOwnership) {
		t.Fatal("later queue edit attributed to the already confirmed append")
	}
}

func TestMultipartRechecksFadeBoundaryAfterJournalBeforeSend(t *testing.T) {
	c, j, d, req, cmd := multipartFixture(t)
	clock := c.clock.(*advancingClock)
	began := clock.Now()
	cmd.Automation = &Automation{TargetLevel: 10, DurationSeconds: 4, FadeSeconds: 2}
	delayed := false
	c.publish = func(o journal.Operation) {
		p := ProjectOperation(o).QueueLoading
		if delayed || o.Phase != "sending_queue" || p == nil || p.ConfirmedParts != 1 {
			return
		}
		delayed = true
		clock.mu.Lock()
		clock.now = began.Add(3 * time.Second)
		clock.mu.Unlock()
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if !delayed || o.ErrorCode != "queue_loading_incomplete" || ProjectOperation(o).QueueLoading.ConfirmedParts != 1 || !clock.Now().Equal(began.Add(4*time.Second)) {
		t.Fatal(o.State, o.ErrorCode, string(o.Progress))
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range d.writes {
		if m.Append {
			t.Fatal("expired append guard reached device", m)
		}
	}
	if d.s.State != heos.PlayStateStop || *d.s.Volume != 0 {
		t.Fatal(d.s.State, *d.s.Volume)
	}
}
