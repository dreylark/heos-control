package control

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
	"github.com/dreylark/heos-control/internal/telemetry"
)

type memoryJournal struct {
	mu   sync.Mutex
	ops  map[string]journal.Operation
	keys map[string]string
	down bool
}

func (j *memoryJournal) Ready(context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.down {
		return ErrUnavailable
	}
	return nil
}
func (j *memoryJournal) Get(_ context.Context, id string) (journal.Operation, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	o, ok := j.ops[id]
	if !ok {
		return o, journal.ErrNotFound
	}
	return o, nil
}
func (j *memoryJournal) Active(_ context.Context, p string) (journal.Operation, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, o := range j.ops {
		if o.Player == p && o.FinishedAt == nil && o.Phase != "cancelling" {
			return o, nil
		}
	}
	return journal.Operation{}, journal.ErrNotFound
}
func (j *memoryJournal) Lookup(ctx context.Context, r journal.Request) (journal.Operation, error) {
	if e := j.Ready(ctx); e != nil {
		return journal.Operation{}, e
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	id, ok := j.keys[r.Key]
	if !ok {
		return journal.Operation{}, journal.ErrNotFound
	}
	return j.ops[id], nil
}
func (j *memoryJournal) Admit(ctx context.Context, r journal.Request, p journal.Proposal) (journal.Admission, error) {
	if e := j.Ready(ctx); e != nil {
		return journal.Admission{}, e
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if p.ReplaceID != "" {
		o := j.ops[p.ReplaceID]
		o.Revision++
		o.State = journal.Released
		if p.ReplaceUncertain {
			o.State = journal.Uncertain
		}
		now := time.Now()
		o.FinishedAt = &now
		if p.Kind == "cancel" {
			o.FinishedAt = nil
			o.State = journal.Running
			o.Phase = "cancelling"
		}
		j.ops[o.ID] = o
	}
	id := "op_" + r.Key
	o := journal.Operation{ID: id, Player: r.Player, Principal: r.Principal, Kind: p.Kind, State: journal.Accepted, Revision: 1, EffectiveArguments: p.EffectiveArguments}
	j.ops[id] = o
	j.keys[r.Key] = id
	return journal.Admission{Operation: o, Created: true}, nil
}
func (j *memoryJournal) Transition(ctx context.Context, id string, rev int64, u journal.Update) (journal.Operation, error) {
	if e := j.Ready(ctx); e != nil {
		return journal.Operation{}, e
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	o := j.ops[id]
	if o.Revision != rev || o.FinishedAt != nil {
		return o, journal.ErrRevision
	}
	o.Revision++
	o.State = u.State
	o.Phase = u.Phase
	o.ErrorCode = u.ErrorCode
	o.Outcome = u.Outcome
	o.Progress = u.Progress
	if u.State != journal.Running {
		now := time.Now()
		o.FinishedAt = &now
	}
	j.ops[id] = o
	return o, nil
}

type fakeDevice struct {
	mu      sync.Mutex
	s       heos.Snapshot
	writes  []heos.Mutation
	handler func(heos.Event)
	before  func(heos.Mutation)
	block   <-chan struct{}
	now     func() time.Time
}

func (d *fakeDevice) Snapshot() heos.Snapshot { d.mu.Lock(); defer d.mu.Unlock(); return d.s }
func (d *fakeDevice) Refresh(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.now != nil {
		d.s.ObservedAt = d.now()
	}
	return ctx.Err()
}
func (d *fakeDevice) Write(ctx context.Context, m heos.Mutation, _ heos.Guard) (heos.Response, error) {
	if d.block != nil && m.Kind == heos.MutationKindVolume {
		select {
		case <-d.block:
		case <-ctx.Done():
			return heos.Response{}, ctx.Err()
		}
	}
	if d.before != nil {
		d.before(m)
	}
	if e := ctx.Err(); e != nil {
		return heos.Response{}, e
	}
	var events []heos.Event
	d.mu.Lock()
	defer func() {
		d.mu.Unlock()
		for _, event := range events {
			d.handler(event)
		}
	}()
	d.writes = append(d.writes, m)
	switch m.Kind {
	case heos.MutationKindMode:
		d.s.Repeat = m.Repeat
		d.s.Shuffle = m.Shuffle
		// The ordinary fake applies mode synchronously and emits Denon
		// 5.10/5.11 notifications. Delayed/missing-event fixtures override this.
		if d.handler != nil {
			shuffle := "off"
			if m.Shuffle {
				shuffle = "on"
			}
			events = []heos.Event{
				{Command: "event/repeat_mode_changed", Params: url.Values{"pid": {string(d.s.Player.ID)}, "repeat": {string(m.Repeat)}}},
				{Command: "event/shuffle_mode_changed", Params: url.Values{"pid": {string(d.s.Player.ID)}, "shuffle": {shuffle}}},
			}
		}
	case heos.MutationKindVolume:
		v := m.Level
		d.s.Volume = &v
	case heos.MutationKindMute:
		v := m.Muted
		d.s.Muted = &v
	case heos.MutationKindTransport:
		d.s.State = m.State
	case heos.MutationKindQueue:
		d.s.State = heos.PlayStatePlay
		d.s.Media = &heos.Media{Source: "1024", ID: m.Item.MediaID, QueueID: "1", Album: m.Item.Name}
		d.s.Queue.Items = []heos.Media{*d.s.Media}
		total := 1
		d.s.Queue.Total = &total
	}
	if (m.Kind == heos.MutationKindVolume || m.Kind == heos.MutationKindMute) && d.handler != nil {
		mute := "off"
		if *d.s.Muted {
			mute = "on"
		}
		events = append(events, heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {string(d.s.Player.ID)}, "level": {fmt.Sprint(*d.s.Volume)}, "mute": {mute}}})
	}
	return heos.Response{}, nil
}
func (d *fakeDevice) BeginPriority() func()              { return func() {} }
func (d *fakeDevice) SetEventHandler(h func(heos.Event)) { d.handler = h }
func (d *fakeDevice) BrowseAll(context.Context, heos.ID, heos.ID) ([]heos.Item, error) {
	return nil, nil
}
func (d *fakeDevice) Queue(context.Context, heos.ID, int, int) (heos.QueuePage, error) {
	return heos.QueuePage{}, nil
}
func (d *fakeDevice) PlayerView(heos.ID) heos.View { return heos.View{Connected: true} }
func (d *fakeDevice) View() heos.View              { return heos.View{Connected: true} }
func fixtureCoordinator(t *testing.T) (*Coordinator, *memoryJournal, *fakeDevice, journal.Request) {
	t.Helper()
	v := 20
	m := false
	d := &fakeDevice{s: heos.Snapshot{Player: heos.Player{ID: "1", Serial: "serial"}, State: heos.PlayStateStop, Volume: &v, Muted: &m, Connected: true, Verified: true, ObservedAt: time.Now()}}
	ceiling := 40
	reads := NewReads("test", []Device{{Config: config.Player{Key: "room", Serial: "serial", WritesEnabled: true, VolumeCeiling: &ceiling}, Observer: d, Client: d}}, nil)
	j := &memoryJournal{ops: map[string]journal.Operation{}, keys: map[string]string{}}
	c := NewCoordinator(context.Background(), reads, j, nil, nil)
	d.now = func() time.Time { return c.clock.Now() }
	t.Cleanup(c.Close)
	p, _ := reads.Player("room")
	return c, j, d, journal.Request{Principal: "owner", Player: "room", Key: "one", Method: "PUT", Endpoint: "/v1/players/room/volume", IfMatch: `"` + p.Revision + `"`, Body: json.RawMessage(`{"unit":"heos","level":10,"takeover":false}`)}
}
func awaitOperation(t *testing.T, j *memoryJournal, id string) journal.Operation {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		o, _ := j.Get(context.Background(), id)
		if o.FinishedAt != nil {
			return o
		}
		select {
		case <-deadline:
			t.Fatal("operation did not finish")
		case <-time.After(time.Millisecond):
		}
	}
}
func TestAcceptedWorkSurvivesDisconnectAndRetry(t *testing.T) {
	c, j, d, r := fixtureCoordinator(t)
	ctx, cancel := context.WithCancel(context.Background())
	a, e := c.Submit(ctx, r, Command{Kind: CommandKindVolume, Level: 10})
	if e != nil {
		t.Fatal(e)
	}
	cancel()
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded {
		t.Fatal(o)
	}
	b, e := c.Submit(context.Background(), r, Command{Kind: CommandKindVolume, Level: 10})
	if e != nil || b.ID != a.ID {
		t.Fatal(b, e)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != 1 {
		t.Fatal(d.writes)
	}
}
func TestAdmissionSafety(t *testing.T) {
	for _, kind := range []string{"stale", "unknown", "grouped", "disabled", "database", "revision"} {
		t.Run(kind, func(t *testing.T) {
			c, _, d, r := fixtureCoordinator(t)
			switch kind {
			case "stale":
				d.s.Stale = true
			case "unknown":
				d.s.State = heos.PlayStateUnknown
			case "grouped":
				d.s.Grouped = true
			case "disabled":
				c.lanes["room"].device.Config.WritesEnabled = false
			case "database":
				c.db.(*memoryJournal).down = true
			case "revision":
				r.IfMatch = `"wrong"`
			}
			if _, e := c.Submit(context.Background(), r, Command{Kind: CommandKindVolume, Level: 10}); e == nil {
				t.Fatal("unsafe admission")
			}
			if len(d.writes) != 0 {
				t.Fatal("rejection wrote")
			}
		})
	}
}

type changingObservation struct {
	*fakeDevice
	onRefresh func() error
}

func (o changingObservation) Refresh(context.Context) error { return o.onRefresh() }

func TestIdleCacheCannotBypassFreshChecksBeforeWriting(t *testing.T) {
	for _, change := range []string{"volume", "group", "offline", "unknown"} {
		t.Run(change, func(t *testing.T) {
			c, j, d, request := fixtureCoordinator(t)
			// A quiet event subscription permits displaying a four-minute-old
			// snapshot. A lost event must never turn that cache into a write grant.
			d.s.ObservedAt = time.Now().Add(-4 * time.Minute)
			var refreshed bool
			c.lanes["room"].device.Observer = changingObservation{d, func() error {
				refreshed = true
				d.mu.Lock()
				defer d.mu.Unlock()
				d.s.ObservedAt = time.Now()
				switch change {
				case "volume":
					level := 25
					d.s.Volume = &level
				case "group":
					d.s.Grouped = true
				case "offline":
					return ErrUnavailable
				case "unknown":
					d.s.State = heos.PlayStateUnknown
				}
				return nil
			}}
			player, _ := c.reads.Player("room")
			request.IfMatch = `"` + player.Revision + `"`
			op, err := c.Submit(context.Background(), request, Command{Kind: CommandKindVolume, Level: 30})
			if err != nil {
				t.Fatal(err)
			}
			result := awaitOperation(t, j, op.ID)
			if !refreshed || result.State == journal.Succeeded || len(d.writes) != 0 {
				t.Fatalf("cached state bypassed fresh checks: refreshed=%t operation=%+v writes=%v", refreshed, result, d.writes)
			}
		})
	}
}
func TestManualSameAlbumSkipRevokesFutureWrites(t *testing.T) {
	c, j, d, r := fixtureCoordinator(t)
	d.before = func(heos.Mutation) {
		d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: map[string][]string{"pid": {"1"}}})
	}
	a, e := c.Submit(context.Background(), r, Command{Kind: CommandKindVolume, Level: 10})
	if e != nil {
		t.Fatal(e)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Released && o.State != journal.Uncertain {
		t.Fatal(o)
	}
	if len(d.writes) != 0 {
		t.Fatal("write after manual event")
	}
}

func TestStopPreemptsPendingWriteAndRecordsUncertainty(t *testing.T) {
	c, j, d, r := fixtureCoordinator(t)
	block := make(chan struct{})
	d.block = block
	a, e := c.Submit(context.Background(), r, Command{Kind: CommandKindVolume, Level: 10})
	if e != nil {
		t.Fatal(e)
	}
	deadline := time.After(time.Second)
	for {
		o, _ := j.Get(context.Background(), a.ID)
		if o.Phase == "sending_volume" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("write did not start")
		case <-time.After(time.Millisecond):
		}
	}
	r.Key = "stop"
	r.Endpoint = "/v1/players/room/stop"
	r.Method = "POST"
	r.IfMatch = ""
	r.Body = json.RawMessage(`{"fade_seconds":0}`)
	b, e := c.Submit(context.Background(), r, Command{Kind: CommandKindStop})
	if e != nil {
		t.Fatal(e)
	}
	o := awaitOperation(t, j, b.ID)
	if o.State != journal.Succeeded {
		t.Fatal(o)
	}
	old, _ := j.Get(context.Background(), a.ID)
	if old.State != journal.Uncertain {
		t.Fatal("lost interrupted outcome", old)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != 1 || d.writes[0].State != heos.PlayStateStop {
		t.Fatal(d.writes)
	}
}

type advancingClock struct {
	mu   sync.Mutex
	now  time.Time
	hook func()
}

func (f *advancingClock) Now() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.now }
func (f *advancingClock) Wait(ctx context.Context, d time.Duration, wake <-chan struct{}) error {
	select {
	case <-wake:
		return context.Cause(ctx)
	default:
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
	if f.hook != nil {
		f.hook()
	}
	return ctx.Err()
}
func TestFadeWritesEachIntegerOnceAndStops(t *testing.T) {
	c, j, d, r := fixtureCoordinator(t)
	c.clock = &advancingClock{now: time.Now()}
	r.IfMatch = ""
	r.Key = "fade"
	r.Method = "POST"
	r.Endpoint = "/v1/players/room/stop"
	r.Body = json.RawMessage(`{"fade_seconds":3}`)
	a, e := c.Submit(context.Background(), r, Command{Kind: CommandKindStop, FadeSeconds: 3})
	if e != nil {
		t.Fatal(e)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded {
		t.Fatal(o)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	last := 21
	for _, m := range d.writes {
		if m.Kind == heos.MutationKindVolume {
			if m.Level >= last {
				t.Fatal("repeated/increasing level", d.writes)
			}
			last = m.Level
		}
	}
	if last != 0 || d.writes[len(d.writes)-1].State != heos.PlayStateStop {
		t.Fatal(d.writes)
	}
}

// The clock pauses an active fade without real-time sleeps, leaving ownership
// observable for cancellation and a second, immediate operator stop.
type heldClock struct {
	entered chan struct{}
	once    sync.Once
}

func (h *heldClock) Now() time.Time { return time.Now() }
func (h *heldClock) Wait(ctx context.Context, _ time.Duration, wake <-chan struct{}) error {
	h.once.Do(func() { close(h.entered) })
	<-ctx.Done()
	return context.Cause(ctx)
}
func TestCancelAndImmediateStopDuringFade(t *testing.T) {
	for _, mode := range []string{"release", "stop_owned", "operator_stop"} {
		t.Run(mode, func(t *testing.T) {
			c, j, d, r := fixtureCoordinator(t)
			clock := &heldClock{entered: make(chan struct{})}
			c.clock = clock
			r.Method = "POST"
			r.Endpoint = "/v1/players/room/stop"
			r.IfMatch = ""
			r.Body = json.RawMessage(`{"fade_seconds":30}`)
			a, e := c.Submit(context.Background(), r, Command{Kind: CommandKindStop, FadeSeconds: 30})
			if e != nil {
				t.Fatal(e)
			}
			select {
			case <-clock.entered:
			case <-time.After(time.Second):
				t.Fatal("fade never started")
			}
			r.Key = "cancel"
			r.Endpoint = "/v1/operations/" + a.ID + "/cancel"
			r.Body = json.RawMessage(`{"mode":"` + mode + `"}`)
			cmd := Command{Kind: CommandKindCancel, Target: a.ID, Mode: mode}
			if mode == "operator_stop" {
				cmd = Command{Kind: CommandKindStop}
			}
			b, e := c.Submit(context.Background(), r, cmd)
			if e != nil {
				t.Fatal(e)
			}
			o := awaitOperation(t, j, b.ID)
			if o.State != journal.Succeeded {
				t.Fatal(o)
			}
			old, _ := j.Get(context.Background(), a.ID)
			want := journal.Cancelled
			if mode == "operator_stop" {
				want = journal.Released
			}
			if old.State != want {
				t.Fatal(old)
			}
			if _, e = c.Submit(context.Background(), r, cmd); e != nil {
				t.Fatal(e)
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			count := 0
			for _, m := range d.writes {
				if m.Kind == heos.MutationKindTransport {
					count++
				}
			}
			expected := 1
			if mode == "release" {
				expected = 0
			}
			if count != expected {
				t.Fatal(d.writes)
			}
		})
	}
}

type failedPhaseJournal struct {
	*memoryJournal
	count   int
	dropAck bool
}

func (f *failedPhaseJournal) Transition(ctx context.Context, id string, rev int64, u journal.Update) (journal.Operation, error) {
	if u.State == journal.Running {
		f.count++
		if !f.dropAck && f.count == 2 {
			return journal.Operation{}, ErrUnavailable
		}
	}
	o, e := f.memoryJournal.Transition(ctx, id, rev, u)
	if e == nil && f.dropAck {
		return journal.Operation{}, journal.ErrCommitUncertain
	}
	return o, e
}
func TestJournalFailureNeverReplaysDeviceWork(t *testing.T) {
	for _, dropAck := range []bool{false, true} {
		t.Run(fmt.Sprint(dropAck), func(t *testing.T) {
			c, j, d, r := fixtureCoordinator(t)
			c.db = &failedPhaseJournal{memoryJournal: j, dropAck: dropAck}
			c.clock = &advancingClock{now: time.Now()}
			r.Method = "POST"
			r.Endpoint = "/v1/players/room/stop"
			r.IfMatch = ""
			r.Body = json.RawMessage(`{"fade_seconds":2}`)
			a, e := c.Submit(context.Background(), r, Command{Kind: CommandKindStop, FadeSeconds: 2})
			if e != nil {
				t.Fatal(e)
			}
			o := awaitOperation(t, j, a.ID)
			d.mu.Lock()
			defer d.mu.Unlock()
			if dropAck {
				if o.State != journal.Succeeded || d.writes[len(d.writes)-1].State != heos.PlayStateStop {
					t.Fatal(o, d.writes)
				}
			} else {
				if o.State != journal.Uncertain || len(d.writes) != 1 {
					t.Fatal(o, d.writes)
				}
			}
		})
	}
}

func TestExplicitTakeoverCanRevokeAFade(t *testing.T) {
	c, j, _, r := fixtureCoordinator(t)
	clock := &heldClock{entered: make(chan struct{})}
	c.clock = clock
	r.Method = "POST"
	r.IfMatch = ""
	r.Body = json.RawMessage(`{"fade_seconds":30}`)
	old, e := c.Submit(context.Background(), r, Command{Kind: CommandKindStop, FadeSeconds: 30})
	if e != nil {
		t.Fatal(e)
	}
	select {
	case <-clock.entered:
	case <-time.After(time.Second):
		t.Fatal("fade did not start")
	}
	p, _ := c.reads.Player("room")
	r.Key = "takeover"
	r.Method = "PUT"
	r.Endpoint = "/v1/players/room/mute"
	r.IfMatch = fmt.Sprintf("%q", p.Revision)
	r.Body = json.RawMessage(`{"muted":true,"takeover":true}`)
	a, e := c.Submit(context.Background(), r, Command{Kind: CommandKindMute, Muted: true, Takeover: true})
	if e != nil {
		t.Fatal(e)
	}
	if o := awaitOperation(t, j, a.ID); o.State != journal.Succeeded {
		t.Fatal(o)
	}
	if o, _ := j.Get(context.Background(), old.ID); o.State != journal.Released {
		t.Fatal(o)
	}
}

type observedWaitClock struct{ entered chan struct{} }

func (c *observedWaitClock) Now() time.Time { return time.Now() }
func (c *observedWaitClock) Wait(ctx context.Context, _ time.Duration, wake <-chan struct{}) error {
	select {
	case c.entered <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	<-ctx.Done()
	return context.Cause(ctx)
}
func TestStopInterruptingCancellationFinishesTheOriginalTarget(t *testing.T) {
	c, j, _, r := fixtureCoordinator(t)
	metrics := telemetry.NewPlayer("room")
	c.lanes["room"].device.Metrics = metrics
	clock := &observedWaitClock{entered: make(chan struct{}, 3)}
	c.clock = clock
	r.Method = "POST"
	r.IfMatch = ""
	r.Body = json.RawMessage(`{"fade_seconds":30}`)
	original, e := c.Submit(context.Background(), r, Command{Kind: CommandKindStop, FadeSeconds: 30})
	if e != nil {
		t.Fatal(e)
	}
	wait := func() {
		t.Helper()
		select {
		case <-clock.entered:
		case <-time.After(time.Second):
			t.Fatal("fade did not enter wait")
		}
	}
	wait()
	r.Key = "cancel"
	r.Endpoint = "/v1/operations/" + original.ID + "/cancel"
	r.Body = json.RawMessage(`{"mode":"stop_owned","fade_seconds":30}`)
	cancelled, e := c.Submit(context.Background(), r, Command{Kind: CommandKindCancel, Mode: "stop_owned", Target: original.ID, FadeSeconds: 30})
	if e != nil {
		t.Fatal(e)
	}
	wait()
	r.Key = "stop-now"
	r.Endpoint = "/v1/players/room/stop"
	r.Body = json.RawMessage(`{"fade_seconds":0}`)
	stop, e := c.Submit(context.Background(), r, Command{Kind: CommandKindStop})
	if e != nil {
		t.Fatal(e)
	}
	for _, id := range []string{stop.ID, cancelled.ID, original.ID} {
		awaitOperation(t, j, id)
	}
	c.Close()
	total := 0.0
	for _, sample := range metricSamples(t, metrics, "heos_operations_finished_total") {
		total += sample.GetCounter().GetValue()
	}
	if total != 3 {
		t.Fatal("original, cancellation and stop must each finish once", total)
	}
	for _, sample := range metricSamples(t, metrics, "heos_active_operations") {
		if sample.GetGauge().GetValue() != 0 {
			t.Fatal("superseded cancellation left an active phase", sample)
		}
	}
}

func TestShutdownJoinsAcceptedWorkWithoutCleanupWrites(t *testing.T) {
	c, j, d, r := fixtureCoordinator(t)
	d.block = make(chan struct{})
	a, e := c.Submit(context.Background(), r, Command{Kind: CommandKindVolume, Level: 10})
	if e != nil {
		t.Fatal(e)
	}
	c.Close()
	o, e := j.Get(context.Background(), a.ID)
	if e != nil || o.FinishedAt == nil || (o.State != journal.Interrupted && o.State != journal.Uncertain) {
		t.Fatal(o, e)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != 0 {
		t.Fatal(d.writes)
	}
}
func TestUnknownOrIncompleteEventsCannotBorrowAcknowledgements(t *testing.T) {
	for _, event := range []heos.Event{{Command: "event/player_volume_changed", Params: map[string][]string{"level": {"10"}, "mute": {"off"}}}, {Command: "event/new_global_event", Params: map[string][]string{"pid": {"other"}}}} {
		c, j, d, r := fixtureCoordinator(t)
		d.before = func(heos.Mutation) { d.handler(event) }
		a, e := c.Submit(context.Background(), r, Command{Kind: CommandKindVolume, Level: 10})
		if e != nil {
			t.Fatal(e)
		}
		o := awaitOperation(t, j, a.ID)
		if o.State == journal.Succeeded {
			t.Fatal("ambiguous event accepted", event)
		}
	}
}
