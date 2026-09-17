package control

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type pendingDevice struct {
	*albumDevice
	kind, change string
	pending      bool
	reads        int
	states       []string
	noMedia      bool
}

func (d *pendingDevice) Write(ctx context.Context, m heos.Mutation, g heos.Guard) (heos.Response, error) {
	before := d.Snapshot().State
	r, err := d.albumDevice.Write(ctx, m, g)
	if err == nil && m.Kind == d.kind {
		d.mu.Lock()
		d.s.State = before // Successful reply, but the device has not changed state.
		d.pending = true
		d.mu.Unlock()
	}
	return r, err
}

func (d *pendingDevice) Refresh(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pending {
		d.reads++
		if len(d.states) != 0 {
			d.s.State = d.states[min(d.reads-1, len(d.states)-1)]
			if d.noMedia {
				// State and media are read separately (Denon 4.2.3/4.2.5).
				d.s.Media = nil
			}
		}
		if d.reads == 2 {
			switch d.change {
			case "hybrid-queued-media":
				media := d.tracks[1]
				media.QueueID = d.tracks[0].QueueID
				d.s.Media = &media
			case "reset-media":
				media := d.tracks[0]
				d.s.Media = &media
			case "empty-media":
				d.s.Media = nil
			case "volume":
				v := *d.s.Volume + 1
				d.s.Volume = &v
			case "source":
				media := *d.s.Media
				media.Source = "foreign"
				d.s.Media = &media
			case "queue":
				d.s.Queue.Items = append([]heos.Media(nil), d.s.Queue.Items...)
				d.s.Queue.Items[0].ID = "foreign"
			case "generation":
				d.s.Token.Generation++
			case "pause":
				d.s.State = "pause"
			}
		}
		if d.reads == 3 && d.change == "hybrid-queued-media" {
			media := d.tracks[1]
			d.s.Media = &media
		}
	}
	return ctx.Err()
}

func TestPlayRetainedShuffledQueueWaitsForFinalMIDQID(t *testing.T) {
	c, j, base, req, _ := albumFixture(t)
	total := len(base.tracks)
	base.s.State, base.s.Shuffle = "stop", true
	base.s.Queue = heos.QueuePage{Items: append([]heos.Media(nil), base.tracks...), Total: &total}
	first := base.tracks[0]
	base.s.Media = &first
	d := &pendingDevice{albumDevice: base, kind: "transport", change: "hybrid-queued-media", states: []string{"stop", "unknown", "play"}}
	c.lanes["room"].writer, c.lanes["room"].device.Observer = d, d
	c.clock = &advancingClock{now: time.Now()}
	p, _ := c.reads.Player("room")
	req.IfMatch = fmt.Sprintf("%q", p.Revision)
	a, err := c.Submit(context.Background(), req, Command{Kind: "transport", State: "play"})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded || d.reads != 3 || len(d.writes) != 1 {
		t.Fatal(o, d.reads, d.writes)
	}
}

func TestQueueLoadingStopBeforePlaybackConfirmation(t *testing.T) {
	for _, initial := range []string{"pause", "stop"} {
		for _, tc := range []struct {
			name    string
			states  []string
			noMedia bool
			event   string
			change  string
			code    string
		}{
			{name: "loading", states: []string{initial, "stop", "play"}},
			{name: "unknown-loading", states: []string{initial, "stop", "unknown", "unknown", "play"}},
			{name: "unknown-timeout", states: []string{"unknown"}, code: "device_unavailable"},
			{name: "unknown-after-play-event", states: []string{"unknown", "unknown", "play"}, event: "play"},
			{name: "unknown-after-play-readback", states: []string{"play", "unknown"}, noMedia: true, code: "device_unavailable"},
			{name: "unknown-manual-pause", states: []string{"unknown"}, event: "pause", code: "ownership_lost"},
			{name: "unknown-volume-change", states: []string{"unknown"}, change: "volume", code: "ownership_lost"},
			{name: "unknown-source-change", states: []string{"unknown"}, change: "source", code: "ownership_lost"},
			{name: "unknown-queue-change", states: []string{"unknown"}, change: "queue", code: "ownership_lost"},
			{name: "unknown-generation-change", states: []string{"unknown"}, change: "generation", code: "ownership_lost"},
			{name: "stays-stopped", states: []string{"stop"}, code: "device_unavailable"},
			{name: "repeated-stop-deadline", states: []string{"stop"}, event: "stop", code: "device_unavailable"},
			{name: "stop-after-play-readback", states: []string{"play", "stop"}, noMedia: true, code: "ownership_lost"},
			{name: "pause-after-play-readback", states: []string{"play", "pause"}, noMedia: true, code: "ownership_lost"},
			{name: "stop-event-after-play-readback", states: []string{"play"}, noMedia: true, event: "stop", code: "ownership_lost"},
			{name: "late-play-event", states: []string{"play"}, noMedia: true, event: "play", code: "device_unavailable"},
		} {
			t.Run(initial+"/"+tc.name, func(t *testing.T) {
				c, j, base, req, cmd := albumFixture(t)
				base.s.State = initial
				p, _ := c.reads.Player("room")
				req.IfMatch = fmt.Sprintf("%q", p.Revision)
				d := &pendingDevice{albumDevice: base, kind: "queue", states: tc.states, noMedia: tc.noMedia, change: tc.change}
				c.lanes["room"].writer, c.lanes["room"].device.Observer = d, d
				clock := &advancingClock{now: time.Now()}
				c.clock = clock
				if tc.event != "" {
					clock.hook = func() {
						d.mu.Lock()
						pending, reads, handler := d.pending, d.reads, d.handler
						d.mu.Unlock()
						if pending && (reads == 1 || tc.name == "repeated-stop-deadline") {
							handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {tc.event}}})
						}
					}
				}
				// Finish this short command on confirmation; the TLS regression
				// also verifies the complete bounded ramp/fade/stop sequence.
				cmd.Automation = nil
				began := clock.Now()
				a, err := c.Submit(context.Background(), req, cmd)
				if err != nil {
					t.Fatal(err)
				}
				o := awaitOperation(t, j, a.ID)
				want := journal.Succeeded
				if tc.code != "" {
					want = journal.Uncertain
				}
				if o.State != want || o.ErrorCode != tc.code {
					t.Fatal("unexpected confirmation outcome", o)
				}
				if tc.code == "device_unavailable" && clock.Now().Sub(began) != 12*time.Second {
					t.Fatal("stopped queue escaped confirmation deadline")
				}
				d.mu.Lock()
				defer d.mu.Unlock()
				if len(d.writes) != 4 || d.writes[3].Kind != "queue" {
					t.Fatal("queue setter replayed or extra cleanup sent", d.writes)
				}
			})
		}
	}
}

func TestPendingReadbackHasDeadlineAndNeverRepeatsWrites(t *testing.T) {
	for _, scenario := range []string{"queue-timeout", "stop-timeout", "volume", "source", "queue", "generation", "pause"} {
		t.Run(scenario, func(t *testing.T) {
			c, j, base, req, cmd := albumFixture(t)
			d := &pendingDevice{albumDevice: base, kind: "queue", change: scenario}
			if scenario == "stop-timeout" {
				d.kind = "transport"
				d.s.State = "play"
				cmd = Command{Kind: "stop"}
				req.Endpoint = "/v1/players/room/stop"
			}
			c.lanes["room"].writer = d
			c.lanes["room"].device.Observer = d
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			began := clock.Now()
			a, err := c.Submit(context.Background(), req, cmd)
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if o.State != journal.Uncertain {
				t.Fatal("unconfirmed write was not uncertain", o)
			}
			elapsed := clock.Now().Sub(began)
			if scenario == "queue-timeout" || scenario == "stop-timeout" {
				if elapsed != 12*time.Second || o.ErrorCode != "device_unavailable" {
					t.Fatal("confirmation was not bounded", elapsed, o)
				}
			} else if elapsed != time.Second || o.ErrorCode != "ownership_lost" {
				t.Fatal("intervention was ignored while waiting", elapsed, o)
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			count := 0
			for _, m := range d.writes {
				if m.Kind == d.kind {
					count++
				}
			}
			if count != 1 {
				t.Fatal("pending setter replayed", d.writes)
			}
		})
	}
}

// Home 150 can reset its selected MID/QID to the first unchanged queue entry
// when Stop completes; Denon 4.2.4 does not promise to retain display metadata.
func TestStopConfirmationChecksQueueWhenPositionResets(t *testing.T) {
	for _, scenario := range []string{"reset-media", "empty-media", "foreign-media", "queue", "volume", "unknown"} {
		t.Run(scenario, func(t *testing.T) {
			c, j, base, req, _ := albumFixture(t)
			base.s.State = "play"
			total := len(base.tracks)
			base.s.Queue = heos.QueuePage{Items: append([]heos.Media(nil), base.tracks...), Total: &total}
			current := base.tracks[2]
			base.s.Media = &current
			change := scenario
			if scenario == "foreign-media" {
				change = "reset-media"
				base.tracks[0].ID = "foreign"
			}
			states := []string{"play", "stop"}
			if scenario == "unknown" {
				states = []string{"unknown", "stop"}
				change = "reset-media"
			}
			d := &pendingDevice{albumDevice: base, kind: "transport", states: states, change: change}
			c.lanes["room"].writer, c.lanes["room"].device.Observer = d, d
			c.clock = &advancingClock{now: time.Now()}
			req.Endpoint = "/v1/players/room/stop"
			p, _ := c.reads.Player("room")
			req.IfMatch = fmt.Sprintf("%q", p.Revision)
			a, err := c.Submit(context.Background(), req, Command{Kind: "stop"})
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			valid := scenario == "reset-media" || scenario == "empty-media" || scenario == "unknown"
			if (o.State == journal.Succeeded) != valid {
				t.Fatal(o)
			}
			if !valid && o.ErrorCode != "ownership_lost" {
				t.Fatal(o)
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			if len(d.writes) != 1 || d.writes[0].Kind != "transport" {
				t.Fatal("stop replayed", d.writes)
			}
		})
	}
}

func TestTransportConfirmationWaitsForRequestedState(t *testing.T) {
	for _, target := range []string{"play", "pause", "stop"} {
		for _, scenario := range []string{"delayed", "timeout", "changed-volume"} {
			t.Run(target+"/"+scenario, func(t *testing.T) {
				c, j, base, req, _ := albumFixture(t)
				initial := "play"
				if target == "play" {
					initial = "stop"
				}
				base.s.State = initial
				states := []string{initial, "unknown", target}
				change := ""
				if scenario == "timeout" {
					states = []string{"unknown"}
				}
				if scenario == "changed-volume" {
					change = "volume"
				}
				d := &pendingDevice{albumDevice: base, kind: "transport", states: states, change: change}
				c.lanes["room"].writer, c.lanes["room"].device.Observer = d, d
				clock := &advancingClock{now: time.Now()}
				c.clock = clock
				began := clock.Now()
				req.Endpoint = "/v1/players/room/transport"
				p, _ := c.reads.Player("room")
				req.IfMatch = fmt.Sprintf("%q", p.Revision)
				a, err := c.Submit(context.Background(), req, Command{Kind: "transport", State: target})
				if err != nil {
					t.Fatal(err)
				}
				o := awaitOperation(t, j, a.ID)
				switch scenario {
				case "delayed":
					if o.State != journal.Succeeded {
						t.Fatal(o)
					}
				case "timeout":
					if o.ErrorCode != "device_unavailable" || clock.Now().Sub(began) != 12*time.Second {
						t.Fatal(o, clock.Now().Sub(began))
					}
				case "changed-volume":
					if o.ErrorCode != "ownership_lost" {
						t.Fatal(o)
					}
				}
				d.mu.Lock()
				defer d.mu.Unlock()
				if len(d.writes) != 1 {
					t.Fatal("transport write replayed", d.writes)
				}
			})
		}
	}
}

func TestTransportMediaEventsRequireFreshMatchingReadback(t *testing.T) {
	for _, target := range []string{"play", "pause", "stop"} {
		for _, foreign := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/foreign=%t", target, foreign), func(t *testing.T) {
				c, j, d, req := fixtureCoordinator(t)
				d.s.Media = &heos.Media{Source: "1024", ID: "track", QueueID: "1"}
				d.s.State = "play"
				if target == "play" {
					d.s.State = "stop"
				}
				p, _ := c.reads.Player("room")
				req.IfMatch = fmt.Sprintf("%q", p.Revision)
				d.before = func(heos.Mutation) {
					for range 2 {
						d.handler(heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}})
					}
					if foreign {
						d.mu.Lock()
						m := *d.s.Media
						m.ID = "foreign"
						d.s.Media = &m
						d.mu.Unlock()
					}
				}
				a, err := c.Submit(context.Background(), req, Command{Kind: "transport", State: target})
				if err != nil {
					t.Fatal(err)
				}
				o := awaitOperation(t, j, a.ID)
				if foreign {
					if o.ErrorCode != "ownership_lost" {
						t.Fatal(o)
					}
				} else if o.State != journal.Succeeded {
					t.Fatal(o)
				}
				if len(d.writes) != 1 {
					t.Fatal("transport setter replayed or not exercised", d.writes)
				}
			})
		}
	}
}
