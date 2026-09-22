package control

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func TestSkipWaitsForHybridMediaToSettle(t *testing.T) {
	for _, first := range []string{"mid", "qid"} {
		t.Run(first, func(t *testing.T) {
			c, j, d, req := skipFixture(t, "stuck", 0, 1)
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			hybrid := d.tracks[1]
			if first == "mid" {
				hybrid.QueueID = d.tracks[0].QueueID
			} else {
				hybrid.ID = d.tracks[0].ID
			}
			skipStartsWithMedia(d, hybrid)
			clock.hook = func() {
				d.mu.Lock()
				defer d.mu.Unlock()
				settled := d.tracks[1]
				d.s.Media = &settled
			}
			a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if o.State != journal.Succeeded || d.reads != 2 || len(d.writes) != 1 {
				t.Fatalf("hybrid transition: state=%s code=%s reads=%d writes=%v", o.State, o.ErrorCode, d.reads, d.writes)
			}
		})
	}
}

func TestSkipRejectsForeignTransitionIdentifiers(t *testing.T) {
	for _, field := range []string{"source", "mid", "qid"} {
		t.Run(field, func(t *testing.T) {
			c, j, d, req := skipFixture(t, "stuck", 0, 1)
			c.clock = &advancingClock{now: time.Now()}
			media := d.tracks[1]
			switch field {
			case "source":
				media.Source = "foreign"
			case "mid":
				media.ID = "foreign"
			case "qid":
				media.QueueID = "foreign"
			}
			skipStartsWithMedia(d, media)
			a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, j, a.ID)
			if o.State != journal.Uncertain || o.ErrorCode != "ownership_lost" || d.reads != 1 || len(d.writes) != 1 {
				t.Fatalf("foreign %s: state=%s code=%s reads=%d writes=%v", field, o.State, o.ErrorCode, d.reads, d.writes)
			}
		})
	}
}

func TestSkipHybridMediaAndDuplicateEventsKeepOriginalDeadline(t *testing.T) {
	c, j, d, req := skipFixture(t, "stuck", 0, 1)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	hybrid := d.tracks[1]
	hybrid.QueueID = d.tracks[0].QueueID
	skipStartsWithMedia(d, hybrid)
	clock.hook = func() {
		for range 8 {
			d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"play"}}})
			d.handler(mediaObservationEvent())
		}
	}
	began := clock.Now()
	a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Uncertain || o.ErrorCode != "device_unavailable" || clock.Now().Sub(began) != 12*time.Second {
		t.Fatalf("unresolved transition: state=%s code=%s elapsed=%s", o.State, o.ErrorCode, clock.Now().Sub(began))
	}
	if d.reads < 2 || d.reads > 48 || len(d.writes) != 1 {
		t.Fatalf("confirmation exceeded read/write budget: reads=%d writes=%v", d.reads, d.writes)
	}
}

func TestSkipConfirmationReconcilesEventsAfterSnapshot(t *testing.T) {
	for _, event := range []string{"stop", "now_playing"} {
		for _, settles := range []bool{false, true} {
			name := event + "/unresolved"
			if settles {
				name = event + "/settled"
			}
			t.Run(name, func(t *testing.T) {
				c, j, d, req := skipFixture(t, "", 0, 1)
				clock := &advancingClock{now: time.Now()}
				c.clock = clock
				observer := &skipEventAfterSnapshot{skipDevice: d, event: event}
				l := c.lanes["room"]
				l.device.Observer = observer
				c.reads.devices[0] = l.device
				if settles {
					clock.hook = func() {
						d.mu.Lock()
						defer d.mu.Unlock()
						next := d.tracks[1]
						d.s.State, d.s.Media = "play", &next
					}
				}
				began := clock.Now()
				a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
				if err != nil {
					t.Fatal(err)
				}
				o := awaitOperation(t, j, a.ID)
				want := journal.Uncertain
				if settles {
					want = journal.Succeeded
				}
				if !observer.injected || o.State != want || d.reads < 2 || len(d.writes) != 1 {
					t.Fatalf("event after assembled snapshot: injected=%v state=%s want=%s code=%s reads=%d writes=%v", observer.injected, o.State, want, o.ErrorCode, d.reads, d.writes)
				}
				if settles && d.reads != 2 {
					t.Fatalf("settled confirmation reads=%d, want 2", d.reads)
				}
				if !settles && (o.ErrorCode != "device_unavailable" || clock.Now().Sub(began) != 12*time.Second) {
					t.Fatalf("unresolved event: code=%s elapsed=%s", o.ErrorCode, clock.Now().Sub(began))
				}
			})
		}
	}
}

func TestSkipConfirmedReadbackClosesTransitionExpectations(t *testing.T) {
	c, _, d, _ := skipFixture(t, "stuck", 0, 1)
	c.clock = &advancingClock{now: time.Now()}
	before := d.Snapshot()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	r := &execution{ctx: ctx, cancel: cancel, now: c.clock.Now, expected: before,
		wake: make(chan struct{}, 1), events: map[string][]map[string]string{}}
	m := heos.Mutation{Kind: "skip", Direction: "next"}
	r.expect(m)
	d.sent = true
	next := d.tracks[1]
	d.s.Media = &next
	if _, err := c.confirmReadback(c.lanes["room"], r, m, before); err != nil {
		t.Fatal(err)
	}
	r.event(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
	if !errors.Is(context.Cause(ctx), ErrOwnership) {
		t.Fatal("Stop after confirmed readback borrowed the completed skip's transition allowance")
	}
}

func TestSkipReadbackRequiresCurrentFullEventCoverage(t *testing.T) {
	for _, tc := range []struct {
		name      string
		confirmed bool
		want      error
	}{
		{name: "current full read", confirmed: true},
		{name: "older token"},
		{name: "different token"},
		{name: "event projection"},
		{name: "unadvanced player revision"},
		{name: "cancelled", want: ErrOwnership},
		{name: "controls changed", want: ErrOwnership},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, d, _ := skipFixture(t, "stuck", 0, 1)
			before := d.Snapshot()
			before.Token.Player = 1
			after := before
			after.Media = &d.tracks[1]
			after.Token.Player = 2
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			r := &execution{ctx: ctx, cancel: cancel, now: c.clock.Now, expected: after,
				events: map[string][]map[string]string{}, eventRevision: 1}
			r.expect(heos.Mutation{Kind: "skip", Direction: "next"})
			switch tc.name {
			case "older token":
				after.Token.Player = 1
			case "different token":
				after.Token.Catalog++
			case "event projection":
				after.EventUpdated = true
			case "unadvanced player revision":
				before.Token.Player = 2
			case "cancelled":
				cancel(ErrOwnership)
			case "controls changed":
				volume := *after.Volume + 1
				after.Volume = &volume
			}
			confirmed, err := r.acceptSkipReadback(ctx, before, after)
			if confirmed != tc.confirmed || !errors.Is(err, tc.want) {
				t.Fatalf("confirmed=%v err=%v, want confirmed=%v err=%v", confirmed, err, tc.confirmed, tc.want)
			}
			if tc.confirmed {
				if r.observedRevision != r.eventRevision || r.keepExpectations || len(r.events) != 0 {
					t.Fatal("covered confirmation retained pending events or transition expectations")
				}
			} else if r.observedRevision != 0 || !r.keepExpectations || len(r.events) == 0 {
				t.Fatal("unconfirmed readback consumed the pending event or closed transition expectations")
			}
		})
	}
}

func skipStartsWithMedia(d *skipDevice, media heos.Media) {
	before := d.before
	d.before = func(m heos.Mutation) {
		before(m)
		if m.Kind == "skip" {
			d.mu.Lock()
			d.s.Media = &media
			d.mu.Unlock()
		}
	}
}

// Inject the callback after the full Play snapshot has been assembled. Returning
// that older snapshot reproduces the event/read race without scheduling sleeps.
type skipEventAfterSnapshot struct {
	*skipDevice
	event    string
	injected bool
}

func (d *skipEventAfterSnapshot) Snapshot() heos.Snapshot {
	s := d.skipDevice.Snapshot()
	d.mu.Lock()
	inject := d.sent && d.reads > 0 && !d.injected
	if inject {
		d.injected = true
		d.s.Token.Player++
		if d.event == "stop" {
			d.s.State, d.s.Media = "stop", nil
		} else {
			previous := d.tracks[0]
			d.s.Media = &previous
		}
	}
	token := d.s.Token
	d.mu.Unlock()
	if inject {
		e := mediaObservationEvent()
		if d.event == "stop" {
			e = heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}}
		}
		e.Token = token
		d.handler(e)
	}
	return s
}
