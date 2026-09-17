package control

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type countedObservation struct {
	Observation
	read func()
}

func (o countedObservation) Refresh(ctx context.Context) error {
	o.read()
	return o.Observation.Refresh(ctx)
}

func TestQuietHoldDoesNotPollDevice(t *testing.T) {
	c, j, d, req, cmd := albumFixture(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	cmd.Automation = &Automation{TargetLevel: cmd.Level, DurationSeconds: 30}
	prepared, reads := false, 0
	d.before = func(m heos.Mutation) {
		if m.Kind == "queue" {
			prepared = true
		}
	}
	c.lanes["room"].device.Observer = countedObservation{d, func() {
		if prepared {
			reads++
		}
	}}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded {
		t.Fatal(o)
	}
	// Queue readback + final Stop before/after; no five-second hold polling.
	if reads != 3 {
		t.Fatalf("30s quiet hold made %d complete reads; want 3", reads)
	}
	if len(d.writes) != 5 || d.writes[4].State != "stop" {
		t.Fatal(d.writes)
	}
}

func TestPendingQueueReadBudgetWithoutEvents(t *testing.T) {
	c, j, base, req, cmd := albumFixture(t)
	d := &pendingDevice{albumDevice: base, kind: "queue", states: []string{"unknown"}}
	c.lanes["room"].writer, c.lanes["room"].device.Observer = d, d
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	began := clock.Now()
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Uncertain || o.ErrorCode != "device_unavailable" {
		t.Fatal(o)
	}
	if clock.Now().Sub(began) != 12*time.Second {
		t.Fatal("changed confirmation deadline")
	}
	if d.reads < 12 || d.reads > 13 {
		t.Fatalf("12s confirmation made %d complete reads; want 12..13", d.reads)
	}
	if len(d.writes) != 4 {
		t.Fatal("setter replay", d.writes)
	}
}

func TestDuplicateStopWaitHasBoundedReadBudget(t *testing.T) {
	c, j, d, req, cmd := albumFixture(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	cmd.Automation = &Automation{TargetLevel: cmd.Level, DurationSeconds: 30}
	began := clock.Now()
	stopped, reads := false, 0
	c.lanes["room"].device.Observer = countedObservation{d, func() {
		if stopped {
			reads++
		}
	}}
	clock.hook = func() {
		if clock.Now().Sub(began) < time.Second {
			return
		}
		stopped = true
		d.mu.Lock()
		d.s.State = "stop"
		d.mu.Unlock()
		for range 100 {
			d.handler(heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"stop"}}})
		}
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Released || o.ErrorCode != "ownership_lost" {
		t.Fatal(o)
	}
	if clock.Now().Sub(began) != 13*time.Second {
		t.Fatal("duplicate Stop extended deadline", clock.Now().Sub(began))
	}
	if reads > 14 {
		t.Fatalf("duplicate Stop produced %d complete reads in 12s; want at most 14", reads)
	}
	if len(d.writes) != 4 {
		t.Fatal("write while waiting or after release", d.writes)
	}
}

func TestVolumeStepsDoNotAddPeriodicReadBeforeSetter(t *testing.T) {
	c, j, d, req, cmd := albumFixture(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	cmd.Automation = &Automation{TargetLevel: 20, RampSeconds: 10, DurationSeconds: 10}
	reads, prepared := 0, false
	d.before = func(m heos.Mutation) {
		if m.Kind == "queue" {
			prepared = true
		}
	}
	c.lanes["room"].device.Observer = countedObservation{d, func() {
		if prepared {
			reads++
		}
	}}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Succeeded {
		t.Fatal(o)
	}
	// One queue readback and final Stop before/after. Every scalar step is
	// confirmed by its event, with no prewrite/readback GETs.
	want := 3
	if reads != want {
		t.Fatalf("%d reads for %d post-start writes, want %d", reads, len(d.writes)-4, want)
	}
}
