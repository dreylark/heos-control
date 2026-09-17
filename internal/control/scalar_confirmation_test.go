package control

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

type scalarEventDevice struct {
	*modeEventDevice
	scalarReads  []string
	onScalarRead func() error
}

func (d *scalarEventDevice) RefreshScalars(ctx context.Context, kind string) error {
	d.scalarReads = append(d.scalarReads, kind)
	if d.onScalarRead != nil {
		if err := d.onScalarRead(); err != nil {
			return err
		}
	}
	return context.Cause(ctx)
}

func scalarFixture(t *testing.T) (*Coordinator, *execution, *scalarEventDevice, *modeEventClock) {
	c, r, base, clock := modeEventFixture(t)
	d := &scalarEventDevice{modeEventDevice: base}
	l := c.lanes["room"]
	l.device.Observer, l.device.Client, l.writer = d, d, d
	return c, r, d, clock
}

func volumeEvent(level, mute string) heos.Event {
	return heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {level}, "mute": {mute}}}
}

// Denon 5.9 carries volume AND mute; 5.10/5.11 carry independent mode values.
// A matching notification is the actual observation, even if GET would lag it.
func TestScalarConfirmationUsesEventsWithoutReadback(t *testing.T) {
	for _, kind := range []string{"volume", "mute", "mode"} {
		for _, timing := range []string{"before-reply", "after-reply", "duplicates"} {
			t.Run(kind+"/"+timing, func(t *testing.T) {
				c, r, d, clock := scalarFixture(t)
				m := heos.Mutation{Kind: kind, Level: 21, Muted: true, Repeat: "off", Shuffle: true}
				e := volumeEvent("21", "off")
				if kind == "mute" {
					e = volumeEvent("20", "on")
				}
				if kind == "mode" {
					e = modeEvent("shuffle", "on")
				}
				began := clock.Now()
				emit := func() { r.event(e) }
				if timing == "before-reply" {
					d.onWrite = func(heos.Mutation) error { emit(); return nil }
				} else {
					clock.events = []modeClockEvent{{began.Add(350 * time.Millisecond), func() {
						emit()
						if timing == "duplicates" {
							for range 10 {
								emit()
							}
						}
					}}}
				}
				if err := c.write(c.lanes["room"], r, m); err != nil {
					t.Fatal(err)
				}
				if len(d.reads) != 1 || len(d.scalarReads) != 0 || len(d.writes) != 1 || r.confirmed != 1 {
					t.Fatalf("confirmation traffic: full=%d scalar=%v setters=%d confirmed=%d", len(d.reads), d.scalarReads, len(d.writes), r.confirmed)
				}
				if timing != "before-reply" && clock.Now().Sub(began) != 350*time.Millisecond {
					t.Fatal("event did not finish wait promptly")
				}
			})
		}
	}
}

func TestScalarMissingEventHasOneTargetedFallbackAtDeadline(t *testing.T) {
	for _, outcome := range []string{"applied", "unchanged", "read-failed", "intervention", "canceled"} {
		t.Run(outcome, func(t *testing.T) {
			c, r, d, clock := scalarFixture(t)
			began := clock.Now()
			d.onWrite = func(heos.Mutation) error {
				if outcome == "applied" {
					level := 21
					d.s.Volume = &level
				}
				return nil
			}
			d.onScalarRead = func() error {
				switch outcome {
				case "read-failed":
					return heos.ErrProtocol
				case "intervention":
					r.event(volumeEvent("22", "off"))
				case "canceled":
					r.cancel(context.Canceled)
				}
				return nil
			}
			err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "volume", Level: 21})
			if outcome == "applied" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("unconfirmed or canceled command succeeded")
			}
			if len(d.reads) != 1 || len(d.scalarReads) != 1 || len(d.writes) != 1 || clock.Now().Sub(began) != 11*time.Second {
				t.Fatalf("fallback not bounded/targeted: err=%v full=%d scalar=%v writes=%d elapsed=%s", err, len(d.reads), d.scalarReads, len(d.writes), clock.Now().Sub(began))
			}
		})
	}
}

func TestScalarWaitCancelsOnInterventionWithoutReading(t *testing.T) {
	for _, event := range []heos.Event{
		volumeEvent("22", "off"), volumeEvent("21", "on"),
		{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {"pause"}}},
		{Command: "event/player_queue_changed", Params: url.Values{"pid": {"1"}}},
		{Command: "event/groups_changed"}, {Gap: true},
	} {
		t.Run(event.Command+event.Params.Encode(), func(t *testing.T) {
			c, r, d, clock := scalarFixture(t)
			clock.events = []modeClockEvent{{clock.Now().Add(100 * time.Millisecond), func() { r.event(event) }}}
			err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "volume", Level: 21})
			if !errors.Is(err, ErrOwnership) || len(d.reads) != 1 || len(d.scalarReads) != 0 || len(d.writes) != 1 {
				t.Fatalf("intervention: %v full=%d scalar=%v writes=%d", err, len(d.reads), d.scalarReads, len(d.writes))
			}
		})
	}
}
