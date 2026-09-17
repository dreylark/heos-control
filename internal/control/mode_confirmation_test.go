package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

// Denon 5.10/5.11 carry independent fields. An event before the reply updates
// pending evidence; the worker must still wait for the successful setter reply.
func TestModeEventsRetainEvidenceUntilSuccessfulReply(t *testing.T) {
	for _, field := range []string{"repeat", "shuffle"} {
		t.Run(field, func(t *testing.T) {
			_, r, _, _ := modeEventFixture(t)
			r.expected.Repeat, r.expected.Shuffle = "on_all", true
			if field == "shuffle" {
				r.expected.Repeat, r.expected.Shuffle = "off", false
			}
			before := r.expected
			r.inflight = true
			r.expect(heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: true})
			value := "off"
			if field == "shuffle" {
				value = "on"
			}
			r.event(modeEvent(field, value))
			if !r.scalar.ready() || r.confirmed != 0 || r.expected.Repeat != before.Repeat || r.expected.Shuffle != before.Shuffle || r.ctx.Err() != nil {
				t.Fatal("event lost or committed before successful reply")
			}
			if r.observationPending() {
				t.Fatal("complete mode event requested a full observation")
			}
			select {
			case <-r.wake:
			default:
				t.Fatal("event did not wake confirmation")
			}
		})
	}
}

func TestModeIndependentFieldProgressAndReversal(t *testing.T) {
	for _, first := range []string{"repeat", "shuffle"} {
		for _, reversal := range []bool{false, true} {
			t.Run(first+map[bool]string{false: "/progress", true: "/reversal"}[reversal], func(t *testing.T) {
				c, r, d, clock := modeEventFixture(t)
				d.s.Repeat, r.expected.Repeat = "on_all", "on_all"
				began := clock.Now()
				other := "shuffle"
				if first == "shuffle" {
					other = "repeat"
				}
				target := map[string]string{"repeat": "off", "shuffle": "on"}
				old := map[string]string{"repeat": "on_all", "shuffle": "off"}
				clock.events = []modeClockEvent{
					{began.Add(100 * time.Millisecond), func() { r.event(modeEvent(first, old[first])) }},
					{began.Add(200 * time.Millisecond), func() { r.event(modeEvent(first, target[first])) }},
					{began.Add(300 * time.Millisecond), func() {
						if reversal {
							r.event(modeEvent(first, old[first]))
						} else {
							for range 20 {
								r.event(modeEvent(first, target[first]))
							}
						}
					}},
					{began.Add(400 * time.Millisecond), func() { r.event(modeEvent(other, target[other])) }},
				}
				err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: true})
				wantElapsed := 400 * time.Millisecond
				if reversal {
					wantElapsed = 300 * time.Millisecond
					if !errors.Is(err, ErrOwnership) {
						t.Fatal("target reversal accepted", err)
					}
				} else if err != nil || r.confirmed != 1 {
					t.Fatal("independent progress failed", err)
				}
				if len(d.reads) != 1 || len(d.scalarReads) != 0 || len(d.writes) != 1 || clock.Now().Sub(began) != wantElapsed {
					t.Fatalf("extra traffic or delayed intervention: reads=%d fallback=%d writes=%d elapsed=%s", len(d.reads), len(d.scalarReads), len(d.writes), clock.Now().Sub(began))
				}
			})
		}
	}
}

func TestModeConfirmationFallbackRespectsOriginalAndPlaybackDeadline(t *testing.T) {
	for _, scenario := range []string{"original", "playback", "fallback-crosses-original", "event-after-playback"} {
		t.Run(scenario, func(t *testing.T) {
			c, r, d, clock := modeEventFixture(t)
			began := clock.Now()
			budget, wantFallback := 12*time.Second, 1
			if scenario == "playback" || scenario == "event-after-playback" {
				budget, wantFallback = 4*time.Second, 0
				r.playbackDeadline = began.Add(budget)
			}
			if scenario == "event-after-playback" {
				clock.events = []modeClockEvent{{began.Add(budget + time.Millisecond), func() { r.event(modeEvent("shuffle", "on")) }}}
			}
			d.onScalarRead = func(string) error {
				if scenario == "fallback-crosses-original" {
					clock.now = began.Add(budget + time.Millisecond)
					d.s.Shuffle = true
				}
				return nil
			}
			err := c.write(c.lanes["room"], r, heos.Mutation{Kind: "mode", Repeat: "off", Shuffle: true})
			if !errors.Is(err, context.DeadlineExceeded) || r.confirmed != 0 || len(d.writes) != 1 || len(d.reads) != 1 || len(d.scalarReads) != wantFallback {
				t.Fatalf("confirmation escaped deadline: %v confirmed=%d full=%d fallback=%d writes=%d", err, r.confirmed, len(d.reads), len(d.scalarReads), len(d.writes))
			}
			if wantFallback != 0 && d.scalarReads[0] != began.Add(budget-time.Second) {
				t.Fatal("fallback not reserved within original deadline", d.scalarReads)
			}
			if wantFallback == 0 && clock.Now() != began.Add(budget) {
				t.Fatal("playback deadline moved", clock.Now().Sub(began))
			}
		})
	}
}
