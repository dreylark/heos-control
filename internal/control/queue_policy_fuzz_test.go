package control

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

func checkedQueueObservation(t *testing.T, state queuePolicyState, facts queueObservationFacts) queueDecision {
	t.Helper()
	beforeState, beforeFacts := state, policyFuzzJSON(t, facts)
	d := decideQueueObservation(state, facts)
	if state != beforeState || !bytes.Equal(beforeFacts, policyFuzzJSON(t, facts)) {
		t.Fatal("active queue policy mutated inputs")
	}
	if d.Rule == "" || (d.Action != queuePass && d.Action != queueWait && d.Action != queueResume && d.Action != queueRelease) || (d.Action == queueRelease) != (d.Err != nil) {
		t.Fatalf("invalid observation decision: %+v", d)
	}
	return d
}

func checkedQueueEvent(t *testing.T, state queuePolicyState, event heos.Event, now time.Time) queueDecision {
	t.Helper()
	beforeState, beforeEvent := state, policyFuzzJSON(t, event)
	d := decideQueueEvent(state, event, now)
	if state != beforeState || !bytes.Equal(beforeEvent, policyFuzzJSON(t, event)) {
		t.Fatal("queue event policy mutated inputs")
	}
	// Denon 5.4/5.5 report a state or announce a media change; neither carries
	// the complete owned queue and a verified MID/QID pair needed for resumption.
	if (d.Action != queuePass && d.Action != queueWait && d.Action != queueObserve && d.Action != queueIgnore) || d.Err != nil || d.Rule == "" {
		t.Fatalf("event fabricated observation/ownership evidence: %+v", d)
	}
	return d
}

func policyFuzzEvent(index byte) heos.Event {
	if index%4 == 3 {
		return heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}}.Decode()
	}
	state := [...]string{"stop", "unknown", "play"}[index%4]
	return heos.Event{Command: "event/player_state_changed", Params: url.Values{"pid": {"1"}, "state": {state}}}.Decode()
}

func FuzzQueueTransitionPolicy(f *testing.F) {
	// Header: queue size, inactive/wait flags, playback window, observed state,
	// media variation, controls, queue edit, pending/cancel/unsafe/expired,
	// deadline edge, selected settlement step. Tail: ordered hints/time deltas.
	// Seeds reconstruct Home 150 Stop/unknown navigation and the 2026-09-11
	// Play-only MID/QID hybrid; firmware source findings are in docs/DEVICE_COMPATIBILITY.md.
	for _, seed := range [][]byte{
		{},
		{1, 8, 20},                     // waiting -> valid selected Play
		{1, 0, 20, 1},                  // Stop starts a bounded wait
		{1, 0, 20, 2},                  // unknown starts the same wait
		{1, 0, 20, 0, 1},               // hybrid pair while still Play
		{1, 8, 20, 0, 1, 0, 0, 1},      // hybrid plus newer notification
		{1, 8, 20, 0, 0, 0, 0, 1},      // exact pair cannot cover a newer event
		{7, 8, 2, 0, 0, 255, 1, 15, 1}, // just before playback deadline with competing faults
		{7, 8, 2, 0, 0, 255, 1, 15, 2}, // original deadline wins over cancellation
		{1, 8, 20, 0, 0, 0, 0, 0, 4, 63, 0, 8, 16, 24, 1, 17, 25, 3, 3, 3},
	} {
		f.Add(seed)
	}
	for bit := range 8 {
		f.Add([]byte{1, 8, 20, 0, 1, 1 << bit, 0, 1})
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		n := 1 + int(policyFuzzByte(data, 0)%8)
		before, members := policyFuzzQueue(n, policyFuzzByte(data, 9))
		first := before.Queue.Items[0]
		before.Media = &first
		observed := policyFuzzCopy(before)
		last := observed.Queue.Items[n-1]
		observed.Media = &last
		origin := time.Date(2026, 9, 11, 7, 0, 0, 0, time.UTC)
		flags := policyFuzzByte(data, 1)
		state := queuePolicyState{Owned: flags&1 == 0, Automating: flags&2 == 0, ExpectedState: "play",
			PlaybackUntil: origin.Add(time.Duration(policyFuzzByte(data, 2)%32) * time.Second)}
		if flags&4 != 0 {
			state.ExpectedState = "stop"
		}
		if flags&8 != 0 {
			state.WaitUntil = origin.Add(12 * time.Second)
		}
		observed.State = [...]string{"play", "stop", "unknown", "pause"}[policyFuzzByte(data, 3)%4]
		mediaCase := policyFuzzByte(data, 4) % 8
		hybrid := mediaCase == 1 && n > 1
		switch mediaCase {
		case 1:
			observed.Media.QueueID = "1"
			if n == 1 {
				observed.Media.QueueID = "999"
			}
		case 2:
			observed.Media.ID = "9999"
		case 3:
			observed.Media.QueueID = "999"
		case 4:
			observed.Media.Source = "foreign"
		case 5:
			observed.Media = nil
		case 6:
			observed.Media = &heos.Media{}
		case 7:
			observed.Media = policyFuzzCopy(before).Media
		}
		controls := policyFuzzByte(data, 5)
		policyFuzzControls(&observed, controls)
		queueEdit := policyFuzzByte(data, 6)%4 != 0
		// Before is always a previously verified complete queue. Edited observed
		// queues remain valid complete values: pagination/truncation handling is
		// an executor responsibility and is not bypassed to invent unsafe facts.
		switch policyFuzzByte(data, 6) % 4 {
		case 1:
			observed.Queue.Items[0].ID = "9998"
		case 2:
			observed.Queue.Items[0].Song = "different fixture metadata"
		case 3:
			observed.Queue.Items = observed.Queue.Items[1:]
			count := len(observed.Queue.Items)
			observed.Queue.Total = &count
		}
		facts := queueObservationFacts{Now: origin, Before: before, Observed: observed}
		faults := policyFuzzByte(data, 7)
		facts.PendingEvents, facts.Expired = faults&1 != 0, faults&8 != 0
		if faults&2 != 0 {
			facts.Cancellation = context.Canceled
		}
		if faults&4 != 0 {
			facts.Unsafe = heos.ErrStale
		}
		waitBoundary := origin.Add(12 * time.Second)
		facts.Now = [...]time.Time{origin, state.PlaybackUntil.Add(-time.Nanosecond), state.PlaybackUntil, state.PlaybackUntil.Add(time.Nanosecond),
			waitBoundary.Add(-time.Nanosecond), waitBoundary, waitBoundary.Add(time.Nanosecond)}[policyFuzzByte(data, 8)%7]
		d := checkedQueueObservation(t, state, facts)
		event := policyFuzzEvent(policyFuzzByte(data, 3))
		eventDecision := checkedQueueEvent(t, state, event, facts.Now)
		if !state.Owned && eventDecision.Action != queuePass {
			t.Fatalf("event took over a queue which is not owned: %+v", eventDecision)
		}
		if !state.WaitUntil.IsZero() && eventDecision.WaitUntil != state.WaitUntil {
			t.Fatalf("event changed an existing wait: %+v", eventDecision)
		}
		if eventDecision.Action == queueWait {
			if !state.Owned || !state.Automating || !state.WaitUntil.IsZero() || (event.Data().State != "stop" && event.Data().State != "unknown") ||
				eventDecision.WaitUntil != facts.Now.Add(12*time.Second) {
				t.Fatalf("event started an unauthorized or unbounded wait: %+v", eventDecision)
			}
		}
		if event.Data().State == "play" && state.ExpectedState != "play" && eventDecision.Action != queuePass {
			t.Fatalf("unexpected Play bypassed ordinary attribution: %+v", eventDecision)
		}
		active := state.Owned && state.Automating && state.ExpectedState == "play"
		pending := facts.Observed.State == "stop" || facts.Observed.State == "unknown" || facts.Observed.State == "play" && hybrid
		waiting := !state.WaitUntil.IsZero() || active && pending
		if !active || !waiting {
			// Pass delegates to ordinary safety/attribution. It never authorizes
			// a setter, including when cancellation or intervention is present.
			if d.Action != queuePass {
				t.Fatalf("inactive/no-wait policy did not delegate: %+v", d)
			}
		} else {
			firstDeadline := state.WaitUntil
			if firstDeadline.IsZero() {
				firstDeadline = facts.Now.Add(12 * time.Second)
			}
			effective := firstDeadline
			if state.PlaybackUntil.Before(effective) {
				effective = state.PlaybackUntil
			}
			if d.Deadline != effective || d.Action != queueResume && d.WaitUntil != firstDeadline {
				t.Fatalf("transition changed original deadline/playback cap: %+v want %v / %v", d, firstDeadline, effective)
			}
			switch {
			case facts.Expired || !facts.Now.Before(effective):
				if d.Action != queueRelease || !errors.Is(d.Err, errQueueTransitionTimeout) {
					t.Fatalf("timeout lost precedence: %+v", d)
				}
			case facts.Cancellation != nil:
				if d.Action != queueRelease || !errors.Is(d.Err, facts.Cancellation) {
					t.Fatalf("cancellation lost precedence: %+v", d)
				}
			case facts.Unsafe != nil:
				if d.Action != queueRelease || !errors.Is(d.Err, facts.Unsafe) {
					t.Fatalf("unsafe observation confirmed/waited: %+v", d)
				}
			case controls != 0 || queueEdit || observed.State == "pause" || mediaCase == 2 || mediaCase == 4 || observed.State == "play" && (mediaCase == 3 || mediaCase == 1 && n == 1):
				if d.Action != queueRelease || !errors.Is(d.Err, ErrOwnership) {
					t.Fatalf("queue/control/identity intervention accepted: %+v", d)
				}
			}
		}
		if d.Action == queueResume {
			if facts.PendingEvents || !active || !waiting || !d.WaitUntil.IsZero() || !policyFuzzCompleteSelection(facts.Observed, members) || !reflect.DeepEqual(before.Queue, observed.Queue) || controls != 0 {
				t.Fatalf("resumed without independent proof of owned Play: %+v", d)
			}
		}
		// Always exercise a positive owned transition with the generated queue.
		state = queuePolicyState{Owned: true, Automating: true, ExpectedState: "play", WaitUntil: origin.Add(12 * time.Second), PlaybackUntil: origin.Add(20 * time.Second)}
		settled := policyFuzzCopy(before)
		media := settled.Queue.Items[n-1]
		settled.Media = &media
		facts = queueObservationFacts{Now: origin, Before: before, Observed: settled}
		if d := checkedQueueObservation(t, state, facts); d.Action != queueResume {
			t.Fatalf("unchanged owned queue did not make progress: %+v", d)
		}
		facts.PendingEvents = true
		if d := checkedQueueObservation(t, state, facts); d.Action != queueWait {
			t.Fatalf("newer event did not suspend valid Play: %+v", d)
		}
		policyFuzzTransitionHints(t, data, before)
	})
}

func policyFuzzTransitionHints(t *testing.T, data []byte, before heos.Snapshot) {
	t.Helper()
	origin := time.Date(2026, 9, 11, 7, 0, 0, 0, time.UTC)
	state := queuePolicyState{Owned: true, Automating: true, ExpectedState: "play", PlaybackUntil: origin.Add(time.Duration(1+policyFuzzByte(data, 2)%24) * time.Second)}
	first := checkedQueueEvent(t, state, policyFuzzEvent(policyFuzzByte(data, 3)%2), origin)
	if first.Action != queueWait || first.WaitUntil != origin.Add(12*time.Second) || !first.Notify {
		t.Fatalf("initial Stop/unknown did not start a bounded read-only wait: %+v", first)
	}
	state.WaitUntil = first.WaitUntil
	deadline := first.WaitUntil
	if state.PlaybackUntil.Before(deadline) {
		deadline = state.PlaybackUntil
	}
	facts := queueObservationFacts{Now: origin, Before: before, Observed: policyFuzzCopy(before)}
	// At most 62 generated hints plus the initial transition and final deadline
	// observation. Event input has already passed validity/identity/continuity
	// checks, exactly as required by decideQueueEvent's caller contract.
	for i := 10; i < len(data) && i < 10+62; i++ {
		code := data[i]
		facts.Now = facts.Now.Add(policyFuzzDelay(code & 7))
		event := checkedQueueEvent(t, state, policyFuzzEvent(code>>3), facts.Now)
		if event.WaitUntil != first.WaitUntil {
			t.Fatal("repeated state/media hint renewed first wait", event)
		}
		facts.Observed = policyFuzzCopy(before)
		facts.PendingEvents = code&128 != 0
		switch (code >> 5) % 3 {
		case 0:
			facts.Observed.State = "stop"
		case 1:
			facts.Observed.State = "unknown"
		case 2:
			if len(before.Queue.Items) > 1 {
				facts.Observed.Media.QueueID = before.Queue.Items[len(before.Queue.Items)-1].QueueID
			} else {
				facts.Observed.Media = nil
			}
		}
		settles := i-10 == int(policyFuzzByte(data, 9)%64)
		if settles {
			facts.Observed, facts.PendingEvents = policyFuzzCopy(before), false
		}
		d := checkedQueueObservation(t, state, facts)
		if d.Deadline != deadline || d.Action != queueResume && d.WaitUntil != first.WaitUntil {
			t.Fatalf("repeated observation extended transition/playback deadline: %+v", d)
		}
		if !facts.Now.Before(deadline) {
			if d.Action != queueRelease || !errors.Is(d.Err, errQueueTransitionTimeout) {
				t.Fatalf("duplicate storm concealed timeout: %+v", d)
			}
			return
		}
		if settles {
			if d.Action != queueResume || !d.WaitUntil.IsZero() {
				t.Fatalf("valid navigation failed to resume before original deadline: %+v", d)
			}
			return
		}
		if d.Action != queueWait {
			t.Fatalf("unresolved/hybrid media escaped read-only wait: %+v", d)
		}
	}
	facts.Now, facts.Observed.State = deadline, "play"
	facts.Observed.Media = policyFuzzCopy(before).Media
	facts.PendingEvents = true
	facts.Cancellation = context.Canceled // A secondary cancellation cannot hide the original timeout.
	if d := checkedQueueObservation(t, state, facts); d.Action != queueRelease || !errors.Is(d.Err, errQueueTransitionTimeout) {
		t.Fatalf("original deadline lost after duplicate hints: %+v", d)
	}
}
