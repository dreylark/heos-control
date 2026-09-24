package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

// The byte format describes small variations of valid observations, not raw
// protocol frames. Missing bytes are zero so shrinking remains meaningful.
func policyFuzzByte(data []byte, index int) byte {
	if index < len(data) {
		return data[index]
	}
	return 0
}

func policyFuzzCopy(s heos.Snapshot) heos.Snapshot {
	if s.Volume != nil {
		v := *s.Volume
		s.Volume = &v
	}
	if s.Muted != nil {
		v := *s.Muted
		s.Muted = &v
	}
	if s.Media != nil {
		m := *s.Media
		s.Media = &m
	}
	s.Queue.Items = append([]heos.Media(nil), s.Queue.Items...)
	if s.Queue.Total != nil {
		n := *s.Queue.Total
		s.Queue.Total = &n
	}
	if s.Queue.Next != nil {
		n := *s.Queue.Next
		s.Queue.Next = &n
	}
	return s
}

// Build reviewed symbolic identities independently of production membership
// helpers. MID/QID relations model Denon 4.2.5/4.2.15; SID 1024 is local media.
func policyFuzzQueue(size int, volume byte) (heos.Snapshot, map[heos.ID]bool) {
	level, muted := int(volume)%101, false
	s := heos.Snapshot{Player: heos.Player{ID: "1", Serial: "fixture-player"}, State: heos.PlayStatePlay,
		Volume: &level, Muted: &muted, Repeat: heos.RepeatOff, Shuffle: true, Connected: true, Verified: true,
		Token: heos.Token{Generation: 1, Player: 1}}
	members := make(map[heos.ID]bool, size)
	for i := range size {
		m := heos.Media{Source: "1024", ID: heos.ID(strconv.Itoa(100 + i)), QueueID: heos.ID(strconv.Itoa(i + 1))}
		s.Queue.Items = append(s.Queue.Items, m)
		members[m.ID] = true
	}
	s.Queue.Total = &size
	m := s.Queue.Items[size-1]
	s.Media = &m
	return s, members
}

func policyFuzzControls(s *heos.Snapshot, mask byte) {
	if mask&1 != 0 {
		v := (*s.Volume + 1) % 101
		s.Volume = &v
	}
	if mask&2 != 0 {
		m := !*s.Muted
		s.Muted = &m
	}
	if mask&4 != 0 {
		s.Repeat = heos.RepeatOnAll
	}
	if mask&8 != 0 {
		s.Shuffle = false
	}
	if mask&16 != 0 {
		s.Grouped = true
	}
	if mask&32 != 0 {
		s.Player.ID = "2"
	}
	if mask&64 != 0 {
		s.Player.Serial = "another-fixture-player"
	}
	if mask&128 != 0 {
		s.Token.Generation++
	}
}

func policyFuzzJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func policyFuzzDelay(value byte) time.Duration {
	return [...]time.Duration{0, time.Nanosecond, time.Millisecond, 250 * time.Millisecond, time.Second, 12 * time.Second}[int(value)%6]
}

// This oracle checks the public bounded-queue contract directly. It does not
// call completeOwnedQueue, queuedMedia, queueResultProblem or a decision helper.
func policyFuzzCompleteSelection(s heos.Snapshot, selected map[heos.ID]bool) bool {
	if s.State != heos.PlayStatePlay || s.Media == nil || s.Media.Source != "1024" || s.Media.ID == "" || s.Media.QueueID == "" ||
		s.Queue.Total == nil || *s.Queue.Total != len(s.Queue.Items) || len(s.Queue.Items) == 0 || s.Queue.Next != nil {
		return false
	}
	positions := map[heos.ID]bool{}
	found := false
	for _, m := range s.Queue.Items {
		if !selected[m.ID] || m.QueueID == "" || positions[m.QueueID] {
			return false
		}
		positions[m.QueueID] = true
		if m.ID == s.Media.ID && m.QueueID == s.Media.QueueID {
			found = true
		}
	}
	return found
}

func checkedQueueStart(t *testing.T, facts queueStartFacts) queueStartDecision {
	t.Helper()
	before := policyFuzzJSON(t, facts)
	d := decideQueueStart(facts)
	if !bytes.Equal(before, policyFuzzJSON(t, facts)) {
		t.Fatal("initial queue policy mutated its inputs")
	}
	if d.Rule == "" || (d.Action != queueStartWait && d.Action != queueStartConfirm && d.Action != queueStartAbort) {
		t.Fatalf("invalid queue-start decision: %+v", d)
	}
	if (d.Action == queueStartAbort) != (d.Err != nil) {
		t.Fatalf("queue-start error/action mismatch: %+v", d)
	}
	return d
}

func FuzzQueueStartPolicy(f *testing.F) {
	// Header: queue size, old Stop/Pause, phase, short/bounded, event/cancel/
	// unsafe flags, observed state, media variation, queue variation, controls,
	// volume, deadline edge, time origin. Tail: duplicate hints/time advances.
	// Reconstructed 2026-09-13 unresolved-MID loading and 2026-09-11 hybrid
	// metadata findings are in docs/DEVICE_COMPATIBILITY.md; bytes are synthetic.
	for _, seed := range [][]byte{
		{},
		{1, 0, 0, 0, 0, 0, 0, 0, 0, 10}, // selected Play
		{1, 1, 0, 0, 0, 1, 2, 0, 0, 10, 0, 0, 8, 9, 10, 11}, // paused -> unresolved unknown
		{1, 0, 1, 0, 0, 1, 2},                               // unresolved MID after Play must abort
		{1, 0, 0, 0, 0, 0, 1},                               // selected MID with another QID cannot confirm bounded playback
		{1, 0, 0, 0, 1},                                     // valid Play with an uncovered newer event
		{1, 0, 0, 1, 0, 0, 0, 1},                            // short command retains its less strict queue readback
		{7, 0, 0, 0, 7, 0, 0, 0, 255, 100, 1},               // just before deadline with competing faults
		{7, 0, 0, 0, 7, 0, 0, 0, 255, 100, 2},               // exactly at deadline
		{7, 0, 0, 0, 0, 0, 0, 0, 0, 100, 3},                 // just after deadline
	} {
		f.Add(seed)
	}
	for bit := range 8 {
		f.Add([]byte{1, 0, 0, 0, 1, 1, 2, 0, 1 << bit})
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		selected, members := policyFuzzQueue(1+int(policyFuzzByte(data, 0)%8), policyFuzzByte(data, 9))
		origin := time.Date(2026, 9, 13, 7, 0, 0, 0, time.UTC).Add(time.Duration(policyFuzzByte(data, 11)) * time.Hour)
		old := policyFuzzCopy(selected)
		old.State = [...]heos.PlayState{heos.PlayStateStop, heos.PlayStatePause}[policyFuzzByte(data, 1)%2]
		old.Media = &heos.Media{Source: "1024", ID: "9000", QueueID: "1"}
		old.Queue.Items = []heos.Media{*old.Media}
		one := 1
		old.Queue.Total = &one
		facts := queueStartFacts{Now: origin, Deadline: origin.Add(12 * time.Second), Before: old,
			Observed: policyFuzzCopy(selected), Members: members, Bounded: policyFuzzByte(data, 3)%2 == 0,
			Phase: [...]queueStartPhase{queueAwaitingPlay, queuePlayObserved, queueNotStarting}[policyFuzzByte(data, 2)%3]}
		flags := policyFuzzByte(data, 4)
		facts.PendingEvents = flags&1 != 0
		if flags&2 != 0 {
			facts.Cancellation = context.Canceled
		}
		if flags&4 != 0 {
			facts.Unsafe = heos.ErrStale
		}
		facts.Observed.State = [...]heos.PlayState{heos.PlayStatePlay, heos.PlayStateUnknown, heos.PlayStateStop, heos.PlayStatePause, "invalid"}[policyFuzzByte(data, 5)%5]
		switch policyFuzzByte(data, 6) % 8 {
		case 1:
			facts.Observed.Media.QueueID = "1"
			if len(selected.Queue.Items) == 1 {
				facts.Observed.Media.QueueID = "999"
			}
		case 2:
			facts.Observed.Media.ID = "9999" // unresolved identity, distinct from old and selected
		case 3:
			facts.Observed.Media.Source = "foreign"
		case 4:
			facts.Observed.Media = nil
		case 5:
			facts.Observed.Media = &heos.Media{}
		case 6:
			facts.Observed.Media = policyFuzzCopy(old).Media
		case 7:
			facts.Observed.Media.QueueID = ""
		}
		switch policyFuzzByte(data, 7) % 8 {
		case 1:
			facts.Observed.Queue.Total = nil
		case 2:
			total := len(selected.Queue.Items) + 1
			facts.Observed.Queue.Total = &total
		case 3:
			next := len(selected.Queue.Items)
			facts.Observed.Queue.Next = &next
		case 4:
			facts.Observed.Queue.Items[0].ID = "9998"
		case 5:
			facts.Observed.Queue.Items[0].QueueID = selected.Media.QueueID
			if len(selected.Queue.Items) == 1 {
				facts.Observed.Queue.Items[0].QueueID = ""
			}
		case 6:
			facts.Observed.Queue = policyFuzzCopy(old).Queue
		case 7:
			facts.Observed.Queue.Items = nil
			zero := 0
			facts.Observed.Queue.Total = &zero
		}
		controls := policyFuzzByte(data, 8)
		policyFuzzControls(&facts.Observed, controls)
		facts.Now = [...]time.Time{origin, facts.Deadline.Add(-time.Nanosecond), facts.Deadline, facts.Deadline.Add(time.Nanosecond)}[policyFuzzByte(data, 10)%4]
		d := checkedQueueStart(t, facts)
		switch {
		case facts.Phase == queueNotStarting:
			if d.Action != queueStartAbort || !errors.Is(d.Err, ErrOwnership) {
				t.Fatalf("inactive queue confirmed/waited: %+v", d)
			}
		case !facts.Now.Before(facts.Deadline):
			if d.Action != queueStartAbort || !errors.Is(d.Err, context.DeadlineExceeded) {
				t.Fatalf("deadline lost priority: %+v", d)
			}
		case facts.Cancellation != nil:
			if d.Action != queueStartAbort || !errors.Is(d.Err, facts.Cancellation) {
				t.Fatalf("cancellation lost priority: %+v", d)
			}
		case facts.Unsafe != nil:
			if d.Action != queueStartAbort || !errors.Is(d.Err, facts.Unsafe) {
				t.Fatalf("unsafe observation accepted: %+v", d)
			}
		case controls != 0:
			if d.Action != queueStartAbort || !errors.Is(d.Err, ErrOwnership) {
				t.Fatalf("control/identity intervention accepted: %+v", d)
			}
		}
		if d.Action == queueStartConfirm {
			m := facts.Observed.Media
			if facts.PendingEvents || facts.Observed.State != heos.PlayStatePlay || m == nil || m.Source != "1024" || m.ID == "" || m.QueueID == "" || !members[m.ID] {
				t.Fatalf("unverified playback confirmed: %+v facts=%+v", d, facts)
			}
			if facts.Bounded && !policyFuzzCompleteSelection(facts.Observed, members) {
				t.Fatalf("bounded playback confirmed an incomplete queue or mismatched MID/QID: %+v", facts.Observed)
			}
			for _, track := range facts.Observed.Queue.Items {
				if !members[track.ID] {
					t.Fatal("initial confirmation accepted an unselected queue item")
				}
			}
		}
		// Positive progress is tested for every mutation, so an implementation
		// which simply aborts all unusual observations cannot satisfy the oracle.
		progress := queueStartFacts{Now: origin, Deadline: origin.Add(12 * time.Second), Phase: queueAwaitingPlay,
			Bounded: true, Members: members, Before: old, Observed: policyFuzzCopy(selected)}
		if d := checkedQueueStart(t, progress); d.Action != queueStartConfirm {
			t.Fatalf("valid selected Play failed to confirm: %+v", d)
		}
		progress.Observed.State, progress.Observed.Media.ID = "unknown", "9999"
		if d := checkedQueueStart(t, progress); d.Action != queueStartWait {
			t.Fatalf("documented unresolved loading MID did not wait: %+v", d)
		}
		progress.Phase = queuePlayObserved
		if d := checkedQueueStart(t, progress); d.Action != queueStartAbort {
			t.Fatalf("first Play did not close unresolved MID allowance: %+v", d)
		}
		progress.Phase = queueAwaitingPlay
		deadline := progress.Deadline
		for i := 12; i < len(data) && i < 12+62; i++ {
			progress.Now = progress.Now.Add(policyFuzzDelay(data[i]))
			progress.PendingEvents = data[i]&8 != 0
			d := checkedQueueStart(t, progress)
			if progress.Deadline != deadline {
				t.Fatal("duplicate loading hint renewed deadline")
			}
			if !progress.Now.Before(deadline) {
				if d.Action != queueStartAbort || !errors.Is(d.Err, context.DeadlineExceeded) {
					t.Fatalf("loading hints hid original timeout: %+v", d)
				}
				break
			}
			if d.Action != queueStartWait {
				t.Fatalf("unresolved duplicate confirmed or aborted prematurely: %+v", d)
			}
		}
		progress.Now, progress.PendingEvents = deadline, false
		if d := checkedQueueStart(t, progress); d.Action != queueStartAbort || !errors.Is(d.Err, context.DeadlineExceeded) {
			t.Fatalf("unresolved media survived the original deadline: %+v", d)
		}
	})
}
