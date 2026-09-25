package control

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

func repeatedRemovalFixture(t *testing.T) (*Coordinator, *lane, *execution, heos.Snapshot, heos.Snapshot) {
	t.Helper()
	c, _, _, _, r := ownedSkipFixture(t)
	before := r.expected
	before.Queue = heos.QueuePage{Total: intPtr(4)}
	for i := range 4 {
		before.Queue.Items = append(before.Queue.Items, heos.Media{Source: "1024", ID: "repeated", QueueID: heos.ID(fmt.Sprint(i + 1))})
	}
	current := before.Queue.Items[2]
	before.Media = &current
	after := before
	after.Queue = heos.QueuePage{Items: append([]heos.Media(nil), before.Queue.Items[1:]...), Total: intPtr(3)}
	for i := range after.Queue.Items {
		after.Queue.Items[i].QueueID = heos.ID(fmt.Sprint(i + 1))
	}
	r.expected = before
	r.ordered = []heos.ID{"repeated", "repeated", "repeated"}
	return c, c.lanes["room"], r, before, after
}

func TestRemoveWaitsForRemappedRepeatedOccurrence(t *testing.T) {
	c, l, r, before, after := repeatedRemovalFixture(t)
	// The old a/QID3 pair is a different valid occurrence after renumbering.
	// It must not advance progress or permit a subsequent unsafe prefix prune.
	if !queuedMedia(after.Queue, after.Media) {
		t.Fatal("fixture did not expose an ambiguous repeated pair")
	}
	confirmed, err := c.removeObservation(context.Background(), l, r, before, after, 1)
	if err != nil || confirmed {
		t.Fatal("stale repeated pair confirmed removal", confirmed, err)
	}
	settled := after.Queue.Items[1]
	after.Media = &settled
	confirmed, err = c.removeObservation(context.Background(), l, r, before, after, 1)
	if err != nil || !confirmed {
		t.Fatal("mapped original occurrence did not confirm", confirmed, err)
	}
}

func TestRemoveRepeatedOccurrenceDoesNotRenewTransitionDeadline(t *testing.T) {
	c, l, r, before, after := repeatedRemovalFixture(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	deadline := clock.Now().Add(playbackConfirmationTimeout)
	r.queueWait = deadline
	for range 3 {
		confirmed, err := c.removeObservation(context.Background(), l, r, before, after, 1)
		if err != nil || confirmed || !r.queueWait.Equal(deadline) {
			t.Fatal(confirmed, err, r.queueWait)
		}
		if err = clock.Wait(context.Background(), 4*time.Second, nil); err != nil {
			t.Fatal(err)
		}
	}
	if confirmed, err := c.removeObservation(context.Background(), l, r, before, after, 1); confirmed || !errors.Is(err, errQueueTransitionTimeout) {
		t.Fatal(confirmed, err)
	}
}

func TestRemoveCannotLoseOriginalCurrentOccurrence(t *testing.T) {
	for _, scenario := range []string{"missing", "removed", "foreign-source"} {
		t.Run(scenario, func(t *testing.T) {
			c, l, r, before, after := repeatedRemovalFixture(t)
			switch scenario {
			case "missing":
				before.Media = nil
			case "removed":
				current := before.Queue.Items[0]
				before.Media = &current
			case "foreign-source":
				current := *before.Media
				current.Source = "foreign"
				before.Media = &current
			}
			if confirmed, err := c.removeObservation(context.Background(), l, r, before, after, 1); confirmed || !errors.Is(err, ErrOwnership) {
				t.Fatal(confirmed, err)
			}
		})
	}
}

func TestRemoveWaitsForConcurrentNavigationAndPartialPrefix(t *testing.T) {
	c, l, r, before, after := repeatedRemovalFixture(t)
	// A user Next cannot be mistaken for uninterrupted playback while queue
	// identifiers are being reassigned, even with a complete matching suffix.
	next := after.Queue.Items[2]
	after.Media = &next
	if confirmed, err := c.removeObservation(context.Background(), l, r, before, after, 1); confirmed || err != nil {
		t.Fatal(confirmed, err)
	}
	// Removing one of two requested entries is only a pending observation.
	r.ordered = []heos.ID{"repeated", "repeated"}
	current := after.Queue.Items[1]
	after.Media = &current
	if confirmed, err := c.removeObservation(context.Background(), l, r, before, after, 2); confirmed || err != nil {
		t.Fatal(confirmed, err)
	}
}

func TestBufferedPruneRechecksPreviousBeforeSending(t *testing.T) {
	c, _, d, _, r := ownedSkipFixture(t)
	r.buffered.policy.RetainPrevious = 1
	before := d.Snapshot()
	last := before.Queue.Items[len(before.Queue.Items)-1]
	before.Media = &last
	r.expected = before
	r.parts = []playbackPart{{CatalogToken: before.Token}}
	var original []heos.ID
	for _, item := range before.Queue.Items {
		original = append(original, item.ID)
	}
	r.ordered = append([]heos.ID(nil), original...)
	// Prefix selection happened at the fourth entry. A Previous then moves
	// into that prefix before the last observation/guard may permit removal.
	d.mu.Lock()
	previous := before.Queue.Items[1]
	d.s.Media = &previous
	d.mu.Unlock()
	err := c.pruneBuffered(c.lanes["room"], r, 2, before.Queue)
	if !errors.Is(err, errPlaybackTimelineChanged) || len(d.writes) != 0 || r.buffered.pruned != 0 {
		t.Fatal(err, d.writes, r.buffered.pruned)
	}
	if !reflect.DeepEqual(r.ordered, original) {
		t.Fatal("unsent removal changed logical queue", r.ordered, original)
	}
	r.mu.Lock()
	observed := r.expected.Media.QueueID
	r.mu.Unlock()
	if observed != previous.QueueID {
		t.Fatal("prune replan retained stale position and could spin on the same unsafe prefix", observed, previous.QueueID)
	}
}
