package control

import (
	"fmt"
	"testing"

	"github.com/dreylark/heos-control/internal/heos"
)

func FuzzOrderedQueueAppend(f *testing.F) {
	f.Add([]byte{1, 0, 1, 2, 0}, uint8(0))
	f.Add([]byte{0, 0, 0, 0}, uint8(1))
	f.Add([]byte{2, 1}, uint8(2))
	f.Fuzz(func(t *testing.T, data []byte, mutation uint8) {
		if len(data) < 2 || len(data) > 8 {
			return
		}
		ids := make([]heos.ID, len(data))
		items := make([]heos.Media, len(data))
		for i, b := range data {
			ids[i] = heos.ID(fmt.Sprint("track-", b%3))
			items[i] = heos.Media{ID: ids[i], QueueID: heos.ID(fmt.Sprint(i + 1))}
		}
		prefix := len(items) / 2
		total := len(items)
		before := heos.QueuePage{Items: append([]heos.Media(nil), items[:prefix]...), Total: &prefix}
		after := heos.QueuePage{Items: append([]heos.Media(nil), items...), Total: &total}
		if !orderedQueue(after, ids) || appendQueueProblem(before, after, ids) {
			t.Fatal("valid repeated ordered tracks rejected")
		}
		// Partial exact suffixes may wait; only the complete sequence can confirm.
		partialCount := total - 1
		partial := heos.QueuePage{Items: items[:partialCount], Total: &partialCount}
		if orderedQueue(partial, ids) || appendQueueProblem(before, partial, ids) {
			t.Fatal("partial suffix confirmation or false rejection")
		}
		switch mutation % 5 {
		case 0:
			after.Items[0].QueueID = "changed-prefix"
		case 1:
			after.Items[len(items)-1].ID = "foreign"
		case 2:
			after.Items[len(items)-1].QueueID = after.Items[0].QueueID
		case 3:
			after.Total = nil
		case 4:
			after.Next = &total
		}
		if !appendQueueProblem(before, after, ids) {
			t.Fatal("queue edit admitted")
		}
	})
}
