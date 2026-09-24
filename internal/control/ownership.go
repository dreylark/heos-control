package control

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

// ownedState compares the whole owned queue, not the currently playing track.
// Call with r.mu held. Denon 4.2.5/4.2.15 supply MID/QID; 5.5 supplies no origin.
// By contract both natural and manual transitions within this queue are allowed.
func (r *execution) ownedState(a, b heos.Snapshot) bool {
	return len(r.ownedChanges(a, b)) == 0
}

func (r *execution) ownedChanges(a, b heos.Snapshot) []string {
	if a.Token.Generation != b.Token.Generation {
		return []string{"connection_generation"}
	}
	if r.queueOwned && a.State == b.State &&
		((b.State == heos.PlayStatePlay && queuedMedia(a.Queue, b.Media)) ||
			(b.State == heos.PlayStateStop && (emptyMedia(b.Media) || queuedMedia(a.Queue, b.Media)))) {
		a.Media = b.Media
	}
	return stateChanges(a, b)
}

func emptyMedia(m *heos.Media) bool {
	return m == nil || (m.Source == "" && m.ID == "" && m.QueueID == "")
}

func queuedMedia(q heos.QueuePage, m *heos.Media) bool {
	if m == nil || m.Source != "1024" || m.ID == "" || m.QueueID == "" {
		return false
	}
	for _, item := range q.Items {
		if item.QueueID == m.QueueID && item.ID == m.ID {
			return true
		}
	}
	return false
}

func completeOwnedQueue(q heos.QueuePage, members map[heos.ID]bool) bool {
	if q.Total == nil || *q.Total != len(q.Items) || len(q.Items) == 0 || q.Next != nil {
		return false
	}
	seen := make(map[heos.ID]bool, len(q.Items))
	for _, m := range q.Items {
		if m.QueueID == "" || seen[m.QueueID] || !members[m.ID] {
			return false
		}
		seen[m.QueueID] = true
	}
	return true
}

// Complete the observation's first queue page under the same revision. Large
// queues remain bounded by 100 pages/10,000 tracks and a shared read timeout.
func completeQueue(ctx context.Context, l *lane, s heos.Snapshot) (heos.Snapshot, error) {
	q := s.Queue
	if q.Next == nil {
		return s, nil
	}
	q.Items = append([]heos.Media(nil), q.Items...)
	for pages := 1; q.Next != nil; pages++ {
		if pages >= heos.MaxBrowsePages || q.Total == nil || *q.Total > heos.MaxBrowseItems || *q.Next != len(q.Items) {
			return s, heos.ErrBounds
		}
		page, err := l.device.Client.Queue(ctx, s.Player.ID, *q.Next, 100)
		if err != nil {
			return s, err
		}
		if page.Token != s.Token || !reflect.DeepEqual(q.Total, page.Total) {
			return s, heos.ErrStale
		}
		if len(page.Items) == 0 || len(q.Items)+len(page.Items) > heos.MaxBrowseItems {
			return s, heos.ErrBounds
		}
		q.Items = append(q.Items, page.Items...)
		q.Next = page.Next
	}
	if *q.Total != len(q.Items) {
		return s, heos.ErrProtocol
	}
	view := l.device.Client.PlayerView(s.Player.ID)
	if !view.Connected || view.Token != s.Token {
		return s, heos.ErrStale
	}
	s.Queue = q
	return s, nil
}

// A media event can invalidate a read while a natural transition is in flight.
// Retry only read observations; never hide a manual cancellation or I/O failure.
func observe(ctx context.Context, l *lane, bounded bool) (heos.Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var s heos.Snapshot
	var err error
	for range 3 {
		if err = context.Cause(ctx); err != nil {
			return s, err
		}
		err = l.device.Observer.Refresh(ctx)
		if err == nil {
			s = l.device.Observer.Snapshot()
			if s.EventUpdated {
				err = heos.ErrStale // Events after Refresh cannot stand in for complete readback.
			} else if bounded {
				s, err = completeQueue(ctx, l, s)
			}
		}
		if !errors.Is(err, heos.ErrStale) {
			return s, err
		}
	}
	return s, err
}
