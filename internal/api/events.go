package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Event struct {
	ID        string `json:"-"`
	Kind      string `json:"-"`
	Player    string `json:"player,omitempty"`
	Operation string `json:"operation,omitempty"`
	Principal string `json:"-"`
}
type Events struct {
	mu                    sync.Mutex
	epoch                 string
	seq                   uint64
	history               []Event
	capacity, buffer, max int
	subs                  map[*subscription]bool
	closed                bool
}
type subscription struct {
	broker *Events
	events chan Event
	done   chan struct{}
	allow  func(Event) bool
}

var errEventCapacity = errors.New("event subscriber capacity reached")

func NewEvents(epoch string, capacity, buffer, max int) *Events {
	if epoch == "" || capacity < 1 || capacity > 1024 || buffer < 1 || buffer > 256 || max < 1 || max > 128 {
		panic("invalid event bounds")
	}
	return &Events{epoch: epoch, capacity: capacity, buffer: buffer, max: max, subs: map[*subscription]bool{}}
}
func (b *Events) remove(s *subscription) {
	if b.subs[s] {
		delete(b.subs, s)
		close(s.done)
	}
}
func (s *subscription) Close() { s.broker.mu.Lock(); defer s.broker.mu.Unlock(); s.broker.remove(s) }
func (b *Events) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for s := range b.subs {
		b.remove(s)
	}
}
func (b *Events) Publish(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.seq++
	e.ID = fmt.Sprintf("%s:%d", b.epoch, b.seq)
	if len(b.history) == b.capacity {
		copy(b.history, b.history[1:])
		b.history = b.history[:len(b.history)-1]
	}
	b.history = append(b.history, e)
	for s := range b.subs {
		if !s.allow(e) {
			continue
		}
		select {
		case s.events <- e:
		default:
			b.remove(s)
		}
	}
}
func (b *Events) Subscribe(last string, allow func(Event) bool) (*subscription, []Event, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || len(b.subs) >= b.max {
		return nil, nil, errEventCapacity
	}
	epoch, n, ok := strings.Cut(last, ":")
	seq, err := strconv.ParseUint(n, 10, 64)
	gap := !ok || err != nil || epoch != b.epoch || seq > b.seq
	if len(b.history) > 0 && seq < b.seq-uint64(len(b.history)) {
		gap = true
	}
	replay := []Event{}
	if gap {
		replay = append(replay, Event{ID: fmt.Sprintf("%s:%d", b.epoch, b.seq), Kind: "snapshot_required"})
	} else {
		for _, e := range b.history {
			_, n, _ := strings.Cut(e.ID, ":")
			id, _ := strconv.ParseUint(n, 10, 64)
			if id > seq && allow(e) {
				replay = append(replay, e)
			}
		}
	}
	s := &subscription{broker: b, events: make(chan Event, b.buffer), done: make(chan struct{}), allow: allow}
	b.subs[s] = true
	return s, replay, nil
}

// A stream owns no detached goroutines. Per-write deadlines also bound slow peers
// when there are no new events; broker overflow closes the subscription.
type eventStream struct {
	sub     *subscription
	replay  []Event
	request *http.Request
}

func (s eventStream) VisitGetEventsResponse(w http.ResponseWriter) error {
	defer s.sub.Close()
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	write := func(e *Event) error {
		select {
		case <-s.request.Context().Done():
			return s.request.Context().Err()
		case <-s.sub.done:
			return context.Canceled
		default:
		}
		if err := rc.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		if e == nil {
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return err
			}
		} else {
			b, _ := json.Marshal(e)
			if _, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", e.ID, e.Kind, b); err != nil {
				return err
			}
		}
		return rc.Flush()
	}
	for _, e := range s.replay {
		if err := write(&e); err != nil {
			return nil
		}
	}
	if len(s.replay) == 0 {
		if err := write(nil); err != nil {
			return nil
		}
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.request.Context().Done():
			return nil
		case <-s.sub.done:
			return nil
		case e := <-s.sub.events:
			if err := write(&e); err != nil {
				return nil
			}
		case <-ticker.C:
			if err := write(nil); err != nil {
				return nil
			}
		}
	}
}
