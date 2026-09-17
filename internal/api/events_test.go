package api

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEventReplayGapAuthorizationAndBound(t *testing.T) {
	b := NewEvents("epoch", 3, 2, 2)
	allow := func(e Event) bool { return e.Player == "room" }
	sub, backlog, err := b.Subscribe("old:1", allow)
	if err != nil || len(backlog) != 1 || backlog[0].Kind != "snapshot_required" {
		t.Fatal(backlog, err)
	}
	defer sub.Close()
	b.Publish(Event{Kind: "player_changed", Player: "other"})
	if len(sub.events) != 0 {
		t.Fatal("unauthorized event leaked")
	}
	b.Publish(Event{Kind: "player_changed", Player: "room"})
	b.Publish(Event{Kind: "player_changed", Player: "room"})
	b.Publish(Event{Kind: "player_changed", Player: "room"})
	select {
	case <-sub.done:
	default:
		t.Fatal("slow subscriber not disconnected")
	}
	_, replay, err := b.Subscribe("epoch:3", allow)
	if err != nil || len(replay) != 1 || replay[0].ID != "epoch:4" {
		t.Fatal(replay, err)
	}
	_, gap, err := b.Subscribe("epoch:0", allow)
	if err != nil || gap[0].Kind != "snapshot_required" {
		t.Fatal(gap, err)
	}
	if _, _, err := b.Subscribe("", allow); err == nil {
		t.Fatal("subscriber bound ignored")
	}
	b.Close()
	if _, _, err := b.Subscribe("", allow); err == nil {
		t.Fatal("subscribe after close")
	}
}

type cancellingWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
	writes int
}

func (w *cancellingWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *cancellingWriter) Write(b []byte) (int, error) {
	w.writes++
	w.cancel()
	return w.ResponseRecorder.Write(b)
}
func TestSSEReplayStopsOnCancellation(t *testing.T) {
	b := NewEvents("epoch", 4, 2, 2)
	defer b.Close()
	sub, _, e := b.Subscribe("", func(Event) bool { return true })
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &cancellingWriter{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
	stream := eventStream{sub: sub, replay: []Event{{ID: "epoch:1", Kind: "player_changed"}, {ID: "epoch:2", Kind: "player_changed"}}, request: httptest.NewRequest("GET", "/v1/events", nil).WithContext(ctx)}
	if e := stream.VisitGetEventsResponse(w); e != nil {
		t.Fatal(e)
	}
	if w.writes != 1 {
		t.Fatalf("replay continued after cancellation: %d writes", w.writes)
	}
}
