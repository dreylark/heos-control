package heos

import (
	"context"
	"errors"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

// Denon 2.1.1 recommends bootstrap reads before event registration. This shared
// socket intentionally subscribes first, then accepts only a coherent post-
// subscription snapshot, so a read/subscribe gap cannot publish trusted state.
func TestSubscribeThenObserveWithDelayedDiscovery(t *testing.T) {
	var registered atomic.Bool
	var discoveries atomic.Int32
	s := newFakeHEOSWithRegistration(t, func(c net.Conn, u *url.URL, _ int64) {
		if !registered.Load() || !readCommand(commandName(u)) {
			t.Error("unsubscribed or mutating read")
		}
		if commandName(u) == "player/get_players" && discoveries.Add(1) == 1 {
			sendReply(c, u, nil, []any{})
			return
		}
		observationReply(c, u, "serial-A")
	}, func(c net.Conn, u *url.URL) {
		if u.Query().Get("enable") != "on" {
			t.Error("unexpected registration")
		}
		registered.Store(true)
		sendReply(c, u, nil, nil)
	})
	c := fakeClient(t, s)
	o, err := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if s.connections.Load() != 0 || o.Snapshot().Verified {
		t.Fatal("construction contacted/trusted device")
	}
	if err := o.Refresh(context.Background()); !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
	if state := o.Snapshot(); state.Verified || !state.Stale || state.State != PlayStateUnknown {
		t.Fatal(state)
	}
	// A later caller/poll retries observations; the client does not replay them.
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !o.Snapshot().Verified || s.connections.Load() != 1 {
		t.Fatal(o.Snapshot(), s.connections.Load())
	}
}
