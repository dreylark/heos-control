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

func TestGuardedWritesFollowDenonAndInvalidateSnapshots(t *testing.T) {
	var writes atomic.Int64
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if u.Host+u.Path == "player/set_volume" {
			writes.Add(1)
			if u.Query().Get("level") != "10" {
				t.Error(u.String())
			}
		}
		sendReply(c, u, nil, nil)
	})
	client, e := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin, EnableWrites: true})
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e = client.Read(ctx, "player/get_players", nil); e != nil {
		t.Fatal(e)
	}
	guard := Guard{Token: client.observePlayer("-42").Token, ExpiresAt: time.Now().Add(time.Second)}
	if _, e = client.Write(ctx, Mutation{Kind: "volume", Player: "-42", Level: 10}, guard); e != nil {
		t.Fatal(e)
	}
	if client.PlayerView("-42").Token == guard.Token {
		t.Fatal("write did not invalidate cached observation")
	}
	if _, e = client.Write(ctx, Mutation{Kind: "volume", Player: "-42", Level: 11}, guard); !errors.Is(e, ErrStale) {
		t.Fatal("stale write accepted", e)
	}
	if writes.Load() != 1 {
		t.Fatal(writes.Load())
	}
}

func TestExpiredObservationNeverAuthorizesWrite(t *testing.T) {
	var writes atomic.Int64
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/set_volume" {
			writes.Add(1)
		}
		sendReply(c, u, nil, nil)
	})
	c, e := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin, EnableWrites: true})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if _, e = c.Read(context.Background(), "player/get_players", nil); e != nil {
		t.Fatal(e)
	}
	guard := Guard{Token: c.observePlayer("1").Token, ExpiresAt: time.Now().Add(-time.Second)}
	if _, e = c.Write(context.Background(), Mutation{Kind: "volume", Player: "1", Level: 10}, guard); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if writes.Load() != 0 {
		t.Fatal("expired state authorized a setter")
	}
}

func TestWriteRevisionStorageRequiresObservedPlayer(t *testing.T) {
	var writes atomic.Int32
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/set_volume" {
			writes.Add(1)
		}
		sendReply(c, u, nil, nil)
	})
	c, err := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin, EnableWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Read(context.Background(), "player/get_players", nil); err != nil {
		t.Fatal(err)
	}
	guard := Guard{Token: c.PlayerView("unobserved").Token, ExpiresAt: time.Now().Add(time.Second)}
	if _, err := c.Write(context.Background(), Mutation{Kind: "volume", Player: "unobserved", Level: 10}, guard); !errors.Is(err, ErrStale) || writes.Load() != 0 {
		t.Fatal("unobserved player created unbounded write bookkeeping", err, writes.Load())
	}
	if len(c.writes) != 0 {
		t.Fatal("rejected write retained a player revision")
	}
}

func TestMutationWireAllowlistAndDisabledGate(t *testing.T) {
	// Denon 4.2.4/7/11/14, 4.2.21/4.2.22 and 4.4.11/12: absolute values, aid=4,
	// playable containers versus tracks; no raw URL, toggle or queue-edit endpoint.
	for _, m := range []Mutation{{Kind: "volume", Player: "1", Level: 101}, {Kind: "transport", Player: "1", State: "toggle"}, {Kind: "skip", Player: "1", Direction: "toggle"}, {Kind: "mode", Player: "1", Repeat: "yes"}, {Kind: "queue", Player: "1", Item: Item{Container: "yes", Playable: "no", ContainerID: "album"}}, {Kind: "reboot", Player: "1"}} {
		if _, _, e := m.command(); e == nil {
			t.Fatalf("accepted invalid mutation %+v", m)
		}
	}
	for _, tc := range []struct{ direction, command string }{{"next", "player/play_next"}, {"previous", "player/play_previous"}} {
		name, args, err := (Mutation{Kind: "skip", Player: "1", Direction: tc.direction}).command()
		if err != nil || name != tc.command || len(args) != 1 || args.Get("pid") != "1" {
			t.Fatal(name, args, err)
		}
	}
	m := Mutation{Kind: "queue", Player: "1", Item: Item{Source: "900", ContainerID: "Album/+%&", Container: "yes", Playable: "yes"}}
	name, args, e := m.command()
	if e != nil || name != "browse/add_to_queue" || args.Get("aid") != "4" || args.Get("cid") != "Album/+%&" {
		t.Fatal(name, args, e)
	}
	client, e := New(context.Background(), Config{Address: "127.0.0.1:1", Fingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close()
	if _, e = client.Write(context.Background(), m, Guard{}); !errors.Is(e, ErrReadOnly) {
		t.Fatal("default client allows writes", e)
	}
}

func TestPriorityStopInterruptsBrowseOnTheWire(t *testing.T) {
	started := make(chan struct{})
	var stopped atomic.Int64
	server := newFakeHEOS(t, func(conn net.Conn, u *url.URL, _ int64) {
		switch commandName(u) {
		case "browse/browse":
			close(started)
			return
		case "player/set_play_state":
			if u.Query().Get("state") != "stop" {
				t.Error(u.String())
			}
			stopped.Add(1)
		}
		sendReply(conn, u, nil, nil)
	})
	c, e := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin, EnableWrites: true, ReconnectDelay: time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	browse := make(chan error, 1)
	go func() { _, e := c.Read(ctx, "browse/browse", url.Values{"sid": {"900"}}); browse <- e }()
	<-started
	release := c.BeginPriority()
	defer release()
	if e := <-browse; e == nil {
		t.Fatal("browse was not interrupted")
	}
	if _, e := c.Read(ctx, "player/get_players", nil); !errors.Is(e, ErrQueueFull) {
		t.Fatal("ordinary path entered priority session", e)
	}
	priority := Priority(ctx)
	eventually(t, func() bool { _, e := c.Read(priority, "player/get_players", nil); return e == nil })
	guard := Guard{Token: c.observePlayer("1").Token, ExpiresAt: time.Now().Add(time.Second)}
	if _, e := c.Write(priority, Mutation{Kind: "transport", Player: "1", State: "stop"}, guard); e != nil {
		t.Fatal(e)
	}
	if stopped.Load() != 1 {
		t.Fatal(stopped.Load())
	}
}

func TestLostWriteReplyRemainsUncertainAndIsNeverReplayed(t *testing.T) {
	var writes atomic.Int64
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/set_volume" {
			writes.Add(1)
			_ = c.Close()
			return
		}
		sendReply(c, u, nil, nil)
	})
	c, e := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin, EnableWrites: true})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := c.Read(ctx, "player/get_players", nil); e != nil {
		t.Fatal(e)
	}
	guard := Guard{Token: c.observePlayer("1").Token, ExpiresAt: time.Now().Add(time.Second)}
	_, e = c.Write(ctx, Mutation{Kind: "volume", Player: "1", Level: 10}, guard)
	var ce *CommandError
	if !errors.As(e, &ce) || ce.Delivery != Uncertain {
		t.Fatal(e)
	}
	if _, e = c.Write(ctx, Mutation{Kind: "volume", Player: "1", Level: 10}, guard); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if writes.Load() != 1 {
		t.Fatal("write replayed", writes.Load())
	}
}
