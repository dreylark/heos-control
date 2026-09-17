package heos

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeHEOS struct {
	listener    net.Listener
	pin         string
	connections atomic.Int64
	commands    atomic.Int64
	closed      chan struct{}
	mu          sync.Mutex
	conns       []net.Conn
	wg          sync.WaitGroup
}

func newFakeHEOS(t *testing.T, handle func(net.Conn, *url.URL, int64)) *fakeHEOS {
	return newFakeHEOSWithRegistration(t, handle, func(c net.Conn, u *url.URL) { sendReply(c, u, nil, nil) })
}

func newFakeHEOSWithRegistration(t *testing.T, handle func(net.Conn, *url.URL, int64), register func(net.Conn, *url.URL)) *fakeHEOS {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	l, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	pin := sha256.Sum256(der)
	s := &fakeHEOS{listener: l, pin: hex.EncodeToString(pin[:]), closed: make(chan struct{})}
	s.wg.Go(func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			id := s.connections.Add(1)
			s.mu.Lock()
			s.conns = append(s.conns, conn)
			s.mu.Unlock()
			s.wg.Go(func() {
				defer func() { _ = conn.Close() }()
				r := bufio.NewReader(conn)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					u, err := url.Parse(strings.TrimSuffix(line, "\r\n"))
					if err != nil {
						return
					}
					s.commands.Add(1)
					if commandName(u) == "system/register_for_change_events" {
						register(conn, u)
						continue
					}
					handle(conn, u, id)
				}
			})
		}
	})
	t.Cleanup(func() {
		close(s.closed)
		_ = l.Close()
		s.mu.Lock()
		for _, c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func commandName(u *url.URL) string { return u.Host + u.Path }
func sendReply(c net.Conn, u *url.URL, fields url.Values, payload any) {
	params := u.Query()
	for key, values := range fields {
		params[key] = values
	}
	frame := map[string]any{"heos": map[string]string{"command": commandName(u), "result": "success", "message": strings.ReplaceAll(params.Encode(), "+", "%20")}}
	if payload != nil {
		frame["payload"] = payload
	}
	raw, _ := json.Marshal(frame)
	_, _ = c.Write(append(raw, '\r', '\n'))
}
func sendEvent(c net.Conn, name, message string) {
	raw, _ := json.Marshal(map[string]any{"heos": map[string]string{"command": name, "message": message}})
	_, _ = c.Write(append(raw, '\r', '\n'))
}
func fakeClient(t *testing.T, s *fakeHEOS) *Client {
	t.Helper()
	c, err := New(context.Background(), Config{Address: s.listener.Addr().String(), Fingerprint: s.pin,
		ConnectTimeout: time.Second, CommandTimeout: time.Second, ReconnectDelay: time.Millisecond, QueueDepth: 2, EventBuffer: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !f() {
		select {
		case <-deadline:
			t.Fatal("condition was not reached")
		case <-ticker.C:
		}
	}
}

func TestPinnedTLSAndPersistentReader(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		sendEvent(c, "event/player_state_changed", "pid=1&state=pause")
		sendReply(c, u, url.Values{"state": {"pause"}}, nil)
	})
	bad, err := New(context.Background(), Config{Address: s.listener.Addr().String(), Fingerprint: strings.Repeat("00", 32)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = bad.Read(context.Background(), "player/get_play_state", url.Values{"pid": {"1"}})
	bad.Close()
	if err == nil || s.commands.Load() != 0 {
		t.Fatal("wrong certificate allowed commands", err)
	}
	c := fakeClient(t, s)
	for range 3 {
		r, err := c.Read(context.Background(), "player/get_play_state", url.Values{"pid": {"1"}})
		if err != nil || r.Params.Get("state") != "pause" {
			t.Fatal(r, err)
		}
	}
	if s.connections.Load() != 2 {
		t.Fatal("did not reuse pinned connection")
	}
	before := s.commands.Load()
	if _, err := c.Read(context.Background(), "player/set_volume", url.Values{"level": {"90"}}); !errors.Is(err, ErrReadOnly) {
		t.Fatal(err)
	}
	if s.commands.Load() != before {
		t.Fatal("read path sent a mutation")
	}
	select {
	case event := <-c.Events():
		if !event.Gap && event.Command != "event/player_state_changed" {
			t.Fatal(event)
		}
	default:
		t.Fatal("event lost without gap")
	}
}

func TestCancelDiscardsGenerationAndLateReplies(t *testing.T) {
	received, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, id int64) {
		if id == 1 {
			close(received)
			<-release
			sendReply(c, u, url.Values{"level": {"11"}}, nil)
			return
		}
		sendReply(c, u, url.Values{"level": {"22"}}, nil)
	})
	c := fakeClient(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := c.Read(ctx, "player/get_volume", url.Values{"pid": {"1"}}); done <- err }()
	<-received
	old := c.View().Token
	cancel()
	var failure *CommandError
	if err := <-done; !errors.As(err, &failure) || failure.Delivery != Uncertain {
		t.Fatalf("expected uncertain sent command, got %v", err)
	}
	eventually(t, func() bool { return !c.View().Connected })
	eventually(t, func() bool {
		r, err := c.Read(context.Background(), "player/get_volume", url.Values{"pid": {"1"}})
		return err == nil && r.Params.Get("level") == "22" && r.Token.Generation > old.Generation
	})
	// Cancelling an already finished request cannot close the reused generation.
	ctx2, cancel2 := context.WithCancel(context.Background())
	if _, err := c.Read(ctx2, "player/get_volume", url.Values{"pid": {"1"}}); err != nil {
		t.Fatal(err)
	}
	current := c.View().Token.Generation
	cancel2()
	if r, err := c.Read(context.Background(), "player/get_volume", url.Values{"pid": {"1"}}); err != nil || r.Token.Generation != current {
		t.Fatal(r, err)
	}
}

func TestEventFloodAndInterrupt(t *testing.T) {
	blocked, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "browse/browse" {
			close(blocked)
			<-release
			return
		}
		for range 100 {
			sendEvent(c, "event/player_volume_changed", "pid=1&level=10")
		}
		sendReply(c, u, url.Values{"level": {"10"}}, nil)
	})
	c := fakeClient(t, s)
	if _, err := c.Read(context.Background(), "player/get_volume", nil); err != nil {
		t.Fatal(err)
	}
	if len(c.Events()) > 2 {
		t.Fatal("unbounded event queue")
	}
	found := false
	for len(c.Events()) > 0 {
		if (<-c.Events()).Gap {
			found = true
		}
	}
	if !found {
		t.Fatal("overflow did not notify a gap")
	}
	done := make(chan error, 1)
	go func() { _, err := c.Read(context.Background(), "browse/browse", nil); done <- err }()
	<-blocked
	c.Interrupt()
	select {
	case err := <-done:
		if !errors.Is(err, ErrInterrupted) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("interrupt waited for browse timeout")
	}
}

func TestMalformedReplyAndEchoMismatch(t *testing.T) {
	for _, raw := range []string{"{}\r\n", "{\r\n", strings.Repeat("x", DefaultFrameBytes+1) + "\r\n", `{"heos":{"command":"player/get_volume","result":"success","message":"pid=wrong&level=10"}}` + "\r\n", `{"heos":{"command":"player/get_volume","result":"success","message":"level=10"}}`} {
		t.Run(fmt.Sprint(len(raw)), func(t *testing.T) {
			s := newFakeHEOS(t, func(c net.Conn, _ *url.URL, _ int64) { _, _ = c.Write([]byte(raw)); _ = c.Close() })
			c := fakeClient(t, s)
			if _, err := c.Read(context.Background(), "player/get_volume", url.Values{"pid": {"1"}}); err == nil {
				t.Fatal("accepted malformed/mismatched reply")
			}
		})
	}
}

func TestQueueBoundAndCancellationBeforeSend(t *testing.T) {
	received, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		close(received)
		<-release
		sendReply(c, u, nil, nil)
	})
	c := fakeClient(t, s)
	active := make(chan error, 1)
	go func() { _, err := c.Read(context.Background(), "system/heart_beat", nil); active <- err }()
	<-received
	queued := make(chan error, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for range 2 {
		go func() { _, err := c.Read(ctx, "system/heart_beat", nil); queued <- err }()
	}
	eventually(t, func() bool { return len(c.requests) == 2 })
	if _, err := c.Read(context.Background(), "system/heart_beat", nil); !errors.Is(err, ErrQueueFull) {
		t.Fatal("unbounded command queue", err)
	}
	cancel()
	for range 2 {
		var failure *CommandError
		if err := <-queued; !errors.As(err, &failure) || failure.Delivery != NotSent || !errors.Is(err, context.Canceled) {
			t.Fatal("cancelled queued command was sent", err)
		}
	}
	c.Interrupt()
	if err := <-active; !errors.Is(err, ErrInterrupted) {
		t.Fatal(err)
	}
	c.Close()
	if s.commands.Load() != 2 { // registration and the active heartbeat only
		t.Fatal("queued commands reached the device", s.commands.Load())
	}
}

func TestDeadlineReconnectAndReaderShutdown(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "system/heart_beat" {
			sendReply(c, u, nil, nil)
		}
		// Volume reads intentionally never receive a reply.
	})
	c := fakeClient(t, s)
	for range 3 {
		eventually(t, func() bool {
			_, err := c.Read(context.Background(), "system/heart_beat", nil)
			return err == nil
		})
		c.mu.Lock()
		old := c.connection
		c.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		_, err := c.Read(ctx, "player/get_volume", url.Values{"pid": {"1"}})
		cancel()
		var failure *CommandError
		if !errors.As(err, &failure) || failure.Delivery != Uncertain || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		select {
		case <-old.done:
		default:
			t.Fatal("old reader survived completed timeout")
		}
	}
	if s.connections.Load() != 3 || c.View().Connected {
		t.Fatal("unexpected retry or retained connection", s.connections.Load(), c.View())
	}
	c.Close()
	if _, err := c.Read(context.Background(), "system/heart_beat", nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestOfflineCooldownDoesNotDialPerRequest(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, _ *url.URL, _ int64) { _ = c.Close() })
	c, err := New(context.Background(), Config{Address: s.listener.Addr().String(), Fingerprint: s.pin, ReconnectDelay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Read(context.Background(), "system/heart_beat", nil); err == nil {
		t.Fatal("closed socket accepted")
	}
	for range 100 {
		if _, err := c.Read(context.Background(), "system/heart_beat", nil); !errors.Is(err, ErrOffline) {
			t.Fatal(err)
		}
	}
	if s.connections.Load() != 1 {
		t.Fatal("offline requests caused reconnect storm", s.connections.Load())
	}
}

func TestPendingAndSequenceMismatch(t *testing.T) {
	var mismatch atomic.Bool
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		raw, _ := json.Marshal(map[string]any{"heos": map[string]string{
			"command": commandName(u), "result": "success", "message": "command under process&" + u.RawQuery,
		}})
		_, _ = c.Write(append(raw, '\r', '\n'))
		fields := url.Values{"count": {"0"}}
		if mismatch.Load() {
			fields.Set("SEQUENCE", "wrong")
		}
		sendReply(c, u, fields, []any{})
	})
	c := fakeClient(t, s)
	if _, err := c.BrowsePage(context.Background(), "1024", "", 0, 100); err != nil {
		t.Fatal("pending reply treated as final", err)
	}
	mismatch.Store(true)
	if _, err := c.BrowsePage(context.Background(), "1024", "", 0, 100); !errors.Is(err, ErrProtocol) {
		t.Fatal("mismatched sequence accepted", err)
	}
}

func TestCancellationRacingReplyCannotCloseNextRequest(t *testing.T) {
	type raceGate struct{ received, fire chan struct{} }
	gates := make(chan raceGate, 1)
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if u.Query().Get("pid") == "race" {
			gate := <-gates
			close(gate.received)
			<-gate.fire
		}
		sendReply(c, u, url.Values{"level": {"12"}}, nil)
	})
	c := fakeClient(t, s)
	for range 25 {
		gate := raceGate{make(chan struct{}), make(chan struct{})}
		gates <- gate
		ctx, cancel := context.WithCancel(context.Background())
		done, cancelled := make(chan error, 1), make(chan struct{})
		go func() {
			_, err := c.Read(ctx, "player/get_volume", url.Values{"pid": {"race"}})
			done <- err
		}()
		<-gate.received
		go func() { <-gate.fire; cancel(); close(cancelled) }()
		close(gate.fire) // Release the reply and cancellation concurrently.
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		<-cancelled
		eventually(t, func() bool {
			_, err := c.Read(context.Background(), "player/get_volume", url.Values{"pid": {"stable"}})
			return err == nil
		})
		generation := c.View().Token.Generation
		if r, err := c.Read(context.Background(), "player/get_volume", url.Values{"pid": {"stable"}}); err != nil || r.Token.Generation != generation {
			t.Fatal("old cancellation closed the next request's socket", r, err)
		}
	}
}

func TestPageEchoAllowsShortFinalPage(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		sendReply(c, u, url.Values{"range": {"10, 11"}, "count": {"12"}, "returned": {"2"}}, []any{
			map[string]any{"cid": "a"}, map[string]any{"cid": "b"},
		})
	})
	page, err := fakeClient(t, s).BrowsePage(context.Background(), "server", "album", 10, 100)
	if err != nil || len(page.Items) != 2 || page.Next != nil {
		t.Fatal(page, err)
	}
}

func TestEmptyIdentityAndInvalidRangeEcho(t *testing.T) {
	for _, fields := range []url.Values{
		{"gid": {""}}, {"range": {"0,101"}}, {"range": {"0,invalid"}}, {"range": {"1,0"}},
	} {
		s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) { sendReply(c, u, fields, []any{}) })
		c := fakeClient(t, s)
		var err error
		if fields.Has("gid") {
			_, err = c.Read(context.Background(), "group/get_group_info", url.Values{"gid": {"1"}})
		} else {
			_, err = c.BrowsePage(context.Background(), "server", "album", 0, 100)
		}
		if !errors.Is(err, ErrProtocol) {
			t.Fatal(fields, err)
		}
	}
}
