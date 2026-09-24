package heos

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const DefaultFrameBytes = 256 * 1024

var (
	ErrReadOnly    = errors.New("HEOS command is not an allowed observation")
	ErrQueueFull   = errors.New("HEOS command queue full")
	ErrInterrupted = errors.New("HEOS command interrupted")
	ErrClosed      = errors.New("HEOS client closed")
	ErrOffline     = errors.New("HEOS reconnect cooldown")
	ErrRejected    = errors.New("HEOS rejected command")
	ErrConnect     = errors.New("HEOS connection failed")
	ErrTLS         = errors.New("HEOS TLS handshake failed")
	ErrPinMismatch = errors.New("HEOS certificate pin mismatch")
)

// Preserve the phase and underlying cause without including a private address
// or certificate detail in the rendered error. Callers classify with errors.Is.
type connectionError struct{ phase, cause error }

func (e *connectionError) Error() string        { return e.phase.Error() }
func (e *connectionError) Unwrap() error        { return e.cause }
func (e *connectionError) Is(target error) bool { return target == e.phase }

type Delivery string

const (
	NotSent   Delivery = "not_sent"
	Uncertain Delivery = "uncertain"
	Rejected  Delivery = "rejected"
)

type CommandError struct {
	Delivery Delivery
	Cause    error
}

func (e *CommandError) Error() string { return fmt.Sprintf("HEOS %s: %v", e.Delivery, e.Cause) }
func (e *CommandError) Unwrap() error { return e.Cause }

type Config struct {
	Logger         *slog.Logger
	Metrics        Metrics
	EnableWrites   bool
	Address        string
	Fingerprint    string
	ConnectTimeout time.Duration
	CommandTimeout time.Duration
	ReconnectDelay time.Duration
	QueueDepth     int
	EventBuffer    int
	FrameBytes     int
}

type Event struct {
	Command   string
	Params    url.Values
	Token     Token
	Gap       bool
	GapReason string
	data      EventData
	decoded   bool
	// progressSequence orders display samples independently of control revisions.
	progressSequence uint64
}
type View struct {
	Token     Token
	Connected bool
}
type result struct {
	response Response
	err      error
}
type request struct {
	guard    *Guard
	mutation Mutation
	priority bool
	ctx      context.Context
	command  string
	args     url.Values
	fence    uint64
	state    atomic.Int32 // 0 queued, 1 owned, 2 cancelled before ownership
	result   chan result
}
type incoming struct {
	response Response
	err      error
}
type generation struct {
	conn       net.Conn
	frames     chan incoming
	stop       chan struct{}
	done       chan struct{}
	once       sync.Once
	subscribed bool // Owner only; a failed bootstrap never leaves a usable socket.
}

func (g *generation) close() { g.once.Do(func() { close(g.stop); _ = g.conn.Close() }) }

// Client has one command owner and at most one reader/socket. New performs no
// I/O until the first read. Failed commands are never automatically replayed.
type Client struct {
	cfg          Config
	pin          []byte
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	requests     chan *request
	priority     chan *request
	priorityMode int
	onEvent      func(Event)
	events       chan Event
	mu           sync.Mutex
	view         View
	fence        uint64
	active       context.CancelCauseFunc
	connection   *generation
	sequence     uint64        // owner goroutine only
	everReady    bool          // Owner only; a previous usable subscription existed.
	players      map[ID]uint64 // Only explicitly observed players; bounded, mutex protected.
	writes       map[ID]playerWrite

	// Received progress samples never participate in the control token.
	progressSequence atomic.Uint64
}

type playerWrite struct {
	revision uint64
	mutation Mutation
}

func New(parent context.Context, cfg Config) (*Client, error) {
	if _, port, err := net.SplitHostPort(cfg.Address); err != nil || port == "" {
		return nil, errors.New("HEOS address must include a port")
	}
	pin, err := hex.DecodeString(strings.NewReplacer(":", "", " ", "").Replace(strings.TrimSpace(cfg.Fingerprint)))
	if err != nil || len(pin) != sha256.Size {
		return nil, errors.New("HEOS SHA-256 certificate fingerprint required")
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.CommandTimeout == 0 {
		cfg.CommandTimeout = 20 * time.Second
	}
	if cfg.ReconnectDelay == 0 {
		cfg.ReconnectDelay = time.Second
	}
	if cfg.QueueDepth == 0 {
		cfg.QueueDepth = 16
	}
	if cfg.EventBuffer == 0 {
		cfg.EventBuffer = 32
	}
	if cfg.FrameBytes == 0 {
		cfg.FrameBytes = DefaultFrameBytes
	}
	if cfg.ConnectTimeout <= 0 || cfg.ConnectTimeout > 30*time.Second || cfg.CommandTimeout <= 0 || cfg.CommandTimeout > 30*time.Second ||
		cfg.ReconnectDelay <= 0 || cfg.ReconnectDelay > time.Minute || cfg.QueueDepth < 1 || cfg.QueueDepth > 64 || cfg.EventBuffer < 1 || cfg.EventBuffer > 256 || cfg.FrameBytes < 1024 || cfg.FrameBytes > 1<<20 {
		return nil, errors.New("HEOS limits out of bounds")
	}
	ctx, cancel := context.WithCancel(parent)
	c := &Client{cfg: cfg, pin: pin, ctx: ctx, cancel: cancel, done: make(chan struct{}), requests: make(chan *request, cfg.QueueDepth), priority: make(chan *request, 4), events: make(chan Event, cfg.EventBuffer), players: make(map[ID]uint64), writes: make(map[ID]playerWrite)}
	go c.run()
	return c, nil
}

func (c *Client) Close()               { c.cancel(); <-c.done }
func (c *Client) Events() <-chan Event { return c.events }
func (c *Client) View() View           { c.mu.Lock(); defer c.mu.Unlock(); return c.view }

// PlayerView combines global invalidation with one observed player's revision.
func (c *Client) PlayerView(pid ID) View {
	c.mu.Lock()
	defer c.mu.Unlock()
	view := c.view
	view.Token.Player = c.players[pid]
	view.Token.Write = c.writes[pid].revision
	return view
}

func (c *Client) observePlayer(pid ID) View {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.players[pid]; !exists {
		if len(c.players) == 64 {
			// Eviction must invalidate every snapshot that used the old map.
			clear(c.players)
			clear(c.writes)
			c.view.Token.Revision++
		}
		c.players[pid] = 0
	}
	view := c.view
	view.Token.Player = c.players[pid]
	view.Token.Write = c.writes[pid].revision
	return view
}

// Interrupt cancels current I/O and invalidates already-queued work. The future
// priority-stop coordinator can then submit its own newly authorized work.
func (c *Client) Interrupt() {
	c.mu.Lock()
	c.fence++
	if c.active != nil {
		c.active(ErrInterrupted)
	}
	g := c.connection
	c.view.Connected = false
	c.view.Token.Revision++
	c.view.Token.Catalog++
	c.mu.Unlock()
	if g != nil {
		g.close()
	}
}

func readCommand(name string) bool {
	switch name {
	case "system/heart_beat", "player/get_players", "player/get_player_info", "player/get_play_state", "player/get_volume", "player/get_mute",
		"player/get_play_mode", "player/get_now_playing_media", "player/get_queue", "group/get_groups", "group/get_group_info",
		"browse/get_music_sources", "browse/get_source_info", "browse/browse":
		return true
	}
	return false
}

func (c *Client) Read(ctx context.Context, name string, args url.Values) (Response, error) {
	if !readCommand(name) {
		return Response{}, &CommandError{NotSent, ErrReadOnly}
	}
	return c.submit(ctx, name, args, nil, Mutation{})
}

func (c *Client) submit(ctx context.Context, name string, args url.Values, guard *Guard, mutation Mutation) (Response, error) {
	if _, err := encodeCommand(name, args); err != nil {
		return Response{}, &CommandError{NotSent, err}
	}
	if err := ctx.Err(); err != nil {
		return Response{}, &CommandError{NotSent, err}
	}
	copyArgs := url.Values{}
	for key, values := range args {
		copyArgs[key] = append([]string(nil), values...)
	}
	c.mu.Lock()
	fence := c.fence
	priority, _ := ctx.Value(priorityKey{}).(bool)
	blocked := c.priorityMode > 0 && !priority
	c.mu.Unlock()
	if blocked {
		return Response{}, &CommandError{NotSent, ErrQueueFull}
	}
	r := &request{ctx: ctx, command: name, args: copyArgs, fence: fence, guard: guard, mutation: mutation, priority: priority, result: make(chan result, 1)}
	// This mutex-free bounded mailbox never creates a goroutine per caller.
	select {
	case <-c.done:
		return Response{}, &CommandError{NotSent, ErrClosed}
	default:
	}
	queue := c.requests
	if priority {
		queue = c.priority
	}
	select {
	case queue <- r:
	default:
		return Response{}, &CommandError{NotSent, ErrQueueFull}
	}
	select {
	case out := <-r.result:
		return out.response, out.err
	case <-ctx.Done():
		if r.state.CompareAndSwap(0, 2) {
			return Response{}, &CommandError{NotSent, ctx.Err()}
		}
		select {
		case out := <-r.result:
			return out.response, out.err
		case <-c.done:
			return Response{}, &CommandError{Uncertain, ErrClosed}
		}
	case <-c.done:
		select {
		case out := <-r.result:
			return out.response, out.err
		default:
		}
		delivery := NotSent
		if r.state.Load() == 1 {
			delivery = Uncertain
		}
		return Response{}, &CommandError{delivery, ErrClosed}
	}
}

func (c *Client) dial(ctx context.Context) (*generation, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.ConnectTimeout)
	defer cancel()
	host, _, _ := net.SplitHostPort(c.cfg.Address)
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host,
		// The explicit certificate pin replaces public-CA/hostname trust.
		InsecureSkipVerify: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("HEOS certificate missing")
			}
			actual := sha256.Sum256(state.PeerCertificates[0].Raw)
			if subtle.ConstantTimeCompare(actual[:], c.pin) != 1 {
				return ErrPinMismatch
			}
			return nil
		}}
	// One connect deadline bounds both phases. Splitting them preserves the
	// existing pinned trust while distinguishing TCP failure from TLS failure.
	var dialer net.Dialer
	raw, err := dialer.DialContext(ctx, "tcp", c.cfg.Address)
	if err != nil {
		return nil, &connectionError{phase: ErrConnect, cause: err}
	}
	conn := tls.Client(raw, tlsConfig)
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, &connectionError{phase: ErrTLS, cause: err}
	}
	g := &generation{conn: conn, frames: make(chan incoming, 1), stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(g.done)
		r := bufio.NewReaderSize(conn, 4096)
		for {
			raw, err := readFrame(r, c.cfg.FrameBytes)
			var response Response
			if err == nil {
				response, err = decodeResponse(raw)
			}
			c.traceFrame(response, err)
			select {
			case g.frames <- incoming{response, err}:
			case <-g.stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	c.mu.Lock()
	c.connection = g
	c.view.Connected = true
	c.view.Token.Generation++
	c.view.Token.Revision++
	c.view.Token.Catalog++
	c.mu.Unlock()
	return g, nil
}

func (c *Client) drop(g *generation) {
	if g == nil {
		return
	}
	g.close()
	<-g.done
	c.mu.Lock()
	if c.connection == g {
		c.connection = nil
		c.view.Connected = false
		c.view.Token.Revision++
		c.view.Token.Catalog++
	}
	token := c.view.Token
	c.mu.Unlock()
	c.publish(Event{Token: token, Gap: true, GapReason: "connection_closed"})
}

func (c *Client) publish(event Event) {
	if event.Gap {
		c.recordGap(event.GapReason)
	}
	c.mu.Lock()
	handler := c.onEvent
	c.mu.Unlock()
	if handler != nil {
		handler(event)
	}
	select {
	case c.events <- event:
		return
	default:
	}
	// A gap replaces the bounded backlog. Even a consumer that misses events
	// observes changed revision/catalog tokens and must refresh its snapshots.
drain:
	for {
		select {
		case <-c.events:
			continue
		default:
			break drain
		}
	}
	c.mu.Lock()
	c.view.Token.Revision++
	c.view.Token.Catalog++
	token := c.view.Token
	c.mu.Unlock()
	gap := Event{Token: token, Gap: true, GapReason: "event_buffer_overflow"}
	c.recordGap(gap.GapReason)
	if handler != nil {
		handler(gap)
	}
	c.events <- gap
}

func (c *Client) event(r Response) {
	event := (Event{Command: r.Command, Params: r.Params}).Decode()
	data := event.Data()
	// Denon 5.6 progress is best-effort telemetry, not a control revision.
	// Dropping only progress when full must not erase queued control events.
	if data.Kind == EventProgress && data.Valid {
		c.mu.Lock()
		event.progressSequence = c.progressSequence.Add(1)
		event.Token = c.view.Token
		event.Token.Player = c.players[data.Player]
		event.Token.Write = c.writes[data.Player].revision
		c.mu.Unlock()
		select {
		case c.events <- event:
		default:
		}
		return
	}
	c.mu.Lock()
	pid := data.Player
	playerEvent := false
	switch data.Kind {
	case EventState, EventNowPlaying, EventPlaybackError, EventQueue, EventVolume, EventRepeat, EventShuffle:
		playerEvent = pid != ""
	}
	if playerEvent {
		if _, observed := c.players[pid]; observed {
			c.players[pid]++
		}
	} else {
		c.view.Token.Revision++
	}
	if data.Kind == EventSources || data.Kind == EventPlayers || data.Kind == EventUser {
		c.view.Token.Catalog++
	}
	token := c.view.Token
	token.Player = c.players[pid]
	token.Write = c.writes[pid].revision
	c.mu.Unlock()
	event.Token = token
	c.publish(event)
}

func (c *Client) exchange(ctx context.Context, g *generation, name string, args url.Values) (out Response, err error) {
	began := time.Now()
	sent := 0
	// Registered first: observe the final cancellation/delivery classification.
	defer func() { c.recordWire(name, sent, err, time.Since(began)) }()
	if c.cfg.Logger != nil && c.cfg.Logger.Enabled(ctx, slog.LevelDebug) {
		c.cfg.Logger.Debug("HEOS request", "command", diagnosticCommand(name), "params", diagnosticParams(args), "generation", c.View().Token.Generation)
		defer func() {
			delivery := "confirmed"
			var command *CommandError
			if errors.As(err, &command) {
				delivery = string(command.Delivery)
			}
			c.cfg.Logger.Debug("HEOS command completed", "command", diagnosticCommand(name), "elapsed_ms", time.Since(began).Milliseconds(), "delivery", delivery, "error_kind", diagnosticError(err), diagnosticDeviceError(err))
		}()
	}
	c.sequence++
	if strings.HasPrefix(name, "browse/") {
		args.Set("SEQUENCE", strconv.FormatUint(c.sequence, 10))
	}
	wire, err := encodeCommand(name, args)
	if err != nil {
		return out, &CommandError{NotSent, err}
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { g.close(); close(callbackDone) })
	defer func() {
		if !stop() {
			<-callbackDone
		}
		if ctx.Err() != nil {
			delivery := NotSent
			if sent > 0 {
				delivery = Uncertain
			}
			err = &CommandError{delivery, context.Cause(ctx)}
		}
		_ = g.conn.SetWriteDeadline(time.Time{})
	}()
	if ctx.Err() != nil {
		return out, &CommandError{NotSent, context.Cause(ctx)}
	}
	deadline, _ := ctx.Deadline()
	if err := g.conn.SetWriteDeadline(deadline); err != nil {
		return out, &CommandError{NotSent, err}
	}
	sent, err = writeAll(g.conn, wire)
	if err != nil {
		delivery := NotSent
		if sent > 0 {
			delivery = Uncertain
		}
		return out, &CommandError{delivery, err}
	}
	for {
		select {
		case <-ctx.Done():
			return out, &CommandError{Uncertain, context.Cause(ctx)}
		case frame := <-g.frames:
			if frame.err != nil {
				return out, &CommandError{Uncertain, frame.err}
			}
			r := frame.response
			if r.Event {
				c.event(r)
				continue
			}
			if r.Command != name {
				return out, &CommandError{Uncertain, ErrProtocol}
			}
			if err := validateEcho(args, r.Params); err != nil {
				return out, &CommandError{Uncertain, err}
			}
			if r.Pending {
				continue
			}
			r.Token = c.PlayerView(ID(args.Get("pid"))).Token
			if r.Result != "success" {
				return r, rejection(r)
			}
			return r, nil
		}
	}
}

func (c *Client) run() {
	var g *generation
	var nextDial time.Time
	defer func() { c.drop(g); close(c.events); close(c.done) }()
	for {
		if c.ctx.Err() != nil {
			return
		}
		select {
		case r := <-c.priority:
			g, nextDial = c.process(r, g, nextDial)
			continue
		default:
		}
		var frames <-chan incoming
		if g != nil {
			frames = g.frames
		}
		select {
		case <-c.ctx.Done():
			return
		case frame := <-frames:
			if frame.err != nil || !frame.response.Event {
				c.drop(g)
				g = nil
				nextDial = time.Now().Add(c.cfg.ReconnectDelay)
			} else {
				c.event(frame.response)
			}
		case r := <-c.requests:
			g, nextDial = c.process(r, g, nextDial)
		case r := <-c.priority:
			g, nextDial = c.process(r, g, nextDial)
		}
	}
}

func (c *Client) process(r *request, g *generation, nextDial time.Time) (*generation, time.Time) {
	if !r.state.CompareAndSwap(0, 1) {
		r.result <- result{err: &CommandError{NotSent, r.ctx.Err()}}
		return g, nextDial
	}
	c.mu.Lock()
	fence := c.fence
	blocked := c.priorityMode > 0 && !r.priority
	c.mu.Unlock()
	if r.fence != fence || blocked {
		r.result <- result{err: &CommandError{NotSent, ErrInterrupted}}
		return g, nextDial
	}
	ctx, timeout := context.WithTimeout(r.ctx, c.cfg.CommandTimeout)
	ctx, cancel := context.WithCancelCause(ctx)
	stopParent := context.AfterFunc(c.ctx, func() { cancel(ErrClosed) })
	c.mu.Lock()
	c.active = cancel
	if r.fence != c.fence {
		cancel(ErrInterrupted)
	}
	c.mu.Unlock()
	var out Response
	var err error
	// Process already received events before the last write guard. Bound the
	// drain so a progress flood cannot monopolize the owner or hide shutdown.
	if r.guard != nil && g != nil {
	drain:
		for i := 0; i < 32; i++ {
			select {
			case frame := <-g.frames:
				if frame.err != nil || !frame.response.Event {
					c.drop(g)
					g = nil
					err = &CommandError{NotSent, ErrStale}
					break drain
				}
				c.event(frame.response)
				if i == 31 {
					err = &CommandError{NotSent, ErrStale}
				}
			default:
				break drain
			}
		}
	}
	if r.guard != nil {
		view := c.PlayerView(ID(r.args.Get("pid")))
		if !view.Connected || (view.Token != r.guard.Token || !time.Now().Before(r.guard.ExpiresAt)) {
			err = &CommandError{NotSent, ErrStale}
		}
	}
	if g == nil && err == nil {
		if time.Now().Before(nextDial) {
			err = &CommandError{NotSent, ErrOffline}
		} else {
			g, err = c.dial(ctx)
			if err == nil {
				_, err = c.exchange(ctx, g, "system/register_for_change_events", url.Values{"enable": {"on"}})
				g.subscribed = err == nil
				if g.subscribed {
					if c.everReady && c.cfg.Metrics != nil {
						c.cfg.Metrics.Reconnected()
					}
					c.everReady = true
				}
			}
			if err != nil {
				err = &CommandError{NotSent, err}
			}
		}
	}
	if err == nil && r.guard != nil {
		view := c.PlayerView(ID(r.args.Get("pid")))
		if !view.Connected || (view.Token != r.guard.Token || !time.Now().Before(r.guard.ExpiresAt)) {
			err = &CommandError{NotSent, ErrStale}
		}
	}
	if err == nil {
		if r.guard != nil {
			// A command consumes its guard, but is not a missing HEOS event.
			// Stamp before sending so events preceding the reply carry the same
			// write revision as events that arrive after it (Denon 5.9–5.11).
			c.mu.Lock()
			pid := ID(r.args.Get("pid"))
			c.writes[pid] = playerWrite{revision: c.writes[pid].revision + 1, mutation: r.mutation}
			c.mu.Unlock()
		}
		out, err = c.exchange(ctx, g, r.command, r.args)
	}
	c.mu.Lock()
	c.active = nil
	c.mu.Unlock()
	stopParent()
	cancel(nil)
	timeout()
	if err != nil && !errors.Is(err, ErrStale) && (!errors.Is(err, ErrRejected) || (g != nil && !g.subscribed)) {
		c.drop(g)
		g = nil
		if !errors.Is(err, ErrOffline) {
			nextDial = time.Now().Add(c.cfg.ReconnectDelay)
		}
	}
	r.result <- result{out, err}
	return g, nextDial
}
