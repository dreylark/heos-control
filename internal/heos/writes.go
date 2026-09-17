package heos

import (
	"context"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Mutation is a closed set of explicit HEOS commands. Policy/physical identity
// are checked by the coordinator; the owner rechecks the observation token
// immediately before any bytes are sent. There is no automatic write replay.
type Mutation struct {
	Kind    string
	Player  ID
	Level   int
	Muted   bool
	State   string
	Repeat  string
	Shuffle bool
	Item    Item
}

func (m Mutation) command() (string, url.Values, error) {
	if m.Player == "" {
		return "", nil, ErrBounds
	}
	a := url.Values{"pid": {string(m.Player)}}
	switch m.Kind {
	case "volume":
		if m.Level < 0 || m.Level > 100 {
			return "", nil, ErrBounds
		}
		a.Set("level", strconv.Itoa(m.Level))
		return "player/set_volume", a, nil
	case "mute":
		state := "off"
		if m.Muted {
			state = "on"
		}
		a.Set("state", state)
		return "player/set_mute", a, nil
	case "transport":
		if m.State != "play" && m.State != "pause" && m.State != "stop" {
			return "", nil, ErrBounds
		}
		a.Set("state", m.State)
		return "player/set_play_state", a, nil
	case "mode":
		if m.Repeat != "off" && m.Repeat != "on_all" && m.Repeat != "on_one" {
			return "", nil, ErrBounds
		}
		a.Set("repeat", m.Repeat)
		shuffle := "off"
		if m.Shuffle {
			shuffle = "on"
		}
		a.Set("shuffle", shuffle)
		return "player/set_play_mode", a, nil
	case "queue":
		i := m.Item
		if i.Playable != "yes" || i.Source == "" || i.ContainerID == "" {
			return "", nil, ErrBounds
		}
		a.Set("sid", string(i.Source))
		a.Set("cid", string(i.ContainerID))
		a.Set("aid", "4")
		if i.Container != "yes" {
			if i.MediaID == "" || (i.Type != "song" && i.Type != "track") {
				return "", nil, ErrBounds
			}
			a.Set("mid", string(i.MediaID))
		}
		return "browse/add_to_queue", a, nil
	default:
		return "", nil, ErrReadOnly
	}
}

type Guard struct {
	Token     Token
	ExpiresAt time.Time
}

func (c *Client) Write(ctx context.Context, m Mutation, guard Guard) (Response, error) {
	if !c.cfg.EnableWrites {
		return Response{}, &CommandError{NotSent, ErrReadOnly}
	}
	if !time.Now().Before(guard.ExpiresAt) || guard.ExpiresAt.After(time.Now().Add(5*time.Second)) {
		return Response{}, &CommandError{NotSent, ErrStale}
	}
	name, args, e := m.command()
	if e != nil {
		return Response{}, &CommandError{NotSent, e}
	}
	c.mu.Lock()
	_, observed := c.players[m.Player]
	c.mu.Unlock()
	if !observed {
		return Response{}, &CommandError{NotSent, ErrStale}
	}
	return c.submit(ctx, name, args, &guard, m)
}

type priorityKey struct{}

func Priority(ctx context.Context) context.Context {
	return context.WithValue(ctx, priorityKey{}, true)
}

// BeginPriority fences existing I/O and temporarily rejects ordinary requests.
// Its caller must release the bounded exclusive path on every exit.
func (c *Client) BeginPriority() func() {
	c.mu.Lock()
	c.priorityMode++
	c.mu.Unlock()
	c.Interrupt()
	return sync.OnceFunc(func() { c.mu.Lock(); c.priorityMode--; c.mu.Unlock() })
}

// SetEventHandler installs a synchronous, nonblocking owner notification.
// The handler must not perform device I/O. It may cancel its operation context.
func (c *Client) SetEventHandler(handler func(Event)) {
	c.mu.Lock()
	c.onEvent = handler
	c.mu.Unlock()
}
