package heos

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Mutation is a closed set of explicit HEOS commands. Policy/physical identity
// are checked by the coordinator; the owner rechecks the observation token
// immediately before any bytes are sent. There is no automatic write replay.
type Mutation struct {
	Kind      MutationKind
	Player    ID
	Level     int
	Muted     bool
	State     PlayState
	Direction string
	Repeat    Repeat
	Shuffle   bool
	Item      Item
	Append    bool // Queue only: append a resolved track or container without restarting playback.
	QueueIDs  []ID // Remove only: exact observed queue occurrences; never a numeric range.
}

func (m Mutation) command() (string, url.Values, error) {
	if m.Player == "" {
		return "", nil, ErrBounds
	}
	a := url.Values{"pid": {string(m.Player)}}
	switch m.Kind {
	case MutationKindVolume:
		if m.Level < 0 || m.Level > 100 {
			return "", nil, ErrBounds
		}
		a.Set("level", strconv.Itoa(m.Level))
		return "player/set_volume", a, nil
	case MutationKindMute:
		state := "off"
		if m.Muted {
			state = "on"
		}
		a.Set("state", state)
		return "player/set_mute", a, nil
	case MutationKindTransport:
		if !m.State.Writable() {
			return "", nil, ErrBounds
		}
		a.Set("state", string(m.State))
		return "player/set_play_state", a, nil
	case MutationKindSkip:
		// HEOS CLI Protocol Specification 1.17, 4.2.21 and 4.2.22. The success
		// reply carries pid only and does not identify the resulting queue entry.
		switch m.Direction {
		case "next":
			return "player/play_next", a, nil
		case "previous":
			return "player/play_previous", a, nil
		default:
			return "", nil, ErrBounds
		}
	case MutationKindMode:
		if !m.Repeat.Known() {
			return "", nil, ErrBounds
		}
		a.Set("repeat", string(m.Repeat))
		shuffle := "off"
		if m.Shuffle {
			shuffle = "on"
		}
		a.Set("shuffle", shuffle)
		return "player/set_play_mode", a, nil
	case MutationKindQueue:
		i := m.Item
		// Denon 4.4.11 queues a container by sid+cid+aid. Browse may report
		// playable=no for a folder the controller app still replaces the queue with.
		if i.Source == "" || i.ContainerID == "" {
			return "", nil, ErrBounds
		}
		if i.Container != "yes" && (i.Playable != "yes" || i.MediaID == "" || (i.Type != "song" && i.Type != "track")) {
			return "", nil, ErrBounds
		}
		a.Set("sid", string(i.Source))
		a.Set("cid", string(i.ContainerID))
		a.Set("aid", "4")
		if m.Append {
			a.Set("aid", "3")
		}
		if i.Container != "yes" {
			a.Set("mid", string(i.MediaID))
		}
		return "browse/add_to_queue", a, nil
	case MutationKindRemove:
		if len(m.QueueIDs) == 0 || len(m.QueueIDs) > 1000 {
			return "", nil, ErrBounds
		}
		ids := make([]string, len(m.QueueIDs))
		for i, id := range m.QueueIDs {
			ids[i] = string(id)
		}
		if err := validateQueueRemoval(ids); err != nil {
			return "", nil, err
		}
		a.Set("qid", strings.Join(ids, ","))
		return "player/remove_from_queue", a, nil
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
