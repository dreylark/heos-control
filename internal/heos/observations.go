package heos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"
)

var ErrIdentity = errors.New("HEOS physical identity missing or ambiguous")
var ErrStale = errors.New("HEOS observation changed during refresh")

type Identity struct{ Key, Serial, Model string }
type Player struct {
	ID      ID   `json:"pid"`
	Name    Text `json:"name"`
	Model   Text `json:"model"`
	Serial  Text `json:"serial"`
	IP      Text `json:"ip"`
	GroupID ID   `json:"gid,omitempty"`
}
type GroupMember struct {
	ID   ID     `json:"pid"`
	Role string `json:"role"`
}
type Group struct {
	ID      ID            `json:"gid"`
	Name    Text          `json:"name"`
	Players []GroupMember `json:"players"`
}
type Media struct {
	Source  ID   `json:"sid,omitempty"`
	ID      ID   `json:"mid,omitempty"`
	QueueID ID   `json:"qid,omitempty"`
	Song    Text `json:"song"`
	Album   Text `json:"album"`
	Artist  Text `json:"artist"`
}
type Source struct {
	ID   ID     `json:"sid"`
	Name Text   `json:"name"`
	Type string `json:"type"`
}
type QueuePage struct {
	Items []Media
	Total *int
	Next  *int
	Token Token
}

func payload[T any](r Response) (T, error) {
	var value T
	if len(r.Payload) == 0 || string(r.Payload) == "null" {
		return value, ErrProtocol
	}
	if err := json.Unmarshal(r.Payload, &value); err != nil {
		return value, fmt.Errorf("HEOS payload: %w", ErrProtocol)
	}
	return value, nil
}

func ResolveIdentity(players []Player, identity Identity) (Player, error) {
	if identity.Key == "" || identity.Serial == "" {
		return Player{}, ErrIdentity
	}
	var found Player
	count := 0
	for _, p := range players {
		if string(p.Serial) == identity.Serial {
			found = p
			count++
		}
	}
	if count != 1 || found.ID == "" || (identity.Model != "" && string(found.Model) != identity.Model) {
		return Player{}, ErrIdentity
	}
	count = 0
	for _, p := range players {
		if p.ID == found.ID {
			count++
		}
	}
	if count != 1 {
		return Player{}, ErrIdentity
	}
	return found, nil
}

func (c *Client) Sources(ctx context.Context) ([]Source, error) {
	r, err := c.Read(ctx, "browse/get_music_sources", nil)
	if err != nil {
		return nil, err
	}
	return payload[[]Source](r)
}

func (c *Client) Queue(ctx context.Context, pid ID, start, limit int) (QueuePage, error) {
	if pid == "" || start < 0 || start > MaxBrowseItems || limit < 1 || limit > 100 {
		return QueuePage{}, ErrBounds
	}
	r, err := c.Read(ctx, "player/get_queue", url.Values{"pid": {string(pid)}, "range": {fmt.Sprintf("%d,%d", start, start+limit-1)}})
	if err != nil {
		return QueuePage{}, err
	}
	items, err := payload[[]Media](r)
	if err != nil {
		return QueuePage{}, err
	}
	for _, item := range items {
		if item.QueueID == "" {
			return QueuePage{}, ErrProtocol
		}
	}
	total, next, err := pageBounds(r, start, limit, len(items))
	return QueuePage{Items: items, Total: total, Next: next, Token: r.Token}, err
}

type Snapshot struct {
	Repeat     string
	Shuffle    bool
	Key        string
	Player     Player
	Groups     []Group
	Grouped    bool
	State      string
	Volume     *int
	Muted      *bool
	Media      *Media
	Queue      QueuePage
	ObservedAt time.Time
	Token      Token
	Stale      bool
	Connected  bool
	Verified   bool
	// MediaStale retains display data that needs a native media observation
	// associated with Play/Pause. It does not affect control verification.
	MediaStale bool
	// MediaUnavailable prevents display data from a previous connection baseline
	// being reused before the current connection establishes physical identity.
	MediaUnavailable bool
	// EventUpdated distinguishes a continuously maintained projection or targeted
	// scalar/metadata read from a complete identity, group and queue audit.
	EventUpdated bool
	// Playhead is the last accepted §5.6 sample bound to one media identity.
	// It is display data: progress events do not renew ObservedAt or the control token.
	Playhead *Playhead
}

// Playhead is a last-sampled now-playing position. DurationMS is zero when the
// device reports an unknown duration. Source, Media and Queue bind the sample
// to the media identity that was current when it was accepted.
type Playhead struct {
	PositionMS int64
	DurationMS int64
	At         time.Time
	Source     ID
	Media      ID
	Queue      ID
}

// Observer starts with a complete same-generation, event-stable read, then applies
// contiguous events to its projection. EventUpdated distinguishes these updates
// from full readback. Outages retain stale values; unknown is never inferred stop.
type Observer struct {
	client       *Client
	identity     Identity
	ttl          time.Duration
	now          func() time.Time
	gate         chan struct{}
	mu           sync.Mutex
	last         Snapshot
	invalid      bool
	mediaPending bool
	writePending observedFields
	// Samples received before a new baseline/media identity cannot be rebound to it.
	progressAfter uint64
	wake          chan struct{}
	changed       chan struct{}
	newTimer      func(time.Duration) observationTimer
}

func NewObserver(client *Client, identity Identity, ttl time.Duration) (*Observer, error) {
	if client == nil || identity.Key == "" || identity.Serial == "" || ttl <= 0 || ttl > 10*time.Minute {
		return nil, ErrIdentity
	}
	return &Observer{client: client, identity: identity, ttl: ttl, now: time.Now, gate: make(chan struct{}, 1), invalid: true,
		wake: make(chan struct{}, 1), changed: make(chan struct{}), newTimer: newObservationTimer,
		last: Snapshot{Key: identity.Key, State: "unknown", Stale: true}}, nil
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
func cloneSnapshot(s Snapshot) Snapshot {
	s.Volume = clonePtr(s.Volume)
	s.Muted = clonePtr(s.Muted)
	s.Media = clonePtr(s.Media)
	s.Playhead = clonePtr(s.Playhead)
	s.Groups = append([]Group(nil), s.Groups...)
	for i := range s.Groups {
		s.Groups[i].Players = append([]GroupMember(nil), s.Groups[i].Players...)
	}
	s.Queue.Items = append([]Media(nil), s.Queue.Items...)
	s.Queue.Total = clonePtr(s.Queue.Total)
	s.Queue.Next = clonePtr(s.Queue.Next)
	return s
}

func (o *Observer) Snapshot() Snapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	s := cloneSnapshot(o.last)
	view := o.client.PlayerView(s.Player.ID)
	age := o.now().Sub(s.ObservedAt)
	s.Connected = view.Connected
	s.MediaUnavailable = view.Token.Generation != s.Token.Generation
	s.Stale = o.invalid || o.mediaPending || o.writePending != 0 || !view.Connected || view.Token != s.Token || age < 0 || age >= o.ttl
	s.Verified = !s.Stale && string(s.Player.Serial) == o.identity.Serial
	return s
}

func (o *Observer) Refresh(ctx context.Context) error {
	err := o.refresh(ctx, true)
	if err != nil {
		// Foreground failures may invalidate state without a transport event
		// (for example, incomplete identity discovery). Recover promptly.
		o.requestRefresh()
	}
	return err
}

func (o *Observer) refresh(ctx context.Context, force bool) error {
	ctx, cancel := context.WithTimeout(ctx, o.client.cfg.CommandTimeout)
	defer cancel()
	select {
	case o.gate <- struct{}{}:
		defer func() { <-o.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	// A foreground read may already have reconciled the event while we waited.
	// Explicit Refresh calls always read the device, including before writes.
	if !force {
		cached := o.Snapshot()
		if !cached.Stale && o.now().Sub(cached.ObservedAt) < IdleObservationInterval {
			return nil
		}
		o.mu.Lock()
		s, mediaOnly, invalid, writePending := o.last, o.mediaPending && !o.invalid, o.invalid, o.writePending
		o.mu.Unlock()
		view, age := o.client.PlayerView(s.Player.ID), o.now().Sub(s.ObservedAt)
		if !invalid && view.Connected && sameGlobal(view.Token, s.Token) && age >= 0 && age < o.ttl &&
			(view.Token != s.Token || writePending != 0) {
			return ErrStale // The event consumer or pending setter has not caught up.
		}
		if mediaOnly && view.Connected && view.Token == s.Token && age >= 0 && age < min(o.ttl, IdleObservationInterval) {
			return o.refreshMedia(ctx, s)
		}
	}
	o.mu.Lock()
	o.invalid = true
	o.signalChanged()
	o.mu.Unlock()
	o.recordObservation(ctx, "full")
	r, err := o.client.Read(ctx, "player/get_players", nil)
	if err != nil {
		return err
	}
	players, err := payload[[]Player](r)
	if err != nil {
		return err
	}
	player, err := ResolveIdentity(players, o.identity)
	if err != nil {
		return err
	}
	view := o.client.observePlayer(player.ID)
	if !view.Connected || !sameGlobal(view.Token, r.Token) {
		return ErrStale
	}
	s := Snapshot{Key: o.identity.Key, Player: player, Token: view.Token, State: "unknown"}
	read := func(name string, args url.Values) (Response, error) {
		r, err := o.client.Read(ctx, name, args)
		if err == nil && (!sameGlobal(r.Token, s.Token) || o.client.PlayerView(player.ID).Token != s.Token) {
			err = ErrStale
		}
		if err != nil {
			err = fmt.Errorf("%s: %w", name, err)
		}
		return r, err
	}
	r, err = read("group/get_groups", nil)
	if err != nil {
		return err
	}
	s.Groups, err = payload[[]Group](r)
	if err != nil {
		return err
	}
	s.Grouped = player.GroupID != ""
	for _, group := range s.Groups {
		if group.ID == "" {
			return ErrProtocol
		}
		for _, member := range group.Players {
			if member.ID == "" {
				return ErrProtocol
			}
			if member.ID == player.ID {
				s.Grouped = true
			}
		}
	}
	args := url.Values{"pid": {string(player.ID)}}
	r, err = read("player/get_play_state", args)
	if err != nil {
		return err
	}
	s.State = r.Params.Get("state")
	switch s.State {
	case "play", "pause", "stop", "unknown":
	default:
		return ErrProtocol
	}
	r, err = read("player/get_volume", args)
	if err != nil {
		return err
	}
	volume, err := strconv.Atoi(r.Params.Get("level"))
	if err != nil || volume < 0 || volume > 100 {
		return ErrProtocol
	}
	s.Volume = &volume
	r, err = read("player/get_mute", args)
	if err != nil {
		return err
	}
	mute := r.Params.Get("state")
	if mute != "on" && mute != "off" {
		return ErrProtocol
	}
	muted := mute == "on"
	s.Muted = &muted
	r, err = read("player/get_play_mode", args)
	if err != nil {
		return err
	}
	s.Repeat = r.Params.Get("repeat")
	if s.Repeat != "off" && s.Repeat != "on_all" && s.Repeat != "on_one" {
		return ErrProtocol
	}
	shuffle := r.Params.Get("shuffle")
	if shuffle != "on" && shuffle != "off" {
		return ErrProtocol
	}
	s.Shuffle = shuffle == "on"

	r, err = read("player/get_now_playing_media", args)
	if err != nil {
		return err
	}
	media, err := payload[Media](r)
	if err != nil {
		return err
	}
	s.Media = &media
	s.MediaStale = s.State != "play" && s.State != "pause"
	s.Queue, err = o.client.Queue(ctx, player.ID, 0, 100)
	if err != nil {
		return fmt.Errorf("player/get_queue: %w", err)
	}
	s.ObservedAt = o.now()
	s.Connected = true
	s.Verified = true
	o.mu.Lock()
	defer o.mu.Unlock()
	view = o.client.PlayerView(player.ID)
	if s.Queue.Token != s.Token || view.Token != s.Token || !view.Connected {
		return ErrStale
	}
	o.progressAfter = o.client.progressSequence.Load()
	o.last = s
	o.invalid = false
	o.mediaPending = false
	o.writePending = 0
	o.signalChanged()
	return nil
}
