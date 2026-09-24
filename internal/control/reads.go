// Package control implements device policy independently of HTTP and wire DTOs.
package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/telemetry"
)

var ErrNotFound = errors.New("resource not found")
var ErrAmbiguous = errors.New("source or container is ambiguous")
var ErrUnavailable = errors.New("device state unavailable")

type Observation interface {
	Snapshot() heos.Snapshot
	Refresh(context.Context) error
}
type Reader interface {
	BrowseAll(context.Context, heos.ID, heos.ID) ([]heos.Item, error)
	Queue(context.Context, heos.ID, int, int) (heos.QueuePage, error)
	PlayerView(heos.ID) heos.View
	View() heos.View
}
type Browser interface {
	Browse(context.Context, heos.ID, string, string, int) (heos.CatalogPage, error)
}
type Device struct {
	Config   config.Player
	Observer Observation
	Client   Reader
	Catalogs map[string]Browser
	Metrics  *telemetry.Player
}
type Reads struct {
	epoch   string
	devices []Device
	sources []config.Source
}

func NewReads(epoch string, devices []Device, sources []config.Source) *Reads {
	return &Reads{epoch, devices, sources}
}
func (s *Reads) Devices() []Device { return append([]Device(nil), s.devices...) }
func (s *Reads) device(key string) (Device, error) {
	for _, d := range s.devices {
		if d.Config.Key == key {
			return d, nil
		}
	}
	return Device{}, ErrNotFound
}

type Volume struct {
	Unit  string `json:"unit"`
	Level *int   `json:"level"`
}
type Capabilities struct {
	Read     string `json:"read"`
	Playback string `json:"playback"`
	Volume   string `json:"volume"`
}
type Player struct {
	Key             string       `json:"key"`
	Availability    string       `json:"availability"`
	ObservedAt      *time.Time   `json:"observed_at"`
	Stale           bool         `json:"stale"`
	Revision        string       `json:"revision"`
	PlaybackState   string       `json:"playback_state"`
	NowPlaying      *NowPlaying  `json:"now_playing"`
	Volume          Volume       `json:"volume"`
	VolumeCeiling   *int         `json:"volume_ceiling"`
	Muted           *bool        `json:"muted"`
	Grouped         *bool        `json:"grouped"`
	Capabilities    Capabilities `json:"capabilities"`
	ActiveOperation *string      `json:"active_operation"`
}

func cloneInt(v *int) *int {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}

func (s *Reads) Player(key string) (Player, error) {
	d, e := s.device(key)
	if e != nil {
		return Player{}, e
	}
	v := d.Observer.Snapshot()
	p := Player{Key: key, Availability: "offline", Stale: v.Stale, PlaybackState: string(heos.PlayStateUnknown), Volume: Volume{Unit: "heos", Level: v.Volume}, VolumeCeiling: cloneInt(d.Config.VolumeCeiling), Muted: v.Muted, Capabilities: Capabilities{"unverified", "unverified", "unverified"}}
	if v.Connected {
		p.Availability = "unknown"
	}
	if v.Verified && !v.Stale {
		p.Availability = "online"
		p.Capabilities.Read = "supported"
		if d.Config.WritesEnabled && d.Config.VolumeCeiling != nil && !v.Grouped {
			p.Capabilities.Playback = "supported"
			p.Capabilities.Volume = "supported"
		}
	}
	if !v.ObservedAt.IsZero() {
		at := v.ObservedAt.UTC()
		p.ObservedAt = &at
		p.PlaybackState = string(v.State)
		g := v.Grouped
		p.Grouped = &g
	}
	p.NowPlaying = s.nowPlaying(d.Config, v)
	// Include freshness and observation identity, not just a socket-local counter.
	b, _ := json.Marshal(struct {
		Epoch                      string
		Token                      heos.Token
		At                         time.Time
		Stale, Connected, Verified bool
		NowPlaying                 *NowPlaying
	}{s.epoch, v.Token, v.ObservedAt, v.Stale, v.Connected, v.Verified, revisionNowPlaying(p.NowPlaying)})
	p.Revision = fmt.Sprintf("%s-%x", s.epoch, sha256.Sum256(b))
	return p, nil
}

type Group struct {
	ID      string   `json:"id"`
	Player  string   `json:"player"`
	Name    string   `json:"name"`
	Members []string `json:"members"`
	Stale   bool     `json:"stale"`
}

// Groups are projected per authorized player; names/raw PIDs of other rooms are not exposed.
func (s *Reads) Groups(allowed func(string) bool) []Group {
	out := []Group{}
	seen := map[string]bool{}
	for _, d := range s.devices {
		if !allowed(d.Config.Key) {
			continue
		}
		v := d.Observer.Snapshot()
		for _, g := range v.Groups {
			member := false
			for _, m := range g.Players {
				if m.ID == v.Player.ID {
					member = true
				}
			}
			if !member {
				continue
			}
			id := d.Config.Key + ":" + string(g.ID)
			if seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, Group{ID: id, Player: d.Config.Key, Name: string(g.Name), Members: []string{d.Config.Key}, Stale: v.Stale})
		}
	}
	return out
}

type Source struct {
	Key          string `json:"key"`
	Player       string `json:"player"`
	Name         string `json:"name"`
	Availability string `json:"availability"`
}

func (s *Reads) SourcePlayer(key string) (string, error) {
	for _, v := range s.sources {
		if v.Key == key {
			return v.Player, nil
		}
	}
	return "", ErrNotFound
}
func (s *Reads) source(key string) (config.Source, Device, error) {
	for _, v := range s.sources {
		if v.Key == key {
			d, e := s.device(v.Player)
			return v, d, e
		}
	}
	return config.Source{}, Device{}, ErrNotFound
}
func (s *Reads) resolveSource(ctx context.Context, key string) (config.Source, Device, heos.ID, error) {
	c, d, e := s.source(key)
	if e != nil {
		return c, d, "", e
	}
	if !d.Observer.Snapshot().Verified {
		return c, d, "", ErrUnavailable
	}
	// Denon 4.4.3: browse Local Music (1024) without range to enumerate servers.
	items, e := d.Client.BrowseAll(ctx, "1024", "")
	if e != nil {
		return c, d, "", e
	}
	var id heos.ID
	count := 0
	for _, i := range items {
		if string(i.Name) == c.Name && i.Source != "" && (i.Type == "heos_server" || i.Type == "dlna_server") {
			id = i.Source
			count++
		}
	}
	if count > 1 {
		return c, d, "", ErrAmbiguous
	}
	if count == 0 {
		return c, d, "", ErrNotFound
	}
	// A duplicated physical source ID cannot establish a unique logical root,
	// even when the firmware advertises different display names for it.
	identities := 0
	for _, item := range items {
		if item.Source == id {
			identities++
		}
	}
	if identities != 1 {
		return c, d, "", ErrAmbiguous
	}
	return c, d, id, nil
}
func (s *Reads) Sources(ctx context.Context, allowed func(string) bool) []Source {
	out := []Source{}
	for _, c := range s.sources {
		if !allowed(c.Player) {
			continue
		}
		_, _, _, e := s.resolveSource(ctx, c.Key)
		a := "available"
		if e != nil {
			a = "unavailable"
			if errors.Is(e, ErrAmbiguous) {
				a = "ambiguous"
			}
		}
		out = append(out, Source{c.Key, c.Player, c.Name, a})
	}
	return out
}

type Item struct {
	Name      string `json:"name"`
	Ref       string `json:"item_ref"`
	Kind      string `json:"kind"`
	Browsable bool   `json:"browsable"`
	Playable  bool   `json:"playable"`
}
type Items struct {
	Items []Item  `json:"items"`
	Total *int    `json:"total"`
	Next  *string `json:"next_cursor"`
}

func (s *Reads) Items(ctx context.Context, key, parent, cursor string, limit int) (Items, error) {
	_, d, id, e := s.resolveSource(ctx, key)
	if e != nil {
		return Items{}, e
	}
	browser := d.Catalogs[key]
	if browser == nil {
		return Items{}, ErrUnavailable
	}
	page, e := browser.Browse(ctx, id, parent, cursor, limit)
	if e != nil {
		return Items{}, e
	}
	out := Items{Items: []Item{}, Total: page.Total}
	if page.Next != "" {
		out.Next = &page.Next
	}
	for _, i := range page.Items {
		out.Items = append(out.Items, Item{i.Name, i.Ref, i.Kind, i.Browsable, i.Playable})
	}
	return out, nil
}

type Track struct {
	QueueID string `json:"queue_id"`
	Song    string `json:"song"`
	Album   string `json:"album"`
	Artist  string `json:"artist"`
}
type Queue struct {
	Revision string  `json:"revision"`
	Stale    bool    `json:"stale"`
	Items    []Track `json:"items"`
	Total    *int    `json:"total"`
	Next     *int    `json:"next_offset"`
}

func (s *Reads) Queue(ctx context.Context, key, revision string, start, limit int) (Queue, error) {
	d, e := s.device(key)
	if e != nil {
		return Queue{}, e
	}
	p, _ := s.Player(key)
	v := d.Observer.Snapshot()
	if revision != "" && p.Revision != revision {
		return Queue{}, heos.ErrStaleReference
	}
	if start > 0 && revision == "" {
		return Queue{}, heos.ErrStaleReference
	}
	q := v.Queue
	if !v.Stale {
		q, e = d.Client.Queue(ctx, v.Player.ID, start, limit)
		if e != nil {
			return Queue{}, e
		}
		if q.Token != v.Token || d.Client.PlayerView(v.Player.ID).Token != v.Token {
			return Queue{}, heos.ErrStale
		}
		current, _ := s.Player(key)
		if current.Revision != p.Revision {
			return Queue{}, heos.ErrStale
		}
	} else {
		if start != 0 || revision != "" {
			return Queue{}, heos.ErrStaleReference
		}
		// A bounded cached first page remains inspectable offline.
		if len(q.Items) > limit {
			q.Items = q.Items[:limit]
			n := limit
			q.Next = &n
		}
	}
	out := Queue{Revision: p.Revision, Stale: v.Stale, Items: []Track{}, Total: q.Total, Next: q.Next}
	for _, m := range q.Items {
		out.Items = append(out.Items, Track{string(m.QueueID), string(m.Song), string(m.Album), string(m.Artist)})
	}
	return out, nil
}
