package heos

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const MaxBrowsePages = 100
const MaxBrowseItems = 10000

var ErrBounds = errors.New("HEOS traversal limit exceeded")
var ErrStaleReference = errors.New("catalog reference expired or invalid")
var ErrCatalogFull = errors.New("catalog reference cache full")
var ErrIncompleteSource = errors.New("HEOS unpaged source response may be incomplete")

type Item struct {
	Name        Text   `json:"name"`
	Source      ID     `json:"sid,omitempty"`
	ContainerID ID     `json:"cid,omitempty"`
	MediaID     ID     `json:"mid,omitempty"`
	Container   string `json:"container"`
	Playable    string `json:"playable"`
	Type        string `json:"type"`
}

func (i Item) isSource() bool {
	if i.Source == "" || i.ContainerID != "" {
		return false
	}
	switch i.Type {
	case "heos_server", "heos_service", "dlna_server", "music_service":
		return true
	}
	return false
}

type BrowsePage struct {
	Items   []Item
	Total   *int
	Next    *int
	Token   Token
	unpaged []Item // Immutable bounded source snapshot for local cursors.
}

func pageBounds(r Response, start, limit, count int) (total, next *int, err error) {
	if count > limit {
		return nil, nil, ErrProtocol
	}
	if value := r.Params.Get("returned"); value != "" {
		n, e := strconv.Atoi(value)
		if e != nil || n != count {
			return nil, nil, ErrProtocol
		}
	}
	end := start + count
	value, hasCount := r.Params["count"]
	if hasCount {
		n, e := strconv.Atoi(value[0])
		if e != nil || n < 0 {
			return nil, nil, ErrProtocol
		}
		if n > 0 {
			total = &n
			if end > n {
				return nil, nil, ErrProtocol
			}
		}
	}
	if count == 0 {
		if total != nil && start < *total {
			return nil, nil, ErrProtocol
		}
		return total, nil, nil
	}
	if total != nil && end >= *total {
		return total, nil, nil
	}
	if !hasCount && count < limit {
		return nil, nil, nil
	}
	return total, &end, nil
}

func (c *Client) BrowsePage(ctx context.Context, sid, cid ID, start, limit int) (BrowsePage, error) {
	if sid == "" || start < 0 || start > MaxBrowseItems || limit < 1 || limit > 100 {
		return BrowsePage{}, ErrBounds
	}
	// Denon 4.4.3: only Favorites supports source-level ranges. Containers
	// use the distinct rules in 4.4.4; never simulate progress with ignored ranges.
	paged := cid != "" || sid == "1028"
	args := url.Values{"sid": {string(sid)}}
	if paged {
		args.Set("range", fmt.Sprintf("%d,%d", start, start+limit-1))
	}
	if cid != "" {
		args.Set("cid", string(cid))
	}
	r, err := c.Read(ctx, "browse/browse", args)
	if err != nil {
		return BrowsePage{}, err
	}
	items, err := payload[[]Item](r)
	if err != nil {
		return BrowsePage{}, err
	}
	if !paged {
		total, _, err := pageBounds(r, 0, 100, len(items))
		if err != nil {
			return BrowsePage{}, err
		}
		if (total != nil && *total != len(items)) || (total == nil && len(items) >= 50) {
			return BrowsePage{}, ErrIncompleteSource
		}
		return sourcePage(items, total, r.Token, start, limit)
	}
	total, next, err := pageBounds(r, start, limit, len(items))
	return BrowsePage{Items: items, Total: total, Next: next, Token: r.Token}, err
}

func sourcePage(items []Item, total *int, token Token, start, limit int) (BrowsePage, error) {
	if start > len(items) {
		return BrowsePage{}, ErrBounds
	}
	end := min(start+limit, len(items))
	page := BrowsePage{Items: append([]Item(nil), items[start:end]...), Total: clonePtr(total), Token: token, unpaged: items}
	if end < len(items) {
		page.Next = &end
	}
	return page, nil
}

func catalogSame(a, b Token) bool { return a.Generation == b.Generation && a.Catalog == b.Catalog }

func (c *Client) BrowseAll(ctx context.Context, sid, cid ID) ([]Item, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.CommandTimeout)
	defer cancel()
	items := []Item{}
	start := 0
	var token Token
	for i := 0; i < MaxBrowsePages; i++ {
		page, err := c.BrowsePage(ctx, sid, cid, start, 100)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			token = page.Token
		} else if !catalogSame(token, page.Token) {
			return nil, ErrStaleReference
		}
		if len(items)+len(page.Items) > MaxBrowseItems {
			return nil, ErrBounds
		}
		items = append(items, page.Items...)
		if page.Next == nil {
			return items, nil
		}
		start = *page.Next
	}
	return nil, ErrBounds
}

type CatalogItem struct {
	Name      string
	Ref       string
	Container bool
	Playable  bool
	Kind      string // source, container, or media
	Browsable bool
}
type CatalogPage struct {
	Items []CatalogItem
	Total *int
	Next  string
}
type reference struct {
	source       ID
	target       ID // Actual HEOS source, scoped inside the logical root source.
	item         Item
	cursor       bool
	start, limit int
	token        Token
	expires      time.Time
	snapshot     []Item
	total        *int
}
type Catalog struct {
	client   *Client
	capacity int
	ttl      time.Duration
	now      func() time.Time
	mu       sync.Mutex
	refs     map[string]reference
}

func NewCatalog(client *Client, capacity int, ttl time.Duration) (*Catalog, error) {
	if client == nil || capacity < 1 || capacity > 4096 || ttl <= 0 || ttl > 5*time.Minute {
		return nil, ErrBounds
	}
	return &Catalog{client: client, capacity: capacity, ttl: ttl, now: time.Now, refs: map[string]reference{}}, nil
}
func (c *Catalog) valid(ref reference, source ID, view View) bool {
	return ref.source == source && view.Connected && catalogSame(ref.token, view.Token) && c.now().Before(ref.expires)
}

func (c *Catalog) Resolve(source ID, ref string) (Item, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.refs[ref]
	if !ok || r.cursor || !c.valid(r, source, c.client.View()) {
		return Item{}, ErrStaleReference
	}
	return r.item, nil
}

func (c *Catalog) Browse(ctx context.Context, source ID, parent, cursor string, limit int) (CatalogPage, error) {
	if source == "" || limit < 1 || limit > 100 {
		return CatalogPage{}, ErrBounds
	}
	var cid ID
	target := source
	start := 0
	var cached reference
	c.mu.Lock()
	view := c.client.View()
	if parent != "" {
		ref, ok := c.refs[parent]
		if !ok || ref.cursor || !c.valid(ref, source, view) {
			c.mu.Unlock()
			return CatalogPage{}, ErrStaleReference
		}
		if ref.item.isSource() {
			target = ref.item.Source
		} else if ref.item.Container == "yes" && ref.item.ContainerID != "" {
			target, cid = ref.target, ref.item.ContainerID
		} else {
			c.mu.Unlock()
			return CatalogPage{}, ErrStaleReference
		}
	}
	if cursor != "" {
		ref, ok := c.refs[cursor]
		if !ok || !ref.cursor || ref.target != target || ref.item.ContainerID != cid || ref.limit != limit || !c.valid(ref, source, view) {
			c.mu.Unlock()
			return CatalogPage{}, ErrStaleReference
		}
		start = ref.start
		cached = ref
	}
	c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return CatalogPage{}, err
	}
	var page BrowsePage
	var err error
	if cached.snapshot != nil {
		page, err = sourcePage(cached.snapshot, cached.total, cached.token, start, limit)
	} else {
		page, err = c.client.BrowsePage(ctx, target, cid, start, limit)
	}
	if err != nil {
		return CatalogPage{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.client.View()
	if !current.Connected || !catalogSame(page.Token, current.Token) || ((parent != "" || cursor != "") && !catalogSame(view.Token, page.Token)) {
		return CatalogPage{}, ErrStaleReference
	}
	for _, key := range []string{parent, cursor} {
		if key != "" && !c.valid(c.refs[key], source, current) {
			return CatalogPage{}, ErrStaleReference
		}
	}
	for key, ref := range c.refs {
		if !current.Connected || !catalogSame(ref.token, current.Token) || !c.now().Before(ref.expires) {
			delete(c.refs, key)
		}
	}
	needed := len(page.Items)
	if page.Next != nil {
		needed += 1 + len(page.unpaged)
	}
	used := len(c.refs)
	for _, ref := range c.refs {
		used += len(ref.snapshot)
	}
	if used+needed > c.capacity {
		return CatalogPage{}, ErrCatalogFull
	}
	result := CatalogPage{Items: []CatalogItem{}, Total: clonePtr(page.Total)}
	expires := c.now().Add(c.ttl)
	if cached.snapshot != nil && cached.expires.Before(expires) {
		expires = cached.expires // Reading a cached page cannot refresh its evidence.
	}
	for _, item := range page.Items {
		kind := "media"
		if item.isSource() {
			kind = "source"
		} else if item.Container == "yes" {
			kind = "container"
		}
		if item.Source == "" {
			item.Source = target
		}
		if kind == "media" && item.ContainerID == "" {
			item.ContainerID = cid // Denon 4.4.12 needs the containing cid for tracks.
		}
		ref := "item_" + rand.Text()
		c.refs[ref] = reference{source: source, target: target, item: item, token: page.Token, expires: expires}
		result.Items = append(result.Items, CatalogItem{Name: string(item.Name), Ref: ref, Container: kind == "container", Playable: item.Playable == "yes", Kind: kind, Browsable: kind != "media"})
	}
	if page.Next != nil {
		result.Next = "cursor_" + rand.Text()
		c.refs[result.Next] = reference{source: source, target: target, item: Item{ContainerID: cid}, cursor: true, start: *page.Next, limit: limit, token: page.Token, expires: expires, snapshot: page.unpaged, total: clonePtr(page.Total)}
	}
	return result, nil
}
