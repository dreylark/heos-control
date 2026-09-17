package heos

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBrowseZeroCountsAndReservedIDs(t *testing.T) {
	var calls atomic.Int64
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		calls.Add(1)
		if u.Query().Get("cid") != "a&b=c/+% ?" {
			t.Error("unescaped container ID")
		}
		start := strings.Split(u.Query().Get("range"), ",")[0]
		items := []any{}
		if start == "0" {
			items = append(items, map[string]any{"name": "album", "cid": "x/y&z", "container": "yes", "playable": "yes"})
		}
		sendReply(c, u, url.Values{"count": {"0"}, "returned": {strconv.Itoa(len(items))}}, items)
	})
	c := fakeClient(t, s)
	items, err := c.BrowseAll(context.Background(), "1024", "a&b=c/+% ?")
	if err != nil || len(items) != 1 || calls.Load() != 2 {
		t.Fatal(items, calls.Load(), err)
	}
}

func TestBrowseBoundsAndInvalidCounts(t *testing.T) {
	for _, count := range []string{"-1", "unknown", "0"} {
		t.Run(count, func(t *testing.T) {
			var calls atomic.Int64
			s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				calls.Add(1)
				sendReply(c, u, url.Values{"count": {count}}, []any{map[string]any{"name": "item", "cid": "same", "container": "yes"}})
			})
			_, err := fakeClient(t, s).BrowseAll(context.Background(), "1024", "root")
			if err == nil {
				t.Fatal("unbounded/malformed catalog accepted")
			}
			if calls.Load() > MaxBrowsePages {
				t.Fatal("page bound exceeded")
			}
		})
	}
}

func TestCatalogReferencesAndCursors(t *testing.T) {
	var invalidate atomic.Bool
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "system/heart_beat" {
			if invalidate.Load() {
				sendEvent(c, "event/sources_changed", "")
			}
			sendReply(c, u, nil, nil)
			return
		}
		// Denon 4.4.3: source responses are unpaged except for Favorites.
		sendReply(c, u, url.Values{"count": {"2"}}, []any{
			map[string]any{"name": "first", "cid": "a%26b%3Dc/+%25 ?", "container": "yes", "playable": "yes"},
			map[string]any{"name": "second", "cid": "two", "container": "yes"},
		})
	})
	c := fakeClient(t, s)
	catalog, err := NewCatalog(c, 8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	catalog.now = func() time.Time { return now }
	page, err := catalog.Browse(context.Background(), "1024", "", "", 1)
	if err != nil || len(page.Items) != 1 || page.Next == "" {
		t.Fatal(page, err)
	}
	ref := page.Items[0].Ref
	item, err := catalog.Resolve("1024", ref)
	if err != nil || item.ContainerID != "a&b=c/+% ?" {
		t.Fatal(item, err)
	}
	if _, err := catalog.Resolve("different-source", ref); !errors.Is(err, ErrStaleReference) {
		t.Fatal(err)
	}
	next, err := catalog.Browse(context.Background(), "1024", "", page.Next, 1)
	if err != nil || len(next.Items) != 1 || next.Items[0].Name != "second" || next.Next != "" {
		t.Fatal(next, err)
	}
	now = now.Add(time.Minute)
	if _, err := catalog.Resolve("1024", ref); !errors.Is(err, ErrStaleReference) {
		t.Fatal(err)
	}
	page, err = catalog.Browse(context.Background(), "1024", "", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	invalidate.Store(true)
	if _, err := c.Read(context.Background(), "system/heart_beat", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Resolve("1024", page.Items[0].Ref); !errors.Is(err, ErrStaleReference) {
		t.Fatal("source change did not invalidate ref", err)
	}
}

func TestSourceCursorUsesBoundedSnapshot(t *testing.T) {
	var calls atomic.Int64
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		calls.Add(1)
		if u.Query().Has("range") {
			t.Error("range sent to unpaged source")
		}
		sendReply(c, u, url.Values{"count": {"2"}}, []any{
			map[string]any{"name": "first", "sid": "-1", "type": "dlna_server"},
			map[string]any{"name": "second", "sid": "-2", "type": "dlna_server"},
		})
	})
	catalog, err := NewCatalog(fakeClient(t, s), 8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first, err := catalog.Browse(context.Background(), "1024", "", "", 1)
	if err != nil || first.Next == "" {
		t.Fatal(first, err)
	}
	second, err := catalog.Browse(context.Background(), "1024", "", first.Next, 1)
	if err != nil || len(second.Items) != 1 || second.Items[0].Name != "second" || second.Next != "" || calls.Load() != 1 {
		t.Fatal(second, err, calls.Load())
	}
	if _, err := catalog.Browse(context.Background(), "1025", "", first.Next, 1); !errors.Is(err, ErrStaleReference) {
		t.Fatal(err)
	}
}

func TestSourceTruncationAndFavoritesRange(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if u.Query().Get("sid") == "1028" {
			if u.Query().Get("range") != "1,1" {
				t.Error("Favorites range missing")
			}
			sendReply(c, u, url.Values{"count": {"2"}}, []any{map[string]any{"name": "favorite", "mid": "station"}})
			return
		}
		sendReply(c, u, url.Values{"count": {"2"}}, []any{map[string]any{"name": "truncated", "sid": "-1", "type": "dlna_server"}})
	})
	c := fakeClient(t, s)
	if _, err := c.BrowseAll(context.Background(), "1024", ""); err == nil {
		t.Fatal("truncated unpaged source accepted")
	}
	page, err := c.BrowsePage(context.Background(), "1028", "", 1, 1)
	if err != nil || page.Next != nil || len(page.Items) != 1 {
		t.Fatal(page, err)
	}
}

func TestCatalogCapacity(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		sendReply(c, u, url.Values{"count": {"3"}}, []any{map[string]any{"cid": "a"}, map[string]any{"cid": "b"}, map[string]any{"cid": "c"}})
	})
	catalog, err := NewCatalog(fakeClient(t, s), 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Browse(context.Background(), "1024", "", "", 3); !errors.Is(err, ErrCatalogFull) {
		t.Fatal(err)
	}
}

func TestBrowseRejectsTruncatedKnownTotal(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		sendReply(c, u, url.Values{"count": {"2"}}, []any{})
	})
	if _, err := fakeClient(t, s).BrowseAll(context.Background(), "1024", ""); !errors.Is(err, ErrProtocol) {
		t.Fatalf("silently accepted incomplete catalog: %v", err)
	}
}

func TestCatalogExpiryDuringBrowse(t *testing.T) {
	var elapsed atomic.Int64
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if u.Query().Get("cid") != "" {
			elapsed.Store(int64(time.Minute))
		}
		sendReply(c, u, url.Values{"count": {"1"}}, []any{map[string]any{"cid": "album", "container": "yes"}})
	})
	catalog, err := NewCatalog(fakeClient(t, s), 8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	catalog.now = func() time.Time { return base.Add(time.Duration(elapsed.Load())) }
	page, err := catalog.Browse(context.Background(), "1024", "", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Browse(context.Background(), "1024", page.Items[0].Ref, "", 1); !errors.Is(err, ErrStaleReference) {
		t.Fatalf("expired parent minted fresh references: %v", err)
	}
}

func TestCachedSourceCursorCannotRenewSnapshotTTL(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		sendReply(c, u, url.Values{"count": {"3"}}, []any{
			map[string]any{"cid": "a"}, map[string]any{"cid": "b"}, map[string]any{"cid": "c"},
		})
	})
	catalog, err := NewCatalog(fakeClient(t, s), 16, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	catalog.now = func() time.Time { return now }
	first, err := catalog.Browse(context.Background(), "1024", "", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(59 * time.Second)
	second, err := catalog.Browse(context.Background(), "1024", "", first.Next, 1)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if _, err := catalog.Browse(context.Background(), "1024", "", second.Next, 1); !errors.Is(err, ErrStaleReference) {
		t.Fatal("cached cursor renewed stale source snapshot", err)
	}
}

func TestGerberaReferencePathPreservesSourceAndParent(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		var item map[string]any
		switch u.Query().Get("sid") + ":" + u.Query().Get("cid") {
		case "1024:":
			item = map[string]any{"name": "Gerbera", "sid": "-123", "type": "dlna_server"}
		case "-123:":
			item = map[string]any{"name": "Audio", "cid": "Audio", "container": "yes"}
		case "-123:Audio":
			item = map[string]any{"name": "Albums", "cid": "Albums", "container": "yes"}
		case "-123:Albums":
			item = map[string]any{"name": "A%26B", "cid": "album%26x", "container": "yes", "playable": "yes"}
		case "-123:album&x":
			item = map[string]any{"name": "Song", "mid": "track%2526", "type": "song", "playable": "yes"}
		default:
			t.Errorf("wrong route: %s", u)
		}
		sendReply(c, u, url.Values{"count": {"1"}}, []any{item})
	})
	catalog, err := NewCatalog(fakeClient(t, s), 16, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	parent := ""
	for _, name := range []string{"Gerbera", "Audio", "Albums", "A&B", "Song"} {
		page, err := catalog.Browse(context.Background(), "1024", parent, "", 10)
		if err != nil || len(page.Items) != 1 || page.Items[0].Name != name {
			t.Fatal(name, page, err)
		}
		parent = page.Items[0].Ref
	}
	track, err := catalog.Resolve("1024", parent)
	if err != nil || track.Source != "-123" || track.MediaID != "track%26" || track.ContainerID != "album&x" {
		t.Fatal("lost source/parent context needed by Denon 4.4.12", track, err)
	}
	if _, err := catalog.Resolve("-123", parent); !errors.Is(err, ErrStaleReference) {
		t.Fatal("reference escaped its logical root")
	}
	if _, err := catalog.Browse(context.Background(), "1024", parent, "", 10); !errors.Is(err, ErrStaleReference) {
		t.Fatal("track accepted as browse parent")
	}
}

func TestUnknownUnpagedSourceAtServiceLimitFailsExplicitly(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		items := make([]map[string]any, 50) // Denon 4.4.3 permits a 50-record cap.
		for i := range items {
			items[i] = map[string]any{"cid": strconv.Itoa(i)}
		}
		sendReply(c, u, url.Values{"count": {"0"}}, items)
	})
	if _, err := fakeClient(t, s).BrowseAll(context.Background(), "1024", ""); !errors.Is(err, ErrIncompleteSource) {
		t.Fatal("possibly truncated source silently accepted", err)
	}
}
