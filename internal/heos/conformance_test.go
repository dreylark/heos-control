// Regressions promoted from the Denon HEOS CLI 1.17 audit, 2026-09-06.
package heos

import (
	"context"
	"encoding/json"
	"net"
	"net/url"
	"testing"
	"time"
)

// Sections 3.1–3.2: response strings are already encoded on the wire.
func TestDenonEncodedContainerRoundTrip(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if u.Query().Get("cid") == "root" {
			sendReply(c, u, url.Values{"count": {"1"}}, []any{map[string]any{
				"name": "A%26B", "cid": "albums%26part%3D1%25", "container": "yes", "playable": "yes",
			}})
			return
		}
		if got := u.Query().Get("cid"); got != "albums&part=1%" {
			t.Errorf("double-encoded CID: after one wire decode got %q", got)
		}
		sendReply(c, u, url.Values{"count": {"0"}}, []any{})
	})
	c := fakeClient(t, s)
	page, err := c.BrowsePage(context.Background(), "server", "root", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.BrowsePage(context.Background(), "server", page.Items[0].ContainerID, 0, 1); err != nil {
		t.Fatal(err)
	}
}

func TestDenonDisplayStrings(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		sendReply(c, u, url.Values{"count": {"1"}}, []any{map[string]any{
			"name": "A%26B", "cid": "album", "container": "yes",
		}})
	})
	catalog, err := NewCatalog(fakeClient(t, s), 8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	page, err := catalog.Browse(context.Background(), "server", "", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if page.Items[0].Name != "A&B" {
		t.Fatalf("wire encoding leaked into display name: %q", page.Items[0].Name)
	}
}

// Section 4.4.3: source-level range is supported only for Favorites.
func TestDenonSourceBrowseDoesNotSendUnsupportedRange(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if u.Query().Has("range") {
			raw, _ := json.Marshal(map[string]any{"heos": map[string]string{
				"command": commandName(u), "result": "fail", "message": "eid=3&text=Unsupported%20argument",
			}})
			_, _ = c.Write(append(raw, '\r', '\n'))
			return
		}
		sendReply(c, u, url.Values{"count": {"0"}}, []any{})
	})
	if _, err := fakeClient(t, s).BrowsePage(context.Background(), "1024", "", 0, 100); err != nil {
		t.Fatalf("Local Music source browse used unsupported range: %v", err)
	}
}

// Section 4.4.3: Local Music entries identify child sources by sid/type, not cid.
func TestDenonVirtualSourceNavigation(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if u.Query().Get("sid") == "1024" {
			sendReply(c, u, url.Values{"count": {"1"}}, []any{map[string]any{
				"name": "Gerbera", "sid": "-123", "type": "heos_server",
			}})
			return
		}
		sendReply(c, u, url.Values{"count": {"0"}}, []any{})
	})
	catalog, err := NewCatalog(fakeClient(t, s), 8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	page, err := catalog.Browse(context.Background(), "1024", "", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Browse(context.Background(), "1024", page.Items[0].Ref, "", 1); err != nil {
		t.Fatalf("cannot follow the returned Gerbera source reference: %v", err)
	}
}

// Section 5.6: progress changes position, not the observed state/queue/volume.
// This is an observation-policy regression driven by a valid protocol event.
func TestDenonProgressDoesNotPreventStateRefresh(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/get_volume" {
			sendEvent(c, "event/player_now_playing_progress", "pid=9007199254740993&cur_pos=1000&duration=300000")
		}
		observationReply(c, u, "serial-A")
	})
	o, err := NewObserver(fakeClient(t, s), Identity{Key: "room", Serial: "serial-A"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatalf("ordinary progress prevented a complete state observation: %v", err)
	}
}

// Sections 4.3.2 and 4.4.4 define the identity/range of returned data.
// These are defensive validation probes, not claims that Denon emits wrong echoes.
func TestDenonRejectsWrongGroupEcho(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		sendReply(c, u, url.Values{"gid": {"2"}}, map[string]any{"gid": "2", "players": []any{}})
	})
	if _, err := fakeClient(t, s).Read(context.Background(), "group/get_group_info", url.Values{"gid": {"1"}}); err == nil {
		t.Fatal("response for gid=2 accepted as gid=1")
	}
}

func TestDenonRejectsWrongPageEcho(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		sendReply(c, u, url.Values{"range": {"1,1"}, "count": {"2"}, "returned": {"1"}}, []any{map[string]any{"cid": "second"}})
	})
	if _, err := fakeClient(t, s).BrowsePage(context.Background(), "server", "album", 0, 1); err == nil {
		t.Fatal("page starting at 1 accepted as page starting at 0")
	}
}

// Section 4.2.15 does not require browse-specific count/returned fields.
func TestDenonQueueWithoutBrowseCounts(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		sendReply(c, u, nil, []any{map[string]any{"qid": "1", "mid": "track", "song": "Song"}})
	})
	page, err := fakeClient(t, s).Queue(context.Background(), "1", 0, 100)
	if err != nil || len(page.Items) != 1 || page.Next != nil || page.Total != nil {
		t.Fatal(page, err)
	}
}

// Sections 5.1–5.3: these event envelopes have neither result nor message.
func TestDenonPayloadlessEvents(t *testing.T) {
	for _, name := range []string{"event/sources_changed", "event/players_changed", "event/groups_changed"} {
		raw, _ := json.Marshal(map[string]any{"heos": map[string]string{"command": name}})
		if r, err := decodeResponse(raw); err != nil || !r.Event {
			t.Fatal(r, err)
		}
	}
}
