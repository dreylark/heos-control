package heos

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// HEOS CLI Protocol Specification 1.17, 4.2.17 defines a list of queue IDs,
// not a numeric range or a media-ID selection. Preserve each opaque occurrence.
func TestQueueRemovalBoundsAndEncoding(t *testing.T) {
	ids := []ID{"9007199254740993", "entry/+%&=value", "literal%2Ccomma", "with space"}
	m := Mutation{Kind: MutationKindRemove, Player: "-42", QueueIDs: ids}
	name, args, err := m.command()
	if err != nil || name != "player/remove_from_queue" || len(args) != 2 {
		t.Fatal(name, args, err)
	}
	wire, err := encodeCommand(name, args)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wire), "qid=9007199254740993,entry%2F%2B%25%26%3Dvalue,literal%252Ccomma,with%20space") {
		t.Fatalf("queue IDs or list delimiters changed: %q", wire)
	}
	u, err := url.Parse(strings.TrimSuffix(string(wire), "\r\n"))
	if err != nil || u.Query().Get("pid") != "-42" || len(u.Query()) != 2 || !slices.Equal(strings.Split(u.Query().Get("qid"), ","), []string{"9007199254740993", "entry/+%&=value", "literal%2Ccomma", "with space"}) {
		t.Fatal("opaque IDs failed round trip", u, err)
	}
	for _, tc := range []struct {
		name string
		ids  []ID
	}{
		{"empty", nil}, {"empty ID", []ID{"a", ""}}, {"duplicate", []ID{"a", "a"}},
		{"comma", []ID{"a,b"}}, {"nul", []ID{"a\x00b"}}, {"newline", []ID{"a\nb"}},
		{"carriage return", []ID{"a\rb"}}, {"control", []ID{"a\x7fb"}}, {"invalid UTF-8", []ID{"\xff"}},
		{"oversized ID", []ID{ID(strings.Repeat("a", 4097))}}, {"too many", make([]ID, 1001)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m.QueueIDs = tc.ids
			if _, _, err := m.command(); !errors.Is(err, ErrBounds) {
				t.Fatalf("unsafe removal accepted: %v", err)
			}
		})
	}
	m.QueueIDs = make([]ID, 1000)
	for i := range m.QueueIDs {
		m.QueueIDs[i] = ID(strconv.Itoa(i + 1))
	}
	name, args, err = m.command()
	if err != nil {
		t.Fatal("bounded queue cannot be removed", err)
	}
	if _, err := encodeCommand(name, args); err != nil {
		t.Fatal(err)
	}
	m.QueueIDs = []ID{ID(strings.Repeat("a", 4096)), ID(strings.Repeat("b", 4096)), ID(strings.Repeat("c", 4096)), ID(strings.Repeat("d", 4096))}
	name, args, err = m.command()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encodeCommand(name, args); !errors.Is(err, ErrProtocol) {
		t.Fatal("oversized removal frame accepted", err)
	}
}

func TestQueueRemovalOverPinnedTLS(t *testing.T) {
	for _, scenario := range []string{"success", "pending", "missing qid", "wrong qid", "reordered qid", "missing pid", "duplicate qid", "rejection", "disconnect"} {
		t.Run(scenario, func(t *testing.T) {
			var writes atomic.Int32
			metrics := &recordingMetrics{}
			server := newFakeHEOS(t, func(conn net.Conn, u *url.URL, _ int64) {
				if commandName(u) != "player/remove_from_queue" {
					sendReply(conn, u, nil, nil)
					return
				}
				writes.Add(1)
				if u.RawQuery != "pid=1&qid=entry%2F%2B%25%26,9007199254740993" {
					t.Error("incorrect removal wire", u.RawQuery)
				}
				params := u.Query()
				result := "success"
				switch scenario {
				case "pending":
					raw, _ := json.Marshal(map[string]any{"heos": map[string]string{"command": commandName(u), "result": "success", "message": "command under process"}})
					_, _ = conn.Write(append(raw, '\r', '\n'))
				case "missing qid":
					params.Del("qid")
				case "wrong qid":
					params.Set("qid", "another,9007199254740993")
				case "reordered qid":
					params.Set("qid", "9007199254740993,entry/+%&")
				case "missing pid":
					params.Del("pid")
				case "duplicate qid":
					params.Add("qid", "another")
				case "rejection":
					params = url.Values{"eid": {"9"}}
					result = "fail"
				case "disconnect":
					_ = conn.Close()
					return
				}
				raw, _ := json.Marshal(map[string]any{"heos": map[string]string{"command": commandName(u), "result": result, "message": strings.ReplaceAll(params.Encode(), "+", "%20")}})
				_, _ = conn.Write(append(raw, '\r', '\n'))
			})
			client := queueRemovalClient(t, server, true, metrics)
			guard := Guard{Token: client.observePlayer("1").Token, ExpiresAt: time.Now().Add(time.Second)}
			m := Mutation{Kind: MutationKindRemove, Player: "1", QueueIDs: []ID{"entry/+%&", "9007199254740993"}}
			_, err := client.Write(context.Background(), m, guard)
			wantResult := "reply_success"
			if scenario == "success" || scenario == "pending" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				wantDelivery := Uncertain
				wantResult = "uncertain"
				if scenario == "rejection" {
					wantDelivery, wantResult = Rejected, "reply_rejected"
				}
				var commandErr *CommandError
				if !errors.As(err, &commandErr) || commandErr.Delivery != wantDelivery {
					t.Fatalf("incorrect removal delivery: %v", err)
				}
			}
			if _, err := client.Write(context.Background(), m, guard); !errors.Is(err, ErrStale) || writes.Load() != 1 {
				t.Fatalf("removal replayed with consumed guard: writes=%d err=%v", writes.Load(), err)
			}
			wires, _, _, _ := metrics.snapshot()
			removals := 0
			for _, w := range wires {
				if w.command == "player/remove_from_queue" {
					removals++
					if w.result != wantResult {
						t.Fatal("incorrect removal wire result", w)
					}
				}
			}
			if removals != 1 {
				t.Fatal("removal metrics did not count the single send", removals)
			}
		})
	}
}

func TestQueueRemovalCannotBypassWriteGuards(t *testing.T) {
	for _, scenario := range []string{"writes disabled", "expired", "stale token", "unobserved player", "read API", "invalid list"} {
		t.Run(scenario, func(t *testing.T) {
			var writes atomic.Int32
			server := newFakeHEOS(t, func(conn net.Conn, u *url.URL, _ int64) {
				if commandName(u) == "player/remove_from_queue" {
					writes.Add(1)
				}
				sendReply(conn, u, nil, nil)
			})
			client := queueRemovalClient(t, server, scenario != "writes disabled", nil)
			guard := Guard{Token: client.observePlayer("1").Token, ExpiresAt: time.Now().Add(time.Second)}
			m := Mutation{Kind: MutationKindRemove, Player: "1", QueueIDs: []ID{"first"}}
			switch scenario {
			case "expired":
				guard.ExpiresAt = time.Now().Add(-time.Second)
			case "stale token":
				guard.Token.Revision++
			case "unobserved player":
				m.Player = "other"
			case "invalid list":
				m.QueueIDs = []ID{"first,second"}
			}
			var err error
			if scenario == "read API" {
				_, err = client.Read(context.Background(), "player/remove_from_queue", url.Values{"pid": {"1"}, "qid": {"first"}})
			} else {
				_, err = client.Write(context.Background(), m, guard)
			}
			var commandErr *CommandError
			if !errors.As(err, &commandErr) || commandErr.Delivery != NotSent || writes.Load() != 0 {
				t.Fatalf("unapproved removal sent: writes=%d err=%v", writes.Load(), err)
			}
		})
	}
}

func queueRemovalClient(t *testing.T, server *fakeHEOS, enable bool, metrics Metrics) *Client {
	t.Helper()
	client, err := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin, EnableWrites: enable, Metrics: metrics, CommandTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	if _, err := client.Read(context.Background(), "player/get_players", nil); err != nil {
		t.Fatal(err)
	}
	return client
}

func FuzzHEOSQueueRemoval(f *testing.F) {
	for _, seed := range []string{"first|second", "entry/+%&=value|literal%2Ccomma", "9007199254740993|-7", "with space|music", "duplicate|duplicate", "comma,in,ID", "\x00|other", "\xff"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 4096 {
			return
		}
		ids := strings.Split(value, "|")
		if len(ids) > 8 {
			return
		}
		m := Mutation{Kind: MutationKindRemove, Player: "-42", QueueIDs: make([]ID, len(ids))}
		for i, id := range ids {
			m.QueueIDs[i] = ID(id)
		}
		name, args, err := m.command()
		if err != nil {
			return
		}
		wire, err := encodeCommand(name, args)
		if err != nil {
			t.Fatal("bounded accepted removal failed encoding", err)
		}
		if !strings.HasSuffix(string(wire), "\r\n") || strings.Count(string(wire), "\r") != 1 || strings.Count(string(wire), "\n") != 1 {
			t.Fatal("queue occurrence escaped frame bounds")
		}
		u, err := url.Parse(strings.TrimSuffix(string(wire), "\r\n"))
		if err != nil || commandName(u) != "player/remove_from_queue" || u.Fragment != "" || len(u.Query()) != 2 || u.Query().Get("pid") != "-42" {
			t.Fatal("queue occurrence escaped command arguments", err)
		}
		if !slices.Equal(strings.Split(u.Query().Get("qid"), ","), ids) || strings.Count(u.RawQuery, ",") != len(ids)-1 {
			t.Fatal("queue occurrence or list delimiter changed")
		}
	})
}
