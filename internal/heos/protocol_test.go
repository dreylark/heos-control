package heos

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"
)

func TestIDsAndEncoding(t *testing.T) {
	for _, input := range []string{`9007199254740993`, `-512`, `"a%26b%3Dc/+%25 ?"`} {
		var id ID
		if err := json.Unmarshal([]byte(input), &id); err != nil {
			t.Fatal(err)
		}
		if input == `9007199254740993` && id != "9007199254740993" {
			t.Fatal("rounded identifier")
		}
		wire, err := encodeCommand("browse/browse", url.Values{"cid": {string(id)}})
		if err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(strings.TrimSuffix(string(wire), "\r\n"))
		if err != nil || u.Query().Get("cid") != string(id) {
			t.Fatal(string(wire), err)
		}
	}
	for _, input := range []string{`null`, `true`, `1.5`, `1e3`, `[]`, `""`} {
		var id ID
		if err := json.Unmarshal([]byte(input), &id); err == nil {
			t.Fatalf("accepted ID %s", input)
		}
	}
	if _, err := encodeCommand("player/get_players\r\nheos://player/set_mute", nil); err == nil {
		t.Fatal("command injection accepted")
	}
}

// Denon 3.1–3.2: percent encoding is distinct from HTML form encoding.
func TestWireStringsDecodeOnce(t *testing.T) {
	for _, tc := range []struct{ wire, want string }{
		{`"A%26B"`, "A&B"}, {`"literal%2526"`, "literal%26"},
		{`"C++%20music"`, "C++ music"}, {`"a%3db%2Fc"`, "a=b/c"},
	} {
		var item Item
		if err := json.Unmarshal([]byte(`{"name":`+tc.wire+`,"cid":`+tc.wire+`}`), &item); err != nil {
			t.Fatal(err)
		}
		if string(item.Name) != tc.want || string(item.ContainerID) != tc.want {
			t.Fatalf("decoded %s as %+v, want %q", tc.wire, item, tc.want)
		}
	}
	r, err := decodeResponse([]byte(`{"heos":{"command":"event/player_playback_error","message":"pid=1&error=C++%20decoder%3B%20A%26B"}}`))
	if err != nil || r.Params.Get("error") != "C++ decoder; A&B" {
		t.Fatal(r, err)
	}
	wire, err := encodeCommand("browse/browse", url.Values{"cid": {"A B+C"}})
	if err != nil || !strings.Contains(string(wire), "cid=A%20B%2BC") {
		t.Fatal(string(wire), err)
	}
}

type byteReader struct{ io.Reader }

func (r byteReader) Read(p []byte) (int, error) { return r.Reader.Read(p[:min(len(p), 1)]) }

func TestFrameBoundaries(t *testing.T) {
	r := bufio.NewReader(byteReader{strings.NewReader("{}\r\n[]\r\n")})
	for _, want := range []string{"{}", "[]"} {
		got, err := readFrame(r, 16)
		if err != nil || string(got) != want {
			t.Fatalf("got %q %v", got, err)
		}
	}
	for _, input := range []string{"{}", "{}\r", "{}\n", "\r\n", strings.Repeat("x", 17) + "\r\n"} {
		if _, err := readFrame(bufio.NewReader(strings.NewReader(input)), 16); err == nil {
			t.Fatalf("accepted frame %q", input)
		}
	}
	if _, err := readFrame(bufio.NewReader(strings.NewReader("")), 16); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func TestResponseValidation(t *testing.T) {
	for _, input := range []string{`{}`, `{"heos":{"command":"x","result":"maybe"}}`, `{"heos":{"command":"x","result":"success","message":"pid=1&pid=2"}}`, `{"heos":{"command":"x","result":"success","message":"pid=%xx"}}`, `{`} {
		if _, err := decodeResponse([]byte(input)); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	r, err := decodeResponse([]byte(`{"heos":{"command":"event/player_state_changed","message":"pid=1&state=pause"}}`))
	if err != nil || r.Params.Get("state") != "pause" || !r.Event {
		t.Fatal(r, err)
	}
}

type shortWriter struct {
	bytes.Buffer
	zero bool
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if w.zero {
		return 0, nil
	}
	return w.Buffer.Write(p[:min(len(p), 2)])
}
func TestPartialWrites(t *testing.T) {
	w := &shortWriter{}
	n, err := writeAll(w, []byte("command\r\n"))
	if err != nil || n != 9 || w.String() != "command\r\n" {
		t.Fatal(n, w.String(), err)
	}
	if _, err := writeAll(&shortWriter{zero: true}, []byte("x")); !errors.Is(err, io.ErrNoProgress) {
		t.Fatal(err)
	}
}

// Denon 4.2.15 and 4.4.4 use a literal comma as the range delimiter.
// Home 150 rejects an escaped delimiter in get_queue (2026-09-07).
func TestRangeWireDelimiterAndOpaqueValues(t *testing.T) {
	for _, command := range []string{"player/get_queue", "browse/browse"} {
		wire, err := encodeCommand(command, url.Values{"range": {"0,99"}, "cid": {"a,b&range=1,2%"}})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(wire), "range=0,99") || !strings.Contains(string(wire), "cid=a%2Cb%26range%3D1%2C2%25") {
			t.Fatalf("wrong escaping: %s", wire)
		}
	}
	for _, value := range []string{"0%2C99", "0,99&pid=2", "-1,99", "99,0", "0,1,2"} {
		if _, err := encodeCommand("player/get_queue", url.Values{"range": {value}}); !errors.Is(err, ErrProtocol) {
			t.Fatalf("accepted invalid range %q: %v", value, err)
		}
	}
}
