// Package heos provides bounded, pinned-TLS HEOS observations. It does not own
// device admission or playback policy. Guarded writes require explicit opt-in.
package heos

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

var ErrProtocol = errors.New("invalid HEOS protocol")

// ID preserves decimal IDs without converting through floating point; browse
// identifiers may instead be opaque strings containing reserved URL characters.
type ID string

// Text is decoded domain text. Only JSON input from HEOS is percent-decoded;
// assigning an already-decoded string must not decode literal percent sequences.
type Text string

func decodeText(s string) (string, error) {
	// Denon 3.1–3.2 uses percent escaping, not form encoding: '+' stays '+'.
	v, err := url.PathUnescape(s)
	if err != nil || !utf8.ValidString(v) || strings.ContainsRune(v, '\x00') {
		return "", ErrProtocol
	}
	return v, nil
}

func (s *Text) UnmarshalJSON(data []byte) error {
	var wire string
	if err := json.Unmarshal(data, &wire); err != nil {
		return ErrProtocol
	}
	v, err := decodeText(wire)
	if err == nil {
		*s = Text(v)
	}
	return err
}

func (id *ID) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	var value string
	if len(data) > 0 && data[0] == '"' {
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		var err error
		value, err = decodeText(value)
		if err != nil {
			return err
		}
	} else {
		value = string(data)
		number := strings.TrimPrefix(value, "-")
		if number == "" {
			return ErrProtocol
		}
		for _, c := range number {
			if c < '0' || c > '9' {
				return ErrProtocol
			}
		}
	}
	if value == "" || len(value) > 4096 || strings.ContainsRune(value, '\x00') {
		return ErrProtocol
	}
	*id = ID(value)
	return nil
}

type Response struct {
	Command string
	Result  string
	Params  url.Values
	Payload json.RawMessage
	Event   bool
	Pending bool
	Token   Token
}

// Token scopes observations and catalog references to a connection and its
// event history. A reconnect invalidates even references to unchanged IDs.
type Token struct{ Generation, Revision, Catalog, Player, Write uint64 }

func sameGlobal(a, b Token) bool {
	a.Player, b.Player = 0, 0
	a.Write, b.Write = 0, 0
	return a == b
}

func parseRange(value string) (int, int, error) {
	first, last, ok := strings.Cut(value, ",")
	start, a := strconv.Atoi(strings.TrimSpace(first))
	end, b := strconv.Atoi(strings.TrimSpace(last))
	if !ok || a != nil || b != nil || start < 0 || end < start {
		return 0, 0, ErrProtocol
	}
	return start, end, nil
}

func validateEcho(args, params url.Values) error {
	for _, key := range []string{"pid", "gid", "sid", "cid", "SEQUENCE"} {
		if args.Has(key) && params.Has(key) && args.Get(key) != params.Get(key) {
			return ErrProtocol
		}
	}
	if args.Has("range") && params.Has("range") {
		start, end, err := parseRange(args.Get("range"))
		gotStart, gotEnd, gotErr := parseRange(params.Get("range"))
		if err != nil || gotErr != nil || gotStart != start || gotEnd > end {
			return ErrProtocol
		}
	}
	return nil
}

func encodeCommand(name string, args url.Values) ([]byte, error) {
	if name == "" || len(name) > 128 {
		return nil, ErrProtocol
	}
	for _, c := range name {
		if (c < 'a' || c > 'z') && c != '_' && c != '/' {
			return nil, ErrProtocol
		}
	}
	for k, values := range args {
		if k == "" || len(values) != 1 {
			return nil, ErrProtocol
		}
		for _, c := range k {
			if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && c != '_' {
				return nil, ErrProtocol
			}
		}
	}
	query := strings.ReplaceAll(args.Encode(), "+", "%20")
	if args.Has("range") {
		start, end, err := parseRange(args.Get("range"))
		if err != nil {
			return nil, err
		}
		// Denon 4.2.15/4.4.4: the comma is range syntax. Home 150
		// rejects get_queue with %2C. Keep opaque values fully escaped.
		encoded := "range=" + strings.ReplaceAll(url.QueryEscape(args.Get("range")), "+", "%20")
		parts := strings.Split(query, "&")
		for i, part := range parts {
			if part == encoded {
				parts[i] = fmt.Sprintf("range=%d,%d", start, end)
			}
		}
		query = strings.Join(parts, "&")
	}
	wire := []byte("heos://" + name + "?" + query + "\r\n")
	if len(wire) > 16*1024 {
		return nil, ErrProtocol
	}
	return wire, nil
}

func readFrame(r *bufio.Reader, limit int) ([]byte, error) {
	frame := make([]byte, 0, min(limit, 4096))
	for {
		b, err := r.ReadByte()
		if err != nil {
			if len(frame) > 0 {
				return nil, fmt.Errorf("unterminated frame: %w", io.ErrUnexpectedEOF)
			}
			return nil, err
		}
		if b == '\n' {
			if len(frame) < 2 || frame[len(frame)-1] != '\r' {
				return nil, ErrProtocol
			}
			return frame[:len(frame)-1], nil
		}
		if len(frame) > 0 && frame[len(frame)-1] == '\r' {
			return nil, ErrProtocol
		}
		if len(frame) >= limit && b != '\r' || len(frame) > limit {
			return nil, fmt.Errorf("frame limit: %w", ErrProtocol)
		}
		frame = append(frame, b)
	}
}

func decodeResponse(frame []byte) (Response, error) {
	var raw struct {
		HEOS struct {
			Command string `json:"command"`
			Result  string `json:"result"`
			Message string `json:"message"`
		} `json:"heos"`
		Payload json.RawMessage `json:"payload"`
	}
	if !utf8.Valid(frame) {
		return Response{}, ErrProtocol
	}
	if err := json.Unmarshal(frame, &raw); err != nil {
		return Response{}, fmt.Errorf("JSON frame: %w", ErrProtocol)
	}
	r := Response{Command: strings.TrimSpace(raw.HEOS.Command), Result: raw.HEOS.Result, Payload: raw.Payload}
	r.Event = strings.HasPrefix(r.Command, "event/")
	if r.Command == "" || (!r.Event && r.Result != "success" && r.Result != "fail") {
		return Response{}, ErrProtocol
	}
	r.Pending = strings.HasPrefix(raw.HEOS.Message, "command under process")
	params := url.Values{}
	for _, pair := range strings.Split(raw.HEOS.Message, "&") {
		if pair == "" {
			continue
		}
		key, value, _ := strings.Cut(pair, "=")
		key, keyErr := decodeText(key)
		value, valueErr := decodeText(value)
		if keyErr != nil || valueErr != nil || key == "" || params.Has(key) {
			return Response{}, ErrProtocol
		}
		params.Set(key, value)
	}
	r.Params = params
	return r, nil
}

func writeAll(w io.Writer, p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n, err := w.Write(p)
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
		p = p[n:]
	}
	return total, nil
}
