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
	"testing"
)

// Denon 3.1–3.2: opaque values are percent-escaped inside one CRLF command.
// Use the standard URL parser as an independent observer of the encoded wire,
// not encodeCommand/decodeResponse to compute each other's expected values.
func FuzzHEOSCommandEncoding(f *testing.F) {
	for _, seed := range []struct {
		value string
		id    int64
	}{
		{"album", 1}, {"A B+C&pid=2=3%", 9007199254740993},
		{"literal%2526,range=0,99", -9007199254740993},
		{"Музыка/日本語", -1}, {"\r\nheos://player/set_volume?pid=1&level=99", 0},
		{strings.Repeat("%", 4096), 9223372036854775807},
	} {
		f.Add(seed.value, seed.id, uint8(17))
	}
	f.Fuzz(func(t *testing.T, value string, number int64, start uint8) {
		if len(value) > 4096 {
			return
		}
		pid := strconv.FormatInt(number, 10)
		// Numeric and quoted decimal response IDs must remain exact across the
		// float64 precision boundary, before becoming command parameters.
		for _, raw := range []string{pid, strconv.Quote(pid)} {
			var id ID
			if err := json.Unmarshal([]byte(raw), &id); err != nil || string(id) != pid {
				t.Fatalf("ID lost precision/type semantics: raw=%s got=%q err=%v", raw, id, err)
			}
		}
		rangeValue := fmt.Sprintf("%d,%d", start, int(start)+99)
		args := url.Values{"pid": {pid}, "cid": {value}, "range": {rangeValue}}
		wire, err := encodeCommand("browse/browse", args)
		if err != nil {
			t.Fatalf("bounded valid arguments rejected: %v", err)
		}
		if len(wire) > 16*1024 || !bytes.HasSuffix(wire, []byte("\r\n")) || bytes.ContainsAny(wire[:len(wire)-2], "\r\n") {
			t.Fatalf("encoding escaped the single bounded frame: %q", wire)
		}
		u, err := url.Parse(string(wire[:len(wire)-2]))
		if err != nil || u.Scheme != "heos" || u.Host != "browse" || u.Path != "/browse" || u.Fragment != "" || u.User != nil {
			t.Fatalf("opaque argument altered command routing: %q, %v", wire, err)
		}
		decoded, err := url.ParseQuery(u.RawQuery)
		if err != nil || len(decoded) != len(args) {
			t.Fatalf("opaque argument introduced parameters: %q, %v", wire, err)
		}
		for key, want := range args {
			if got := decoded[key]; len(got) != 1 || got[0] != want[0] {
				t.Fatalf("parameter %s changed: got=%q want=%q", key, got, want)
			}
		}
		// Home 150 requires the syntactic comma unescaped (Denon 4.4.4),
		// while commas and percent signs within cid remain opaque data.
		if !strings.Contains(u.RawQuery, "range="+rangeValue) || strings.Contains(u.RawQuery, "+") {
			t.Fatal("range delimiter or space encoding changed", u.RawQuery)
		}
	})
}

// fragmentedReader changes only delivery boundaries, never byte ordering.
// It guarantees progress and finite input; fuzzing needs no sockets or timeout.
type fragmentedReader struct {
	data, pattern []byte
	step          int
}

func (r *fragmentedReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	size := 1
	if len(r.pattern) != 0 {
		size += int(r.pattern[r.step%len(r.pattern)])
		r.step++
	}
	n := copy(p[:min(len(p), size)], r.data)
	r.data = r.data[n:]
	return n, nil
}

func fuzzFrameReader(wire, pattern []byte) *bufio.Reader {
	return bufio.NewReaderSize(&fragmentedReader{data: wire, pattern: pattern}, 16)
}

func FuzzHEOSFrameBoundaries(f *testing.F) {
	f.Add([]byte(`{"heos":{}}`), []byte(`[]`), []byte{0})
	f.Add([]byte("opaque\r\nbytes"), []byte("next"), []byte{1, 0, 7, 255})
	f.Add([]byte{}, []byte{}, []byte{})
	f.Add(bytes.Repeat([]byte{'x'}, 2048), bytes.Repeat([]byte{'y'}, 2047), []byte{255})
	f.Fuzz(func(t *testing.T, first, second, pattern []byte) {
		if len(first)+len(second)+len(pattern) > 4096 || len(pattern) > 64 {
			return
		}
		// Frame parsing is independent of JSON parsing. Construct nonempty
		// bodies with arbitrary bytes except CR/LF, then alter only framing.
		body := func(raw []byte) []byte {
			out := append([]byte{'x'}, raw...)
			for i, b := range out {
				if b == '\r' || b == '\n' {
					out[i] = '_'
				}
			}
			return out
		}
		a, b := body(first), body(second)
		wire := append(append(append(append([]byte{}, a...), '\r', '\n'), b...), '\r', '\n')
		r := fuzzFrameReader(wire, pattern)
		for _, want := range [][]byte{a, b} {
			got, err := readFrame(r, max(len(a), len(b)))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("fragmentation changed frame: got=%q want=%q err=%v", got, want, err)
			}
		}
		if _, err := readFrame(r, max(len(a), len(b))); !errors.Is(err, io.EOF) {
			t.Fatal("extra frame or missing EOF", err)
		}
		exact := append(append([]byte{}, a...), '\r', '\n')
		if got, err := readFrame(fuzzFrameReader(exact, pattern), len(a)); err != nil || !bytes.Equal(got, a) {
			t.Fatal("exact frame boundary rejected", len(a), err)
		}
		if len(a) > 1 {
			if _, err := readFrame(fuzzFrameReader(exact, pattern), len(a)-1); !errors.Is(err, ErrProtocol) {
				t.Fatal("oversized frame accepted", len(a), err)
			}
		}
		for _, suffix := range []string{"", "\r"} {
			truncated := append(append([]byte{}, a...), suffix...)
			if _, err := readFrame(fuzzFrameReader(truncated, pattern), len(a)); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatal("truncated frame not reported", suffix, err)
			}
		}
		for _, suffix := range []string{"\n", "\rX\n"} {
			broken := append(append([]byte{}, a...), suffix...)
			if _, err := readFrame(fuzzFrameReader(broken, pattern), len(a)+1); !errors.Is(err, ErrProtocol) {
				t.Fatal("broken CRLF accepted", suffix, err)
			}
		}
	})
}

// Fuzz mutation inputs stay small. This ordinary regression exercises the
// configured 256 KiB limit rather than substituting a reduced test-only limit.
func TestHEOSDefaultFrameByteBoundary(t *testing.T) {
	if DefaultFrameBytes != 256*1024 {
		t.Fatal("review the frame-boundary regression for the changed default")
	}
	const prefix = `{"heos":{"command":"player/get_players","result":"success"},"payload":"`
	const suffix = `"}`
	for _, extra := range []int{0, 1} {
		frame := []byte(prefix + strings.Repeat("x", DefaultFrameBytes-len(prefix)-len(suffix)+extra) + suffix)
		wire := append(frame, '\r', '\n')
		got, err := readFrame(fuzzFrameReader(wire, []byte{255, 0, 31}), DefaultFrameBytes)
		if extra == 1 {
			if !errors.Is(err, ErrProtocol) {
				t.Fatal("actual oversized HEOS frame accepted", err)
			}
			continue
		}
		if err != nil || len(got) != DefaultFrameBytes || !bytes.Equal(got, frame) {
			t.Fatal("actual maximum HEOS frame rejected", len(got), err)
		}
		if _, err := decodeResponse(got); err != nil {
			t.Fatal("maximum-sized valid response could not be decoded", err)
		}
	}
}
