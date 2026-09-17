package heos

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

type fuzzEventField struct {
	key, value string
	encodedKey bool
}

type fuzzProjectionCase struct {
	command string
	fields  []fuzzEventField
	valid   bool
	project bool
	want    Snapshot
}

// Deliberately hand-built state: expected projections do not call production
// clone/membership/decision helpers. Non-scalar fields act as mutation sentinels.
func fuzzProjectionSnapshot() Snapshot {
	volume, muted, total := 12, false, 1
	media := Media{Source: "1024", ID: "track%26one", QueueID: "7", Song: "synthetic"}
	return Snapshot{
		Key: "synthetic", Player: Player{ID: "-9007199254740993", Serial: "synthetic-serial", Model: "synthetic-model"},
		Groups: []Group{{ID: "group", Players: []GroupMember{{ID: "other", Role: "leader"}}}},
		State:  "stop", Volume: &volume, Muted: &muted, Repeat: "off", Shuffle: false,
		Media: &media, Queue: QueuePage{Items: []Media{media}, Total: &total},
		ObservedAt: time.Unix(1700000000, 0).UTC(), Token: Token{Generation: 3, Revision: 7, Catalog: 2, Player: 4, Write: 1},
		Connected: true, Verified: true,
	}
}

// Denon 5.4–5.11 supplies scalar values or player identity. Unknown is the
// separately documented Home 150 loading state, not an invented Denon enum.
func makeFuzzProjectionCase(selector uint8) fuzzProjectionCase {
	c := fuzzProjectionCase{valid: true, project: true, want: fuzzProjectionSnapshot(), fields: []fuzzEventField{{key: "pid", value: "-9007199254740993"}}}
	add := func(key, value string) { c.fields = append(c.fields, fuzzEventField{key: key, value: value}) }
	switch selector % 8 {
	case 0:
		c.command = "event/player_state_changed"
		state := []string{"play", "pause", "stop", "unknown"}[(selector/8)%4]
		add("state", state)
		c.want.State = state
	case 1:
		c.command = "event/player_volume_changed"
		level, mute := int(selector)%101, selector&16 != 0
		add("level", strconv.Itoa(level))
		if mute {
			add("mute", "on")
		} else {
			add("mute", "off")
		}
		c.want.Volume, c.want.Muted = &level, &mute
	case 2:
		c.command = "event/repeat_mode_changed"
		repeat := []string{"off", "on_one", "on_all"}[(selector/8)%3]
		add("repeat", repeat)
		c.want.Repeat = repeat
	case 3:
		c.command = "event/shuffle_mode_changed"
		shuffle := selector&8 != 0
		if shuffle {
			add("shuffle", "on")
		} else {
			add("shuffle", "off")
		}
		c.want.Shuffle = shuffle
	case 4:
		c.command = "event/player_now_playing_changed"
	case 5:
		c.command, c.project = "event/player_now_playing_progress", false
		add("cur_pos", "9223372036854775808")
		add("duration", "0") // HEOS may not know a stream's total duration.
	case 6:
		c.command, c.project = "event/player_queue_changed", false
	default:
		c.command, c.valid, c.project = "event/future_state_changed", false, false
		add("state", "play")
	}
	return c
}

func fuzzEventEnvelope(t *testing.T, command string, fields []fuzzEventField, extra string) []byte {
	t.Helper()
	escape := func(value string) string {
		// Escape HEOS separators, but retain a literal '+' to test Denon
		// percent decoding independently of HTML form decoding (3.2).
		return strings.ReplaceAll(strings.ReplaceAll(url.QueryEscape(value), "+", "%20"), "%2B", "+")
	}
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		key := field.key
		if !field.encodedKey {
			key = escape(key)
		}
		parts = append(parts, key+"="+escape(field.value))
	}
	raw, err := json.Marshal(map[string]any{"heos": map[string]string{"command": command, "message": strings.Join(parts, "&") + extra}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func FuzzHEOSEventProjection(f *testing.F) {
	for selector := range uint8(8) {
		for mutation := range uint8(8) {
			f.Add(selector, mutation, "A+B%26&pid=other")
		}
	}
	f.Add(uint8(24), uint8(0), "") // Firmware unknown remains valid state.
	f.Add(uint8(1), uint8(4), "Music")
	f.Add(uint8(1), uint8(7), "mute=off")
	f.Add(uint8(5), uint8(7), "duration=18446744073709551616")
	f.Fuzz(func(t *testing.T, selector, mutation uint8, value string) {
		if len(value) > 4096 {
			return
		}
		c := makeFuzzProjectionCase(selector)
		mode := mutation % 8
		required := int(selector/8) % len(c.fields)
		wantValid, wantProject := c.valid, c.project
		duplicate, extra := false, ""
		switch mode {
		case 1: // One missing required field must never become confirmation.
			c.fields = append(c.fields[:required], c.fields[required+1:]...)
			wantValid, wantProject = false, false
		case 2, 6: // Even identical values remain ambiguous on the wire.
			field := c.fields[required]
			if mode == 6 {
				field.key, field.encodedKey = fmt.Sprintf("%%%02x%s", field.key[0], field.key[1:]), true
			}
			c.fields = append(c.fields, field)
			duplicate = true
		case 3:
			if len(c.fields) == 1 {
				c.fields[0].value = "" // Empty identity on media/queue event.
			} else {
				c.fields[1].value = "!" + value // Outside every scalar domain.
			}
			wantValid, wantProject = false, false
		case 4:
			c.fields = append(c.fields, fuzzEventField{key: "unrelated", value: value})
		case 5:
			wantValid, wantProject = false, false
		case 7:
			extra = "&" + value // Fuzz message grammar around a valid envelope.
		}
		response, err := decodeResponse(fuzzEventEnvelope(t, c.command, c.fields, extra))
		if duplicate {
			if err == nil {
				t.Fatal("duplicate or percent-aliased required field accepted")
			}
			return
		}
		if err != nil {
			if mode == 7 || ((mode == 3 || mode == 4) && (!utf8.ValidString(value) || strings.ContainsRune(value, '\x00'))) {
				return // Rejection is permitted for malformed wire bytes.
			}
			t.Fatal("valid wire envelope rejected", err)
		}
		if !response.Event || response.Command != c.command {
			t.Fatal("event envelope changed identity", response)
		}
		e := (Event{Command: response.Command, Params: response.Params, Gap: mode == 5}).Decode()
		data := e.Data()
		if data.Valid != wantValid {
			t.Fatalf("mode=%d valid=%t want=%t: %+v", mode, data.Valid, wantValid, data)
		}
		if data.HasVolume && (data.Volume < 0 || data.Volume > 100) {
			t.Fatal("partial evidence includes invalid volume", data)
		}
		// A later mutation of raw fields cannot repair invalid data or change
		// the evidence shared by ownership, observation and diagnostics.
		for _, key := range []string{"pid", "state", "level", "mute", "repeat", "shuffle"} {
			e.Params.Set(key, value)
		}
		if e.Data() != data {
			t.Fatal("decoded evidence aliases mutable wire fields")
		}
		got := fuzzProjectionSnapshot()
		want := fuzzProjectionSnapshot()
		if wantProject {
			want = c.want
		}
		if applied := applyObservedEvent(&got, e); applied != wantProject || !reflect.DeepEqual(got, want) {
			t.Fatalf("incorrect or partial projection: applied=%t want=%t\ngot=%+v\nwant=%+v", applied, wantProject, got, want)
		}
		if applied := applyObservedEvent(&got, e); applied != wantProject || !reflect.DeepEqual(got, want) {
			t.Fatal("repeating current values changed projection/age/identity/queue")
		}
		gap := e
		gap.Gap = true
		if gap.Data().Valid || applyObservedEvent(&got, gap) || !reflect.DeepEqual(got, want) {
			t.Fatal("gap confirmed state or changed the projection")
		}
	})
}
