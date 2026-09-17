package heos

import (
	"net/url"
	"testing"
)

// Denon 5.4–5.12: scalar events carry values, media/queue events only identity.
func TestEventData(t *testing.T) {
	for _, tc := range []struct {
		name, command, message string
		valid                  bool
	}{
		{"state", "player_state_changed", "pid=-9007199254740993&state=play", true},
		{"firmware-unknown", "player_state_changed", "pid=1&state=unknown", true},
		{"bad-state", "player_state_changed", "pid=1&state=buffering", false},
		{"missing-state", "player_state_changed", "pid=1", false},
		{"duplicate-state", "player_state_changed", "pid=1&state=play&state=stop", false},
		{"missing-player", "player_state_changed", "state=play", false},
		{"duplicate-player", "player_state_changed", "pid=1&pid=2&state=play", false},
		{"volume-zero", "player_volume_changed", "pid=1&level=0&mute=off", true},
		{"volume-max", "player_volume_changed", "pid=1&level=100&mute=on", true},
		{"volume-overflow", "player_volume_changed", "pid=1&level=999999999999999999999999&mute=off", false},
		{"volume-negative", "player_volume_changed", "pid=1&level=-1&mute=off", false},
		{"volume-high", "player_volume_changed", "pid=1&level=101&mute=off", false},
		{"volume-missing", "player_volume_changed", "pid=1&mute=off", false},
		{"mute-missing", "player_volume_changed", "pid=1&level=10", false},
		{"mute-invalid", "player_volume_changed", "pid=1&level=10&mute=false", false},
		{"volume-duplicate", "player_volume_changed", "pid=1&level=10&level=10&mute=off", false},
		{"mute-duplicate", "player_volume_changed", "pid=1&level=10&mute=on&mute=off", false},
		{"repeat", "repeat_mode_changed", "pid=1&repeat=on_one", true},
		{"repeat-all", "repeat_mode_changed", "pid=1&repeat=on_all", true},
		{"repeat-off", "repeat_mode_changed", "pid=1&repeat=off", true},
		{"repeat-invalid", "repeat_mode_changed", "pid=1&repeat=unknown", false},
		{"shuffle", "shuffle_mode_changed", "pid=1&shuffle=on", true},
		{"shuffle-off", "shuffle_mode_changed", "pid=1&shuffle=off", true},
		{"shuffle-invalid", "shuffle_mode_changed", "pid=1&shuffle=yes", false},
		{"media", "player_now_playing_changed", "pid=1", true},
		{"queue", "player_queue_changed", "pid=1", true},
		{"progress", "player_now_playing_progress", "pid=1&cur_pos=1&duration=2", true},
		{"progress-unknown-duration", "player_now_playing_progress", "pid=1&cur_pos=10&duration=0", true},
		{"progress-past-end", "player_now_playing_progress", "pid=1&cur_pos=3&duration=2", false},
		{"progress-missing", "player_now_playing_progress", "pid=1&cur_pos=0", false},
		{"progress-overflow", "player_now_playing_progress", "pid=1&cur_pos=18446744073709551616&duration=0", false},
		{"progress-negative", "player_now_playing_progress", "pid=1&cur_pos=-1&duration=0", false},
		{"progress-duplicate", "player_now_playing_progress", "pid=1&cur_pos=1&cur_pos=2&duration=2", false},
		{"playback-error", "player_playback_error", "pid=1&error=private-text", true},
		{"playback-error-missing", "player_playback_error", "pid=1", false},
		{"group-volume", "group_volume_changed", "gid=1&level=10&mute=off", true},
		{"group-volume-missing-group", "group_volume_changed", "level=10&mute=off", false},
		{"sources", "sources_changed", "", true},
		{"players", "players_changed", "", true},
		{"groups", "groups_changed", "", true},
		{"user-in", "user_changed", "signed_in&un=private-user", true},
		{"user-out", "user_changed", "signed_out", true},
		{"user-invalid", "user_changed", "", false},
		{"unknown", "future_event", "pid=1&level=10&mute=off", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params, err := url.ParseQuery(tc.message)
			if err != nil {
				t.Fatal(err)
			}
			d := (Event{Command: "event/" + tc.command, Params: params}).Data()
			if d.Valid != tc.valid {
				t.Fatalf("valid=%t, want %t: %+v", d.Valid, tc.valid, d)
			}
			if tc.name == "unknown" && d.Kind != EventUnknown {
				t.Fatal("unknown event was attributed", d)
			}
		})
	}
}

func TestEventDataKeepsOnlyValidPartialEvidence(t *testing.T) {
	d := (Event{Command: "event/player_volume_changed", Params: url.Values{
		"pid": {"1"}, "level": {"10"}, "mute": {"invalid"}, "state": {"play"},
	}}).Data()
	if d.Valid || d.Kind != EventVolume || !d.HasVolume || d.Volume != 10 || d.HasMute || d.State != "" {
		t.Fatal("lost partial evidence or invented fields", d)
	}
	d = (Event{Command: "event/player_volume_changed", Params: url.Values{
		"pid": {"1", "2"}, "level": {"10", "20"}, "mute": {"off"},
	}}).Data()
	if d.Valid || d.Player != "" || d.HasVolume || !d.HasMute || d.Muted {
		t.Fatal("ambiguous field accepted or valid evidence lost", d)
	}
}

func TestDecodedEventDoesNotAliasWireParameters(t *testing.T) {
	params := url.Values{"pid": {"1"}, "level": {"10"}, "mute": {"off"}}
	e := (Event{Command: "event/player_volume_changed", Params: params}).Decode()
	params.Set("level", "99")
	data := e.Data()
	data.Volume = 80
	if e.Data().Volume != 10 || !e.Data().Valid {
		t.Fatal("parsed event changed with caller-owned values", e.Data())
	}
	gap := e
	gap.Gap = true
	if gap.Data().Valid {
		t.Fatal("gap with cached fields became confirmation")
	}
	params.Del("mute")
	partial := (Event{Command: "event/player_volume_changed", Params: params}).Decode()
	params.Set("mute", "off")
	if partial.Data().Valid || partial.Data().HasMute || !partial.Data().HasVolume {
		t.Fatal("later raw values repaired an incomplete event", partial.Data())
	}
}

func TestTransportSharesDecodedEventWithBothConsumers(t *testing.T) {
	c := &Client{events: make(chan Event, 1), players: map[ID]uint64{"1": 0}}
	params := url.Values{"pid": {"1"}, "level": {"10"}, "mute": {"off"}}
	var synchronous EventData
	c.SetEventHandler(func(e Event) {
		if !e.decoded {
			t.Fatal("ownership callback received an unparsed event")
		}
		synchronous = e.Data()
		// A consumer must not cause another to see different parsed evidence.
		e.Params.Set("level", "99")
	})
	c.event(Response{Command: "event/player_volume_changed", Params: params})
	e := <-c.Events()
	if !e.decoded || !e.Data().Valid || e.Data() != synchronous || synchronous.Volume != 10 || e.Token.Player != 1 {
		t.Fatal("event evidence or revision differs between consumers", e)
	}
}
