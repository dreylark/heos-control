package heos

import (
	"net/url"
	"strconv"
)

// EventKind identifies the Denon 5.1–5.13 notification independently of its
// parameters. Unknown notifications remain visible and cannot confirm state.
type EventKind string

const (
	EventUnknown       EventKind = ""
	EventSources       EventKind = "event/sources_changed"
	EventPlayers       EventKind = "event/players_changed"
	EventGroups        EventKind = "event/groups_changed"
	EventState         EventKind = "event/player_state_changed"
	EventNowPlaying    EventKind = "event/player_now_playing_changed"
	EventProgress      EventKind = "event/player_now_playing_progress"
	EventPlaybackError EventKind = "event/player_playback_error"
	EventQueue         EventKind = "event/player_queue_changed"
	EventVolume        EventKind = "event/player_volume_changed"
	EventRepeat        EventKind = "event/repeat_mode_changed"
	EventShuffle       EventKind = "event/shuffle_mode_changed"
	EventGroupVolume   EventKind = "event/group_volume_changed"
	EventUser          EventKind = "event/user_changed"
)

// EventData contains only validated fields, copied by value. Valid requires all
// fields of a known event; partial values are diagnostic evidence, never command
// confirmation. Empty state/repeat and Has* distinguish absence from zero/off.
// Free-form account and playback-error text stays outside this projection.
type EventData struct {
	Kind       EventKind
	Player     ID
	State      string
	Volume     int
	Muted      bool
	Repeat     string
	Shuffle    bool
	HasVolume  bool
	HasMute    bool
	HasShuffle bool
	Valid      bool
}

// Decode freezes the parsed fields before one event reaches multiple consumers.
// Raw Params remain available for allowlisted wire diagnostics only. Synthetic
// events can use the same boundary without constructing internal cache fields.
func (e Event) Decode() Event {
	if !e.decoded {
		e.data = decodeEventData(e.Command, e.Params)
		e.decoded = true
	}
	return e
}

// Data returns a value, so a consumer cannot change another consumer's evidence.
func (e Event) Data() EventData {
	d := e.Decode().data
	if e.Gap {
		d.Valid = false
	}
	return d
}

func decodeEventData(command string, params url.Values) EventData {
	one := func(key string) string {
		if len(params[key]) == 1 {
			return params.Get(key)
		}
		return ""
	}
	choice := func(key string, allowed ...string) string {
		value := one(key)
		for _, want := range allowed {
			if value == want {
				return value
			}
		}
		return ""
	}
	d := EventData{Kind: EventKind(command), Player: ID(one("pid"))}
	switch d.Kind {
	case EventSources, EventPlayers, EventGroups:
		d.Valid = true
	case EventState:
		// Home 150 reports unknown during loading; see docs/DEVICE_COMPATIBILITY.md.
		d.State = choice("state", "play", "pause", "stop", "unknown")
		d.Valid = d.Player != "" && d.State != ""
	case EventNowPlaying, EventQueue:
		d.Valid = d.Player != ""
	case EventProgress:
		position, posErr := strconv.ParseUint(one("cur_pos"), 10, 64)
		duration, durErr := strconv.ParseUint(one("duration"), 10, 64)
		d.Valid = d.Player != "" && posErr == nil && durErr == nil && (duration == 0 || position <= duration)
	case EventPlaybackError:
		d.Valid = d.Player != "" && one("error") != ""
	case EventVolume, EventGroupVolume:
		level, err := strconv.Atoi(one("level"))
		if err == nil && level >= 0 && level <= 100 {
			d.Volume, d.HasVolume = level, true
		}
		if mute := choice("mute", "on", "off"); mute != "" {
			d.Muted, d.HasMute = mute == "on", true
		}
		identity := d.Player != ""
		if d.Kind == EventGroupVolume {
			identity = one("gid") != ""
		}
		d.Valid = identity && d.HasVolume && d.HasMute
	case EventRepeat:
		d.Repeat = choice("repeat", "off", "on_one", "on_all")
		d.Valid = d.Player != "" && d.Repeat != ""
	case EventShuffle:
		if shuffle := choice("shuffle", "on", "off"); shuffle != "" {
			d.Shuffle, d.HasShuffle = shuffle == "on", true
		}
		d.Valid = d.Player != "" && d.HasShuffle
	case EventUser:
		signedIn, signedOut := len(params["signed_in"]) == 1, len(params["signed_out"]) == 1
		d.Valid = signedIn != signedOut && (!signedIn || one("un") != "")
	default:
		d.Kind = EventUnknown
	}
	return d
}
