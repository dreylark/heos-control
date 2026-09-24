package heos

// PlayState is a player/get_play_state token. Absent is the zero value: the
// field was missing, rejected, or this command does not carry a play state.
// Unknown is a real device report, not the zero value.
type PlayState string

const (
	PlayStateAbsent  PlayState = ""
	PlayStatePlay    PlayState = "play"
	PlayStatePause   PlayState = "pause"
	PlayStateStop    PlayState = "stop"
	PlayStateUnknown PlayState = "unknown"
)

// Active reports Play or Pause, the states that keep a media observation current.
func (s PlayState) Active() bool {
	return s == PlayStatePlay || s == PlayStatePause
}

// Known reports a value the device is allowed to send, including firmware unknown.
func (s PlayState) Known() bool {
	switch s {
	case PlayStatePlay, PlayStatePause, PlayStateStop, PlayStateUnknown:
		return true
	default:
		return false
	}
}

// Writable reports a player/set_play_state argument. Unknown is observation-only.
func (s PlayState) Writable() bool {
	switch s {
	case PlayStatePlay, PlayStatePause, PlayStateStop:
		return true
	default:
		return false
	}
}

// Repeat is a player/get_play_mode repeat token. Absent is the zero value.
type Repeat string

const (
	RepeatAbsent Repeat = ""
	RepeatOff    Repeat = "off"
	RepeatOnAll  Repeat = "on_all"
	RepeatOnOne  Repeat = "on_one"
)

// Known reports a repeat value the device is allowed to send or accept.
func (r Repeat) Known() bool {
	switch r {
	case RepeatOff, RepeatOnAll, RepeatOnOne:
		return true
	default:
		return false
	}
}

// MutationKind is one explicit HEOS write. Absent is the zero value and is not a command.
type MutationKind string

const (
	MutationKindAbsent    MutationKind = ""
	MutationKindVolume    MutationKind = "volume"
	MutationKindMute      MutationKind = "mute"
	MutationKindTransport MutationKind = "transport"
	MutationKindSkip      MutationKind = "skip"
	MutationKindMode      MutationKind = "mode"
	MutationKindQueue     MutationKind = "queue"
)
