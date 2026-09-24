package control

import (
	"log/slog"
	"net/url"
	"strconv"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

// Event attribution is separate from command execution and readback. Events
// may confirm a pending change, report unchanged state, or revoke ownership.
func (r *execution) event(e heos.Event) {
	e = e.Decode()
	data := e.Data()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !e.Gap && data.Player != "" && data.Player != r.expected.Player.ID {
		switch data.Kind {
		case heos.EventState, heos.EventNowPlaying, heos.EventPlaybackError, heos.EventQueue, heos.EventVolume, heos.EventRepeat, heos.EventShuffle, heos.EventProgress:
			return
		}
	}
	if !e.Gap && data.Kind == heos.EventProgress && data.Valid {
		return
	}
	continuous := e.Token.Generation == r.expected.Token.Generation && e.Token.Revision == r.expected.Token.Revision && e.Token.Catalog == r.expected.Token.Catalog
	targetEvent := !e.Gap && continuous && data.Valid && data.Player == r.expected.Player.ID
	if targetEvent && (r.acceptExpectedEvent(e) || r.acceptQueueEvent(e)) {
		r.expected.Token = e.Token
		return
	}
	list := r.events[e.Command]
	if r.inflight {
		r.uncertain = true
	}
	reason := "unexpected_event"
	if e.Gap {
		reason = "event_gap"
	}
	wanted := []heos.Event{}
	for _, params := range list {
		values := url.Values{}
		for k, v := range params {
			values.Set(k, v)
		}
		wanted = append(wanted, heos.Event{Command: e.Command, Params: values})
	}
	// Capture safe immutable details while holding the event/state mutex.
	expected := make([]slog.Attr, len(wanted))
	for i, event := range wanted {
		expected[i] = slog.Any(strconv.Itoa(i), event.LogValue())
	}
	observed, fields := eventScalars(e)
	at := r.now().UTC()
	evidence := journal.Diagnostics{Source: "event", Phase: r.phase, DetectedAt: &at,
		ChangedFields: fields, Expected: snapshotScalars(r.expected), Observed: observed}
	if observed != nil {
		evidence.ChangedFields = scalarChanges(evidence.Expected, observed)
	}
	if e.Gap {
		evidence.ChangedFields, evidence.Observed = nil, nil
	}
	r.cancel(&ownershipError{reason: reason, attrs: []slog.Attr{slog.Any("event", e.LogValue()), slog.Any("expected_events", slog.GroupValue(expected...)),
		slog.Int("expected_event_count", len(expected)), slog.Any("expected", snapshotLog(r.expected)),
		slog.Bool("event_inflight", r.inflight), slog.Bool("event_unconfirmed", r.unconfirmed)}, evidence: evidence})
}

// Call these attribution helpers with r.mu held, for a non-gap target event.
func (r *execution) acceptQueueEvent(e heos.Event) bool {
	d := decideQueueEvent(queuePolicyState{
		Owned: r.queueOwned, Automating: r.automating,
		ExpectedState: r.expected.State, WaitUntil: r.queueWait,
	}, e, r.now())
	r.queueWait = d.WaitUntil
	if d.Notify {
		r.notifyObservation()
	}
	return d.Action != queuePass
}

func (r *execution) acceptExpectedEvent(e heos.Event) bool {
	data := e.Data()
	if !data.Valid {
		return false
	}
	if data.Kind == heos.EventVolume || data.Kind == heos.EventRepeat || data.Kind == heos.EventShuffle {
		return r.acceptScalarEvent(e)
	}
	list := r.events[e.Command]
	for i, want := range list {
		match := true
		for k, v := range want {
			// Scalar expectations are handled above. The only remaining
			// value-bearing expectation is a validated transport state.
			if k != "state" || string(data.State) != v {
				match = false
			}
		}
		if match {
			// Home 150 can repeat loading stop notifications (Denon 5.4).
			// Keep this expectation until play or the confirmation deadline;
			// notification count does not identify a manual controller.
			if r.queueStart == queueAwaitingPlay && data.Kind == heos.EventState && data.State == heos.PlayStateStop {
				return true
			}
			// State/media/queue events can wake asynchronous confirmation.
			// They never replace the mandatory complete readback.
			switch data.Kind {
			case heos.EventState, heos.EventNowPlaying, heos.EventQueue:
				r.notifyObservation()
			}
			// Home 150 reports media both while loading and starting a new
			// queue (Denon 5.5). Fresh readback closes this window;
			// queue membership/current media must still be confirmed.
			if data.Kind != heos.EventNowPlaying && !r.keepExpectations {
				r.events[e.Command] = append(list[:i], list[i+1:]...)
			}
			if data.Kind == heos.EventState && data.State == heos.PlayStatePlay && !r.keepExpectations {
				// Once playback starts, even a stop during queue readback
				// is intervention, not the preceding loading state.
				r.events[e.Command] = nil
				if r.queueStart != queueNotStarting {
					r.queueStart = queuePlayObserved
				}
			}
			return true
		}
	}

	return false
}

// A late duplicate can arrive after readback closed the setter's expectation.
// Only the exact currently confirmed volume AND mute are unchanged state.
func confirmedVolumeEvent(e heos.Event, expected heos.Snapshot) bool {
	data := e.Data()
	if data.Kind != heos.EventVolume || !data.Valid || expected.Volume == nil || expected.Muted == nil {
		return false
	}
	return data.Volume == *expected.Volume && data.Muted == *expected.Muted
}

func (r *execution) expect(m heos.Mutation) {
	r.queueStart = queueNotStarting
	r.scalar = nil
	r.keepExpectations = false
	if scalarMutation(m) {
		r.scalar = newScalarConfirmation(m, r.expected)
	}
	add := func(name string, params map[string]string) {
		list := r.events[name]
		if len(list) >= 4 {
			list = list[1:]
		}
		r.events[name] = append(list, params)
	}
	switch m.Kind {
	case heos.MutationKindVolume, heos.MutationKindMute:
		level, muted := *r.expected.Volume, *r.expected.Muted
		if m.Kind == heos.MutationKindVolume {
			level = m.Level
		} else {
			muted = m.Muted
		}
		mute := "off"
		if muted {
			mute = "on"
		}
		add("event/player_volume_changed", map[string]string{"level": strconv.Itoa(level), "mute": mute})
		if m.Kind == heos.MutationKindVolume && muted {
			// Home 150 may clear mute as a side effect of setting volume.
			// Both outcomes still require readback of the requested level.
			add("event/player_volume_changed", map[string]string{"level": strconv.Itoa(level), "mute": "off"})
		}
	case heos.MutationKindTransport:
		add("event/player_state_changed", map[string]string{"state": string(m.State)})
		// Home 150 also refreshes now-playing metadata during transport.
		// This notification has no MID (Denon 5.5); readback verifies it.
		add("event/player_now_playing_changed", map[string]string{})
	case heos.MutationKindSkip:
		// Play next/previous can report stop or unknown before the new entry
		// (Denon 5.4/5.5). Keep every transitional state: duplicates and either
		// order must not look like a second controller. Readback still decides.
		r.keepExpectations = true
		add("event/player_now_playing_changed", map[string]string{})
		for _, state := range []heos.PlayState{heos.PlayStateStop, heos.PlayStateUnknown, heos.PlayStatePlay, heos.PlayStatePause} {
			add("event/player_state_changed", map[string]string{"state": string(state)})
		}
	case heos.MutationKindMode:
		add("event/repeat_mode_changed", map[string]string{"repeat": string(m.Repeat)})
		shuffle := "off"
		if m.Shuffle {
			shuffle = "on"
		}
		add("event/shuffle_mode_changed", map[string]string{"shuffle": shuffle})
	case heos.MutationKindQueue:
		r.queueStart = queueAwaitingPlay
		add("event/player_queue_changed", map[string]string{})
		add("event/player_now_playing_changed", map[string]string{})
		// Home 150 can pass through stop while replacing a paused queue.
		// This is a loading allowance, not permission to ignore a later Stop.
		if r.expected.State == heos.PlayStateStop || r.expected.State == heos.PlayStatePause {
			add("event/player_state_changed", map[string]string{"state": string(heos.PlayStateStop)})
		}
		add("event/player_state_changed", map[string]string{"state": string(heos.PlayStatePlay)})
	}
}
