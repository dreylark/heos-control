package heos

import (
	"context"
	"net/url"
)

// Notify consumes the existing event stream after synchronous ownership checks.
// Complete scalar events update the observed projection, never full readback.
// Missing history or incomplete data requires the ordinary full reconciliation.
func (o *Observer) Notify(e Event) {
	e = e.Decode()
	data := e.Data()
	if !e.Gap && data.Kind == EventProgress && data.Valid {
		return // Progress can carry a newer Write stamp without a control event.
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	s := &o.last
	if !e.Gap && coveredEvent(e.Token, s.Token) {
		return // A foreground read or a later event already incorporated it.
	}
	if !e.Gap && sameGlobal(e.Token, s.Token) && data.Player != "" && data.Player != s.Player.ID {
		return
	}
	view := o.client.PlayerView(s.Player.ID)
	age := o.now().Sub(s.ObservedAt)
	continuous := !e.Gap && !o.invalid && view.Connected && sameGlobal(view.Token, s.Token) &&
		sameGlobal(e.Token, s.Token) && e.Token.Player == s.Token.Player+1 &&
		data.Player == s.Player.ID && data.Valid && age >= 0 && age < o.ttl
	if continuous && e.Token.Write != s.Token.Write {
		o.client.mu.Lock()
		write := o.client.writes[s.Player.ID]
		o.client.mu.Unlock()
		continuous = e.Token.Write == s.Token.Write+1 && write.revision == e.Token.Write
		if continuous {
			o.writePending = changedWriteFields(*s, write.mutation)
		}
	}
	if continuous && applyObservedEvent(s, e) {
		s.Token = e.Token
		s.EventUpdated = true
		o.writePending &^= observedEventFields(data.Kind)
		o.signalChanged()
		if data.Kind == EventNowPlaying {
			o.mediaPending = true
			o.requestRefresh()
		}
		return
	}
	o.invalid = true
	o.signalChanged()
	o.requestRefresh()
}

func coveredEvent(event, snapshot Token) bool {
	if event.Generation != snapshot.Generation {
		return event.Generation < snapshot.Generation
	}
	if event.Revision != snapshot.Revision {
		return event.Revision < snapshot.Revision
	}
	return event.Catalog == snapshot.Catalog && event.Player <= snapshot.Player && event.Write <= snapshot.Write
}

// Write revisions fence commands separately from the actual event history.
// An unrelated event cannot make an unconfirmed setter's old value current.
type observedFields uint8

const (
	observedVolume observedFields = 1 << iota // Denon 5.9 supplies volume AND mute.
	observedRepeat
	observedShuffle
	observedOther
)

func changedWriteFields(s Snapshot, m Mutation) observedFields {
	switch m.Kind {
	case "volume", "mute":
		return observedVolume
	case "mode":
		var fields observedFields
		if s.Repeat != m.Repeat {
			fields |= observedRepeat
		}
		if s.Shuffle != m.Shuffle {
			fields |= observedShuffle
		}
		return fields
	default:
		return observedOther
	}
}

func observedEventFields(kind EventKind) observedFields {
	switch kind {
	case EventVolume:
		return observedVolume
	case EventRepeat:
		return observedRepeat
	case EventShuffle:
		return observedShuffle
	}
	return 0
}

// Denon 5.4, 5.9–5.11 supply complete values; 5.5 only identifies the player.
// Parse every required value before changing any field. Other events fall back
// to full reads instead of guessing queue, identity, grouping or source state.
func applyObservedEvent(s *Snapshot, e Event) bool {
	d := e.Data()
	if !d.Valid {
		return false
	}
	switch d.Kind {
	case EventState:
		s.State = d.State
	case EventVolume:
		s.Volume, s.Muted = &d.Volume, &d.Muted
	case EventRepeat:
		s.Repeat = d.Repeat
	case EventShuffle:
		s.Shuffle = d.Shuffle
	case EventNowPlaying:
		// Metadata is read by the existing coalesced observer loop.
	default:
		return false
	}
	return true
}

// Called with the observation gate held. A metadata refresh does not verify the
// full queue/control state or renew ObservedAt and the periodic full-read timer.
func (o *Observer) refreshMedia(ctx context.Context, s Snapshot) (err error) {
	defer func() {
		if err != nil {
			o.mu.Lock()
			o.invalid = true
			o.signalChanged()
			o.mu.Unlock()
		}
	}()
	o.recordObservation(ctx, "media")
	r, err := o.client.Read(ctx, "player/get_now_playing_media", url.Values{"pid": {string(s.Player.ID)}})
	if err != nil {
		return err
	}
	media, err := payload[Media](r)
	if err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	view := o.client.PlayerView(s.Player.ID)
	if o.invalid || o.last.Token != s.Token || r.Token != s.Token || view.Token != s.Token || !view.Connected {
		return ErrStale
	}
	s.Media = &media
	s.EventUpdated = true
	o.last = s
	o.mediaPending = false
	o.signalChanged()
	return nil
}
