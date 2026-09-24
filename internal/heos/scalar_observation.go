package heos

import (
	"context"
	"errors"
	"net/url"
	"strconv"
)

// RefreshScalars is a bounded fallback for a scalar command whose event did not
// arrive. It reads only the relevant controls and never repairs lost event
// history, an expired full observation or pending media/queue changes.
func (o *Observer) RefreshScalars(ctx context.Context, kind MutationKind) error {
	fields := scalarFields(kind)
	if fields == 0 {
		return ErrBounds
	}
	ctx, cancel := context.WithTimeout(ctx, o.client.cfg.CommandTimeout)
	defer cancel()
	select {
	case o.gate <- struct{}{}:
		defer func() { <-o.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	o.mu.Lock()
	s := cloneSnapshot(o.last)
	view := o.client.PlayerView(s.Player.ID)
	valid := o.scalarBaselineValid(s) && view.Connected && sameGlobal(view.Token, s.Token) &&
		view.Token.Player == s.Token.Player && view.Token.Write >= s.Token.Write && view.Token.Write-s.Token.Write <= 1 && o.writePending&^fields == 0
	if valid && view.Token.Write != s.Token.Write {
		o.client.mu.Lock()
		write := o.client.writes[s.Player.ID]
		o.client.mu.Unlock()
		valid = write.revision == view.Token.Write && scalarFields(write.mutation.Kind) == fields
	}
	o.mu.Unlock()
	if !valid {
		return ErrStale
	}
	o.recordObservation(ctx, "scalars")
	before := s.Token
	s.Token = view.Token
	read := func(command string) (Response, error) {
		r, err := o.client.Read(ctx, command, url.Values{"pid": {string(s.Player.ID)}})
		if err == nil && (r.Token != s.Token || o.client.PlayerView(s.Player.ID).Token != s.Token) {
			err = ErrStale
		}
		return r, err
	}
	if fields == observedVolume {
		r, err := read("player/get_volume") // Denon 4.2.6.
		if err != nil {
			return err
		}
		level, err := strconv.Atoi(r.Params.Get("level"))
		if err != nil || len(r.Params["level"]) != 1 || level < 0 || level > 100 {
			return ErrProtocol
		}
		r, err = read("player/get_mute") // Denon 4.2.10; volume may also clear mute.
		if err != nil {
			return err
		}
		mute := r.Params.Get("state")
		if len(r.Params["state"]) != 1 || (mute != "on" && mute != "off") {
			return ErrProtocol
		}
		muted := mute == "on"
		s.Volume, s.Muted = &level, &muted
	} else {
		r, err := read("player/get_play_mode") // Denon 4.2.13 returns both controls.
		if err != nil {
			return err
		}
		repeat, shuffle := Repeat(r.Params.Get("repeat")), r.Params.Get("shuffle")
		if len(r.Params["repeat"]) != 1 || len(r.Params["shuffle"]) != 1 ||
			!repeat.Known() || (shuffle != "on" && shuffle != "off") {
			return ErrProtocol
		}
		s.Repeat, s.Shuffle = repeat, shuffle == "on"
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	view = o.client.PlayerView(s.Player.ID)
	if !o.scalarBaselineValid(o.last) || o.last.Token != before || !view.Connected || view.Token != s.Token {
		return ErrStale
	}
	s.Playhead = o.playheadForMedia(s.Media, s.State)
	s.EventUpdated = true
	o.last = s
	o.writePending &^= fields
	o.signalChanged()
	return nil
}

func scalarFields(kind MutationKind) observedFields {
	switch kind {
	case MutationKindVolume, MutationKindMute:
		return observedVolume
	case MutationKindMode:
		return observedRepeat | observedShuffle
	}
	return 0
}

// ConfirmScalars publishes the coordinator's synchronous event confirmation
// before the ordinary event consumer catches up. The caller must have confirmed
// the command reply plus its required events (or an unchanged target). Only
// scalar fields are transferred; identity, queue/media and full age are retained.
// An unresolved media/queue event must not be included in the supplied token.
func (o *Observer) ConfirmScalars(s Snapshot) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	view := o.client.PlayerView(o.last.Player.ID)
	if !o.scalarBaselineValid(o.last) || !view.Connected || view.Token != s.Token ||
		!sameGlobal(o.last.Token, s.Token) || s.Player.ID != o.last.Player.ID || s.Player.Serial != o.last.Player.Serial ||
		o.last.ObservedAt.After(s.ObservedAt) || o.writePending&observedOther != 0 {
		return ErrStale
	}
	if o.last.Token.Write != s.Token.Write {
		o.client.mu.Lock()
		write := o.client.writes[s.Player.ID]
		o.client.mu.Unlock()
		if write.revision != s.Token.Write || scalarFields(write.mutation.Kind) == 0 {
			return ErrStale
		}
	}
	if s.Volume == nil || *s.Volume < 0 || *s.Volume > 100 || s.Muted == nil ||
		!s.Repeat.Known() {
		return ErrProtocol
	}
	o.last.Volume, o.last.Muted = clonePtr(s.Volume), clonePtr(s.Muted)
	o.last.Repeat, o.last.Shuffle = s.Repeat, s.Shuffle
	o.last.Token, o.last.EventUpdated = s.Token, true
	o.writePending = 0
	o.signalChanged()
	return nil
}

// Caller holds o.mu. Scalar updates cannot renew a complete observation or
// conceal the absence of media, queue, grouping or physical identity evidence.
func (o *Observer) scalarBaselineValid(s Snapshot) bool {
	return !o.mediaPending && o.projectionBaselineValid(s)
}

func (o *Observer) projectionBaselineValid(s Snapshot) bool {
	age := o.now().Sub(s.ObservedAt)
	return !o.invalid && age >= 0 && age < o.ttl && s.Player.ID != "" &&
		string(s.Player.Serial) == o.identity.Serial
}

// Reconcile shares the observer's gate and its metadata-only path. A valid
// projection needs no GET; the five-minute audit still performs a full read.
func (o *Observer) Reconcile(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, o.client.cfg.CommandTimeout)
	defer cancel()
	for {
		if err := o.waitForConsumer(ctx); err != nil {
			return err
		}
		trigger := "manual"
		o.mu.Lock()
		if o.now().Sub(o.last.ObservedAt) >= IdleObservationInterval {
			trigger = "audit"
		} else if o.invalid || o.mediaPending {
			trigger = "event"
		}
		o.mu.Unlock()
		err := o.refresh(defaultObservationTrigger(ctx, trigger), false)
		if errors.Is(err, ErrStale) {
			o.mu.Lock()
			invalid := o.invalid
			o.mu.Unlock()
			if !invalid {
				continue // An event raced the gate; wait for its ordinary consumer.
			}
		}
		return err
	}
}

func (o *Observer) waitForConsumer(ctx context.Context) error {
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		o.mu.Lock()
		s, changed := o.last, o.changed
		view, age := o.client.PlayerView(s.Player.ID), o.now().Sub(s.ObservedAt)
		lagging := !o.invalid && view.Connected && sameGlobal(view.Token, s.Token) && age >= 0 && age < o.ttl &&
			(view.Token != s.Token || o.writePending != 0)
		o.mu.Unlock()
		if !lagging {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// Caller holds o.mu. A close-and-replace notification wakes every bounded
// foreground waiter without adding another event reader or a polling loop.
func (o *Observer) signalChanged() {
	close(o.changed)
	o.changed = make(chan struct{})
}
