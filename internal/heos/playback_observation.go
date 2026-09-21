package heos

import (
	"context"
	"net/url"
)

// RefreshPlayback reconciles a pending transition inside a previously confirmed
// queue. Denon 5.4 supplies state and 5.5 only a PID, so missing transition events
// need just Get Play State (4.2.3) and Get Now Playing Media (4.2.5). Queue/global
// invalidation or a new write cannot be repaired by this targeted read.
func (o *Observer) RefreshPlayback(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, o.client.cfg.CommandTimeout)
	defer cancel()
	if err := o.waitForConsumer(ctx); err != nil {
		return err
	}
	if o.now().Sub(o.Snapshot().ObservedAt) >= IdleObservationInterval {
		return o.refresh(defaultObservationTrigger(ctx, "audit"), false) // A due full audit already supplies state/media.
	}
	select {
	case o.gate <- struct{}{}:
		defer func() { <-o.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	o.mu.Lock()
	s := cloneSnapshot(o.last)
	view := o.client.PlayerView(s.Player.ID)
	valid := o.projectionBaselineValid(s) && o.writePending == 0 && view.Connected && view.Token == s.Token
	o.mu.Unlock()
	if !valid {
		return ErrStale
	}
	o.recordObservation(ctx, "playback")
	read := func(command string) (Response, error) {
		r, err := o.client.Read(ctx, command, url.Values{"pid": {string(s.Player.ID)}})
		if err == nil && (r.Token != s.Token || o.client.PlayerView(s.Player.ID).Token != s.Token) {
			err = ErrStale
		}
		return r, err
	}
	r, err := read("player/get_play_state")
	if err != nil {
		return err
	}
	state := r.Params.Get("state")
	if len(r.Params["state"]) != 1 || (state != "play" && state != "pause" && state != "stop" && state != "unknown") {
		return ErrProtocol
	}
	r, err = read("player/get_now_playing_media")
	if err != nil {
		return err
	}
	media, err := payload[Media](r)
	if err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	view = o.client.PlayerView(s.Player.ID)
	if !o.projectionBaselineValid(o.last) || o.writePending != 0 || o.last.Token != s.Token || !view.Connected || view.Token != s.Token {
		return ErrStale
	}
	s.State, s.Media, s.EventUpdated = state, &media, true
	s.MediaStale = state != "play" && state != "pause"
	o.last, o.mediaPending = s, false
	o.signalChanged()
	return nil
}
