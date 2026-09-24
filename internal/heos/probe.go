package heos

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"sync/atomic"
	"time"
)

// ProbeResult contains only closed diagnostic codes and fixed safe messages.
// Success proves read-only connectivity and identity, not playback capability.
type ProbeResult struct {
	OK            bool
	Code, Message string
}

// Probe checks one configured identity through the production pinned connection.
// Denon 4.1.1, 4.2.1 and 4.2.3 require only registration, discovery and state. No
// observer, catalog, scheduler or write-capable controller is constructed.
func Probe(parent context.Context, cfg Config, identity Identity) ProbeResult {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if err := context.Cause(ctx); err != nil {
		return probeError(ctx, err)
	}
	if identity.Key == "" || identity.Serial == "" {
		return probeResult("configuration")
	}
	cfg.EnableWrites, cfg.Logger, cfg.Metrics = false, nil, nil
	client, err := New(ctx, cfg)
	if err != nil {
		return probeResult("configuration")
	}
	return probeClient(ctx, client, identity)
}

func probeClient(ctx context.Context, client *Client, identity Identity) ProbeResult {
	var gap atomic.Bool
	client.SetEventHandler(func(event Event) {
		if event.Gap {
			gap.Store(true)
		}
	})
	// Registration also enables unsolicited events. Consume the bounded stream
	// without constructing an observer or issuing reads; otherwise ordinary
	// notifications could turn accumulated progress into an artificial gap.
	eventsDone := make(chan struct{})
	go func() {
		defer close(eventsDone)
		for range client.Events() {
		}
	}()
	defer func() {
		client.Close()
		<-eventsDone
	}()
	// Read bootstraps the ordinary subscription before its first command.
	discovery, err := client.Read(ctx, "player/get_players", nil)
	if err != nil {
		return probeError(ctx, err)
	}
	players, err := payload[[]Player](discovery)
	if err != nil {
		return probeError(ctx, err)
	}
	player, err := ResolveIdentity(players, identity)
	if err != nil {
		return probeError(ctx, err)
	}
	view := client.observePlayer(player.ID)
	if gap.Load() || !view.Connected || !sameGlobal(view.Token, discovery.Token) {
		return probeResult("changed")
	}
	state, err := client.Read(ctx, "player/get_play_state", url.Values{"pid": {string(player.ID)}})
	if err != nil {
		return probeError(ctx, err)
	}
	// The probe validates required response fields itself rather than exposing
	// an unvalidated, possibly private payload in its diagnostic report.
	if len(state.Params["pid"]) != 1 || state.Params.Get("pid") != string(player.ID) || len(state.Params["state"]) != 1 {
		return probeResult("protocol")
	}
	if !PlayState(state.Params.Get("state")).Known() { // Firmware unknown remains a valid observation.
		return probeResult("protocol")
	}
	if err := context.Cause(ctx); err != nil {
		return probeError(ctx, err)
	}
	view = client.PlayerView(player.ID)
	if gap.Load() || !view.Connected || !sameGlobal(discovery.Token, state.Token) || !sameGlobal(state.Token, view.Token) {
		return probeResult("changed")
	}
	return probeResult("ok")
}

func probeError(ctx context.Context, err error) ProbeResult {
	// The probe owns both the client and request contexts. Their shared caller
	// cancellation can close the client before its command sees the same cause.
	// Preserve that caller cause without hiding an independent wire failure.
	if cause := context.Cause(ctx); cause != nil && errors.Is(err, ErrClosed) {
		err = cause
	}
	var network net.Error
	switch {
	case errors.Is(err, context.Canceled):
		return probeResult("cancelled")
	case errors.Is(err, ErrPinMismatch):
		return probeResult("pin_mismatch")
	case errors.Is(err, ErrTLS):
		return probeResult("tls")
	case errors.Is(err, ErrConnect):
		return probeResult("unreachable")
	case errors.Is(err, context.DeadlineExceeded):
		return probeResult("timeout")
	case errors.Is(err, ErrIdentity):
		return probeResult("identity")
	case errors.Is(err, ErrStale):
		return probeResult("changed")
	case errors.Is(err, ErrProtocol), errors.Is(err, ErrRejected):
		return probeResult("protocol")
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, ErrClosed), errors.Is(err, ErrOffline), errors.As(err, &network):
		return probeResult("unreachable")
	default:
		return probeResult("protocol")
	}
}

func probeResult(code string) ProbeResult {
	message := "HEOS returned an invalid or rejected diagnostic response."
	switch code {
	case "ok":
		message = "Pinned TLS, configured identity and HEOS state read succeeded."
	case "configuration":
		message = "HEOS probe configuration is invalid."
	case "unreachable":
		message = "The configured HEOS device could not be reached."
	case "tls":
		message = "The HEOS TLS handshake failed."
	case "pin_mismatch":
		message = "The HEOS certificate does not match the configured pin."
	case "identity":
		message = "The configured HEOS identity is missing, ambiguous or mismatched."
	case "timeout":
		message = "The HEOS diagnostic check timed out."
	case "cancelled":
		message = "The HEOS diagnostic check was cancelled."
	case "changed":
		message = "The HEOS identity, connection or event history changed during the check."
	}
	return ProbeResult{OK: code == "ok", Code: code, Message: message}
}
