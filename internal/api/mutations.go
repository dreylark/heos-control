package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type Submitter interface {
	Submit(context.Context, journal.Request, control.Command) (journal.Operation, error)
}

// SetCoordinator is startup wiring, before the handler serves requests.
func (s *Server) SetCoordinator(c Submitter) { s.coordinator = c }
func (s *Server) submit(ctx context.Context, player, key, match string, body any, cmd control.Command) (Operation, error) {
	if e := authorize(ctx, player); e != nil {
		return Operation{}, e
	}
	if cmd.Takeover && !identity(ctx).operator() {
		return Operation{}, &apiFailure{403, "operator_required", false}
	}
	if s.coordinator == nil {
		return Operation{}, &apiFailure{503, "control_unavailable", true}
	}
	b, e := json.Marshal(body)
	if e != nil {
		return Operation{}, e
	}
	req := ctx.Value(requestKey{}).(*http.Request)
	op, e := s.coordinator.Submit(ctx, journal.Request{Principal: identity(ctx).Principal, Player: player, Key: key, IfMatch: match, Method: req.Method, Endpoint: req.URL.Path, Body: b}, cmd)
	return control.ProjectOperation(op), e
}
func playbackCommand(body PlaybackInput) control.Command {
	cmd := control.Command{Kind: control.CommandKindPlayback, ItemRef: body.ItemRef, Level: body.InitialVolume.Level, Shuffle: body.Shuffle, Repeat: heos.Repeat(body.Repeat), Takeover: body.Takeover}
	if a := body.Automation; a != nil {
		cmd.Automation = &control.Automation{TargetLevel: a.TargetVolume.Level, RampSeconds: a.RampSeconds, DurationSeconds: a.DurationSeconds, FadeSeconds: a.FadeSeconds}
	}
	return cmd
}

func (s *Server) Playback(ctx context.Context, r PlaybackRequestObject) (PlaybackResponseObject, error) {
	cmd := playbackCommand(*r.Body)
	op, e := s.submit(ctx, r.Player, r.Params.IdempotencyKey, value(r.Params.IfMatch, ""), r.Body, cmd)
	if e != nil {
		return nil, e
	}
	return Playback202JSONResponse{Body: op, Headers: Playback202ResponseHeaders{Location: "/v1/operations/" + op.ID}}, nil
}
func (s *Server) SetVolume(ctx context.Context, r SetVolumeRequestObject) (SetVolumeResponseObject, error) {
	op, e := s.submit(ctx, r.Player, r.Params.IdempotencyKey, value(r.Params.IfMatch, ""), r.Body, control.Command{Kind: control.CommandKindVolume, Level: r.Body.Level, Takeover: r.Body.Takeover})
	if e != nil {
		return nil, e
	}
	return SetVolume202JSONResponse{Body: op, Headers: SetVolume202ResponseHeaders{Location: "/v1/operations/" + op.ID}}, nil
}
func (s *Server) SetMute(ctx context.Context, r SetMuteRequestObject) (SetMuteResponseObject, error) {
	op, e := s.submit(ctx, r.Player, r.Params.IdempotencyKey, value(r.Params.IfMatch, ""), r.Body, control.Command{Kind: control.CommandKindMute, Muted: r.Body.Muted, Takeover: r.Body.Takeover})
	if e != nil {
		return nil, e
	}
	return SetMute202JSONResponse{Body: op, Headers: SetMute202ResponseHeaders{Location: "/v1/operations/" + op.ID}}, nil
}
func (s *Server) SetTransport(ctx context.Context, r SetTransportRequestObject) (SetTransportResponseObject, error) {
	op, e := s.submit(ctx, r.Player, r.Params.IdempotencyKey, value(r.Params.IfMatch, ""), r.Body, control.Command{Kind: control.CommandKindTransport, State: heos.PlayState(r.Body.State), Takeover: r.Body.Takeover})
	if e != nil {
		return nil, e
	}
	return SetTransport202JSONResponse{Body: op, Headers: SetTransport202ResponseHeaders{Location: "/v1/operations/" + op.ID}}, nil
}
func (s *Server) SkipPlayer(ctx context.Context, r SkipPlayerRequestObject) (SkipPlayerResponseObject, error) {
	op, e := s.submit(ctx, r.Player, r.Params.IdempotencyKey, value(r.Params.IfMatch, ""), r.Body, control.Command{Kind: control.CommandKindSkip, Direction: string(r.Body.Direction), Takeover: r.Body.Takeover})
	if e != nil {
		return nil, e
	}
	return SkipPlayer202JSONResponse{Body: op, Headers: SkipPlayer202ResponseHeaders{Location: "/v1/operations/" + op.ID}}, nil
}
func (s *Server) StopPlayer(ctx context.Context, r StopPlayerRequestObject) (StopPlayerResponseObject, error) {
	op, e := s.submit(ctx, r.Player, r.Params.IdempotencyKey, "", r.Body, control.Command{Kind: control.CommandKindStop, FadeSeconds: r.Body.FadeSeconds})
	if e != nil {
		return nil, e
	}
	return StopPlayer202JSONResponse{Body: op, Headers: StopPlayer202ResponseHeaders{Location: "/v1/operations/" + op.ID}}, nil
}
func (s *Server) CancelOperation(ctx context.Context, r CancelOperationRequestObject) (CancelOperationResponseObject, error) {
	target, e := s.journal.Get(ctx, r.Operation)
	if e != nil {
		return nil, e
	}
	p := identity(ctx)
	if !p.allows(target.Player) || (p.Principal != target.Principal && !p.operator()) {
		return nil, &apiFailure{403, "forbidden", false}
	}
	fade := value(r.Body.FadeSeconds, 0)
	if r.Body.Mode == Release && fade != 0 {
		return nil, &apiFailure{422, "release_cannot_fade", false}
	}
	op, e := s.submit(ctx, target.Player, r.Params.IdempotencyKey, "", r.Body, control.Command{Kind: control.CommandKindCancel, Target: target.ID, Mode: string(r.Body.Mode), FadeSeconds: fade})
	if e != nil {
		return nil, e
	}
	return CancelOperation202JSONResponse{Body: op, Headers: CancelOperation202ResponseHeaders{Location: "/v1/operations/" + op.ID}}, nil
}

// Numeric policy violations are 422; malformed JSON/types remain 400. The
// contract validator remains authoritative for the complete body shape.
func mutationOutOfBounds(path string, b []byte) bool {
	var object map[string]json.RawMessage
	if json.Unmarshal(b, &object) != nil {
		return false
	}
	var raw json.RawMessage
	maximum := 100
	switch path {
	case "/v1/players/{player}/volume":
		raw = object["level"]
	case "/v1/players/{player}/playback", "/v1/players/{player}/preflight":
		var automation map[string]json.RawMessage
		if json.Unmarshal(object["automation"], &automation) == nil {
			for _, bound := range []struct {
				field    string
				min, max int
			}{{"ramp_seconds", 0, 7200}, {"duration_seconds", 1, 7200}, {"fade_seconds", 0, 60}} {
				var n int
				if json.Unmarshal(automation[bound.field], &n) == nil && (n < bound.min || n > bound.max) {
					return true
				}
			}
			var target struct {
				Level *int `json:"level"`
			}
			if json.Unmarshal(automation["target_volume"], &target) == nil && target.Level != nil && (*target.Level < 0 || *target.Level > 100) {
				return true
			}
		}
		var volume map[string]json.RawMessage
		if json.Unmarshal(object["initial_volume"], &volume) != nil {
			return false
		}
		raw = volume["level"]
	case "/v1/players/{player}/stop", "/v1/operations/{operation}/cancel":
		raw = object["fade_seconds"]
		maximum = 60
	default:
		return false
	}
	var n int
	return json.Unmarshal(raw, &n) == nil && (n < 0 || n > maximum)
}
