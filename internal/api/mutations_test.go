package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type captureSubmitter struct {
	calls   int
	request journal.Request
	cmd     control.Command
	err     error
}

func TestTransportAuthorizationAndCommandMapping(t *testing.T) {
	for _, tc := range []struct {
		state  string
		scopes []string
		status int
	}{
		{"play", []string{"control"}, 202},
		{"pause", []string{"control"}, 202},
		{"stop", []string{"control"}, 202},
		{"pause", []string{"read"}, 403},
		{"invalid", []string{"control"}, 400},
	} {
		t.Run(tc.state+strings.Join(tc.scopes, ","), func(t *testing.T) {
			s := testAPI(t)
			s.credentials[0].Scopes = tc.scopes
			submit := &captureSubmitter{}
			s.SetCoordinator(submit)
			r := httptest.NewRequest("PUT", "/v1/players/room/transport", strings.NewReader(`{"state":"`+tc.state+`","takeover":false}`))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer "+testToken)
			r.Header.Set("Idempotency-Key", "transport")
			r.Header.Set("If-Match", `"revision"`)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body)
			}
			if tc.status == 202 {
				if submit.calls != 1 || submit.cmd.Kind != control.CommandKindTransport || submit.cmd.State != heos.PlayState(tc.state) || submit.cmd.Takeover || submit.request.IfMatch != `"revision"` {
					t.Fatal(submit)
				}
			} else if submit.calls != 0 {
				t.Fatal("rejected transport was dispatched")
			}
			if ok, errs := s.validator.ValidateHttpResponse(r, w.Result()); !ok {
				t.Fatal(errs)
			}
		})
	}
}

func TestPlaybackProgressResponseContract(t *testing.T) {
	s := testAPI(t)
	j := s.journal.(readJournal)
	s.credentials[0].Principal = j.op.Principal
	j.op.Progress, _ = json.Marshal(control.PlaybackProgress{PlaybackStartedAt: time.Now().UTC(), StopAt: time.Now().UTC().Add(20 * time.Minute), ElapsedSeconds: 300, Level: 40})
	s.journal = j
	r := httptest.NewRequest("GET", "/v1/operations/op", nil)
	r.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"elapsed_seconds":300`) {
		t.Fatal(w.Code, w.Body)
	}
	if ok, errs := s.validator.ValidateHttpResponse(r, w.Result()); !ok {
		t.Fatal(errs)
	}
}

func TestStopBypassesOrdinaryHTTPAdmissionCapacity(t *testing.T) {
	s := testAPI(t)
	s.credentials[0].Scopes = []string{"operator"}
	submit := &captureSubmitter{}
	s.SetCoordinator(submit)
	for i := 0; i < cap(s.requests); i++ {
		s.requests <- struct{}{}
	}
	r := httptest.NewRequest("POST", "/v1/players/room/stop", strings.NewReader(`{"fade_seconds":0}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Header.Set("Idempotency-Key", "stop")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 202 || submit.calls != 1 {
		t.Fatal(w.Code, w.Body, submit.calls)
	}
}

func (c *captureSubmitter) Submit(_ context.Context, r journal.Request, cmd control.Command) (journal.Operation, error) {
	c.calls++
	c.request = r
	c.cmd = cmd
	return journal.Operation{ID: "op_actual_ID", Player: r.Player, Principal: r.Principal, Kind: string(cmd.Kind), State: journal.Accepted, Revision: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()}, c.err
}
func TestMutationScopesValidationAndResponseContract(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body string
		scopes                   []string
		code, calls              int
	}{
		{"control", "PUT", "/v1/players/room/volume", `{"unit":"heos","level":10,"takeover":false}`, []string{"control"}, 202, 1},
		{"skip", "POST", "/v1/players/room/skip", `{"direction":"next","takeover":false}`, []string{"control"}, 202, 1},
		{"skip requires control", "POST", "/v1/players/room/skip", `{"direction":"previous","takeover":false}`, []string{"read"}, 403, 0},
		{"skip takeover", "POST", "/v1/players/room/skip", `{"direction":"next","takeover":true}`, []string{"control"}, 403, 0},
		{"skip operator takeover", "POST", "/v1/players/room/skip", `{"direction":"previous","takeover":true}`, []string{"control", "operator"}, 202, 1},
		{"skip direction", "POST", "/v1/players/room/skip", `{"direction":"play","takeover":false}`, []string{"control"}, 400, 0},
		{"read cannot write", "PUT", "/v1/players/room/volume", `{"unit":"heos","level":10,"takeover":false}`, []string{"read"}, 403, 0},
		{"takeover", "PUT", "/v1/players/room/mute", `{"muted":false,"takeover":true}`, []string{"control"}, 403, 0},
		{"operator takeover", "PUT", "/v1/players/room/mute", `{"muted":false,"takeover":true}`, []string{"control", "operator"}, 202, 1},
		{"stop without read", "POST", "/v1/players/room/stop", `{"fade_seconds":0}`, []string{"operator"}, 202, 1},
		{"stop requires operator", "POST", "/v1/players/room/stop", `{"fade_seconds":0}`, []string{"control"}, 403, 0},
		{"playback requires operator", "POST", "/v1/players/room/playback", `{}`, []string{"control"}, 403, 0},
		{"bounded playback", "POST", "/v1/players/room/playback", `{"item_ref":"current","queue_mode":"replace","shuffle":true,"repeat":"off","initial_volume":{"unit":"heos","level":10},"takeover":false,"automation":{"target_volume":{"unit":"heos","level":40},"ramp_seconds":300,"duration_seconds":1200,"fade_seconds":30}}`, []string{"operator"}, 202, 1},
		{"automation duration bound", "POST", "/v1/players/room/playback", `{"item_ref":"current","queue_mode":"replace","shuffle":true,"repeat":"off","initial_volume":{"unit":"heos","level":10},"takeover":false,"automation":{"target_volume":{"unit":"heos","level":40},"ramp_seconds":300,"duration_seconds":0,"fade_seconds":30}}`, []string{"operator"}, 422, 0},
		{"automation missing field", "POST", "/v1/players/room/playback", `{"item_ref":"current","queue_mode":"replace","shuffle":true,"repeat":"off","initial_volume":{"unit":"heos","level":10},"takeover":false,"automation":{"target_volume":{"unit":"heos","level":40},"duration_seconds":1200,"fade_seconds":30}}`, []string{"operator"}, 400, 0},
		{"volume unit", "PUT", "/v1/players/room/volume", `{"unit":"db","level":10,"takeover":false}`, []string{"control"}, 400, 0},
		{"volume bound", "PUT", "/v1/players/room/volume", `{"unit":"heos","level":101,"takeover":false}`, []string{"control"}, 422, 0},
		{"required boolean", "PUT", "/v1/players/room/volume", `{"unit":"heos","level":10}`, []string{"control"}, 400, 0},
		{"duplicate", "PUT", "/v1/players/room/mute", `{"muted":true,"muted":false,"takeover":false}`, []string{"control"}, 400, 0},
		{"player ACL", "PUT", "/v1/players/other/mute", `{"muted":false,"takeover":false}`, []string{"control"}, 403, 0},
		{"cancel creator", "POST", "/v1/operations/op_actual_ID/cancel", `{"mode":"release"}`, []string{"control"}, 202, 1},
		{"release cannot fade", "POST", "/v1/operations/op_actual_ID/cancel", `{"mode":"release","fade_seconds":1}`, []string{"control"}, 422, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testAPI(t)
			s.credentials[0].Scopes = tc.scopes
			s.credentials[0].Principal = "someone-else"
			submit := &captureSubmitter{}
			s.SetCoordinator(submit)
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer "+testToken)
			r.Header.Set("Idempotency-Key", "key-one")
			r.Header.Set("If-Match", `"revision"`)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != tc.code || submit.calls != tc.calls {
				t.Fatal(w.Code, w.Body, submit.calls)
			}
			if ok, errs := s.validator.ValidateHttpResponse(r, w.Result()); !ok {
				t.Fatal(errs)
			}
			if tc.code == 202 && w.Header().Get("Location") != "/v1/operations/op_actual_ID" {
				t.Fatal(w.Header())
			}
			if tc.name == "bounded playback" && (submit.cmd.Automation == nil || submit.cmd.Automation.TargetLevel != 40 || submit.cmd.Automation.DurationSeconds != 1200 || submit.cmd.Automation.RampSeconds != 300 || submit.cmd.Automation.FadeSeconds != 30) {
				t.Fatal("lost automation settings", submit.cmd)
			}
			if tc.name == "skip" && (submit.cmd.Kind != control.CommandKindSkip || submit.cmd.Direction != "next" || submit.cmd.Takeover) {
				t.Fatal(submit.cmd)
			}
		})
	}
}
func TestMutationHeadersAndJournalErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"missing revision", control.ErrPreconditionRequired, 428}, {"stale revision", control.ErrPrecondition, 412}, {"busy", journal.ErrBusy, 409}, {"idempotency", journal.ErrConflict, 409}, {"uncertain", journal.ErrCommitUncertain, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testAPI(t)
			s.credentials[0].Scopes = []string{"control"}
			s.SetCoordinator(&captureSubmitter{err: tc.err})
			r := httptest.NewRequest("PUT", "/v1/players/room/volume", strings.NewReader(`{"unit":"heos","level":10,"takeover":false}`))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer "+testToken)
			r.Header.Set("Idempotency-Key", "key")
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body)
			}
			if errors.Is(tc.err, journal.ErrCommitUncertain) && !strings.Contains(w.Body.String(), `"outcome":"unknown"`) {
				t.Fatal(w.Body)
			}
		})
	}
	for _, header := range []string{"Idempotency-Key", "If-Match"} {
		s := testAPI(t)
		s.credentials[0].Scopes = []string{"control"}
		cap := &captureSubmitter{}
		s.SetCoordinator(cap)
		r := httptest.NewRequest("PUT", "/v1/players/room/mute", strings.NewReader(`{"muted":true,"takeover":false}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+testToken)
		r.Header.Set("Idempotency-Key", "key")
		r.Header.Set(header, "one")
		r.Header.Add(header, "two")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 400 || cap.calls != 0 {
			t.Fatal(w.Code, cap.calls)
		}
	}
}
