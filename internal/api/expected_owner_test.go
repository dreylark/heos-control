package api

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dreylark/heos-control/internal/control"
)

func TestExpectedOwnerContractAndMapping(t *testing.T) {
	for _, tc := range []struct {
		method, path, body string
		kind               control.CommandKind
	}{
		{"PUT", "volume", `{"unit":"heos","level":0,"takeover":true,"expected_owner":"op_owner"}`, control.CommandKindVolume},
		{"PUT", "mute", `{"muted":true,"takeover":true,"expected_owner":"op_owner"}`, control.CommandKindMute},
		{"PUT", "transport", `{"state":"pause","takeover":true,"expected_owner":"op_owner"}`, control.CommandKindTransport},
		{"POST", "playback", `{"item_ref":"track","queue_mode":"replace","shuffle":false,"repeat":"off","initial_volume":{"unit":"heos","level":0},"takeover":true,"expected_owner":"op_owner"}`, control.CommandKindPlayback},
	} {
		t.Run(tc.path, func(t *testing.T) {
			s := testAPI(t)
			s.credentials[0].Scopes = []string{"read", "control", "operator"}
			submit := &captureSubmitter{}
			s.SetCoordinator(submit)
			r := httptest.NewRequest(tc.method, "/v1/players/room/"+tc.path, strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer "+testToken)
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "owner-fence")
			r.Header.Set("If-Match", `"revision"`)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != 202 || submit.calls != 1 || submit.cmd.Kind != tc.kind || submit.cmd.ExpectedOwner != "op_owner" || !submit.cmd.Takeover {
				t.Fatalf("status=%d command=%+v body=%s", w.Code, submit.cmd, w.Body)
			}
			var frozen map[string]any
			if json.Unmarshal(submit.request.Body, &frozen) != nil || frozen["expected_owner"] != "op_owner" {
				t.Fatal("idempotency identity lost expected_owner")
			}
		})
	}
}

func TestExpectedOwnerContractRejectsInvalidFence(t *testing.T) {
	for _, bad := range []string{`"expected_owner":"op_owner","takeover":false`, `"expected_owner":"","takeover":true`, `"expected_owner":null,"takeover":true`, `"expected_owner":"` + strings.Repeat("x", 129) + `","takeover":true`} {
		s := testAPI(t)
		s.credentials[0].Scopes = []string{"control", "operator"}
		submit := &captureSubmitter{}
		s.SetCoordinator(submit)
		r := httptest.NewRequest("PUT", "/v1/players/room/mute", strings.NewReader(`{"muted":true,`+bad+`}`))
		r.Header.Set("Authorization", "Bearer "+testToken)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "bad-owner-fence")
		r.Header.Set("If-Match", `"revision"`)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 400 && w.Code != 422 {
			t.Fatal(w.Code, w.Body)
		}
		if submit.calls != 0 {
			t.Fatal("invalid expected_owner reached admission")
		}
	}
}

func TestExpectedOwnerMismatchMapsToPrecondition(t *testing.T) {
	s := testAPI(t)
	s.credentials[0].Scopes = []string{"control", "operator"}
	submit := &captureSubmitter{err: control.ErrPrecondition}
	s.SetCoordinator(submit)
	r := httptest.NewRequest("PUT", "/v1/players/room/mute", strings.NewReader(`{"muted":true,"takeover":true,"expected_owner":"op_owner"}`))
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "stale-owner-fence")
	r.Header.Set("If-Match", `"revision"`)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 412 {
		t.Fatal(w.Code, w.Body)
	}
}
