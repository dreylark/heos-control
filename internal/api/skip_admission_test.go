package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dreylark/heos-control/internal/control"
)

func TestSkipNotSkippableResponseContract(t *testing.T) {
	s := testAPI(t)
	s.credentials[0].Scopes = []string{"control"}
	submit := &captureSubmitter{err: control.ErrNotSkippable}
	s.SetCoordinator(submit)
	r := httptest.NewRequest("POST", "/v1/players/room/skip", strings.NewReader(`{"direction":"next","takeover":false}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Header.Set("Idempotency-Key", "skip")
	r.Header.Set("If-Match", `"revision"`)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusUnprocessableEntity || submit.calls != 1 {
		t.Fatal(w.Code, w.Body, submit.calls)
	}
	var body Error
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "not_skippable" || body.Error.Retryable || body.Error.Outcome != NotSent {
		t.Fatal(body.Error)
	}
	if ok, errs := s.validator.ValidateHttpResponse(r, w.Result()); !ok {
		t.Fatal(errs)
	}
}
