package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type apiObservation struct{}

func (apiObservation) Snapshot() heos.Snapshot {
	return heos.Snapshot{State: heos.PlayStateUnknown, Stale: true}
}
func (apiObservation) Refresh(context.Context) error { panic("rejected request reached device") }

type readJournal struct{ op journal.Operation }

func (readJournal) Ready(context.Context) error                              { return nil }
func (j readJournal) Get(context.Context, string) (journal.Operation, error) { return j.op, nil }
func (readJournal) Active(context.Context, string) (journal.Operation, error) {
	return journal.Operation{}, journal.ErrNotFound
}
func (readJournal) List(context.Context, journal.HistoryQuery) (journal.HistoryPage, error) {
	return journal.HistoryPage{}, nil
}

func TestConcurrentRequestValidation(t *testing.T) {
	s := testAPI(t)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 8 {
				r := httptest.NewRequest("GET", "/v1/players/room/queue?limit=50", nil)
				r.Header.Set("Authorization", "Bearer "+testToken)
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				if w.Code != 200 {
					t.Errorf("concurrent request: %d %s", w.Code, w.Body)
				}
			}
		})
	}
	wg.Wait()
}

func TestOperationAccessRequiresPlayerAndCreatorOrOperator(t *testing.T) {
	for _, tc := range []struct {
		principal       string
		scopes, players []string
		code            int
	}{
		{"someone-else", []string{"read"}, []string{"room"}, 200},
		{"operator", []string{"read", "operator"}, []string{"room"}, 200},
		{"operator", []string{"read", "operator"}, []string{"other"}, 403},
		{"someone-else", []string{"control"}, []string{"room"}, 403},
	} {
		s := testAPI(t)
		s.credentials[0].Principal = tc.principal
		s.credentials[0].Scopes = tc.scopes
		s.credentials[0].Players = tc.players
		r := httptest.NewRequest("GET", "/v1/operations/op", nil)
		r.Header.Set("Authorization", "Bearer "+testToken)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != tc.code {
			t.Fatalf("%+v: %d %s", tc, w.Code, w.Body)
		}
		if tc.code == 200 {
			var body map[string]any
			if e := json.Unmarshal(w.Body.Bytes(), &body); e != nil {
				t.Fatal(e)
			}
			for _, field := range []string{"principal", "device_key", "effective_arguments", "epoch"} {
				if _, ok := body[field]; ok {
					t.Fatal("private journal field exposed", field)
				}
			}
		}
	}
}

func TestMalformedBodiesAndHeadersHaveBoundedConsistentErrors(t *testing.T) {
	s := testAPI(t)
	for _, body := range []string{`{"preset":"morning","preset_revision":"1"} {}`, strings.Repeat(" ", 16385), `{"preset":false,"preset_revision":"1"}`, `{"preset":"morning","preset_revision":null}`} {
		r := httptest.NewRequest("POST", "/v1/players/room/preflight", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+testToken)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("status=%d body=%s", w.Code, w.Body)
		}
	}
	r := httptest.NewRequest("GET", "/v1/players", nil)
	r.Header.Add("Authorization", "Bearer "+testToken)
	r.Header.Add("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 401 || w.Header().Get("WWW-Authenticate") == "" {
		t.Fatal(w.Code, w.Header())
	}
}

const testToken = "synthetic-token-00000000000000000000000000000000"

func testAPI(t *testing.T, docs ...bool) *Server {
	t.Helper()
	reads := control.NewReads("test-epoch", []control.Device{{Config: config.Player{Key: "room"}, Observer: apiObservation{}}, {Config: config.Player{Key: "other"}, Observer: apiObservation{}}}, nil)
	credentials := []config.Credential{{Principal: "reader", TokenSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(testToken))), Scopes: []string{"read"}, Players: []string{"room"}}}
	s, err := NewServer(reads, readJournal{op: journal.Operation{ID: "op", Player: "room", Principal: "someone-else"}}, credentials, new(atomic.Bool), NewEvents("epoch", 4, 2, 4), len(docs) > 0 && docs[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}
func TestReadOnlyAuthenticationValidationAndPrivacy(t *testing.T) {
	s := testAPI(t)
	for _, tc := range []struct {
		method, path, body, token string
		code                      int
	}{
		{"GET", "/v1/players", "", "", 401},
		{"GET", "/v1/players", "", "bad", 401},
		{"GET", "/v1/players/other", "", testToken, 403},
		{"GET", "/v1/operations/op", "", testToken, 403},
		{"POST", "/v1/players/room/preflight", `{"preset":"morning","preset_revision":"1","surprise":1}`, testToken, 400},
		{"POST", "/v1/players/room/preflight", `{"preset":"morning","preset":"morning","preset_revision":"1"}`, testToken, 400},
		{"POST", "/v1/players/room/preflight", `{}`, testToken, 400},
		{"POST", "/v1/players/room/preflight", `null`, testToken, 400},
		{"GET", "/v1/players/room/queue?limit=101", "", testToken, 400},
		{"GET", "/v1/players/room/queue?limit=1&limit=2", "", testToken, 400},
		{"GET", "/v1/players?unknown=1", "", testToken, 400},
		{"PUT", "/v1/players/room/volume", `{"level":10}`, testToken, 403},
		{"GET", "/v1/players", "", testToken, 200},
		{"GET", "/v1/players/room", "", testToken, 200},
	} {
		t.Run(tc.path+tc.body+tc.token, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			if tc.body != "" {
				r.Header.Set("Content-Type", "application/json")
			}
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != tc.code {
				t.Fatalf("got %d want %d: %s", w.Code, tc.code, w.Body)
			}
			if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("X-Request-ID") == "" {
				t.Fatal(w.Header())
			}
			if tc.code >= 400 && !strings.Contains(w.Body.String(), `"outcome":"not_sent"`) {
				t.Fatal(w.Body)
			}
			if tc.method != "PUT" {
				if ok, errs := s.validator.ValidateHttpResponse(r, w.Result()); !ok {
					t.Fatalf("response schema: %v", errs)
				}
			}
			if tc.code == 200 && strings.Contains(w.Body.String(), `"other"`) {
				t.Fatal("player ACL leak")
			}
			if tc.path == "/v1/players/room" && tc.code == 200 && w.Header().Get("ETag") == "" {
				t.Fatal("missing ETag")
			}
		})
	}
}

func TestContractRejectsWrongVolumeUnitAndUnknownResponseFields(t *testing.T) {
	s := testAPI(t)
	r := httptest.NewRequest("GET", "/v1/players/room", nil)
	r.Header.Set("Authorization", "Bearer "+testToken)
	original := httptest.NewRecorder()
	s.Handler().ServeHTTP(original, r)
	for _, bad := range []string{strings.Replace(original.Body.String(), `"unit":"heos"`, `"unit":"db"`, 1), strings.Replace(original.Body.String(), `"key":"room"`, `"key":"room","unknown":true`, 1)} {
		w := httptest.NewRecorder()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.WriteString(bad)
		if ok, _ := s.validator.ValidateHttpResponse(r, w.Result()); ok {
			t.Fatal("OpenAPI 3.1 schema accepted invalid response", bad)
		}
	}
}

func TestPresetAPIIsRemoved(t *testing.T) {
	s := testAPI(t)
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/v1/presets", "", 404},
		{"POST", "/v1/players/room/preflight", `{"preset":"morning","preset_revision":"1"}`, 400},
	} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer "+testToken)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Errorf("%s: %d %s", tc.path, w.Code, w.Body)
		}
	}
}
