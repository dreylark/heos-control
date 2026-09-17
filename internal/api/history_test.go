package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/journal"
)

type historyJournal struct {
	readJournal
	queries []journal.HistoryQuery
	page    journal.HistoryPage
	err     error
}

func (j *historyJournal) List(_ context.Context, q journal.HistoryQuery) (journal.HistoryPage, error) {
	j.queries = append(j.queries, q)
	return j.page, j.err
}

func historyRequest(t *testing.T, s *Server, query, token string, status int) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("GET", "/v1/operations"+query, nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != status {
		t.Fatalf("%s: status=%d want=%d: %s", query, w.Code, status, w.Body)
	}
	response := w.Result()
	defer func() { _ = response.Body.Close() }()
	if ok, errs := s.validator.ValidateHttpResponse(r, response); !ok {
		t.Fatalf("response contract: %v; body=%s", errs, w.Body)
	}
	return w
}

func TestHistoryAuthorizationPrecedesJournalAndCursor(t *testing.T) {
	s := testAPI(t)
	j := &historyJournal{}
	s.journal = j
	historyRequest(t, s, "?cursor=broken", "", 401)
	s.credentials[0].Scopes = []string{"operator"}
	historyRequest(t, s, "", testToken, 403)
	s.credentials[0].Scopes = []string{"read"}
	historyRequest(t, s, "?player=other", testToken, 403)
	if len(j.queries) != 0 {
		t.Fatal("rejected history request reached journal")
	}
	w := historyRequest(t, s, "", testToken, 200)
	if strings.TrimSpace(w.Body.String()) != `{"items":[],"next_cursor":null}` {
		t.Fatal(w.Body)
	}
	if len(j.queries) != 1 || j.queries[0].Principal != "reader" || j.queries[0].Operator || !reflect.DeepEqual(j.queries[0].Players, []string{"room"}) || j.queries[0].Limit != 50 {
		t.Fatalf("authorization scope: %+v", j.queries)
	}
	s.credentials[0].Scopes = []string{"read", "operator"}
	historyRequest(t, s, "", testToken, 200)
	if !j.queries[1].Operator || !reflect.DeepEqual(j.queries[1].Players, []string{"room"}) {
		t.Fatalf("operator escaped player ACL: %+v", j.queries[1])
	}
}

func TestHistoryRejectsInvalidFilters(t *testing.T) {
	s := testAPI(t)
	j := &historyJournal{}
	s.journal = j
	for _, query := range []string{
		"limit=0", "limit=101", "limit=1.5", "limit=", "limit=1&limit=2",
		"state=other", "state=", "state=running&state=failed", "delivery=maybe", "delivery=",
		"kind=", "kind=not-a-kind", "kind=playback&kind=alarm", "player=", "player=room&player=room",
		"created_from=2026-09-16", "created_from=2026-09-16T07:00:00", "created_from=", "created_before=",
		"created_from=2026-09-16T7:00:00Z", "created_from=2026-09-16T07:00:00,1Z", "created_from=2026-09-16T07:00:00%2B24:00",
		"created_from=2026-09-16T07:00:00Z&created_before=2026-09-16T07:00:00Z",
		"created_from=2026-09-17T07:00:00Z&created_before=2026-09-16T07:00:00Z",
		"created_before=2026-09-16T07:00:00Z&created_before=2026-09-16T07:00:00Z",
		"cursor=", "cursor=broken", "cursor=" + strings.Repeat("A", 2049), "unknown=1",
	} {
		t.Run(query, func(t *testing.T) { historyRequest(t, s, "?"+query, testToken, 400) })
	}
	if len(j.queries) != 0 {
		t.Fatal("invalid filters reached journal", j.queries)
	}
}

func TestHistoryFiltersCursorAndSafeProjection(t *testing.T) {
	s := testAPI(t)
	created := time.Date(2026, 9, 16, 7, 0, 0, 123456000, time.UTC)
	op := journal.Operation{ID: "op_1", Player: "room", Principal: "reader", Kind: "alarm", State: journal.Released, Phase: "released", Revision: 2, CreatedAt: created, UpdatedAt: created,
		DeviceKey: "private-device", EffectiveArguments: json.RawMessage(`{"private":"arguments"}`),
		Outcome: json.RawMessage(`{"delivery":"not_sent","future_field":true,"diagnostics":{"reason":"unexpected_event","rule":null,"phase":"ramping","detected_at":"2026-09-16T07:00:00Z","source":"event","changed_fields":["volume"],"expected":{"volume":2},"observed":{"volume":3}}}`)}
	j := &historyJournal{readJournal: readJournal{op: op}, page: journal.HistoryPage{Items: []journal.Operation{op}, Next: &journal.HistoryPosition{CreatedAt: created, ID: op.ID}}}
	s.journal = j
	query := "?player=room&state=released&kind=alarm&delivery=not_sent&limit=1&created_from=2026-09-16T09:00:00%2B03:00&created_before=2026-09-17T07:00:00Z"
	w := historyRequest(t, s, query, testToken, 200)
	var body struct {
		Items []json.RawMessage `json:"items"`
		Next  *string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) != 1 || body.Next == nil || len(*body.Next) > 2048 {
		t.Fatal(w.Body)
	}
	if strings.Contains(string(body.Items[0]), "private") || strings.Contains(string(body.Items[0]), "principal") || !strings.Contains(string(body.Items[0]), "future_field") {
		t.Fatal("unsafe or lossy projection", string(body.Items[0]))
	}
	r := httptest.NewRequest("GET", "/v1/operations/op_1", nil)
	r.Header.Set("Authorization", "Bearer "+testToken)
	detail := httptest.NewRecorder()
	s.Handler().ServeHTTP(detail, r)
	if detail.Code != 200 || strings.TrimSpace(detail.Body.String()) != string(body.Items[0]) {
		t.Fatalf("detail/list mismatch: %s / %s", detail.Body, body.Items[0])
	}
	response := detail.Result()
	defer func() { _ = response.Body.Close() }()
	if ok, errs := s.validator.ValidateHttpResponse(r, response); !ok {
		t.Fatal(errs)
	}
	q := j.queries[0]
	if q.Player != "room" || q.State != journal.Released || q.Kind != "alarm" || q.Delivery != "not_sent" || q.Limit != 1 || q.Before != nil || q.CreatedFrom == nil || q.CreatedFrom.Location() != time.UTC || q.CreatedFrom.Hour() != 6 {
		t.Fatalf("query: %+v", q)
	}
	// Equivalent timezone spelling is the same normalized filter.
	next := strings.Replace(query, "2026-09-16T09:00:00%2B03:00", "2026-09-16T06:00:00Z", 1) + "&cursor=" + url.QueryEscape(*body.Next)
	historyRequest(t, s, next, testToken, 200)
	if got := j.queries[1].Before; got == nil || !got.CreatedAt.Equal(created) || got.ID != op.ID {
		t.Fatalf("cursor precision: %+v", got)
	}
	for _, mismatch := range []string{"", "?limit=1", strings.Replace(query, "limit=1", "limit=2", 1), strings.Replace(query, "kind=alarm", "kind=playback", 1), strings.Replace(query, "state=released", "state=running", 1), strings.Replace(query, "delivery=not_sent", "delivery=unknown", 1), strings.Replace(query, "created_before=2026-09-17T07:00:00Z", "created_before=2026-09-18T07:00:00Z", 1)} {
		sep := "&"
		if mismatch == "" {
			sep = "?"
		}
		historyRequest(t, s, mismatch+sep+"cursor="+url.QueryEscape(*body.Next), testToken, 400)
	}
	// Continuation does not carry authorization. Current credential always wins.
	s.credentials[0].Principal = "new-reader"
	historyRequest(t, s, next, testToken, 200)
	if j.queries[len(j.queries)-1].Principal != "new-reader" {
		t.Fatal("cursor cached principal")
	}
	before := len(j.queries)
	s.credentials[0].Players = []string{"other"}
	historyRequest(t, s, next, testToken, 403)
	if len(j.queries) != before {
		t.Fatal("revoked player reached journal")
	}
}

func TestHistoryStrictCursorJSONAndUnknownVersion(t *testing.T) {
	s := testAPI(t)
	j := &historyJournal{page: journal.HistoryPage{Next: &journal.HistoryPosition{CreatedAt: time.Date(2026, 9, 16, 7, 0, 0, 0, time.UTC), ID: "op_1"}}}
	s.journal = j
	w := historyRequest(t, s, "", testToken, 200)
	var body struct {
		Next string `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(body.Next)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"null", "[]", "{}", string(raw) + " {}", strings.Replace(string(raw), `"version":1`, `"version":2`, 1), strings.Replace(string(raw), `"version":1`, `"version":1,"version":1`, 1), strings.Replace(string(raw), `"version":1`, `"version":1,"extra":true`, 1), strings.Replace(string(raw), `"op_1"`, `""`, 1), strings.Replace(string(raw), `"op_1"`, `"invalid/id"`, 1), strings.Replace(string(raw), `2026-09-16T07:00:00Z`, `not-a-time`, 1)} {
		historyRequest(t, s, "?cursor="+base64.RawURLEncoding.EncodeToString([]byte(bad)), testToken, 400)
	}
	historyRequest(t, s, "?cursor="+url.QueryEscape(body.Next+"="), testToken, 400)
	// Positions originate in PostgreSQL, whose persisted timestamps have
	// microsecond precision. Truncating a tampered finer anchor could skip rows.
	submicro := strings.Replace(string(raw), `2026-09-16T07:00:00Z`, `2026-09-16T07:00:00.000000001Z`, 1)
	historyRequest(t, s, "?cursor="+base64.RawURLEncoding.EncodeToString([]byte(submicro)), testToken, 400)
}

func TestHistoryLegacyOutcomeAndDatabaseError(t *testing.T) {
	s := testAPI(t)
	op := journal.Operation{ID: "old", Kind: "alarm", Player: "room", Principal: "reader", State: journal.Interrupted, Phase: "interrupted", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	j := &historyJournal{readJournal: readJournal{op: op}, page: journal.HistoryPage{Items: []journal.Operation{op}}}
	s.journal = j
	w := historyRequest(t, s, "?kind=alarm", testToken, 200)
	if strings.Contains(w.Body.String(), "diagnostics") || !strings.Contains(w.Body.String(), `"outcome":{}`) {
		t.Fatal(w.Body)
	}
	r := httptest.NewRequest("GET", "/v1/operations/old", nil)
	r.Header.Set("Authorization", "Bearer "+testToken)
	detail := httptest.NewRecorder()
	s.Handler().ServeHTTP(detail, r)
	response := detail.Result()
	defer func() { _ = response.Body.Close() }()
	if detail.Code != 200 {
		t.Fatal(detail.Code, detail.Body)
	}
	if ok, errs := s.validator.ValidateHttpResponse(r, response); !ok {
		t.Fatal(errs)
	}
	j.err = errors.New("database secret password")
	w = historyRequest(t, s, "", testToken, 503)
	if strings.Contains(w.Body.String(), "password") {
		t.Fatal("private database error leaked")
	}
}

func TestHistoryDiagnosticsResponseContract(t *testing.T) {
	s := testAPI(t)
	valid := `{"reason":"future_reason","rule":null,"phase":"ramping","detected_at":null,"source":"recovery","changed_fields":[],"observed":{"muted":false}}`
	for _, tc := range []struct {
		name, diagnostics string
		valid             bool
	}{
		{"partial evidence and extensible reason", valid, true},
		{"unknown scalar", strings.Replace(valid, `"muted":false`, `"muted":false,"media_id":"private"`, 1), false},
		{"unknown diagnostic field", strings.Replace(valid, `"reason":`, `"raw_response":"private","reason":`, 1), false},
		{"volume bounds", strings.Replace(valid, `"muted":false`, `"volume":101`, 1), false},
		{"scalar type", strings.Replace(valid, `"muted":false`, `"volume":"3"`, 1), false},
		{"missing source", strings.Replace(valid, `"source":"recovery",`, "", 1), false},
		{"unknown source", strings.Replace(valid, `"recovery"`, `"guessed"`, 1), false},
		{"changed fields bound", strings.Replace(valid, `"changed_fields":[]`, `"changed_fields":["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"]`, 1), false},
		{"unique changed fields", strings.Replace(valid, `"changed_fields":[]`, `"changed_fields":["volume","volume"]`, 1), false},
		{"diagnostics cannot be null", "null", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := journal.Operation{ID: "op", Kind: "playback", Player: "room", Principal: "reader", State: journal.Released, Phase: "released", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(), Outcome: json.RawMessage(`{"diagnostics":` + tc.diagnostics + `}`)}
			s.journal = &historyJournal{readJournal: readJournal{op: op}, page: journal.HistoryPage{Items: []journal.Operation{op}}}
			for _, path := range []string{"/v1/operations", "/v1/operations/op"} {
				r := httptest.NewRequest("GET", path, nil)
				r.Header.Set("Authorization", "Bearer "+testToken)
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				response := w.Result()
				ok, errs := s.validator.ValidateHttpResponse(r, response)
				_ = response.Body.Close()
				if w.Code != 200 || ok != tc.valid {
					t.Fatalf("%s: status=%d valid=%v want=%v errors=%v", path, w.Code, ok, tc.valid, errs)
				}
			}
		})
	}
}
