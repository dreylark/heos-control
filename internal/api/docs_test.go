package api

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"

	contract "github.com/dreylark/heos-control/api"
)

func TestDocumentationDisabled(t *testing.T) {
	s := testAPI(t)
	for _, path := range []string{"/docs", "/docs/", "/docs/init.js", "/openapi.yaml"} {
		for _, token := range []string{"", testToken} {
			r := httptest.NewRequest("GET", path, nil)
			if token != "" {
				r.Header.Set("Authorization", "Bearer "+token)
			}
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != 404 {
				t.Errorf("disabled %s: %d", path, w.Code)
			}
		}
	}
}

func TestDocumentationEnabled(t *testing.T) {
	s := testAPI(t, true)
	// Documentation must work without device observations or journal availability.
	s.reads, s.journal, s.db = nil, nil, nil
	for _, tc := range []struct{ path, contentType, contains string }{
		{"/docs", "text/html", "HEOS control API"},
		{"/docs/", "text/html", "https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.68.0/dist/browser/standalone.js"},
		{"/docs/init.js", "text/javascript", "Scalar.createApiReference"},
		{"/openapi.yaml", "application/yaml", "openapi: 3.1.0"},
	} {
		for _, method := range []string{"GET", "HEAD"} {
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, httptest.NewRequest(method, tc.path, nil))
			if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), tc.contentType) {
				t.Fatalf("%s %s: %d %s", method, tc.path, w.Code, w.Header())
			}
			if w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("missing documentation response headers", w.Header())
			}
			if method == "HEAD" {
				if w.Body.Len() != 0 {
					t.Fatal("HEAD returned a body")
				}
				continue
			}
			if !strings.Contains(w.Body.String(), tc.contains) {
				t.Errorf("%s: missing document content", tc.path)
			}
			if tc.path == "/openapi.yaml" && !bytes.Equal(w.Body.Bytes(), contract.OpenAPI) {
				t.Fatal("published schema differs from the embedded runtime contract")
			}
		}
	}
}

func TestDocumentationDoesNotBypassAPIAuthentication(t *testing.T) {
	s := testAPI(t, true)
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/v1/players", 401},
		{"POST", "/v1/players/room/playback", 401},
		{"GET", "/metrics", 401},
		{"GET", "/docs-other", 401},
		{"GET", "/docs/missing.js", 404},
		{"GET", "/docs/../v1/players", 404},
		{"GET", "/docs/%2e%2e/v1/players", 404},
		{"POST", "/docs", 405},
		{"PUT", "/openapi.yaml", 405},
	} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.status {
			t.Errorf("%s %s: got %d, want %d", tc.method, tc.path, w.Code, tc.status)
		}
		if tc.status == 405 && w.Header().Get("Allow") != "GET, HEAD" {
			t.Fatal("missing allowed methods")
		}
	}
	// Enabling docs must preserve ordinary authenticated reads as well.
	r := httptest.NewRequest("GET", "/v1/players", nil)
	r.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
}
