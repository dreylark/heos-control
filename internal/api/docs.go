package api

import (
	_ "embed"
	"net/http"
	"strconv"
	"strings"

	contract "github.com/dreylark/heos-control/api"
)

// The small documentation shell is embedded; browsers load pinned Scalar from
// the CDN. The schema uses the same bytes as request validation.
//
//go:embed docs/index.html
var documentationHTML []byte

//go:embed docs/init.js
var documentationInit []byte

// serveDocumentation handles only the reserved documentation paths. Opting in
// exposes static documentation without a token, never application data or writes.
func (s *Server) serveDocumentation(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path
	if path != "/openapi.yaml" && path != "/docs" && !strings.HasPrefix(path, "/docs/") {
		return false
	}
	if !s.docsEnabled || r.URL.RawPath != "" {
		writeError(w, r, &apiFailure{404, "not_found", false})
		return true
	}
	var body []byte
	var contentType string
	switch path {
	case "/docs", "/docs/":
		body, contentType = documentationHTML, "text/html; charset=utf-8"
	case "/docs/init.js":
		body, contentType = documentationInit, "text/javascript; charset=utf-8"
	case "/openapi.yaml":
		body, contentType = contract.OpenAPI, "application/yaml"
	default:
		writeError(w, r, &apiFailure{404, "not_found", false})
		return true
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, r, &apiFailure{405, "method_not_allowed", false})
		return true
	}
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self' https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.68.0/dist/browser/standalone.js; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
	return true
}
