package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	contract "github.com/dreylark/heos-control/api"
	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
	"github.com/pb33f/libopenapi"
	validator "github.com/pb33f/libopenapi-validator"
	validationconfig "github.com/pb33f/libopenapi-validator/config"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
)

type JournalReader interface {
	Readiness
	Get(context.Context, string) (journal.Operation, error)
	Active(context.Context, string) (journal.Operation, error)
	List(context.Context, journal.HistoryQuery) (journal.HistoryPage, error)
}
type principalKey struct{}
type requestKey struct{}
type principal struct{ config.Credential }

func (p principal) allows(player string) bool { return slices.Contains(p.Players, player) }
func (p principal) operator() bool            { return slices.Contains(p.Scopes, "operator") }
func identity(ctx context.Context) principal  { return ctx.Value(principalKey{}).(principal) }

type Server struct {
	healthServer
	reads            *control.Reads
	journal          JournalReader
	credentials      []config.Credential
	events           *Events
	validator        validator.Validator
	handler          http.Handler
	requests         chan struct{}
	priorityRequests chan struct{}
	coordinator      Submitter
	docsEnabled      bool
}

var _ StrictServerInterface = (*Server)(nil)

func NewServer(reads *control.Reads, db JournalReader, credentials []config.Credential, stopping *atomic.Bool, events *Events, docsEnabled bool) (*Server, error) {
	doc, e := libopenapi.NewDocument(contract.OpenAPI)
	if e != nil {
		return nil, e
	}
	m, e := doc.BuildV3Model()
	if e != nil {
		return nil, e
	}
	s := &Server{healthServer: healthServer{db, stopping}, reads: reads, journal: db, credentials: credentials, events: events, requests: make(chan struct{}, 32), priorityRequests: make(chan struct{}, 4)}
	s.docsEnabled = docsEnabled
	// Authentication is performed before routing, cached responses and validation.
	s.validator = validator.NewValidatorFromV3Model(&m.Model, validationconfig.WithoutSecurityValidation())
	invalid := func(w http.ResponseWriter, r *http.Request, e error) {
		writeError(w, r, &apiFailure{400, "invalid_request", false})
	}
	strict := NewStrictHandlerWithOptions(s, nil, StrictHTTPServerOptions{RequestErrorHandlerFunc: invalid, ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, e error) { writeError(w, r, e) }})
	generated := HandlerWithOptions(strict, StdHTTPServerOptions{ErrorHandlerFunc: invalid})
	routes := http.NewServeMux()
	for path, item := range m.Model.Paths.PathItems.FromOldest() {
		for method, op := range map[string]*v3.Operation{"GET": item.Get, "POST": item.Post, "PUT": item.Put, "DELETE": item.Delete} {
			if op == nil {
				continue
			}
			routes.Handle(method+" "+path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != method {
					writeError(w, r, &apiFailure{404, "not_found", false})
					return
				}
				if path == "/livez" || path == "/readyz" {
					generated.ServeHTTP(w, r)
					return
				}
				if s.stopping.Load() {
					writeError(w, r, &apiFailure{503, "shutting_down", true})
					return
				}
				permits := s.requests
				if path == "/v1/players/{player}/stop" || path == "/v1/operations/{operation}/cancel" {
					permits = s.priorityRequests
				}
				select {
				case permits <- struct{}{}:
				default:
					writeError(w, r, &apiFailure{429, "request_capacity", true})
					return
				}
				acquired := true
				defer func() {
					if acquired {
						<-permits
					}
				}()
				allowed := map[string]bool{}
				for _, p := range append(append([]*v3.Parameter{}, item.Parameters...), op.Parameters...) {
					if p.In == "query" {
						allowed[p.Name] = true
					}
				}
				q, e := url.ParseQuery(r.URL.RawQuery)
				if e != nil {
					invalid(w, r, e)
					return
				}
				for k, v := range q {
					if !allowed[k] || len(v) != 1 {
						invalid(w, r, nil)
						return
					}
				}
				for _, h := range []string{"Last-Event-ID", "Content-Type", "Idempotency-Key", "If-Match"} {
					if len(r.Header.Values(h)) > 1 {
						invalid(w, r, nil)
						return
					}
				}
				b, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<10))
				if e != nil {
					invalid(w, r, e)
					return
				}
				if len(b) > 0 && ((r.Method != "POST" && r.Method != "PUT") || config.UniqueJSON(b) != nil) {
					invalid(w, r, nil)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(b))
				if ok, _ := s.validator.ValidateHttpRequestSyncWithPathItem(r, item, path); !ok {
					if mutationOutOfBounds(path, b) {
						writeError(w, r, heos.ErrBounds)
						return
					}
					invalid(w, r, nil)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(b))
				if path == "/v1/events" {
					<-permits
					acquired = false
					generated.ServeHTTP(w, r)
					return
				}
				ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
				defer cancel()
				generated.ServeHTTP(w, r.WithContext(ctx))
			}))
		}
	}
	routes.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { writeError(w, r, &apiFailure{404, "not_found", false}) })
	s.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := make([]byte, 16)
		_, _ = rand.Read(id)
		w.Header().Set("X-Request-ID", hex.EncodeToString(id))
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if s.serveDocumentation(w, r) {
			return
		}
		if r.URL.Path != "/livez" && r.URL.Path != "/readyz" {
			p, ok := s.authenticate(r)
			if !ok {
				writeError(w, r, &apiFailure{401, "unauthorized", false})
				return
			}
			scope := "read"
			if (r.Method == "POST" && !strings.HasSuffix(r.URL.Path, "/preflight")) || r.Method == "PUT" {
				scope = "control"
				if strings.HasSuffix(r.URL.Path, "/playback") || strings.HasSuffix(r.URL.Path, "/stop") {
					scope = "operator"
				}
			}
			if !slices.Contains(p.Scopes, scope) {
				writeError(w, r, &apiFailure{403, "forbidden", false})
				return
			}
			ctx := context.WithValue(r.Context(), principalKey{}, p)
			r = r.WithContext(context.WithValue(ctx, requestKey{}, r))
		}
		if strings.Contains(r.URL.Path, "//") || strings.Contains(r.URL.Path, "/../") || strings.Contains(r.URL.Path, "/./") || r.URL.RawPath != "" {
			invalid(w, r, nil)
			return
		}
		routes.ServeHTTP(w, r)
	})
	return s, nil
}
func (s *Server) Handler() http.Handler { return s.handler }
func (s *Server) Close()                { s.events.Close(); s.validator.Release() }
func (s *Server) authenticate(r *http.Request) (principal, bool) {
	if len(r.Header.Values("Authorization")) != 1 {
		return principal{}, false
	}
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || len(token) < 32 || len(token) > 256 || strings.ContainsAny(token, " \t\r\n") {
		return principal{}, false
	}
	digest := sha256.Sum256([]byte(token))
	var found principal
	valid := false
	for _, c := range s.credentials {
		expected, e := hex.DecodeString(c.TokenSHA256)
		if e == nil && subtle.ConstantTimeCompare(digest[:], expected) == 1 {
			found = principal{c}
			valid = true
		}
	}
	return found, valid
}

type apiFailure struct {
	status int
	code   string
	retry  bool
}

func (e *apiFailure) Error() string { return e.code }
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	f := &apiFailure{503, "unavailable", true}
	var typed *apiFailure
	switch {
	case errors.As(err, &typed):
		f = typed
	case errors.Is(err, control.ErrNotFound), errors.Is(err, journal.ErrNotFound):
		f = &apiFailure{404, "not_found", false}
	case errors.Is(err, control.ErrPreconditionRequired):
		f = &apiFailure{428, "precondition_required", false}
	case errors.Is(err, control.ErrPrecondition):
		f = &apiFailure{412, "revision_changed", false}
	case errors.Is(err, control.ErrGrouped):
		f = &apiFailure{409, "grouped_target", false}
	case errors.Is(err, control.ErrOwnership):
		f = &apiFailure{409, "ownership_lost", false}
	case errors.Is(err, journal.ErrConflict):
		f = &apiFailure{409, "idempotency_conflict", false}
	case errors.Is(err, journal.ErrBusy), errors.Is(err, journal.ErrRevision):
		f = &apiFailure{409, "device_reserved", false}
	case errors.Is(err, journal.ErrInvalid):
		f = &apiFailure{400, "invalid_request", false}
	case errors.Is(err, heos.ErrReadOnly):
		f = &apiFailure{422, "writes_disabled", false}
	case errors.Is(err, control.ErrAmbiguous):
		f = &apiFailure{409, "ambiguous_catalog", false}
	case errors.Is(err, heos.ErrStaleReference), errors.Is(err, heos.ErrStale):
		f = &apiFailure{409, "stale_reference", false}
	case errors.Is(err, heos.ErrBounds):
		f = &apiFailure{422, "out_of_bounds", false}
	case errors.Is(err, control.ErrNotSkippable):
		f = &apiFailure{422, "not_skippable", false}
	case errors.Is(err, heos.ErrQueueFull), errors.Is(err, heos.ErrCatalogFull), errors.Is(err, errEventCapacity), errors.Is(err, journal.ErrCapacity):
		f = &apiFailure{429, "capacity_exceeded", true}
	}
	if f.status == 401 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="heos-control"`)
	}
	if f.status == 429 {
		w.Header().Set("Retry-After", "1")
	}
	var body Error
	body.RequestId = w.Header().Get("X-Request-ID")
	body.Error.Code = f.code
	body.Error.Message = http.StatusText(f.status)
	body.Error.Outcome = NotSent
	if errors.Is(err, journal.ErrCommitUncertain) {
		body.Error.Outcome = Unknown
		body.Error.Retryable = false
	}
	body.Error.Retryable = f.retry
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(f.status)
	_ = json.NewEncoder(w).Encode(body)
}
func authorize(ctx context.Context, key string) error {
	if !identity(ctx).allows(key) {
		return &apiFailure{403, "forbidden", false}
	}
	return nil
}
func (s *Server) player(ctx context.Context, key string) (control.Player, error) {
	if e := authorize(ctx, key); e != nil {
		return control.Player{}, e
	}
	p, e := s.reads.Player(key)
	if e != nil {
		return p, e
	}
	op, e := s.journal.Active(ctx, key)
	if e != nil && !errors.Is(e, journal.ErrNotFound) {
		return p, e
	}
	if e == nil && (op.Principal == identity(ctx).Principal || identity(ctx).operator()) {
		p.ActiveOperation = &op.ID
	}
	return p, nil
}
func (s *Server) GetPlayers(ctx context.Context, _ GetPlayersRequestObject) (GetPlayersResponseObject, error) {
	out := []Player{}
	for _, d := range s.reads.Devices() {
		if !identity(ctx).allows(d.Config.Key) {
			continue
		}
		p, e := s.player(ctx, d.Config.Key)
		if e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return GetPlayers200JSONResponse{Body: out}, nil
}
func (s *Server) GetPlayer(ctx context.Context, r GetPlayerRequestObject) (GetPlayerResponseObject, error) {
	p, e := s.player(ctx, r.Player)
	if e != nil {
		return nil, e
	}
	tag := fmt.Sprintf("%q", p.Revision)
	return GetPlayer200JSONResponse{Body: p, Headers: GetPlayer200ResponseHeaders{ETag: &tag}}, nil
}
func (s *Server) GetGroups(ctx context.Context, _ GetGroupsRequestObject) (GetGroupsResponseObject, error) {
	return GetGroups200JSONResponse{Body: s.reads.Groups(identity(ctx).allows)}, nil
}
func (s *Server) GetSources(ctx context.Context, _ GetSourcesRequestObject) (GetSourcesResponseObject, error) {
	return GetSources200JSONResponse{Body: s.reads.Sources(ctx, identity(ctx).allows)}, nil
}
func value[T any](p *T, fallback T) T {
	if p != nil {
		return *p
	}
	return fallback
}
func (s *Server) GetItems(ctx context.Context, r GetItemsRequestObject) (GetItemsResponseObject, error) {
	p, e := s.reads.SourcePlayer(r.Source)
	if e != nil {
		return nil, e
	}
	if e = authorize(ctx, p); e != nil {
		return nil, e
	}
	v, e := s.reads.Items(ctx, r.Source, value(r.Params.ParentRef, ""), value(r.Params.Cursor, ""), value(r.Params.Limit, 50))
	if e != nil {
		return nil, e
	}
	return GetItems200JSONResponse{Body: v}, nil
}
func (s *Server) GetQueue(ctx context.Context, r GetQueueRequestObject) (GetQueueResponseObject, error) {
	if e := authorize(ctx, r.Player); e != nil {
		return nil, e
	}
	v, e := s.reads.Queue(ctx, r.Player, value(r.Params.Revision, ""), value(r.Params.Offset, 0), value(r.Params.Limit, 50))
	if e != nil {
		return nil, e
	}
	return GetQueue200JSONResponse{Body: v}, nil
}
func (s *Server) Preflight(ctx context.Context, r PreflightRequestObject) (PreflightResponseObject, error) {
	if e := authorize(ctx, r.Player); e != nil {
		return nil, e
	}
	if r.Body.Takeover && !identity(ctx).operator() {
		return nil, &apiFailure{403, "operator_required", false}
	}
	v, e := s.reads.Preflight(ctx, r.Player, playbackCommand(*r.Body))
	if e != nil {
		return nil, e
	}
	op, e := s.journal.Active(ctx, r.Player)
	if e != nil && !errors.Is(e, journal.ErrNotFound) {
		return nil, e
	}
	if e == nil {
		if !r.Body.Takeover {
			v.Ready = false
			v.Warnings = append(v.Warnings, "device_reserved")
		}
		if op.Principal == identity(ctx).Principal || identity(ctx).operator() {
			v.Player.ActiveOperation = &op.ID
		}
	}
	return Preflight200JSONResponse{Body: v}, nil
}
func (s *Server) GetOperation(ctx context.Context, r GetOperationRequestObject) (GetOperationResponseObject, error) {
	op, e := s.journal.Get(ctx, r.Operation)
	if e != nil {
		return nil, e
	}
	p := identity(ctx)
	if !p.allows(op.Player) || (p.Principal != op.Principal && !p.operator()) {
		return nil, &apiFailure{403, "forbidden", false}
	}
	v := control.ProjectOperation(op)
	return GetOperation200JSONResponse{Body: v}, nil
}
func (s *Server) GetEvents(ctx context.Context, r GetEventsRequestObject) (GetEventsResponseObject, error) {
	p := identity(ctx)
	sub, replay, e := s.events.Subscribe(value(r.Params.LastEventID, ""), func(e Event) bool {
		return (e.Player == "" || p.allows(e.Player)) && (e.Operation == "" || e.Principal == p.Principal || p.operator())
	})
	if e != nil {
		return nil, e
	}
	// Use the final request context so client disconnect and app shutdown end SSE.
	req := ctx.Value(requestKey{}).(*http.Request).WithContext(ctx)
	return eventStream{sub, replay, req}, nil
}
