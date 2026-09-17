package api

import (
	"context"
	"net/http"
	"sync/atomic"
)

type Readiness interface{ Ready(context.Context) error }

func HealthHandler(db Readiness, stopping *atomic.Bool) http.Handler {
	handler := Handler(NewStrictHandler(&Server{healthServer: healthServer{db: db, stopping: stopping}}, nil))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path != "/livez" && r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		handler.ServeHTTP(w, r)
	})
}

type healthServer struct {
	db       Readiness
	stopping *atomic.Bool
}

func (s *healthServer) GetLiveness(context.Context, GetLivenessRequestObject) (GetLivenessResponseObject, error) {
	return GetLiveness200JSONResponse{Status: Live}, nil
}

func (s *healthServer) GetReadiness(ctx context.Context, _ GetReadinessRequestObject) (GetReadinessResponseObject, error) {
	if s.stopping.Load() || s.db.Ready(ctx) != nil || s.stopping.Load() {
		return GetReadiness503JSONResponse{Status: Unavailable}, nil //nolint:nilerr // Database failure is represented by the typed HTTP 503 response.
	}
	return GetReadiness200JSONResponse{Status: Ready}, nil
}
