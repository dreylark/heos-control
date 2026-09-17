package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/pb33f/libopenapi"
)

type readinessFunc func(context.Context) error

func (f readinessFunc) Ready(ctx context.Context) error { return f(ctx) }

func TestHealthContract(t *testing.T) {
	data, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := libopenapi.NewDocument(data)
	if err != nil {
		t.Fatal(err)
	}
	model, err := doc.BuildV3Model()
	if err != nil {
		t.Fatal(err)
	}
	if model.Model.Version != "3.1.0" {
		t.Fatal("contract must remain OpenAPI 3.1.0")
	}
	for _, tc := range []struct {
		name, path       string
		outage, stopping bool
		code             int
		status           string
	}{
		{"live", "/livez", false, false, 200, "live"},
		{"live despite outage", "/livez", true, false, 200, "live"},
		{"ready", "/readyz", false, false, 200, "ready"},
		{"outage", "/readyz", true, false, 503, "unavailable"},
		{"shutdown", "/readyz", false, true, 503, "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stopping atomic.Bool
			stopping.Store(tc.stopping)
			db := readinessFunc(func(context.Context) error {
				if tc.path == "/livez" || tc.stopping {
					t.Fatal("must not query database")
				}
				if tc.outage {
					return errors.New("synthetic secret must not leak")
				}
				return nil
			})
			w := httptest.NewRecorder()
			HealthHandler(db, &stopping).ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
			if w.Code != tc.code || w.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("unexpected response: %d %v", w.Code, w.Header())
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("health responses must not be cached")
			}
			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if len(body) != 1 || body["status"] != tc.status {
				t.Fatalf("unexpected body %v", body)
			}
			path := model.Model.Paths.PathItems.GetOrZero(tc.path)
			response := path.Get.Responses.Codes.GetOrZero(strconv.Itoa(tc.code))
			if response == nil {
				t.Fatal("response missing from contract")
			}
			schema := response.Content.GetOrZero("application/json").Schema.Schema()
			value := schema.Properties.GetOrZero("status").Schema().Const.Value
			if value != tc.status || len(schema.Required) != 1 || schema.Required[0] != "status" || schema.AdditionalProperties.B {
				t.Fatal("health response drifted from contract")
			}
		})
	}
}
