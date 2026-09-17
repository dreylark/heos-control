//go:build integration

// Kept in the journal test binary so schema-changing integration tests cannot
// run concurrently with the application lifecycle against the same heos_test DB.
package journal_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/app"
	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/journal"
)

func lifecycleConfig(t *testing.T, env string) config.Config {
	t.Helper()
	name := os.Getenv(env)
	if name == "" {
		t.Fatalf("%s is required; use make integration", env)
	}
	cfg, err := config.Load(name)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Name != "heos_test" || (cfg.Database.Host != "localhost" && cfg.Database.Host != "postgres") {
		t.Fatal("application integration requires disposable local heos_test")
	}
	// No configured hardware or credentials are ever inherited by this fixture.
	cfg.Players, cfg.Sources, cfg.CredentialsFile = nil, nil, ""
	cfg.DocsEnabled = false
	cfg.ShutdownSeconds = 2
	return cfg
}

func unusedAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	_ = l.Close()
	return address
}

func runLifecycle(t *testing.T, cfg config.Config, ready int, observe func()) {
	t.Helper()
	cfg.Listen = unusedAddress(t)
	ca, err := os.ReadFile(cfg.Database.CAFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid local CA")
	}
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("application failed to join shutdown/recovery goroutines")
		}
	})
	status := func(path string) int {
		r, err := client.Get("https://" + cfg.Listen + path)
		if err != nil {
			return 0
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		return r.StatusCode
	}
	deadline := time.Now().Add(10 * time.Second)
	for status("/readyz") != ready {
		if time.Now().After(deadline) {
			t.Fatal("readiness did not reach expected state", ready)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status("/livez") != 200 || status("/metrics") != 401 {
		t.Fatal("liveness or default-deny authentication boundary changed")
	}
	docsStatus := 404
	if cfg.DocsEnabled {
		docsStatus = 200
	}
	for _, path := range []string{"/docs", "/docs/init.js", "/openapi.yaml"} {
		if got := status(path); got != docsStatus {
			t.Fatalf("documentation config was not applied to %s: got %d, want %d", path, got, docsStatus)
		}
	}
	if observe != nil {
		observe()
	}
}

func TestApplicationRecoversBeforeReadyAndJoinsShutdown(t *testing.T) {
	cfg := lifecycleConfig(t, "HEOS_TEST_RUNTIME_CONFIG")
	owner := lifecycleConfig(t, "HEOS_TEST_OWNER_CONFIG")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := journal.Migrate(ctx, owner.Database); err != nil {
		t.Fatal(err)
	}
	store, err := journal.Open(ctx, cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	key := "lifecycle-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	r := journal.Request{Principal: "fixture", Method: "POST", Endpoint: "/v1/players/fixture/playback", Player: "fixture", Key: key, Body: []byte(`{}`)}
	now := time.Now()
	a, err := store.Admit(ctx, r, journal.Proposal{Kind: "playback", DeviceKey: key, ConfigRevision: "fixture", EffectiveArguments: []byte(`{}`), NotBefore: now.Add(-time.Second), NotAfter: now.Add(time.Minute)})
	if err != nil || !a.Created {
		t.Fatal(a, err)
	}
	if _, err := store.Transition(ctx, a.Operation.ID, a.Operation.Revision, journal.Update{State: journal.Running, Phase: "playing"}); err != nil {
		t.Fatal(err)
	}
	runLifecycle(t, cfg, 200, func() {
		op, err := store.Get(ctx, a.Operation.ID)
		if err != nil || op.State != journal.Uncertain || op.ErrorCode != "process_interrupted" || op.FinishedAt == nil {
			t.Fatal("readiness preceded interrupted-operation recovery", op, err)
		}
		if _, err := store.Active(ctx, "fixture"); !errors.Is(err, journal.ErrNotFound) {
			t.Fatal("recovery retained the reservation", err)
		}
		if err := store.Ready(ctx); !errors.Is(err, journal.ErrStaleEpoch) {
			t.Fatal("old journal writer was not fenced", err)
		}
	})
}

func TestApplicationStartsAndStopsWhileDatabaseIsUnavailable(t *testing.T) {
	cfg := lifecycleConfig(t, "HEOS_TEST_RUNTIME_CONFIG")
	cfg.DocsEnabled = true
	host, port, err := net.SplitHostPort(unusedAddress(t))
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Database.Host, cfg.Database.Port, cfg.Database.TimeoutSeconds = host, uint16(n), 1
	runLifecycle(t, cfg, 503, nil)
}
