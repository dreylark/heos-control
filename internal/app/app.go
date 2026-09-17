// Package app wires the service dependencies and owns their lifecycle.
package app

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/dreylark/heos-control/internal/api"
	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/journal"
)

func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	credentials, err := config.LoadCredentials(cfg.CredentialsFile, cfg.Players)
	if err != nil {
		return err
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return errors.New("cannot load API TLS certificate/key")
	}
	db, err := journal.Open(ctx, cfg.Database)
	if err != nil {
		return err
	}
	journalCtx, stopJournal := context.WithCancel(ctx)
	journalDone := make(chan struct{})
	go func() {
		defer close(journalDone)
		maintainJournal(journalCtx, db, logger)
	}()
	defer func() {
		stopJournal()
		<-journalDone
		db.Close()
	}()
	var stopping atomic.Bool
	deviceCtx, stopDevices := context.WithCancel(ctx)
	defer stopDevices()
	devices, err := openDevices(deviceCtx, cfg, logger)
	if err != nil {
		return err
	}
	defer devices.Close()
	devices.watchOperations(db)
	apiServer, err := api.NewServer(devices.reads, db, credentials, &stopping, devices.events, cfg.DocsEnabled)
	if err != nil {
		return err
	}
	defer apiServer.Close()
	coordinator := control.NewCoordinator(ctx, devices.reads, db, func(o journal.Operation) {
		if o.State == journal.Accepted || o.FinishedAt != nil {
			logger.Info("operation changed", "operation_id", o.ID, "player", o.Player, "kind", o.Kind, "state", o.State, "phase", o.Phase, "revision", o.Revision, "error_code", o.ErrorCode)
		}
		devices.events.Publish(api.Event{Kind: "operation_changed", Player: o.Player, Operation: o.ID, Principal: o.Principal})
	}, logger)
	defer coordinator.Close()
	apiServer.SetCoordinator(coordinator)
	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           apiServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      35 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}},
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return errors.New("cannot bind API listener")
	}
	done := make(chan error, 1)
	go func() { done <- srv.ServeTLS(listener, "", "") }()
	logger.Info("service started", "listen", cfg.Listen)
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("HTTP server failed")
	case <-ctx.Done():
	}
	stopping.Store(true)
	stopDevices()
	devices.events.Close()
	shutdown, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownSeconds)*time.Second)
	defer cancel()
	if err = srv.Shutdown(shutdown); err != nil {
		_ = srv.Close()
		<-done
		return errors.New("HTTP shutdown deadline exceeded")
	}
	<-done
	logger.Info("service stopped")
	return nil
}

// Startup recovery assumes the old writer is known stopped before this process starts.
// Initialization retries allow liveness while PostgreSQL is initially offline.
func maintainJournal(ctx context.Context, db *journal.Store, logger *slog.Logger) {
	for {
		if err := db.Recover(ctx); err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
	logger.Info("journal recovery complete")
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		if _, err := db.Prune(ctx, journal.MaxBatch); err != nil && ctx.Err() == nil {
			logger.Warn("journal retention deferred")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
