//go:build integration

package journal

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This suite resets only the explicitly selected disposable heos_test schema.
// Requesting integration tests without configuration is an error, never a skip.
func loadIntegrationConfig(t *testing.T, env string) config.Config {
	t.Helper()
	path := os.Getenv(env)
	if path == "" {
		t.Fatalf("%s is required; use make integration", env)
	}
	c, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Database.Name != "heos_test" || (c.Database.Host != "localhost" && c.Database.Host != "postgres") {
		t.Fatal("integration tests require the disposable local heos_test database")
	}
	return c
}

func TestPostgreSQLLifecycle(t *testing.T) {
	catalog, err := serviceMigrations()
	if err != nil {
		t.Fatal(err)
	}
	ownerCfg, runtimeCfg := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG"), loadIntegrationConfig(t, "HEOS_TEST_RUNTIME_CONFIG")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pc, err := poolConfig(ownerCfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := pgx.ConnectConfig(ctx, pc.ConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Close(context.Background()) }()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := owner.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`DROP SCHEMA IF EXISTS heos CASCADE;
		CREATE SCHEMA heos;
		GRANT USAGE ON SCHEMA heos TO heos_runtime;
		ALTER DEFAULT PRIVILEGES IN SCHEMA heos GRANT SELECT ON TABLES TO heos_runtime`)
	store, err := Open(ctx, runtimeCfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// HTTP health behavior is covered by api tests and the Compose smoke test.
	// Journal integration checks its readiness boundary directly to avoid a
	// reverse dependency on the API, which now reads journal operations.
	check := func(ready bool) {
		t.Helper()
		err := store.Ready(ctx)
		if (err == nil) != ready {
			t.Fatalf("readiness=%v: %v", ready, err)
		}
	}
	check(false)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if err := Migrate(ctx, ownerCfg.Database); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	if err := store.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	check(true)
	var appliedAt time.Time
	if err := owner.QueryRow(ctx, "SELECT applied_at FROM heos.schema_migrations WHERE version=1").Scan(&appliedAt); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, ownerCfg.Database); err != nil {
		t.Fatal(err)
	}
	var after time.Time
	if err := owner.QueryRow(ctx, "SELECT applied_at FROM heos.schema_migrations WHERE version=1").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.Equal(appliedAt) {
		t.Fatal("repeat migration rewrote metadata")
	}
	for _, sql := range []string{"CREATE TABLE heos.forbidden(id int)", "CREATE TABLE public.forbidden(id int)", "CREATE SCHEMA forbidden", "CREATE TEMP TABLE forbidden(id int)", "DELETE FROM heos.schema_migrations"} {
		_, err := store.pool.Exec(ctx, sql)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
			t.Fatalf("runtime privileges for %q: %v", sql, err)
		}
	}
	exec("UPDATE heos.schema_migrations SET checksum=$1 WHERE version=1", strings.Repeat("0", 64))
	check(false)
	if err := Migrate(ctx, ownerCfg.Database); err == nil {
		t.Fatal("migration accepted a modified checksum")
	}
	exec("UPDATE heos.schema_migrations SET checksum=$1 WHERE version=1", catalog.steps[0].checksum)
	exec("INSERT INTO heos.schema_migrations(version,checksum,min_runtime) VALUES ($1,$2,$1)", len(catalog.steps)+1, strings.Repeat("0", 64))
	check(false)
	if err := Migrate(ctx, ownerCfg.Database); err == nil {
		t.Fatal("migration accepted a newer schema")
	}
	exec("DELETE FROM heos.schema_migrations WHERE version=$1", len(catalog.steps)+1)
	check(true)
	t.Run("pool acquisition is bounded", func(t *testing.T) {
		d := runtimeCfg.Database
		d.MaxConnections = 1
		s, err := Open(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		conn, err := s.pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		deadline, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		if err := s.Ready(deadline); err == nil {
			t.Fatal("exhausted pool reported ready")
		}
		if time.Since(start) > time.Second {
			t.Fatal("acquisition ignored request deadline")
		}
	})
	t.Run("TLS hostname verification", func(t *testing.T) {
		c, err := poolConfig(runtimeCfg.Database)
		if err != nil {
			t.Fatal(err)
		}
		if c.ConnConfig.TLSConfig == nil || c.ConnConfig.TLSConfig.InsecureSkipVerify {
			t.Fatal("database TLS verification disabled")
		}
		c.ConnConfig.TLSConfig.ServerName = "wrong.invalid"
		p, err := pgxpool.NewWithConfig(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if err := p.Ping(ctx); err == nil || !strings.Contains(err.Error(), "certificate") {
			t.Fatalf("expected certificate rejection, got %v", err)
		}
	})
	t.Run("state survives pool replacement", func(t *testing.T) {
		s, err := Open(ctx, runtimeCfg.Database)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if err := s.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.Ready(ctx); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM heos.schema_migrations").Scan(&count); err != nil || count != len(catalog.steps) {
			t.Fatalf("persisted metadata: %d %v", count, err)
		}
	})
}
