// Package journal owns PostgreSQL connections and schema compatibility.
package journal

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/journal/dbgen"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool          *pgxpool.Pool
	timeout       time.Duration
	epoch         string
	now           func() time.Time
	maxOperations int64
	initialized   atomic.Bool
	recoveryMu    sync.Mutex
	previousEpoch *string
	commit        func(context.Context, pgx.Tx) error
}

func poolConfig(d config.Database) (*pgxpool.Config, error) {
	password, err := config.ReadReferencedFile(d.PasswordFile)
	if err != nil {
		return nil, errors.New("cannot read database password file")
	}
	if len(strings.TrimSpace(string(password))) == 0 {
		return nil, errors.New("database password file is empty")
	}
	ca, err := config.ReadReferencedFile(d.CAFile)
	if err != nil {
		return nil, errors.New("cannot read database CA file")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid database CA certificates")
	}
	u := url.URL{Scheme: "postgres", Host: net.JoinHostPort(d.Host, strconv.Itoa(int(d.Port))), Path: "/" + d.Name, User: url.UserPassword(d.User, strings.TrimSpace(string(password)))}
	q := u.Query()
	q.Set("sslmode", "verify-full")
	// Roots are loaded once through the bounded regular-file reader above.
	// Explicit empty values also prevent pgx from reading environment/default
	// CA, client-certificate, service or password files outside this contract.
	for _, key := range []string{"sslrootcert", "sslcert", "sslkey", "servicefile", "passfile"} {
		q.Set(key, "")
	}
	q.Set("connect_timeout", strconv.Itoa(d.TimeoutSeconds))
	u.RawQuery = q.Encode()
	c, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, errors.New("invalid database TLS configuration")
	}
	configs := []*tls.Config{c.ConnConfig.TLSConfig}
	for _, fallback := range c.ConnConfig.Fallbacks {
		configs = append(configs, fallback.TLSConfig)
	}
	for _, cfg := range configs {
		// pgx ignores sslmode for Unix sockets; reject every such route, including
		// fallback routes. Keep verify-full's hostname and certificate checks.
		if cfg == nil || cfg.InsecureSkipVerify || cfg.ServerName == "" {
			return nil, errors.New("database connection requires verified TLS")
		}
		cfg.RootCAs = roots
	}
	c.MaxConns, c.MinConns = d.MaxConnections, 0
	c.ConnConfig.RuntimeParams["application_name"] = "heos-control"
	c.ConnConfig.RuntimeParams["search_path"] = "pg_catalog"
	c.ConnConfig.RuntimeParams["statement_timeout"] = strconv.Itoa(d.TimeoutSeconds * 1000)
	c.ConnConfig.RuntimeParams["lock_timeout"] = strconv.Itoa(d.LockTimeoutSeconds * 1000)
	c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = strconv.Itoa(d.TimeoutSeconds * 1000)
	return c, nil
}

func Open(ctx context.Context, d config.Database) (*Store, error) {
	c, err := poolConfig(d)
	if err != nil {
		return nil, err
	}
	p, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, errors.New("cannot initialize database pool")
	}
	return &Store{pool: p, timeout: time.Duration(d.TimeoutSeconds) * time.Second,
		epoch: rand.Text(), now: time.Now, maxOperations: 10000,
		commit: func(ctx context.Context, tx pgx.Tx) error { return tx.Commit(ctx) }}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Ready acquires a real connection and verifies all recorded checksums. Pool
// construction alone is deliberately insufficient. No DDL runs on this path.
func (s *Store) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if err := s.checkSchema(ctx); err != nil {
		return err
	}
	if !s.initialized.Load() {
		return ErrNotInitialized
	}
	control, err := dbgen.New(s.pool).ReadJournal(ctx)
	if err != nil {
		return fmt.Errorf("read journal readiness: %w", err)
	}
	if control.Epoch != s.epoch || !control.Ready {
		return ErrStaleEpoch
	}
	return nil
}

func (s *Store) checkSchema(ctx context.Context) error {
	versions, err := dbgen.New(s.pool).ListMigrations(ctx)
	if err != nil {
		return errors.New("database unavailable or schema not initialized")
	}
	catalog, err := serviceMigrations()
	if err != nil {
		return err
	}
	return catalog.validate(versions, true, true)
}
