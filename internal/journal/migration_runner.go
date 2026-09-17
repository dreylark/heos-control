package journal

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"
)

// Migrate is the chart Job's forward-only entrypoint. Each SQL file and both
// metadata records commit in one goose-owned transaction. A failed later file
// leaves earlier committed migrations intact; rerun after resolving the cause.
func Migrate(ctx context.Context, d config.Database) error {
	catalog, err := serviceMigrations()
	if err != nil {
		return err
	}
	return migrateCatalog(ctx, d, catalog)
}

func migrate(ctx context.Context, d config.Database, files fs.FS) error {
	catalog, err := loadMigrations(files)
	if err != nil {
		return err
	}
	return migrateCatalog(ctx, d, catalog)
}

func migrateCatalog(ctx context.Context, d config.Database, catalog migrationCatalog) error {
	c, err := poolConfig(d)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(d.TimeoutSeconds)*time.Second)
	defer cancel()
	// Only migration execution uses database/sql. Runtime queries retain native pgx.
	db := stdlib.OpenDB(*c.ConnConfig)
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()
	return runMigrations(ctx, db, catalog)
}

func runMigrations(ctx context.Context, db *sql.DB, catalog migrationCatalog) error {
	base, err := database.NewStore(database.DialectPostgres, "heos.goose_db_version")
	if err != nil {
		return err
	}
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(72633001),
		lock.WithLockTimeout(1, 1), lock.WithUnlockTimeout(1, 1))
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectCustom, db, catalog.files,
		goose.WithStore(&migrationStore{Store: base, catalog: catalog}),
		goose.WithSessionLocker(boundedSessionLocker{SessionLocker: locker}), goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return fmt.Errorf("initialize goose: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations (inspect state and rerun after resolving the cause): %w", err)
	}
	return nil
}
