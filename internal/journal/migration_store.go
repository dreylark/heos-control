package journal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/dreylark/heos-control/internal/journal/dbgen"
	"github.com/pressly/goose/v3/database"
)

// Goose owns its version table and migration transactions. This small decorator
// adds the immutable SQL checksum and compatibility floor in the same transaction.
type migrationStore struct {
	database.Store
	catalog migrationCatalog
}

func (s *migrationStore) CreateVersionTable(ctx context.Context, db database.DBTxConn) error {
	var existing bool
	if err := db.QueryRowContext(ctx, "SELECT to_regclass('heos.schema_migrations') IS NOT NULL").Scan(&existing); err != nil {
		return err
	}
	if existing {
		return errors.New("application migration metadata exists without goose metadata; clean transition or repair required")
	}
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS heos; REVOKE ALL ON SCHEMA heos FROM PUBLIC"); err != nil {
		return err
	}
	return s.Store.CreateVersionTable(ctx, db)
}

func (s *migrationStore) Insert(ctx context.Context, db database.DBTxConn, req database.InsertRequest) error {
	if req.Version < 0 || req.Version > int64(len(s.catalog.steps)) {
		return errors.New("unknown migration version")
	}
	if err := s.Store.Insert(ctx, db, req); err != nil {
		return err
	}
	if req.Version == 0 {
		return nil // Goose's initial marker precedes creation of application tables.
	}
	step := s.catalog.steps[req.Version-1]
	_, err := db.ExecContext(ctx, `INSERT INTO heos.schema_migrations(version, checksum, min_runtime)
VALUES ($1, $2, $3)`, req.Version, step.checksum, step.minRuntime)
	return err
}

func (*migrationStore) Delete(context.Context, database.DBTxConn, int64) error {
	return errors.New("down migrations are disabled")
}

func (s *migrationStore) ListMigrations(ctx context.Context, db database.DBTxConn) ([]*database.ListMigrationsResult, error) {
	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT to_regclass('heos.schema_migrations') IS NOT NULL").Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		versions, err := s.Store.ListMigrations(ctx, db)
		if err != nil {
			return nil, err
		}
		if len(versions) == 1 && versions[0].Version == 0 && versions[0].IsApplied {
			return versions, nil
		}
		// The initial migration may have committed since the existence query.
		// Read both ledgers together below, rather than comparing two snapshots.
	}
	rows, err := db.QueryContext(ctx, `SELECT g.version_id, g.is_applied, s.version, s.checksum, s.min_runtime
FROM heos.goose_db_version g FULL JOIN heos.schema_migrations s ON s.version = g.version_id
ORDER BY COALESCE(g.version_id, s.version)`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []*database.ListMigrationsResult
	var applied []dbgen.ListMigrationsRow
	for rows.Next() {
		var version, recorded, floor sql.NullInt64
		var installed sql.NullBool
		var digest sql.NullString
		if err := rows.Scan(&version, &installed, &recorded, &digest, &floor); err != nil {
			return nil, err
		}
		if !version.Valid || !installed.Valid || !installed.Bool || version.Int64 != int64(len(result)) {
			return nil, errors.New("inconsistent goose migration ledger")
		}
		if version.Int64 > 0 {
			if !recorded.Valid || !floor.Valid || !digest.Valid || floor.Int64 < 1 || floor.Int64 > version.Int64 ||
				version.Int64 > int64(len(s.catalog.steps)) {
				return nil, errors.New("missing or unknown migration evidence")
			}
			applied = append(applied, dbgen.ListMigrationsRow{Version: int32(version.Int64), Checksum: digest.String, MinRuntime: int32(floor.Int64)})
		}
		result = append(result, &database.ListMigrationsResult{Version: version.Int64, IsApplied: true})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, errors.New("missing goose initial version")
	}
	if err := s.catalog.validate(applied, false, false); err != nil {
		return nil, fmt.Errorf("migration integrity: %w", err)
	}
	slices.Reverse(result) // Goose expects newest versions first.
	return result, nil
}
