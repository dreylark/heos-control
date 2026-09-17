//go:build integration

package journal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/stdlib"
)

func migrationFiles(t *testing.T, additions ...string) fstest.MapFS {
	t.Helper()
	root, err := fs.Sub(migrations, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	files := fstest.MapFS{}
	entries, err := fs.ReadDir(root, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := fs.ReadFile(root, entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Name()] = &fstest.MapFile{Data: data, Mode: 0o444}
	}
	for i, sql := range additions {
		name := fmt.Sprintf("%05d_test.sql", len(entries)+i+1)
		files[name] = &fstest.MapFile{Data: []byte(sql), Mode: 0o444}
	}
	return files
}

func additiveMigration(sql string) string {
	return "-- +goose Up\n-- heos:min-runtime=1\n" + sql + "\n"
}

type migrationLedgerEntry struct {
	Version    int
	Checksum   string
	MinRuntime int
	AppliedAt  time.Time
}

func migrationLedger(t *testing.T, f *journalFixture) []migrationLedgerEntry {
	t.Helper()
	rows, err := f.owner.Query(f.ctx, "SELECT version, checksum, min_runtime, applied_at FROM heos.schema_migrations ORDER BY version")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []migrationLedgerEntry
	for rows.Next() {
		var row migrationLedgerEntry
		if err := rows.Scan(&row.Version, &row.Checksum, &row.MinRuntime, &row.AppliedAt); err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func checkMigrationVersions(t *testing.T, f *journalFixture, expected []int) {
	t.Helper()
	var actual []int
	for _, row := range migrationLedger(t, f) {
		actual = append(actual, row.Version)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("compatibility ledger versions = %v; want %v", actual, expected)
	}
	rows, err := f.owner.Query(f.ctx, "SELECT version_id FROM heos.goose_db_version WHERE is_applied AND version_id > 0 ORDER BY version_id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var gooseVersions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			t.Fatal(err)
		}
		gooseVersions = append(gooseVersions, version)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gooseVersions, expected) {
		t.Fatalf("goose versions = %v; want %v", gooseVersions, expected)
	}
}

func checkPreviousRuntimeWrites(t *testing.T, f *journalFixture, key string) {
	t.Helper()
	if err := f.store.Ready(f.ctx); err != nil {
		t.Fatalf("previous runtime lost readiness on additive schema: %v", err)
	}
	_, operation := f.admit(key, key)
	finished, err := f.store.Transition(f.ctx, operation.ID, operation.Revision, Update{
		State: Succeeded, Phase: "complete", Outcome: []byte(`{"delivery":"confirmed"}`),
	})
	if err != nil {
		t.Fatalf("previous runtime cannot update operation: %v", err)
	}
	stored, err := f.store.Get(f.ctx, operation.ID)
	if err != nil || stored.State != Succeeded || stored.Revision != finished.Revision {
		t.Fatalf("previous runtime cannot read operation: %+v, %v", stored, err)
	}
}

func TestGooseMigrationTransactionsAndPreviousRuntime(t *testing.T) {
	f := newJournalFixture(t)
	d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
	baseline := migrationLedger(t, f)
	files := migrationFiles(t,
		additiveMigration("ALTER TABLE heos.operations ADD COLUMN compatible_note text;"),
		additiveMigration("CREATE TABLE heos.future_history(id bigint PRIMARY KEY); SELECT 1 / 0;"),
		additiveMigration("ALTER TABLE heos.future_history ADD COLUMN note text;"),
	)
	if err := migrate(f.ctx, d, files); err == nil {
		t.Fatal("migration with division by zero succeeded")
	}
	checkMigrationVersions(t, f, []int{1, 2})
	var tableMissing bool
	if err := f.owner.QueryRow(f.ctx, "SELECT to_regclass('heos.future_history') IS NULL").Scan(&tableMissing); err != nil || !tableMissing {
		t.Fatalf("failed migration left a partial table: missing=%v, %v", tableMissing, err)
	}
	partial := migrationLedger(t, f)
	if !reflect.DeepEqual(partial[:1], baseline) {
		t.Fatal("applying a later migration rewrote the initial ledger")
	}
	checkPreviousRuntimeWrites(t, f, "intermediate")

	// Correct only the failed, unapplied migration. Migration 2 is already
	// committed and must not run again while resuming this failed release.
	files["00003_test.sql"].Data = []byte(additiveMigration("CREATE TABLE heos.future_history(id bigint PRIMARY KEY);"))
	if err := migrate(f.ctx, d, files); err != nil {
		t.Fatalf("retry after repairing unapplied migration: %v", err)
	}
	checkMigrationVersions(t, f, []int{1, 2, 3, 4})
	complete := migrationLedger(t, f)
	if !reflect.DeepEqual(complete[:2], partial) {
		t.Fatal("retry rewrote previously committed migration metadata")
	}
	checkPreviousRuntimeWrites(t, f, "complete")
	if err := migrate(f.ctx, d, files); err != nil {
		t.Fatalf("repeat successful migration: %v", err)
	}
	if !reflect.DeepEqual(migrationLedger(t, f), complete) {
		t.Fatal("no-op rerun changed migration checksums, timestamps or floors")
	}
	// An older runtime is allowed to serve this schema, but its migrator must
	// never claim it has verified unknown future migration source files.
	if err := Migrate(f.ctx, d); err == nil {
		t.Fatal("older migrator accepted an unknown future migration")
	}
}

func TestGooseMigrationCompatibilityFloor(t *testing.T) {
	f := newJournalFixture(t)
	d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
	files := migrationFiles(t, "-- +goose Up\n-- heos:min-runtime=2\nALTER TABLE heos.operations ADD COLUMN replacement_note text;\n")
	if err := migrate(f.ctx, d, files); err != nil {
		t.Fatal(err)
	}
	checkMigrationVersions(t, f, []int{1, 2})
	if err := f.store.Ready(f.ctx); err == nil {
		t.Fatal("previous runtime accepted a schema requiring runtime 2")
	}
	r, p := f.request("incompatible", "speaker")
	if _, err := f.store.Admit(f.ctx, r, p); err == nil {
		t.Fatal("previous runtime wrote to an incompatible schema")
	}
	f.counts(0, 0, 0)
}

func TestGooseMigrationRejectsInconsistentLedgers(t *testing.T) {
	for _, tc := range []struct {
		name           string
		sql            string
		runtimeInvalid bool
	}{
		{"changed known checksum", "UPDATE heos.schema_migrations SET checksum = repeat('0', 64) WHERE version = 1", true},
		{"missing known compatibility row", "DELETE FROM heos.schema_migrations WHERE version = 1", true},
		{"missing known goose row", "DELETE FROM heos.goose_db_version WHERE version_id = 1", false},
		{"extra goose row", "INSERT INTO heos.goose_db_version(version_id, is_applied) VALUES (2, true)", false},
		{"goose down state", "UPDATE heos.goose_db_version SET is_applied = false WHERE version_id = 1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newJournalFixture(t)
			d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
			f.exec(tc.sql)
			if err := f.store.Ready(f.ctx); (err != nil) != tc.runtimeInvalid {
				t.Fatalf("runtime readiness must depend on application compatibility evidence; invalid=%v: %v", tc.runtimeInvalid, err)
			}
			if err := Migrate(f.ctx, d); err == nil {
				t.Fatal("migrator accepted inconsistent migration ledgers")
			}
			var count int
			if err := f.owner.QueryRow(f.ctx, "SELECT count(*) FROM heos.operations").Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed metadata validation changed operations: %d, %v", count, err)
			}
		})
	}

	t.Run("gap in future history", func(t *testing.T) {
		f := newJournalFixture(t)
		d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
		files := migrationFiles(t, additiveMigration("SELECT 1;"), additiveMigration("SELECT 1;"))
		if err := migrate(f.ctx, d, files); err != nil {
			t.Fatal(err)
		}
		f.exec("DELETE FROM heos.schema_migrations WHERE version = 2; DELETE FROM heos.goose_db_version WHERE version_id = 2")
		if err := f.store.Ready(f.ctx); err == nil {
			t.Fatal("runtime accepted a gap in otherwise matching future ledgers")
		}
		if err := migrate(f.ctx, d, files); err == nil {
			t.Fatal("migrator accepted a gap in applied migrations")
		}
	})

	t.Run("changed applied future file", func(t *testing.T) {
		f := newJournalFixture(t)
		d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
		files := migrationFiles(t, additiveMigration("SELECT 1;"))
		if err := migrate(f.ctx, d, files); err != nil {
			t.Fatal(err)
		}
		files["00002_test.sql"].Data = []byte(additiveMigration("SELECT 2;"))
		if err := migrate(f.ctx, d, files); err == nil {
			t.Fatal("migrator accepted changed source for an applied migration")
		}
	})
}

func TestGooseMetadataIsReadOnlyForRuntime(t *testing.T) {
	f := newJournalFixture(t)
	for _, statement := range []string{
		"INSERT INTO heos.goose_db_version(version_id, is_applied) VALUES (999, true)",
		"UPDATE heos.goose_db_version SET is_applied = false WHERE version_id = 1",
		"DELETE FROM heos.goose_db_version WHERE version_id = 1",
		"INSERT INTO heos.schema_migrations(version, checksum, min_runtime) VALUES (999, repeat('0', 64), 1)",
		"UPDATE heos.schema_migrations SET min_runtime = 1 WHERE version = 1",
		"DELETE FROM heos.schema_migrations WHERE version = 1",
	} {
		_, err := f.store.pool.Exec(f.ctx, statement)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
			t.Fatalf("runtime metadata privilege for %q: %v", statement, err)
		}
	}
	if err := f.store.Ready(f.ctx); err != nil {
		t.Fatalf("runtime cannot read migration metadata: %v", err)
	}
}

func TestGooseMigrationLockCancellation(t *testing.T) {
	f := newJournalFixture(t)
	d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
	d.TimeoutSeconds, d.LockTimeoutSeconds = 10, 5
	files := migrationFiles(t, additiveMigration("ALTER TABLE heos.operations ADD COLUMN pending_note text;"))
	tx, err := f.owner.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(f.ctx, "LOCK TABLE heos.operations IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	migrationCtx, cancelMigration := context.WithCancel(f.ctx)
	go func() {
		firstDone <- migrate(migrationCtx, d, files)
		close(firstDone)
	}()
	defer func() {
		cancelMigration()
		_ = tx.Rollback(context.Background())
		select {
		case <-firstDone:
		case <-time.After(3 * time.Second):
			t.Error("first migrator did not finish during test cleanup")
		}
	}()
	// Wait until migration 2 is executing under the migration lock, but is
	// blocked on our table lock. This avoids assuming goose's advisory-lock ID.
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		if err := tx.QueryRow(f.ctx, "SELECT EXISTS (SELECT FROM pg_locks WHERE relation = 'heos.operations'::regclass AND NOT granted)").Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-firstDone:
			t.Fatalf("first migration did not wait for table lock: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("migration never reached the locked table")
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 100*time.Millisecond)
	start := time.Now()
	err = migrate(ctx, d, files)
	cancel()
	if err == nil {
		t.Fatal("contending migration ignored context cancellation")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("migration-lock cancellation was not bounded: %v", err)
	}
	if err := tx.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-firstDone; err != nil {
		t.Fatalf("first migrator failed after table lock released: %v", err)
	}
	checkMigrationVersions(t, f, []int{1, 2})
	if err := migrate(f.ctx, d, files); err != nil {
		t.Fatalf("cancelled contender leaked migration lock: %v", err)
	}
}

func TestGooseMigrationStatementLockTimeout(t *testing.T) {
	f := newJournalFixture(t)
	d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
	d.TimeoutSeconds, d.LockTimeoutSeconds = 5, 1
	files := migrationFiles(t, additiveMigration("ALTER TABLE heos.operations ADD COLUMN blocked_note text;"))
	tx, err := f.owner.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(f.ctx, "LOCK TABLE heos.operations IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := migrate(f.ctx, d, files); err == nil {
		t.Fatal("blocked migration ignored PostgreSQL lock timeout")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("migration did not preserve the configured PostgreSQL lock timeout")
	}
	if err := tx.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	checkMigrationVersions(t, f, []int{1})
	if err := f.store.Ready(f.ctx); err != nil {
		t.Fatalf("failed transactional migration broke previous runtime: %v", err)
	}
	if err := migrate(f.ctx, d, files); err != nil {
		t.Fatalf("retry after lock timeout: %v", err)
	}
}

func TestGooseMigrationCancelledBeforeConnect(t *testing.T) {
	f := newJournalFixture(t)
	d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
	before := migrationLedger(t, f)
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	files := migrationFiles(t, additiveMigration("SELECT 1;"))
	if err := migrate(ctx, d, files); err == nil {
		t.Fatal("migration succeeded with an already cancelled context")
	}
	if !reflect.DeepEqual(migrationLedger(t, f), before) {
		t.Fatal("cancelled migration changed the applied ledger")
	}
}

func TestGooseMigrationRejectsIncompleteMetadataPair(t *testing.T) {
	for _, table := range []string{"goose_db_version", "schema_migrations"} {
		t.Run(table, func(t *testing.T) {
			f := newJournalFixture(t)
			d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
			// Both names are fixed test constants. A surviving business schema
			// must not be adopted as a fresh database after metadata was lost.
			f.exec("DROP TABLE heos." + table)
			if err := Migrate(f.ctx, d); err == nil {
				t.Fatal("migration silently repaired incomplete metadata")
			}
			var stillMissing bool
			if err := f.owner.QueryRow(f.ctx, "SELECT to_regclass($1) IS NULL", "heos."+table).Scan(&stillMissing); err != nil || !stillMissing {
				t.Fatalf("migrator recreated missing metadata instead of rejecting corruption: missing=%v, %v", stillMissing, err)
			}
			if err := f.store.Ready(f.ctx); (err != nil) != (table == "schema_migrations") {
				t.Fatalf("runtime readiness must depend on application compatibility evidence, missing %s: %v", table, err)
			}
		})
	}
}

func TestGooseMigrationChecksumRemainsSourceBased(t *testing.T) {
	f := newJournalFixture(t)
	d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
	files := migrationFiles(t)
	for name, file := range files {
		if strings.HasPrefix(name, "00001_") {
			file.Data = append(file.Data, []byte("\n-- applied files are immutable\n")...)
		}
	}
	if err := migrate(f.ctx, d, files); err == nil {
		t.Fatal("migration ignored a comment-only change to applied source")
	}
}

// lostCommitFrontend drops an actual PostgreSQL COMMIT acknowledgement after
// the probe's SQL has been sent. BuildFrontend receives the verified connection's
// decrypted stream, so this fault preserves normal TLS and authentication.
type lostCommitFrontend struct {
	reader  io.Reader
	writer  io.Writer
	frame   []byte
	armed   atomic.Bool
	dropped bool
	fired   *atomic.Bool
}

func (f *lostCommitFrontend) Write(p []byte) (int, error) {
	n, err := f.writer.Write(p)
	if bytes.Contains(p[:n], []byte("heos.commit_ack_probe")) {
		f.armed.Store(true)
	}
	return n, err
}

func (f *lostCommitFrontend) Read(p []byte) (int, error) {
	if f.dropped {
		return 0, io.ErrUnexpectedEOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if len(f.frame) == 0 {
		var header [5]byte
		if _, err := io.ReadFull(f.reader, header[:]); err != nil {
			return 0, err
		}
		length := binary.BigEndian.Uint32(header[1:])
		if length < 4 || length > 1<<20 {
			return 0, fmt.Errorf("unexpected test protocol frame length: %d", length)
		}
		f.frame = make([]byte, int(length)+1)
		copy(f.frame, header[:])
		if _, err := io.ReadFull(f.reader, f.frame[5:]); err != nil {
			return 0, err
		}
		if header[0] == 'C' && bytes.Equal(f.frame[5:], []byte("COMMIT\x00")) && f.armed.Load() && f.fired.CompareAndSwap(false, true) {
			f.dropped = true
			return 0, io.ErrUnexpectedEOF
		}
	}
	n := copy(p, f.frame)
	f.frame = f.frame[n:]
	return n, nil
}

func TestGooseMigrationLostCommitAcknowledgement(t *testing.T) {
	f := newJournalFixture(t)
	d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
	files := migrationFiles(t, additiveMigration("CREATE TABLE heos.commit_ack_probe(id bigint PRIMARY KEY); INSERT INTO heos.commit_ack_probe VALUES (1);"))
	catalog, err := loadMigrations(files)
	if err != nil {
		t.Fatal(err)
	}
	pc, err := poolConfig(d)
	if err != nil {
		t.Fatal(err)
	}
	var fired atomic.Bool
	pc.ConnConfig.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
		fault := &lostCommitFrontend{reader: r, writer: w, fired: &fired}
		return pgproto3.NewFrontend(fault, fault)
	}
	db := stdlib.OpenDB(*pc.ConnConfig)
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()
	if err := runMigrations(f.ctx, db, catalog); err == nil {
		t.Fatal("migration did not report its lost commit acknowledgement")
	}
	if !fired.Load() {
		t.Fatal("test failed before intercepting the real COMMIT response")
	}
	// A separate, unaffected connection observes the committed application DDL
	// and both ledgers. No simulated successful return stands in for the commit.
	checkMigrationVersions(t, f, []int{1, 2})
	committed := migrationLedger(t, f)
	var count int
	if err := f.owner.QueryRow(f.ctx, "SELECT count(*) FROM heos.commit_ack_probe WHERE id = 1").Scan(&count); err != nil || count != 1 {
		t.Fatalf("commit was not durable after acknowledgement loss: count=%d, %v", count, err)
	}
	if err := migrate(f.ctx, d, files); err != nil {
		t.Fatalf("retry after lost acknowledgement replayed committed DDL: %v", err)
	}
	if !reflect.DeepEqual(migrationLedger(t, f), committed) {
		t.Fatal("retry after lost acknowledgement rewrote committed migration metadata")
	}
	checkPreviousRuntimeWrites(t, f, "after-lost-ack")
}

func TestGooseMigrationMetadataFailureRollsBackDDL(t *testing.T) {
	f := newJournalFixture(t)
	d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
	before := migrationLedger(t, f)
	files := migrationFiles(t, additiveMigration(`
CREATE TABLE heos.metadata_atomic_probe(id bigint PRIMARY KEY);
ALTER TABLE heos.schema_migrations ADD CONSTRAINT reject_probe_version CHECK (version <> 2);`))
	if err := migrate(f.ctx, d, files); err == nil {
		t.Fatal("migration succeeded despite rejecting its application metadata row")
	}
	checkMigrationVersions(t, f, []int{1})
	if !reflect.DeepEqual(migrationLedger(t, f), before) {
		t.Fatal("failed metadata insert changed committed migration history")
	}
	var tableMissing, constraintMissing bool
	err := f.owner.QueryRow(f.ctx, `SELECT
to_regclass('heos.metadata_atomic_probe') IS NULL,
NOT EXISTS (SELECT FROM pg_constraint WHERE conrelid = 'heos.schema_migrations'::regclass AND conname = 'reject_probe_version')`).Scan(&tableMissing, &constraintMissing)
	if err != nil || !tableMissing || !constraintMissing {
		t.Fatalf("metadata failure did not roll back the entire SQL file: tableMissing=%v, constraintMissing=%v, %v", tableMissing, constraintMissing, err)
	}
	checkPreviousRuntimeWrites(t, f, "after-metadata-failure")
}

func TestGooseMigrationRejectsLegacyMetadataWithoutAdoption(t *testing.T) {
	f := newJournalFixture(t)
	d := loadIntegrationConfig(t, "HEOS_TEST_OWNER_CONFIG").Database
	// The former executor had only a checksum ledger. Preserve the application
	// tables, but reproduce that legacy metadata shape without a goose table.
	f.exec("DROP TABLE heos.goose_db_version; ALTER TABLE heos.schema_migrations DROP COLUMN min_runtime")
	if err := Migrate(f.ctx, d); err == nil {
		t.Fatal("migration silently adopted legacy metadata instead of requiring reset")
	}
	var gooseMissing, floorMissing bool
	err := f.owner.QueryRow(f.ctx, `SELECT
to_regclass('heos.goose_db_version') IS NULL,
NOT EXISTS (SELECT FROM pg_attribute WHERE attrelid = 'heos.schema_migrations'::regclass AND attname = 'min_runtime' AND NOT attisdropped)`).Scan(&gooseMissing, &floorMissing)
	if err != nil || !gooseMissing || !floorMissing {
		t.Fatalf("legacy rejection altered migration metadata: gooseMissing=%v, floorMissing=%v, %v", gooseMissing, floorMissing, err)
	}
	if err := f.store.Ready(f.ctx); err == nil {
		t.Fatal("runtime accepted the unsupported legacy schema")
	}
}
