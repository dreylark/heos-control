//go:build integration

package journal

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func doctorCheck(t *testing.T, checks []DiagnosticCheck, name, code string, ok bool) {
	t.Helper()
	if len(checks) != 3 {
		t.Fatalf("expected all three diagnostic stages: %+v", checks)
	}
	for _, check := range checks {
		if check.Name == name {
			if check.Code != code || check.OK != ok || check.Message == "" {
				t.Fatalf("%s: got %+v; want code=%s ok=%v", name, check, code, ok)
			}
			return
		}
	}
	t.Fatalf("missing check %s", name)
}

func TestDoctorPreservesJournalWithoutRecovery(t *testing.T) {
	f := newJournalFixture(t)
	f.admit("doctor-keeps-operation", "physical-device")
	const snapshot = `SELECT jsonb_build_object(
		'control', (SELECT jsonb_agg(to_jsonb(c)) FROM heos.journal_control c),
		'operations', (SELECT jsonb_agg(to_jsonb(o) ORDER BY id) FROM heos.operations o),
		'idempotency', (SELECT jsonb_agg(to_jsonb(i) ORDER BY operation_id) FROM heos.idempotency_records i),
		'reservations', (SELECT jsonb_agg(to_jsonb(r) ORDER BY device_key) FROM heos.device_reservations r),
		'migrations', (SELECT jsonb_agg(to_jsonb(m) ORDER BY version) FROM heos.schema_migrations m),
		'goose', (SELECT jsonb_agg(to_jsonb(g) ORDER BY id) FROM heos.goose_db_version g))::text`
	var before, after string
	if err := f.owner.QueryRow(f.ctx, snapshot).Scan(&before); err != nil {
		t.Fatal(err)
	}
	// A live controller may hold the mutation lock. Diagnostics must not acquire it.
	tx, err := f.owner.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(f.ctx, `SELECT singleton FROM heos.journal_control FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	checks := Diagnose(ctx, f.runtime)
	for _, name := range []string{"connection", "schema", "permissions"} {
		doctorCheck(t, checks, name, "ok", true)
	}
	if err := tx.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.QueryRow(f.ctx, snapshot).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("diagnostics changed durable journal or migration data")
	}
	if err := f.store.Ready(f.ctx); err != nil {
		t.Fatalf("diagnostics fenced the live controller: %v", err)
	}
}

func TestDoctorReadOnlySession(t *testing.T) {
	f := newJournalFixture(t)
	c, err := diagnosticPoolConfig(f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	if c.ConnConfig.TLSConfig == nil || c.ConnConfig.TLSConfig.InsecureSkipVerify || c.MaxConns != 1 {
		t.Fatal("diagnostics lost verified TLS or its connection bound")
	}
	conn, err := pgx.ConnectConfig(f.ctx, c.ConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var readOnly string
	if err := conn.QueryRow(f.ctx, `SHOW transaction_read_only`).Scan(&readOnly); err != nil || readOnly != "on" {
		t.Fatalf("session is not read-only: %s %v", readOnly, err)
	}
	_, err = conn.Exec(f.ctx, `UPDATE heos.journal_control SET epoch='doctor-must-not-write'`)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "25006" {
		t.Fatalf("server did not reject a write in diagnostic session: %v", err)
	}
}

func TestDoctorRequiredPermissions(t *testing.T) {
	for _, test := range []struct{ table, privileges string }{
		{"operations", "SELECT"}, {"operations", "INSERT"}, {"operations", "UPDATE"}, {"operations", "DELETE"},
		{"idempotency_records", "SELECT"}, {"idempotency_records", "INSERT"}, {"idempotency_records", "UPDATE"}, {"idempotency_records", "DELETE"},
		{"device_reservations", "SELECT"}, {"device_reservations", "INSERT"}, {"device_reservations", "UPDATE"}, {"device_reservations", "DELETE"},
		{"journal_control", "SELECT"}, {"journal_control", "UPDATE"}, {"schema_migrations", "SELECT"},
	} {
		t.Run(test.table+"/"+test.privileges, func(t *testing.T) {
			f := newJournalFixture(t)
			// These identifiers are test constants, never external input.
			f.exec("REVOKE " + test.privileges + " ON heos." + test.table + " FROM heos_runtime")
			checks := Diagnose(f.ctx, f.runtime)
			doctorCheck(t, checks, "connection", "ok", true)
			doctorCheck(t, checks, "permissions", "runtime_permissions_missing", false)
			if test.table == "schema_migrations" {
				doctorCheck(t, checks, "schema", "schema_permission_denied", false)
			} else {
				doctorCheck(t, checks, "schema", "ok", true)
			}
		})
	}
	t.Run("schema usage", func(t *testing.T) {
		f := newJournalFixture(t)
		f.exec(`REVOKE USAGE ON SCHEMA heos FROM heos_runtime`)
		checks := Diagnose(f.ctx, f.runtime)
		doctorCheck(t, checks, "permissions", "runtime_permissions_missing", false)
		doctorCheck(t, checks, "schema", "schema_permission_denied", false)
	})
}

func TestDoctorSchemaFailures(t *testing.T) {
	for _, test := range []struct{ name, sql, code string }{
		{"uninitialized", `DROP SCHEMA heos CASCADE`, "schema_missing"},
		{"missing table", `DROP TABLE heos.device_reservations`, "schema_missing"},
		{"missing metadata column", `ALTER TABLE heos.schema_migrations RENAME COLUMN min_runtime TO missing_runtime`, "schema_check_failed"},
		{"checksum", `UPDATE heos.schema_migrations SET checksum=repeat('0',64)`, "schema_incompatible"},
		{"missing migration", `DELETE FROM heos.schema_migrations`, "schema_incompatible"},
		{"future incompatible", `INSERT INTO heos.schema_migrations(version,checksum,min_runtime) VALUES(2,repeat('0',64),2)`, "schema_incompatible"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newJournalFixture(t)
			f.exec(test.sql)
			checks := Diagnose(f.ctx, f.runtime)
			doctorCheck(t, checks, "connection", "ok", true)
			doctorCheck(t, checks, "schema", test.code, false)
		})
	}
	t.Run("compatible future migration", func(t *testing.T) {
		f := newJournalFixture(t)
		f.exec(`INSERT INTO heos.schema_migrations(version,checksum,min_runtime) VALUES(2,repeat('0',64),1)`)
		for _, check := range Diagnose(f.ctx, f.runtime) {
			if !check.OK {
				t.Fatalf("runtime-compatible schema rejected: %+v", check)
			}
		}
	})
	t.Run("controller has not started", func(t *testing.T) {
		f := newJournalFixture(t)
		f.exec(`UPDATE heos.journal_control SET ready=false,epoch=''`)
		for _, check := range Diagnose(f.ctx, f.runtime) {
			if !check.OK {
				t.Fatalf("diagnostics required controller recovery: %+v", check)
			}
		}
		var epoch string
		var ready bool
		if err := f.owner.QueryRow(f.ctx, `SELECT epoch,ready FROM heos.journal_control`).Scan(&epoch, &ready); err != nil || epoch != "" || ready {
			t.Fatalf("diagnostics initialized controller ownership: %q %v %v", epoch, ready, err)
		}
	})
}

func TestDoctorMetadataReadDeadline(t *testing.T) {
	f := newJournalFixture(t)
	tx, err := f.owner.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(f.ctx, `LOCK TABLE heos.schema_migrations IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	d := f.runtime
	d.TimeoutSeconds = 1
	start := time.Now()
	checks := Diagnose(f.ctx, d)
	doctorCheck(t, checks, "connection", "ok", true)
	doctorCheck(t, checks, "permissions", "ok", true)
	doctorCheck(t, checks, "schema", "schema_check_failed", false)
	if time.Since(start) > 2*time.Second {
		t.Fatal("schema diagnostics ignored the database deadline")
	}
}

func TestDoctorConnectionFailures(t *testing.T) {
	f := newJournalFixture(t)
	t.Run("untrusted TLS certificate", func(t *testing.T) {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
			IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
		der, err := x509.CreateCertificate(rand.Reader, ca, ca, public, private)
		if err != nil {
			t.Fatal(err)
		}
		d := f.runtime
		d.CAFile = filepath.Join(t.TempDir(), "unrelated-ca.crt")
		if err := os.WriteFile(d.CAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		checks := Diagnose(f.ctx, d)
		doctorCheck(t, checks, "connection", "database_tls_failed", false)
		doctorCheck(t, checks, "schema", "not_checked", false)
		doctorCheck(t, checks, "permissions", "not_checked", false)
	})
	t.Run("authentication", func(t *testing.T) {
		d := f.runtime
		d.PasswordFile = filepath.Join(t.TempDir(), "private-password")
		if err := os.WriteFile(d.PasswordFile, []byte("private-wrong-password"), 0o600); err != nil {
			t.Fatal(err)
		}
		checks := Diagnose(f.ctx, d)
		doctorCheck(t, checks, "connection", "database_authentication_failed", false)
		for _, check := range checks {
			if strings.Contains(check.Message, "private-") || strings.Contains(check.Message, d.User) {
				t.Fatal("connection failure exposed credentials")
			}
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(f.ctx)
		cancel()
		start := time.Now()
		checks := Diagnose(ctx, f.runtime)
		doctorCheck(t, checks, "connection", "database_connection_failed", false)
		doctorCheck(t, checks, "schema", "not_checked", false)
		doctorCheck(t, checks, "permissions", "not_checked", false)
		if time.Since(start) > time.Second {
			t.Fatal("doctor ignored caller cancellation")
		}
	})
}
