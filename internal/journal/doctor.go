package journal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/journal/dbgen"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DiagnosticCheck describes one bounded database check without connection details
// or raw driver errors. Code is stable; Message is safe for operator reports.
type DiagnosticCheck struct {
	Name, Code, Message string
	OK                  bool
}

// Diagnose checks connectivity, schema compatibility and the effective runtime
// privileges. It never initializes the journal, acquires its mutation lock or
// runs migrations. Every connection starts in a server-enforced read-only mode.
func Diagnose(ctx context.Context, d config.Database) []DiagnosticCheck {
	checks := []DiagnosticCheck{
		{Name: "connection", Code: "not_checked", Message: "Database connection was not checked."},
		{Name: "schema", Code: "not_checked", Message: "Database schema requires a working connection."},
		{Name: "permissions", Code: "not_checked", Message: "Database permissions require a working connection."},
	}
	c, err := diagnosticPoolConfig(d)
	if err != nil {
		checks[0] = DiagnosticCheck{Name: "connection", Code: "database_configuration_invalid", Message: "Database connection files or settings are invalid."}
		return checks
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(d.TimeoutSeconds)*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		checks[0] = diagnosticConnectionFailure(err)
		return checks
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		checks[0] = diagnosticConnectionFailure(err)
		return checks
	}
	checks[0] = DiagnosticCheck{Name: "connection", Code: "ok", Message: "Database connection and verified TLS succeeded.", OK: true}
	q := dbgen.New(pool)
	privileges, err := q.InspectRuntimePrivileges(ctx)
	if err != nil {
		checks[1] = DiagnosticCheck{Name: "schema", Code: "schema_check_failed", Message: "Database schema objects could not be inspected."}
		checks[2] = DiagnosticCheck{Name: "permissions", Code: "permissions_check_failed", Message: "Database runtime privileges could not be inspected."}
		return checks
	}
	checks[2] = DiagnosticCheck{Name: "permissions", Code: "runtime_permissions_missing", Message: "Database runtime role lacks required schema or table privileges."}
	if privileges.Permissions {
		checks[2] = DiagnosticCheck{Name: "permissions", Code: "ok", Message: "Database runtime role has all required schema and table privileges.", OK: true}
	}
	if !privileges.SchemaPresent || !privileges.TablesPresent {
		checks[1] = DiagnosticCheck{Name: "schema", Code: "schema_missing", Message: "Required database schema or tables are missing; apply migrations separately."}
		return checks
	}
	rows, err := q.ListMigrations(ctx)
	if err != nil {
		checks[1] = DiagnosticCheck{Name: "schema", Code: "schema_check_failed", Message: "Database migration metadata could not be read."}
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) {
			switch pgerr.Code {
			case "42501":
				checks[1] = DiagnosticCheck{Name: "schema", Code: "schema_permission_denied", Message: "Database runtime role cannot read migration metadata."}
			case "42P01", "3F000":
				checks[1] = DiagnosticCheck{Name: "schema", Code: "schema_missing", Message: "Required database schema or tables are missing; apply migrations separately."}
			}
		}
		return checks
	}
	// Share the exact startup compatibility/checksum policy without Ready or
	// Recover: neither controller ownership nor recovery belongs to diagnostics.
	catalog, err := serviceMigrations()
	if err != nil || catalog.validate(rows, true, true) != nil {
		checks[1] = DiagnosticCheck{Name: "schema", Code: "schema_incompatible", Message: "Database migration versions, checksums or compatibility floor do not match this application."}
		return checks
	}
	checks[1] = DiagnosticCheck{Name: "schema", Code: "ok", Message: "Database schema and migration checksums are compatible.", OK: true}
	return checks
}

func diagnosticPoolConfig(d config.Database) (*pgxpool.Config, error) {
	if d.TimeoutSeconds < 1 || d.TimeoutSeconds > 30 {
		return nil, errors.New("invalid diagnostic timeout")
	}
	c, err := poolConfig(d)
	if err != nil {
		return nil, err
	}
	c.MaxConns = 1
	c.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	return c, nil
}

func diagnosticConnectionFailure(err error) DiagnosticCheck {
	check := DiagnosticCheck{Name: "connection", Code: "database_connection_failed", Message: "Database connection failed or its deadline expired."}
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && (pgerr.Code == "28P01" || pgerr.Code == "28000") {
		check.Code, check.Message = "database_authentication_failed", "Database authentication or connection authorization failed."
		return check
	}
	var hostname x509.HostnameError
	var authority x509.UnknownAuthorityError
	var certificate x509.CertificateInvalidError
	var verification *tls.CertificateVerificationError
	if errors.As(err, &hostname) || errors.As(err, &authority) || errors.As(err, &certificate) || errors.As(err, &verification) {
		check.Code, check.Message = "database_tls_failed", "Database TLS certificate verification failed."
	}
	return check
}
