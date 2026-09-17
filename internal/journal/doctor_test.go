package journal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestDoctorConnectionFailureCategories(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code string
	}{
		{"password", &pgconn.PgError{Code: "28P01", Message: "secret-password"}, "database_authentication_failed"},
		{"authorization", &pgconn.PgError{Code: "28000", Message: "private-role"}, "database_authentication_failed"},
		{"database", &pgconn.PgError{Code: "3D000", Message: "private-database"}, "database_connection_failed"},
		{"hostname", x509.HostnameError{Host: "private-host"}, "database_tls_failed"},
		{"authority", x509.UnknownAuthorityError{}, "database_tls_failed"},
		{"certificate", &tls.CertificateVerificationError{Err: x509.CertificateInvalidError{}}, "database_tls_failed"},
		{"deadline", context.DeadlineExceeded, "database_connection_failed"},
		{"unknown", errors.New("postgres://private-role:secret-password@private-host/private-database"), "database_connection_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := diagnosticConnectionFailure(fmt.Errorf("private-host: %w", test.err))
			if got.Name != "connection" || got.OK || got.Code != test.code || got.Message == "" {
				t.Fatalf("unexpected failure: %+v", got)
			}
			for _, private := range []string{"private-host", "private-role", "private-database", "secret-password", "postgres://"} {
				if strings.Contains(got.Message, private) {
					t.Fatalf("diagnostics leaked %q", private)
				}
			}
		})
	}
}

func TestDoctorInvalidConfigurationDoesNotExposePaths(t *testing.T) {
	d := config.Database{Host: "private-host", User: "private-role", Name: "private-database", PasswordFile: "/private-path/password", TimeoutSeconds: 1}
	checks := Diagnose(context.Background(), d)
	if len(checks) != 3 || checks[0].Code != "database_configuration_invalid" {
		t.Fatalf("unexpected checks: %+v", checks)
	}
	for i, name := range []string{"connection", "schema", "permissions"} {
		if checks[i].Name != name || checks[i].OK || strings.Contains(checks[i].Message, "private-") {
			t.Fatalf("unsafe check: %+v", checks[i])
		}
		if i > 0 && checks[i].Code != "not_checked" {
			t.Fatalf("dependent check ran: %+v", checks[i])
		}
	}
}
