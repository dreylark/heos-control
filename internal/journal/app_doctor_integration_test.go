//go:build integration

package journal_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/app"
	"github.com/dreylark/heos-control/internal/journal"
)

// Keep the complete diagnostic path in the existing exclusive journal test
// binary, alongside the lower-level privilege and nonmutation regressions.
func TestApplicationDoctorChecksRuntimeWithoutRecoveringIt(t *testing.T) {
	cfg := lifecycleConfig(t, "HEOS_TEST_RUNTIME_CONFIG")
	owner := lifecycleConfig(t, "HEOS_TEST_OWNER_CONFIG")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	body, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "runtime.json")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	report := app.Doctor(ctx, path)
	if !report.OK || report.Command != "doctor" || len(report.Checks) != 8 {
		t.Fatalf("full runtime diagnostic failed: %+v", report)
	}
	for i, field := range []string{"database.connection", "database.schema", "database.permissions"} {
		if check := report.Checks[5+i]; check.Field != field || !check.OK {
			t.Fatal("missing database stage", check)
		}
	}
	if err := store.Ready(ctx); err != nil {
		t.Fatal("doctor initialized another controller epoch", err)
	}
}
