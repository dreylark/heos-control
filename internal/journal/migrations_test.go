package journal

import (
	"crypto/sha256"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/dreylark/heos-control/internal/journal/dbgen"
)

func TestMigrationPolicy(t *testing.T) {
	up := "-- +goose Up\n-- heos:min-runtime=1\nSELECT 1;\n"
	for _, tc := range []struct {
		name  string
		files fstest.MapFS
		valid bool
	}{
		{"transactional forward migration", fstest.MapFS{"00001_initial.sql": {Data: []byte(up)}}, true},
		{"empty directory", fstest.MapFS{}, false},
		{"missing compatibility declaration", fstest.MapFS{"00001_initial.sql": {Data: []byte("-- +goose Up\nSELECT 1;")}}, false},
		{"missing goose Up", fstest.MapFS{"00001_initial.sql": {Data: []byte("-- heos:min-runtime=1\nSELECT 1;")}}, false},
		{"version gap", fstest.MapFS{"00002_initial.sql": {Data: []byte(up)}}, false},
		{"duplicate version", fstest.MapFS{"00001_a.sql": {Data: []byte(up)}, "00001_b.sql": {Data: []byte(up)}}, false},
		{"unversioned SQL", fstest.MapFS{"schema.sql": {Data: []byte(up)}}, false},
		{"down migration", fstest.MapFS{"00001_initial.sql": {Data: []byte(up + "-- +goose Down\nDROP TABLE heos.operations;\n")}}, false},
		{"nontransactional SQL", fstest.MapFS{"00001_initial.sql": {Data: []byte("-- +goose NO TRANSACTION\n" + up)}}, false},
		{"nontransactional SQL spaced annotation", fstest.MapFS{"00001_initial.sql": {Data: []byte("--  +goose NO TRANSACTION\n" + up)}}, false},
		{"down migration tab annotation", fstest.MapFS{"00001_initial.sql": {Data: []byte(up + "--\t+goose Down\nSELECT 2;\n")}}, false},
		{"environment interpolation", fstest.MapFS{"00001_initial.sql": {Data: []byte("-- +goose ENVSUB ON\n" + up)}}, false},
		{"duplicate declaration", fstest.MapFS{"00001_initial.sql": {Data: []byte(up + "-- heos:min-runtime=1\n")}}, false},
		{"unsupported minimum", fstest.MapFS{"00001_initial.sql": {Data: []byte(strings.ReplaceAll(up, "runtime=1", "runtime=2"))}}, false},
		{"decreasing minimum", fstest.MapFS{
			"00001_initial.sql": {Data: []byte(up)},
			"00002_next.sql":    {Data: []byte(strings.ReplaceAll(up, "runtime=1", "runtime=2"))},
			"00003_next.sql":    {Data: []byte(up)},
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadMigrations(tc.files)
			if (err == nil) != tc.valid {
				t.Fatalf("loadMigrations error = %v, valid = %v", err, tc.valid)
			}
		})
	}
}

func TestMigrationCompatibility(t *testing.T) {
	up := []byte("-- +goose Up\n-- heos:min-runtime=1\nSELECT 1;\n")
	catalog, err := loadMigrations(fstest.MapFS{"00001_initial.sql": {Data: up}})
	if err != nil {
		t.Fatal(err)
	}
	baseline := dbgen.ListMigrationsRow{Version: 1, Checksum: fmt.Sprintf("%x", sha256.Sum256(up)), MinRuntime: 1}
	future := dbgen.ListMigrationsRow{Version: 2, Checksum: strings.Repeat("a", 64), MinRuntime: 1}
	for _, tc := range []struct {
		name                    string
		rows                    []dbgen.ListMigrationsRow
		complete, future, valid bool
	}{
		{"fresh migrator", nil, false, false, true},
		{"uninitialized runtime", nil, true, true, false},
		{"current runtime", []dbgen.ListMigrationsRow{baseline}, true, true, true},
		{"compatible future runtime", []dbgen.ListMigrationsRow{baseline, future}, true, true, true},
		{"old migrator cannot apply future database", []dbgen.ListMigrationsRow{baseline, future}, false, false, false},
		{"unknown gap", []dbgen.ListMigrationsRow{baseline, {Version: 3, Checksum: future.Checksum, MinRuntime: 1}}, true, true, false},
		{"unsupported runtime", []dbgen.ListMigrationsRow{baseline, {Version: 2, Checksum: future.Checksum, MinRuntime: 2}}, true, true, false},
		{"modified known SQL", []dbgen.ListMigrationsRow{{Version: 1, Checksum: future.Checksum, MinRuntime: 1}}, true, true, false},
		{"invalid unknown checksum", []dbgen.ListMigrationsRow{baseline, {Version: 2, Checksum: "invalid", MinRuntime: 1}}, true, true, false},
		{"invalid floor", []dbgen.ListMigrationsRow{{Version: 1, Checksum: baseline.Checksum, MinRuntime: 0}}, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := catalog.validate(tc.rows, tc.complete, tc.future)
			if (err == nil) != tc.valid {
				t.Fatalf("validate error = %v, valid = %v", err, tc.valid)
			}
		})
	}
}

func TestEmbeddedMigrationPolicy(t *testing.T) {
	files, err := fs.Sub(migrations, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadMigrations(files); err != nil {
		t.Fatal(err)
	}
}

func TestNewRuntimeRequiresCompleteMigrationTarget(t *testing.T) {
	up := []byte("-- +goose Up\n-- heos:min-runtime=1\nSELECT 1;\n")
	catalog, err := loadMigrations(fstest.MapFS{
		"00001_initial.sql": {Data: up},
		"00002_expand.sql":  {Data: up},
	})
	if err != nil {
		t.Fatal(err)
	}
	partial := []dbgen.ListMigrationsRow{{Version: 1, Checksum: catalog.steps[0].checksum, MinRuntime: 1}}
	if err := catalog.validate(partial, true, true); err == nil {
		t.Fatal("new runtime accepted its incomplete target despite an older-compatible floor")
	}
	if err := catalog.validate(partial, false, false); err != nil {
		t.Fatalf("migrator cannot continue an intact applied prefix: %v", err)
	}
	complete := append(partial, dbgen.ListMigrationsRow{Version: 2, Checksum: catalog.steps[1].checksum, MinRuntime: 1})
	if err := catalog.validate(complete, true, true); err != nil {
		t.Fatalf("new runtime rejected its complete target: %v", err)
	}
}
