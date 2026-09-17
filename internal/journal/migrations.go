package journal

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/dreylark/heos-control/internal/journal/dbgen"
)

//go:embed migrations/*.sql
var migrations embed.FS

type migrationStep struct {
	checksum   string
	minRuntime int32
}

type migrationCatalog struct {
	files fs.FS
	steps []migrationStep
}

var serviceMigrations = sync.OnceValues(func() (migrationCatalog, error) {
	files, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return migrationCatalog{}, err
	}
	return loadMigrations(files)
})

// Goose parses and executes SQL. This checks our smaller deployment contract:
// numbered, immutable, transactional Up files with an explicit compatibility floor.
func loadMigrations(files fs.FS) (migrationCatalog, error) {
	catalog := migrationCatalog{files: files}
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return catalog, err
	}
	if len(names) == 0 {
		return catalog, errors.New("no SQL migrations")
	}
	var previousFloor int32
	for i, name := range names {
		if !strings.HasPrefix(path.Base(name), fmt.Sprintf("%05d_", i+1)) {
			return catalog, fmt.Errorf("migration %q: expected consecutive five-digit version %d", name, i+1)
		}
		body, err := fs.ReadFile(files, name)
		if err != nil {
			return catalog, err
		}
		floor, err := migrationPolicy(string(body))
		if err != nil || floor < previousFloor || int64(floor) > int64(i+1) {
			return catalog, fmt.Errorf("migration %q: invalid forward-only compatibility policy", name)
		}
		catalog.steps = append(catalog.steps, migrationStep{checksum: fmt.Sprintf("%x", sha256.Sum256(body)), minRuntime: floor})
		previousFloor = floor
	}
	return catalog, nil
}

func migrationPolicy(body string) (int32, error) {
	var floor int64
	var declarations, up int
	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimSpace(line)
		if value, ok := strings.CutPrefix(line, "-- heos:min-runtime="); ok {
			declarations++
			var err error
			floor, err = strconv.ParseInt(value, 10, 32)
			if err != nil {
				return 0, err
			}
		}
		if strings.HasPrefix(line, "--") && strings.Contains(line, "+goose") {
			// Goose accepts whitespace between the comment marker and annotation.
			// Reject every other shape rather than let a transaction opt-out slip in.
			directive := strings.ToUpper(strings.Join(strings.Fields(line), " "))
			switch directive {
			case "-- +GOOSE UP":
				up++
			case "-- +GOOSE STATEMENTBEGIN", "-- +GOOSE STATEMENTEND":
				// Goose owns statement grouping, including PL/pgSQL blocks.
			default:
				return 0, errors.New("only transactional Up migrations are supported")
			}
		}
	}
	if floor < 1 || declarations != 1 || up != 1 {
		return 0, errors.New("exactly one Up and minimum-runtime declaration required")
	}
	return int32(floor), nil
}

// A runtime needs its complete known prefix, but accepts a contiguous newer
// tail explicitly compatible with that schema baseline. A migrator must know
// every applied file; an old image must never migrate a newer database.
func (c migrationCatalog) validate(rows []dbgen.ListMigrationsRow, complete, allowFuture bool) error {
	if (complete && len(rows) < len(c.steps)) || (!allowFuture && len(rows) > len(c.steps)) {
		return errors.New("incompatible schema version")
	}
	var previousFloor int32
	for i, row := range rows {
		digest, err := hex.DecodeString(row.Checksum)
		if int64(row.Version) != int64(i+1) || err != nil || len(digest) != sha256.Size ||
			row.MinRuntime < 1 || row.MinRuntime < previousFloor || row.MinRuntime > row.Version ||
			int64(row.MinRuntime) > int64(len(c.steps)) {
			return errors.New("incompatible schema metadata or runtime baseline")
		}
		if i < len(c.steps) && (row.Checksum != c.steps[i].checksum || row.MinRuntime != c.steps[i].minRuntime) {
			return errors.New("schema migration checksum or compatibility mismatch")
		}
		previousFloor = row.MinRuntime
	}
	return nil
}
