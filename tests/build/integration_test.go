// Package build checks that Docker's journal binary includes all integration tests.
package build

import (
	"bytes"
	"encoding/json"
	"io"
	"os/exec"
	"reflect"
	"testing"
)

func TestIntegrationPackageScope(t *testing.T) {
	t.Chdir("../..")
	type metadata struct {
		ImportPath                string
		TestGoFiles, XTestGoFiles []string
	}
	packages := func(tags string) map[string]metadata {
		t.Helper()
		cmd := exec.Command("go", "list", "-json", "-tags="+tags, "./...")
		output, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		result := map[string]metadata{}
		decoder := json.NewDecoder(bytes.NewReader(output))
		for {
			var pkg metadata
			if err := decoder.Decode(&pkg); err == io.EOF {
				return result
			} else if err != nil {
				t.Fatal(err)
			}
			if pkg.ImportPath != "github.com/dreylark/heos-control/internal/journal" {
				result[pkg.ImportPath] = pkg
			}
		}
	}
	if !reflect.DeepEqual(packages(""), packages("integration")) {
		t.Fatal("integration-tagged tests outside internal/journal would be omitted from the Docker test binary; update packaging before adding them")
	}
}
