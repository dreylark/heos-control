//go:build ci

package ci

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"
)

// Execute the actual CI compilation steps with a fake Docker executable. This
// catches output accidentally moving into .local before devsetup creates it.
func TestCompilationThenDevelopmentSetup(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, ".github/workflows/build.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct{ ID, Run string }
		}
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	setup := filepath.Join(t.TempDir(), "devsetup")
	build := exec.Command("go", "build", "-o", setup, "./scripts/devsetup")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build devsetup: %v\n%s", err, out)
	}
	for _, job := range []string{"container-tests", "runtime"} {
		t.Run(job, func(t *testing.T) {
			directory := t.TempDir()
			commands := t.TempDir()
			// The fake produces a binary in the real command's output mount.
			fake := `#!/usr/bin/env bash
set -eu
output=""
binary=heos-control
for arg in "$@"; do
  case "$arg" in
    type=bind,src=*,dst=/out) output="${arg#type=bind,src=}"; output="${output%,dst=/out}" ;;
    /out/journal.test) binary=journal.test ;;
  esac
done
test -n "$output"
printf 'synthetic binary\n' > "$output/$binary"
`
			if err := os.WriteFile(filepath.Join(commands, "docker"), []byte(fake), 0755); err != nil {
				t.Fatal(err)
			}
			var script string
			for _, step := range workflow.Jobs[job].Steps {
				if step.ID == "compile" {
					script = step.Run
				}
			}
			if strings.TrimSpace(script) == "" {
				t.Fatal("missing compile step")
			}
			cmd := exec.Command("bash", "-euo", "pipefail", "-c", script)
			cmd.Dir = directory
			cmd.Env = append(os.Environ(), "PATH="+commands+":"+os.Getenv("PATH"), "GOCACHE="+filepath.Join(directory, "cache/build"), "GOMODCACHE="+filepath.Join(directory, "cache/mod"), "VERSION=dev", "COMMIT=test")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("compile: %v\n%s", err, out)
			}
			for range 2 {
				cmd := exec.Command(setup)
				cmd.Dir = directory
				before, _ := os.ReadFile(filepath.Join(directory, ".local/runtime-password"))
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("devsetup: %v\n%s", err, out)
				}
				after, err := os.ReadFile(filepath.Join(directory, ".local/runtime-password"))
				if err != nil || len(after) == 0 || (len(before) > 0 && !bytes.Equal(before, after)) {
					t.Fatal("missing or rotated local credentials")
				}
			}
			binary := "bin/heos-control"
			if job == "container-tests" {
				binary = "bin/integration/journal.test"
			}
			if _, err := os.Stat(filepath.Join(directory, binary)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
