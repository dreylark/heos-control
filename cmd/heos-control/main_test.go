package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIRejectsInvalidInputBeforeStartingService(t *testing.T) {
	before := os.Args
	t.Cleanup(func() { os.Args = before })
	missing := filepath.Join(t.TempDir(), "absent.json")
	for _, tc := range []struct {
		args []string
		want int
	}{
		{[]string{"serve", "-unknown"}, 2},
		{[]string{"serve", "-config", missing, "unexpected"}, 2},
		{[]string{"serve", "-config", missing}, 1},
		{[]string{"migrate", "-config", missing}, 1},
	} {
		os.Args = append([]string{"heos-control"}, tc.args...)
		if got := run(); got != tc.want {
			t.Fatalf("%v: exit %d, want %d", tc.args, got, tc.want)
		}
	}
}

func TestVersionNeedsNoConfigAndReportsBuildIdentity(t *testing.T) {
	args, stdout := os.Args, os.Stdout
	t.Cleanup(func() { os.Args, os.Stdout = args, stdout })
	f, err := os.CreateTemp(t.TempDir(), "version")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	os.Args, os.Stdout = []string{"heos-control", "version"}, f
	if got := run(); got != 0 {
		t.Fatal(got)
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	var result map[string]string
	if err := json.NewDecoder(f).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result["version"] != version || result["commit"] != commit || len(result) != 2 {
		t.Fatal(result)
	}
}

func TestConfiguredLogLevelAppliesBeforeServiceStartup(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			args, stdout := os.Args, os.Stdout
			t.Cleanup(func() { os.Args, os.Stdout = args, stdout })
			dir := t.TempDir()
			path := filepath.Join(dir, "config.json")
			body, err := json.Marshal(map[string]any{
				"log_level": level, "cert_file": "unused-cert", "key_file": "unused-key",
				"database": map[string]any{"host": "127.0.0.1", "name": "heos_test", "user": "runtime",
					"password_file": filepath.Join(dir, "missing-password"), "ca_file": "unused-ca"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			output, err := os.Create(filepath.Join(dir, "stdout.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = output.Close() }()
			os.Args, os.Stdout = []string{"heos-control", "serve", "-config", path}, output
			// Fail on the absent password file, before any database/device I/O.
			if code := run(); code != 1 {
				t.Fatal(code)
			}
			logs, err := os.ReadFile(output.Name())
			if err != nil {
				t.Fatal(err)
			}
			wantInfo := level == "debug" || level == "info"
			if strings.Contains(string(logs), `"msg":"starting"`) != wantInfo || !strings.Contains(string(logs), `"msg":"command failed"`) {
				t.Fatal("configured severity not applied", string(logs))
			}
		})
	}
}
