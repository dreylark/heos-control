package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRejectsMalformedAndUnsafeConfiguration(t *testing.T) {
	base := `{"cert_file":"cert","key_file":"key","database":{"host":"localhost","name":"heos_test","user":"runtime","password_file":"password","ca_file":"ca"}}`
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{
		{"defaults", base, true},
		{"removed presets", strings.Replace(base, `"cert_file":`, `"preset_files":[],"cert_file":`, 1), false},
		{"unknown", strings.Replace(base, `"cert_file"`, `"typo"`, 1), false},
		{"trailing", base + ` {}`, false},
		{"unbounded pool", strings.Replace(base, `"host":`, `"max_connections":1000,"host":`, 1), false},
		{"no TLS", strings.Replace(base, `"ca_file":"ca"`, `"ca_file":""`, 1), false},
		{"privileged port", strings.Replace(base, `"cert_file":`, `"listen":":443","cert_file":`, 1), false},
		{"long shutdown", strings.Replace(base, `"cert_file":`, `"shutdown_seconds":31,"cert_file":`, 1), false},
		{"documentation string", strings.Replace(base, `"cert_file":`, `"docs_enabled":"true","cert_file":`, 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(tc.value), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if (err == nil) != tc.valid {
				t.Fatalf("unexpected validation: %v", err)
			}
			if tc.valid && (c.Database.MaxConnections != 4 || c.Listen != ":8443" || c.DocsEnabled) {
				t.Fatal("defaults missing")
			}
		})
	}
}

func TestLoadDocumentationOptIn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	err := os.WriteFile(path, []byte(`{"docs_enabled":true,"cert_file":"cert","key_file":"key","database":{"host":"localhost","name":"heos_test","user":"runtime","password_file":"password","ca_file":"ca"}}`), 0600)
	if err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil || !c.DocsEnabled {
		t.Fatalf("documentation opt-in was not loaded: %v", err)
	}
}

func TestLogLevelConfiguration(t *testing.T) {
	for _, level := range []string{"", "debug", "info", "warn", "error", "trace", "DEBUG", "debug+1"} {
		t.Run("level_"+level, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			prefix := ""
			if level != "" {
				prefix = `"log_level":"` + level + `",`
			}
			body := `{` + prefix + `"cert_file":"cert","key_file":"key","database":{"host":"localhost","name":"heos_test","user":"runtime","password_file":"password","ca_file":"ca"}}`
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			valid := level == "" || level == "debug" || level == "info" || level == "warn" || level == "error"
			if (err == nil) != valid {
				t.Fatal(level, err)
			}
			if level == "" && cfg.LogLevel != "info" {
				t.Fatal("INFO must remain the default")
			}
			if valid && level != "" && cfg.LogLevel != level {
				t.Fatal(cfg.LogLevel)
			}
		})
	}
}
