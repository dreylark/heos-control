package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNextReleaseVersion(t *testing.T) {
	for _, tc := range []struct{ current, mode, explicit, want string }{
		{"0.8.0", "patch", "", "0.8.1"}, {"0.8.9", "minor", "", "0.9.0"}, {"0.8.9", "major", "", "1.0.0"},
		{"1.2.3-rc.1", "patch", "", "1.2.4"}, {"0.8.0", "explicit", "2.0.0", "2.0.0"},
		{"1.0.0-rc.9", "explicit", "1.0.0-rc.10", "1.0.0-rc.10"}, {"1.0.0-rc.10", "explicit", "1.0.0", "1.0.0"},
		{"1.0.0-alpha", "explicit", "1.0.0-alpha.1", "1.0.0-alpha.1"},
		{"1.0.0-9", "explicit", "1.0.0-alpha", "1.0.0-alpha"},
		{"1.0.0-alpha", "explicit", "1.0.0-beta", "1.0.0-beta"},
		{"1.0.99999999999999999999", "patch", "", "1.0.100000000000000000000"},
	} {
		got, err := nextVersion(tc.current, tc.mode, tc.explicit)
		if err != nil || got != tc.want {
			t.Errorf("%+v: %s %v", tc, got, err)
		}
	}
	for _, tc := range []struct{ current, mode, explicit string }{
		{"0.8.0", "explicit", "0.8.0"}, {"0.8.0", "explicit", "0.7.99"}, {"1.0.0", "explicit", "1.0.0-rc.1"},
		{"1.0.0-rc.10", "explicit", "1.0.0-rc.9"}, {"1.0.0-alpha.1", "explicit", "1.0.0-alpha"},
		{"1.0.0-alpha", "explicit", "1.0.0-9"}, {"1.0.0-beta", "explicit", "1.0.0-alpha"},
		{"0.8.0", "patch", "1.0.0"}, {"0.8.0", "explicit", ""}, {"0.8.0", "auto", ""},
		{"0.8.0", "explicit", "v0.9.0"}, {"0.8.0", "explicit", "0.9.0-01"}, {"0.8.0", "explicit", "0.9.0+build"},
		{"0.8.0", "explicit", "0.9.0\ninjected"}, {"bad", "patch", ""},
	} {
		if got, err := nextVersion(tc.current, tc.mode, tc.explicit); err == nil {
			t.Errorf("accepted %+v: %s", tc, got)
		}
	}
}

func releaseFixture(t *testing.T) (string, map[string]string) {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"VERSION":                        "0.8.0\n",
		"charts/heos-control/Chart.yaml": "# chart\nname: heos-control\nversion: '0.8.0' # chart version\nappVersion: \"0.8.0\"\n",
		"api/openapi.yaml":               "openapi: 3.1.0\ninfo:\n  title: test\n  version: 0.8.0 # API version\npaths: {}\n",
	}
	for path, body := range files {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	return root, files
}

func TestPrepareReleaseChangesOnlyVersionScalars(t *testing.T) {
	root, files := releaseFixture(t)
	got, err := prepareRelease(root, "minor", "")
	if err != nil || got != "0.9.0" {
		t.Fatal(got, err)
	}
	for path, before := range files {
		after, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || string(after) != strings.ReplaceAll(before, "0.8.0", "0.9.0") {
			t.Fatalf("unexpected edit to %s: %s %v", path, after, err)
		}
	}
	if _, err := validateAt(root, "v0.9.0"); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareRejectsDriftAndInvalidVersionsBeforeEditing(t *testing.T) {
	for _, drift := range []bool{false, true} {
		root, files := releaseFixture(t)
		mode, version := "explicit", "0.7.0"
		if drift {
			mode, version = "patch", ""
			files["api/openapi.yaml"] = strings.ReplaceAll(files["api/openapi.yaml"], "0.8.0", "0.7.0")
			if err := os.WriteFile(filepath.Join(root, "api/openapi.yaml"), []byte(files["api/openapi.yaml"]), 0644); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := prepareRelease(root, mode, version); err == nil {
			t.Fatal("accepted invalid preparation")
		}
		for path, before := range files {
			after, err := os.ReadFile(filepath.Join(root, path))
			if err != nil || string(after) != before {
				t.Fatal("failed preparation edited", path, err)
			}
		}
	}
}
