package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mustProfile(t *testing.T, body string) profile {
	t.Helper()
	p, err := parse(strings.NewReader("mode: atomic\n" + body))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDuplicateBlocksCountStatementsOnce(t *testing.T) {
	p := mustProfile(t, "github.com/dreylark/heos-control/internal/api/a.go:1.1,2.2 3 0\ngithub.com/dreylark/heos-control/internal/api/a.go:1.1,2.2 3 7\ngithub.com/dreylark/heos-control/internal/api/a.go:3.1,4.2 2 0\n")
	s := summarize(p, nil)
	if s.Total.Statements != 5 || s.Total.Covered != 3 || s.Total.Percent != 60 {
		t.Fatal(s)
	}
}

func TestRejectMalformedProfiles(t *testing.T) {
	for _, body := range []string{"mode: set\n", "mode: atomic\n", "mode: atomic\nbad\n", "mode: atomic\na.go:1.1,2.2 3 -1\n", "mode: atomic\na.go:1.1,2.2 3 1\na.go:1.1,2.2 4 0\n"} {
		if _, err := parse(strings.NewReader(body)); err == nil {
			t.Fatal("accepted malformed profile", body)
		}
	}
}

func TestGeneratedExclusionsAreExplicitAndNewPackagesFailClosed(t *testing.T) {
	p := mustProfile(t, "github.com/dreylark/heos-control/internal/api/a.go:1.1,2.2 3 1\ngithub.com/dreylark/heos-control/internal/api/api.gen.go:1.1,2.2 100 0\ngithub.com/dreylark/heos-control/internal/new/a.go:1.1,2.2 1 0\n")
	rules := policy{ExcludedFiles: []string{"github.com/dreylark/heos-control/internal/api/api.gen.go"}, Total: 75, Packages: map[string]float64{"github.com/dreylark/heos-control/internal/api": 100}}
	s := summarize(p, rules.ExcludedFiles)
	if s.Total.Statements != 4 || s.Total.Percent != 75 || len(p) != 3 {
		t.Fatal(s)
	}
	if err := check(s, rules); err == nil {
		t.Fatal("new unbudgeted package disappeared")
	}
	rules.Packages["github.com/dreylark/heos-control/internal/new"] = 0
	if err := check(s, rules); err != nil {
		t.Fatal(err)
	}
	rules.Packages["github.com/dreylark/heos-control/internal/missing"] = 1
	if err := check(s, rules); err == nil {
		t.Fatal("missing expected package passed")
	}
}

func TestThresholdDoesNotRoundUpAndChecksPackageRegressions(t *testing.T) {
	p := mustProfile(t, "github.com/dreylark/heos-control/internal/api/a.go:1.1,2.2 89999 1\ngithub.com/dreylark/heos-control/internal/api/a.go:3.1,4.2 10001 0\n")
	s := summarize(p, nil)
	rules := policy{Total: 90, Packages: map[string]float64{"github.com/dreylark/heos-control/internal/api": 0}}
	if err := check(s, rules); err == nil {
		t.Fatal("rounded a total regression up to threshold")
	}
	rules.Total, rules.Packages["github.com/dreylark/heos-control/internal/api"] = 0, 90
	if err := check(s, rules); err == nil {
		t.Fatal("ignored package regression")
	}
}

func TestSingleProfileReportsSurviveThresholdFailureButMissingInputIsRejected(t *testing.T) {
	dir := t.TempDir()
	data := []byte("mode: atomic\ngithub.com/dreylark/heos-control/internal/api/a.go:1.1,2.2 3 0\n")
	if err := os.WriteFile(filepath.Join(dir, "coverage.raw.out"), data, 0600); err != nil {
		t.Fatal(err)
	}
	rules := policy{Total: 1, Packages: map[string]float64{"github.com/dreylark/heos-control/internal/api": 1}}
	b, err := json.Marshal(rules)
	if err != nil {
		t.Fatal(err)
	}
	policyFile := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(policyFile, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(dir, policyFile); err == nil {
		t.Fatal("coverage regression passed")
	}
	for _, name := range []string{"summary.json", "summary.tsv", "coverage.out"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatal("failure discarded diagnostic report", err)
		}
	}
	if err := os.Remove(filepath.Join(dir, "coverage.raw.out")); err != nil {
		t.Fatal(err)
	}
	if err := run(dir, policyFile); err == nil {
		t.Fatal("missing profile was ignored")
	}
}

func TestMissingExplicitExclusionFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "coverage.raw.out"), []byte("mode: atomic\ngithub.com/dreylark/heos-control/internal/api/a.go:1.1,2.2 3 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	rules := policy{ExcludedFiles: []string{"github.com/dreylark/heos-control/internal/api/api.gen.go"}, Packages: map[string]float64{"github.com/dreylark/heos-control/internal/api": 0}}
	b, err := json.Marshal(rules)
	if err != nil {
		t.Fatal(err)
	}
	policyFile := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(policyFile, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(dir, policyFile); err == nil || !strings.Contains(err.Error(), "excluded file missing") {
		t.Fatalf("missing exclusion was not detected: %v", err)
	}
}
