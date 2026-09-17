// coverage reports one full test profile and enforces the reviewed policy.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type block struct {
	File, Range string
	Statements  int
	Covered     bool
}
type profile map[string]block
type policy struct {
	ExcludedFiles []string           `json:"excluded_files"`
	Total         float64            `json:"total_minimum"`
	Packages      map[string]float64 `json:"package_minimums"`
}
type score struct {
	Statements int     `json:"statements"`
	Covered    int     `json:"covered"`
	Percent    float64 `json:"percent"`
}
type summary struct {
	Total    score            `json:"total"`
	Packages map[string]score `json:"packages"`
}

var location = regexp.MustCompile(`^(.+\.go):([0-9]+\.[0-9]+,[0-9]+\.[0-9]+)$`)

func parse(r io.Reader) (profile, error) {
	s := bufio.NewScanner(r)
	if !s.Scan() || s.Text() != "mode: atomic" {
		return nil, errors.New("expected atomic coverage profile")
	}
	p := profile{}
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) != 3 {
			return nil, fmt.Errorf("malformed coverage block: %q", s.Text())
		}
		loc := location.FindStringSubmatch(fields[0])
		n, err := strconv.Atoi(fields[1])
		count, countErr := strconv.ParseUint(fields[2], 10, 64)
		if loc == nil || err != nil || n < 0 || countErr != nil {
			return nil, fmt.Errorf("invalid coverage block: %q", s.Text())
		}
		b := block{loc[1], loc[2], n, count > 0}
		if old, exists := p[fields[0]]; exists {
			if old.Statements != n {
				return nil, fmt.Errorf("conflicting statement count: %s", fields[0])
			}
			b.Covered = b.Covered || old.Covered
		}
		p[fields[0]] = b
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	if len(p) == 0 {
		return nil, errors.New("empty coverage profile")
	}
	return p, nil
}

func summarize(p profile, excluded []string) summary {
	s := summary{Packages: map[string]score{}}
	for _, b := range p {
		if slices.Contains(excluded, b.File) {
			continue
		}
		pkg := path.Dir(b.File)
		v := s.Packages[pkg]
		v.Statements += b.Statements
		s.Total.Statements += b.Statements
		if b.Covered {
			v.Covered += b.Statements
			s.Total.Covered += b.Statements
		}
		s.Packages[pkg] = v
	}
	percent := func(v score) score {
		if v.Statements > 0 {
			v.Percent = 100 * float64(v.Covered) / float64(v.Statements)
		}
		return v
	}
	for pkg, v := range s.Packages {
		s.Packages[pkg] = percent(v)
	}
	s.Total = percent(s.Total)
	return s
}

func check(s summary, rules policy) error {
	var failures []error
	compare := func(name string, v score, minimum float64) {
		if minimum < 0 || minimum > 100 || v.Statements == 0 || v.Percent < minimum {
			failures = append(failures, fmt.Errorf("%s: %.4f%% below minimum %.2f%% or invalid policy/empty denominator", name, v.Percent, minimum))
		}
	}
	compare("total", s.Total, rules.Total)
	for pkg, v := range s.Packages {
		minimum, exists := rules.Packages[pkg]
		if !exists {
			failures = append(failures, fmt.Errorf("package has no reviewed threshold: %s", pkg))
			continue
		}
		compare(pkg, v, minimum)
	}
	for pkg := range rules.Packages {
		if _, exists := s.Packages[pkg]; !exists {
			failures = append(failures, fmt.Errorf("expected package missing: %s", pkg))
		}
	}
	return errors.Join(failures...)
}

func writeProfile(name string, p profile, excluded []string) error {
	var out strings.Builder
	out.WriteString("mode: set\n")
	keys := make([]string, 0, len(p))
	for key := range p {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		b := p[key]
		if slices.Contains(excluded, b.File) {
			continue
		}
		count := 0
		if b.Covered {
			count = 1
		}
		fmt.Fprintf(&out, "%s %d %d\n", key, b.Statements, count)
	}
	return os.WriteFile(name, []byte(out.String()), 0644)
}

func run(directory, policyFile string) error {
	b, err := os.ReadFile(policyFile)
	if err != nil {
		return err
	}
	var rules policy
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err := d.Decode(&rules); err != nil {
		return err
	}
	f, err := os.Open(filepath.Join(directory, "coverage.raw.out"))
	if err != nil {
		return err
	}
	p, err := parse(f)
	_ = f.Close()
	if err != nil {
		return err
	}
	// Exclusions must continue to name real instrumented files. Renaming or adding
	// generated output requires a visible policy change, never a wildcard expansion.
	for _, file := range rules.ExcludedFiles {
		found := false
		for _, block := range p {
			found = found || block.File == file
		}
		if !found {
			return fmt.Errorf("excluded file missing from profile: %s", file)
		}
	}
	if err := writeProfile(filepath.Join(directory, "coverage.out"), p, rules.ExcludedFiles); err != nil {
		return err
	}
	s := summarize(p, rules.ExcludedFiles)
	var table strings.Builder
	fmt.Fprintln(&table, "Package\tCovered\tStatements\tPercent")
	packages := make([]string, 0, len(s.Packages))
	for pkg := range s.Packages {
		packages = append(packages, pkg)
	}
	slices.Sort(packages)
	for _, pkg := range append(packages, "TOTAL") {
		v := s.Packages[pkg]
		if pkg == "TOTAL" {
			v = s.Total
		}
		fmt.Fprintf(&table, "%s\t%d\t%d\t%.2f\n", pkg, v.Covered, v.Statements, v.Percent)
	}
	b, err = json.MarshalIndent(struct {
		Policy   policy  `json:"policy"`
		Coverage summary `json:"coverage"`
	}{rules, s}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, "summary.json"), append(b, '\n'), 0644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, "summary.tsv"), []byte(table.String()), 0644); err != nil {
		return err
	}
	fmt.Print(table.String())
	return check(s, rules)
}

func main() {
	directory := flag.String("dir", "coverage", "profile/report directory")
	policyFile := flag.String("policy", "scripts/coverage/policy.json", "reviewed coverage policy")
	flag.Parse()
	if err := run(*directory, *policyFile); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
