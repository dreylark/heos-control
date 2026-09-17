package main

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.yaml.in/yaml/v4"
)

func decimalCompare(a, b string) int {
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return strings.Compare(a, b)
}

// Compare validated SemVer values without integer overflow. Build metadata is
// already excluded by the registry version contract.
func compareVersions(a, b string) int {
	ac, ap, ahas := strings.Cut(a, "-")
	bc, bp, bhas := strings.Cut(b, "-")
	av, bv := strings.Split(ac, "."), strings.Split(bc, ".")
	for i := range av {
		if c := decimalCompare(av[i], bv[i]); c != 0 {
			return c
		}
	}
	if !ahas && !bhas {
		return 0
	}
	if !ahas {
		return 1
	}
	if !bhas {
		return -1
	}
	av, bv = strings.Split(ap, "."), strings.Split(bp, ".")
	for i := 0; i < min(len(av), len(bv)); i++ {
		an, bn := strings.Trim(av[i], "0123456789") == "", strings.Trim(bv[i], "0123456789") == ""
		if an && !bn {
			return -1
		}
		if !an && bn {
			return 1
		}
		c := strings.Compare(av[i], bv[i])
		if an && bn {
			c = decimalCompare(av[i], bv[i])
		}
		if c != 0 {
			return c
		}
	}
	if len(av) < len(bv) {
		return -1
	}
	if len(av) > len(bv) {
		return 1
	}
	return 0
}

func nextVersion(current, mode, explicit string) (string, error) {
	if err := validateVersion("v"+current, current, current, current, current); err != nil {
		return "", err
	}
	next := explicit
	if mode != "explicit" {
		if explicit != "" {
			return "", errors.New("version is only accepted with mode explicit")
		}
		index := map[string]int{"major": 0, "minor": 1, "patch": 2}
		i, ok := index[mode]
		if !ok {
			return "", errors.New("mode must be patch, minor, major or explicit")
		}
		core, _, _ := strings.Cut(current, "-")
		parts := strings.Split(core, ".")
		n, _ := new(big.Int).SetString(parts[i], 10)
		parts[i] = n.Add(n, big.NewInt(1)).String()
		for j := i + 1; j < len(parts); j++ {
			parts[j] = "0"
		}
		next = strings.Join(parts, ".")
	}
	if err := validateVersion("v"+next, next, next, next, next); err != nil {
		return "", err
	}
	if compareVersions(next, current) <= 0 {
		return "", errors.New("new version must be strictly greater than the current version")
	}
	return next, nil
}

// Use YAML paths to find scalars but edit their byte spans, retaining formatting,
// comments and quotes throughout the large OpenAPI document.
func replaceYAMLVersions(data []byte, paths [][]string, current, next string) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	type edit struct {
		start, end int
		value      string
	}
	edits := []edit{}
	for _, path := range paths {
		if len(doc.Content) != 1 {
			return nil, errors.New("expected one YAML document")
		}
		node := doc.Content[0]
		for _, key := range path {
			if node.Kind != yaml.MappingNode {
				return nil, fmt.Errorf("version path %v is not a mapping", path)
			}
			var found *yaml.Node
			for i := 0; i < len(node.Content); i += 2 {
				if node.Content[i].Value == key {
					if found != nil {
						return nil, errors.New("duplicate version key")
					}
					found = node.Content[i+1]
				}
			}
			if found == nil {
				return nil, fmt.Errorf("missing version path %v", path)
			}
			node = found
		}
		if node.Kind != yaml.ScalarNode || node.Value != current {
			return nil, errors.New("version scalar differs from VERSION")
		}
		old, value := current, next
		switch node.Style {
		case yaml.DoubleQuotedStyle:
			old, value = "\""+old+"\"", "\""+value+"\""
		case yaml.SingleQuotedStyle:
			old, value = "'"+old+"'", "'"+value+"'"
		case 0:
		default:
			return nil, errors.New("version must be a plain or quoted scalar")
		}
		lines := bytes.SplitAfter(data, []byte("\n"))
		start := 0
		for i := 0; i < node.Line-1; i++ {
			start += len(lines[i])
		}
		// YAML columns count characters, not UTF-8 bytes.
		start += len(string([]rune(string(lines[node.Line-1]))[:node.Column-1]))
		if !bytes.HasPrefix(data[start:], []byte(old)) {
			return nil, errors.New("unsupported version scalar encoding")
		}
		edits = append(edits, edit{start, start + len(old), value})
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	for _, e := range edits {
		data = append(append(append([]byte{}, data[:e.start]...), []byte(e.value)...), data[e.end:]...)
	}
	return data, nil
}

// Prepare only source metadata. Generation, checks, Git and GitHub are owned by
// the caller; all input validation happens before changing any file.
func prepareRelease(root, mode, explicit string) (string, error) {
	b, err := os.ReadFile(filepath.Join(root, "VERSION"))
	if err != nil {
		return "", err
	}
	current := strings.TrimSpace(string(b))
	if _, err = validateAt(root, "v"+current); err != nil {
		return "", err
	}
	next, err := nextVersion(current, mode, explicit)
	if err != nil {
		return "", err
	}
	type change struct {
		path string
		body []byte
	}
	changes := []change{{filepath.Join(root, "VERSION"), []byte(next + "\n")}}
	for _, file := range []struct {
		path string
		keys [][]string
	}{
		{"charts/heos-control/Chart.yaml", [][]string{{"version"}, {"appVersion"}}},
		{"api/openapi.yaml", [][]string{{"info", "version"}}},
	} {
		path := filepath.Join(root, file.path)
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		data, err = replaceYAMLVersions(data, file.keys, current, next)
		if err != nil {
			return "", err
		}
		changes = append(changes, change{path, data})
	}
	for _, c := range changes {
		if err := os.WriteFile(c.path, c.body, 0644); err != nil {
			return "", err
		}
	}
	return next, nil
}
