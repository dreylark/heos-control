package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseVersionsMustAgree(t *testing.T) {
	for _, version := range []string{"0.7.0", "1.0.0-rc.1", "2.12.3-beta-1"} {
		if err := validateVersion("v"+version, version, version, version, version); err != nil {
			t.Fatal(err)
		}
	}
	for _, version := range []string{"01.7.0", "1.0", "1.0.0+build", "1.0.0-01", "1.0.0-rc..1", "1.0.0\ninjected"} {
		if err := validateVersion("v"+version, version, version, version, version); err == nil {
			t.Fatal("invalid version accepted", version)
		}
	}
	for field := range 5 {
		values := []string{"v0.7.0", "0.7.0", "0.7.0", "0.7.0", "0.7.0"}
		values[field] = "0.8.0"
		if err := validateVersion(values[0], values[1], values[2], values[3], values[4]); err == nil {
			t.Fatal("version drift accepted", field)
		}
	}
}

func TestStagedChartPinsImageWithoutChangingPortableDefaults(t *testing.T) {
	source, destination := t.TempDir(), filepath.Join(t.TempDir(), "chart")
	files := map[string]string{
		"values.yaml": "image:\n  repository: heos-control\n  tag: dev\n  digest: ''\n  pullPolicy: IfNotPresent\nplayers: []\ndatabase:\n  host: postgres.example.invalid\n",
		"Chart.yaml":  "name: heos-control\nversion: 0.7.0\nappVersion: 0.7.0\nannotations:\n  existing: preserved\n",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(source, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	digest, revision := "sha256:"+strings.Repeat("a", 64), strings.Repeat("b", 40)
	if err := stageChart(source, destination, "ghcr.io/owner/repo", digest, "0.7.0", revision); err != nil {
		t.Fatal(err)
	}
	var values struct {
		Image    struct{ Repository, Tag, Digest, PullPolicy string } `yaml:"image"`
		Players  []any                                                `yaml:"players"`
		Database struct {
			Host string `yaml:"host"`
		} `yaml:"database"`
	}
	if err := loadYAML(filepath.Join(destination, "values.yaml"), &values); err != nil {
		t.Fatal(err)
	}
	if values.Image.Repository != "ghcr.io/owner/repo" || values.Image.Digest != digest || values.Image.Tag != "0.7.0" || len(values.Players) != 0 || values.Database.Host != "postgres.example.invalid" {
		t.Fatal(values)
	}
	var chart struct {
		Annotations map[string]string `yaml:"annotations"`
	}
	if err := loadYAML(filepath.Join(destination, "Chart.yaml"), &chart); err != nil {
		t.Fatal(err)
	}
	if chart.Annotations["existing"] != "preserved" || chart.Annotations["org.opencontainers.image.revision"] != revision || chart.Annotations["org.opencontainers.image.source"] != "https://github.com/owner/repo" {
		t.Fatal(chart)
	}
	for name, want := range files {
		b, err := os.ReadFile(filepath.Join(source, name))
		if err != nil || string(b) != want {
			t.Fatal("portable chart was changed", name, err)
		}
	}
	if err := stageChart(source, filepath.Join(t.TempDir(), "bad"), "ghcr.io/owner/repo", "local-image-id", "0.7.0", revision); err == nil {
		t.Fatal("non-registry digest accepted")
	}
}
