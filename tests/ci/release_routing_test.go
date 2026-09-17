//go:build ci

package ci

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"
)

type releaseRouting struct {
	Jobs map[string]struct {
		Permissions map[string]string
		With        map[string]yaml.Node
		Steps       []struct {
			Uses, Run string
			With      map[string]string
		}
	}
}

func readReleaseRouting(t *testing.T, name string) releaseRouting {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("../../.github/workflows", name+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow releaseRouting
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	return workflow
}

func TestReleasePreparationReliesOnNativePRChecks(t *testing.T) {
	workflow := readReleaseRouting(t, "prepare-release")
	job := workflow.Jobs["prepare"]
	if job.Permissions["actions"] == "write" {
		t.Error("release preparation must not dispatch another CI run")
	}
	for _, step := range job.Steps {
		for _, dispatch := range []string{"gh workflow run", "createWorkflowDispatch", "/dispatches"} {
			if strings.Contains(step.Run, dispatch) {
				t.Errorf("release preparation duplicates native PR CI: %s", step.Run)
			}
		}
	}
}

func TestReleaseVerificationUsesEventCommit(t *testing.T) {
	for _, name := range []string{"lint", "tests", "build"} {
		for jobName, job := range readReleaseRouting(t, name).Jobs {
			checked := false
			for _, step := range job.Steps {
				if strings.HasPrefix(step.Uses, "actions/checkout@") {
					checked = true
					if step.With["ref"] != "${{ github.sha }}" {
						t.Fatalf("%s/%s must check the event's merge SHA: %v", name, jobName, step.With)
					}
				}
			}
			if !checked {
				t.Fatalf("%s/%s has no source checkout", name, jobName)
			}
		}
	}
	release := readReleaseRouting(t, "release")
	if release.Jobs["qualify"].With["full"].Value != "true" {
		t.Fatal("published releases must retain full container qualification")
	}
}
