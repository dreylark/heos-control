//go:build ci

package ci

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"go.yaml.in/yaml/v4"
)

type workflowRouting struct {
	On   map[string]yaml.Node
	Jobs map[string]struct {
		Uses  string
		Needs yaml.Node
		If    string
	}
}

func readRouting(t *testing.T, name string) workflowRouting {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("../../.github/workflows", name+".yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow workflowRouting
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	return workflow
}

func TestCIRoutesMainPushAndPRUpdatesThroughStaticChecks(t *testing.T) {
	workflow := readRouting(t, "ci")
	if len(workflow.On) != 4 {
		t.Fatalf("unexpected automatic CI events: %v", workflow.On)
	}
	for _, event := range []string{"push", "pull_request"} {
		var filter struct{ Branches, Types, Tags []string }
		node := workflow.On[event]
		if err := node.Decode(&filter); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(filter.Branches, []string{"main"}) || len(filter.Tags) != 0 {
			t.Fatalf("%s must target only main: %+v", event, filter)
		}
		if event == "pull_request" && !reflect.DeepEqual(filter.Types, []string{"opened", "synchronize", "reopened"}) {
			t.Fatalf("PR creation and subsequent commits must run CI: %v", filter.Types)
		}
	}
	for _, event := range []string{"workflow_dispatch", "workflow_call"} {
		if _, ok := workflow.On[event]; !ok {
			t.Fatalf("missing explicit %s entrypoint", event)
		}
	}
	if len(workflow.Jobs) != 3 {
		t.Fatal("CI must run its own lint, tests and build phases without a waiting gate")
	}
	for _, name := range []string{"lint", "tests", "build"} {
		job := workflow.Jobs[name]
		if job.Uses != "./.github/workflows/"+name+".yaml" || job.If != "" {
			t.Fatalf("%s must run without reuse/skip conditions: %+v", name, job)
		}
		if name == "lint" {
			if job.Needs.Kind != 0 {
				t.Fatal("static checks must be the first phase")
			}
		} else if job.Needs.Kind != yaml.ScalarNode || job.Needs.Value != "lint" {
			t.Fatalf("%s must wait for lint and run independently of the other phase", name)
		}
	}
}

func TestReusablePhasesCannotCreateDuplicateAutomaticRuns(t *testing.T) {
	for _, name := range []string{"lint", "tests", "build"} {
		t.Run(name, func(t *testing.T) {
			workflow := readRouting(t, name)
			if _, ok := workflow.On["workflow_call"]; !ok || len(workflow.On) != 1 {
				t.Fatal("a phase must only be called by its orchestrator")
			}
		})
	}
	workflow := readRouting(t, "release")
	var event struct{ Types []string }
	node := workflow.On["release"]
	if err := node.Decode(&event); err != nil {
		t.Fatal(err)
	}
	if len(workflow.On) != 1 || !reflect.DeepEqual(event.Types, []string{"published"}) {
		t.Fatal("publishing must only start from a published GitHub Release")
	}
}
