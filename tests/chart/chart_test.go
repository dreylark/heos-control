//go:build chart

// Package chart tests the rendered chart and its consumer contract.
package chart

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dreylark/heos-control/internal/config"
	"go.yaml.in/yaml/v4"
)

func TestChartContract(t *testing.T) {
	t.Chdir("../..")
	if err := check(); err != nil {
		t.Fatal(err)
	}
}

func helm(args ...string) ([]byte, error) {
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("helm %v: %s", args, out)
	}
	return out, nil
}

func check() error {
	version, err := os.ReadFile("VERSION")
	if err != nil {
		return err
	}
	metadata, err := os.ReadFile("charts/heos-control/Chart.yaml")
	if err != nil {
		return err
	}
	var chart struct {
		Version    string `yaml:"version"`
		AppVersion string `yaml:"appVersion"`
	}
	if err := yaml.Unmarshal(metadata, &chart); err != nil {
		return err
	}
	if chart.Version != strings.TrimSpace(string(version)) || chart.AppVersion != chart.Version {
		return fmt.Errorf("chart/application version drift")
	}
	if _, err := helm("lint", "charts/heos-control"); err != nil {
		return err
	}
	out, err := helm("template", "heos-control", "charts/heos-control")
	if err != nil {
		return err
	}
	if err := inspect(out, true); err != nil {
		return err
	}
	if err := inspectLogging(out, "info"); err != nil {
		return err
	}
	debug, err := helm("template", "heos-control", "charts/heos-control", "--set", "logging.level=debug")
	if err != nil {
		return err
	}
	if err := inspectLogging(debug, "debug"); err != nil {
		return err
	}
	if err := inspectMigrations(out); err != nil {
		return err
	}
	if err := inspectDocumentation(out, false); err != nil {
		return err
	}
	out, err = helm("template", "heos-control", "charts/heos-control", "--set", "api.docs.enabled=true")
	if err != nil {
		return err
	}
	if err := inspectDocumentation(out, true); err != nil {
		return err
	}
	if err := checkDependency(chart.Version); err != nil {
		return err
	}
	args := []string{"template", "heos-control", "charts/heos-control", "--set", "networkPolicy.enabled=false", "--set", "image.digest=sha256:" + strings.Repeat("a", 64)}
	out, err = helm(args...)
	if err != nil {
		return err
	}
	if err = inspect(out, false); err != nil {
		return err
	}
	if !bytes.Contains(out, []byte("heos-control@sha256:")) {
		return fmt.Errorf("digest image selection failed")
	}
	out, err = helm("template", "test", "charts/heos-control", "--set", "api.credentialsSecret=machine-credentials", "--set-json", `players=[{"key":"room","address":"speaker.example.invalid:1265","fingerprint_sha256":"`+strings.Repeat("a", 64)+`","serial":"synthetic","model":"test","volume_ceiling":45,"writes_enabled":true}]`, "--set-json", `sources=[{"key":"gerbera-music","player":"room","name":"Gerbera"}]`, "--set-json", `networkPolicy.heosPeers=[{"ipBlock":{"cidr":"192.0.2.1/32"}}]`)
	if err != nil {
		return err
	}
	if err = inspect(out, true); err != nil {
		return err
	}
	for _, required := range []string{"secretName: machine-credentials", "mountPath: /etc/heos-control/credentials", "port: 1265", "192.0.2.1/32"} {
		if !bytes.Contains(out, []byte(required)) {
			return fmt.Errorf("missing configured read-only mount/egress: %s", required)
		}
	}
	for _, value := range []string{"logging.level=trace", "logging.level=DEBUG", "logging.raw=true", "replicaCount=2", "database.host=", "database.caSecret=", "api.tlsSecret=", "api.docs.enabled=yes", "api.docs.unknown=true", "terminationGracePeriodSeconds=24", "database.lockTimeoutSeconds=6", "image.digest=bad", "api.port=443", "unsupportedSetting=true", "migration.credentialsSecret=", "migration.user=", "migration.hooks=disabled", "migration.enabled=false", "migration.activeDeadlineSeconds=0"} {
		if _, err := helm("template", "test", "charts/heos-control", "--set", value); err == nil {
			return fmt.Errorf("chart accepted invalid setting %s", value)
		}
	}
	fmt.Println("Helm standalone/wrapper rendering, resources, digest selection and invalid values passed.")
	return nil
}

func inspectLogging(data []byte, level string) error {
	d := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var r map[string]any
		if err := d.Decode(&r); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if r["kind"] != "ConfigMap" || strings.HasSuffix(r["metadata"].(map[string]any)["name"].(string), "-migration") {
			continue
		}
		var c config.Config
		if err := json.Unmarshal([]byte(r["data"].(map[string]any)["config.json"].(string)), &c); err != nil {
			return err
		}
		if c.LogLevel != level {
			return fmt.Errorf("logging level not rendered: %q", c.LogLevel)
		}
		return c.Validate()
	}
	return fmt.Errorf("missing runtime logging config")
}

// Helm injects global values into dependencies even when the parent sets none.
// Exercise the packaged chart through a real parent, as GitOps wrappers use it.
func checkDependency(version string) error {
	dir, err := os.MkdirTemp("", "heos-chart-wrapper-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	dependencies := filepath.Join(dir, "charts")
	if err := os.Mkdir(dependencies, 0700); err != nil {
		return err
	}
	if _, err := helm("package", "charts/heos-control", "--destination", dependencies); err != nil {
		return err
	}
	metadata := fmt.Sprintf("apiVersion: v2\nname: consumer\nversion: 0.1.0\ndependencies:\n  - name: heos-control\n    version: %q\n", version)
	if err := os.WriteFile(filepath.Join(dir, "Chart.yaml"), []byte(metadata), 0600); err != nil {
		return err
	}
	for _, settings := range [][]string{nil, {"--set", "global.cluster=example"}, {"--set", "heos-control.api.docs.enabled=true"}} {
		args := append([]string{"template", "consumer", dir}, settings...)
		out, err := helm(args...)
		if err != nil {
			return fmt.Errorf("wrapper chart: %w", err)
		}
		if err := inspect(out, true); err != nil {
			return err
		}
		if err := inspectMigrations(out); err != nil {
			return err
		}
		if err := inspectDocumentation(out, len(settings) > 0 && settings[1] == "heos-control.api.docs.enabled=true"); err != nil {
			return err
		}
	}
	if _, err := helm("template", "consumer", dir, "--set", "heos-control.replicaCount=2"); err == nil {
		return fmt.Errorf("wrapper chart bypassed the single-controller constraint")
	}
	return nil
}

// The chart renders the opt-in into runtime JSON; migrations never enable it.
func inspectDocumentation(data []byte, enabled bool) error {
	d := yaml.NewDecoder(bytes.NewReader(data))
	seen := false
	for {
		var r map[string]any
		if err := d.Decode(&r); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if r["kind"] != "ConfigMap" {
			continue
		}
		var c config.Config
		if err := json.Unmarshal([]byte(r["data"].(map[string]any)["config.json"].(string)), &c); err != nil {
			return err
		}
		want := enabled
		if strings.HasSuffix(r["metadata"].(map[string]any)["name"].(string), "-migration") {
			want = false
		} else {
			seen = true
		}
		if c.DocsEnabled != want {
			return fmt.Errorf("documentation opt-in not rendered into the correct configuration")
		}
	}
	if !seen {
		return fmt.Errorf("documentation check found no runtime config")
	}
	return nil
}

func inspect(data []byte, policy bool) error {
	d := yaml.NewDecoder(bytes.NewReader(data))
	seen := map[string]int{}
	for {
		var r map[string]any
		if err := d.Decode(&r); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		kind, _ := r["kind"].(string)
		seen[kind]++
		switch kind {
		case "ConfigMap":
			var c config.Config
			if err := json.Unmarshal([]byte(r["data"].(map[string]any)["config.json"].(string)), &c); err != nil {
				return err
			}
			if err := c.Validate(); err != nil {
				return fmt.Errorf("rendered config: %w", err)
			}
			if r["metadata"].(map[string]any)["labels"] != nil && r["metadata"].(map[string]any)["labels"].(map[string]any)["app.kubernetes.io/component"] == "migration" {
				if len(c.Players) != 0 || len(c.Sources) != 0 {
					return fmt.Errorf("migration config must not configure device work")
				}
				continue
			}
			if len(r["data"].(map[string]any)) != 1 {
				return fmt.Errorf("runtime ConfigMap must contain only config.json")
			}

		case "Deployment":
			s := r["spec"].(map[string]any)
			if s["replicas"] != 1 || s["strategy"].(map[string]any)["type"] != "Recreate" {
				return fmt.Errorf("controller overlap policy changed")
			}
			template := s["template"].(map[string]any)
			selectors, _ := json.Marshal(s["selector"].(map[string]any)["matchLabels"])
			labels, _ := json.Marshal(template["metadata"].(map[string]any)["labels"])
			if !bytes.Equal(selectors, labels) {
				return fmt.Errorf("deployment selector mismatch")
			}
			pod := template["spec"].(map[string]any)
			if pod["automountServiceAccountToken"] != false {
				return fmt.Errorf("service account token mounted")
			}
			security := pod["securityContext"].(map[string]any)
			if security["runAsNonRoot"] != true || security["runAsUser"] != 10001 {
				return fmt.Errorf("pod must be non-root")
			}
			containers := pod["containers"].([]any)
			if len(containers) != 1 {
				return fmt.Errorf("unexpected additional container")
			}
			c := containers[0].(map[string]any)
			cs := c["securityContext"].(map[string]any)
			if cs["readOnlyRootFilesystem"] != true || cs["allowPrivilegeEscalation"] != false {
				return fmt.Errorf("container security changed")
			}
			for _, name := range []string{"livenessProbe", "readinessProbe", "startupProbe"} {
				if c[name].(map[string]any)["httpGet"].(map[string]any)["scheme"] != "HTTPS" {
					return fmt.Errorf("plaintext probe")
				}
				path := c[name].(map[string]any)["httpGet"].(map[string]any)["path"]
				want := "/livez"
				if name == "readinessProbe" {
					want = "/readyz"
				}
				if path != want {
					return fmt.Errorf("probe %s depends on wrong health boundary", name)
				}
			}
			volumes := pod["volumes"].([]any)
			if len(volumes) != 4 && len(volumes) != 5 {
				return fmt.Errorf("unexpected volumes")
			}
			for _, v := range volumes {
				m := v.(map[string]any)
				if m["secret"] == nil && m["configMap"] == nil {
					return fmt.Errorf("unexpected data volume")
				}
			}
		case "Service":
			if r["spec"].(map[string]any)["type"] != "ClusterIP" {
				return fmt.Errorf("public service")
			}
		case "Job", "ServiceAccount", "NetworkPolicy":
		default:
			return fmt.Errorf("unexpected resource kind %s", kind)
		}
	}
	for kind, count := range map[string]int{"Deployment": 1, "Service": 1, "Job": 1, "ServiceAccount": 2, "ConfigMap": 2} {
		if seen[kind] != count {
			return fmt.Errorf("missing or duplicate %s", kind)
		}
	}
	want := 0
	if policy {
		want = 2
	}
	if seen["NetworkPolicy"] != want {
		return fmt.Errorf("network policy toggle failed")
	}
	return nil
}

// Migration admission is a packaging invariant: every installation has one Job,
// separate owner credentials and no device configuration or network access.
func inspectMigrations(data []byte) error {
	d := yaml.NewDecoder(bytes.NewReader(data))
	var job, deployment map[string]any
	configs := map[string]config.Config{}
	accounts := map[string]map[string]any{}
	var migrationPolicy map[string]any
	for {
		var r map[string]any
		if err := d.Decode(&r); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		meta := r["metadata"].(map[string]any)
		switch r["kind"] {
		case "Job":
			job = r
		case "Deployment":
			deployment = r
		case "ServiceAccount":
			accounts[meta["name"].(string)] = r
		case "ConfigMap":
			var c config.Config
			if err := json.Unmarshal([]byte(r["data"].(map[string]any)["config.json"].(string)), &c); err != nil {
				return err
			}
			configs[meta["name"].(string)] = c
		case "NetworkPolicy":
			if strings.HasSuffix(meta["name"].(string), "-migration") {
				migrationPolicy = r
			}
		}
	}
	if job == nil || deployment == nil {
		return fmt.Errorf("missing migration Job or runtime Deployment")
	}
	annotations := job["metadata"].(map[string]any)["annotations"].(map[string]any)
	if annotations["helm.sh/hook"] != "pre-install,pre-upgrade" || annotations["helm.sh/hook-weight"] != "-1" {
		return fmt.Errorf("migration must run before Helm install/upgrade")
	}
	spec := job["spec"].(map[string]any)
	if spec["backoffLimit"] != 0 || spec["activeDeadlineSeconds"] != 300 {
		return fmt.Errorf("unbounded migration retries or deadline")
	}
	pod := spec["template"].(map[string]any)["spec"].(map[string]any)
	runtime := deployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	container := pod["containers"].([]any)[0].(map[string]any)
	runtimeContainer := runtime["containers"].([]any)[0].(map[string]any)
	if container["image"] != runtimeContainer["image"] {
		return fmt.Errorf("migration/runtime image mismatch")
	}
	args, _ := json.Marshal(container["args"])
	if string(args) != `["migrate","-config","/etc/heos-control/config/config.json"]` {
		return fmt.Errorf("Job must execute only migrate")
	}
	if pod["restartPolicy"] != "Never" || pod["automountServiceAccountToken"] != false || pod["serviceAccountName"] == runtime["serviceAccountName"] {
		return fmt.Errorf("migration isolation changed")
	}
	account := accounts[pod["serviceAccountName"].(string)]
	if account == nil || account["metadata"].(map[string]any)["annotations"].(map[string]any)["helm.sh/hook-weight"] != "-2" {
		return fmt.Errorf("migration ServiceAccount must exist before Job")
	}
	secrets := map[string]bool{}
	var owner config.Config
	for _, v := range pod["volumes"].([]any) {
		vol := v.(map[string]any)
		if secret, ok := vol["secret"].(map[string]any); ok {
			secrets[secret["secretName"].(string)] = true
		}
		if cm, ok := vol["configMap"].(map[string]any); ok {
			owner = configs[cm["name"].(string)]
		}
	}
	if !secrets["heos-control-migration-database"] || len(secrets) != 2 || owner.Database.User != "heos_owner" || len(owner.Players) != 0 {
		return fmt.Errorf("migration mounts or owner configuration changed")
	}
	for _, v := range runtime["volumes"].([]any) {
		if secret, ok := v.(map[string]any)["secret"].(map[string]any); ok && secret["secretName"] == "heos-control-migration-database" {
			return fmt.Errorf("owner credentials mounted by runtime")
		}
	}
	if migrationPolicy == nil {
		return fmt.Errorf("missing migration network isolation")
	}
	policy := migrationPolicy["spec"].(map[string]any)
	if len(policy["ingress"].([]any)) != 0 {
		return fmt.Errorf("migration accepts ingress")
	}
	for _, e := range policy["egress"].([]any) {
		for _, p := range e.(map[string]any)["ports"].([]any) {
			port := p.(map[string]any)["port"]
			if port != 53 && port != 5432 {
				return fmt.Errorf("migration can access non-database devices")
			}
		}
	}
	return nil
}
