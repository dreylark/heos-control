// release validates repository release versions and prepares a distributable chart.
// It never logs in, pushes artifacts, or contacts Kubernetes.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v4"
)

var semver = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([0-9A-Za-z-]+)(\.[0-9A-Za-z-]+)*)?$`)
var imageName = regexp.MustCompile(`^ghcr\.io/[a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9._-]*$`)
var digestValue = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var revisionValue = regexp.MustCompile(`^[0-9a-f]{40}$`)

func validateVersion(tag, version, chartVersion, appVersion, apiVersion string) error {
	if !semver.MatchString(version) || tag != "v"+version || chartVersion != version || appVersion != version || apiVersion != version {
		return errors.New("release tag must be v<VERSION>; VERSION, chart version/appVersion and OpenAPI info.version must match")
	}
	if _, pre, ok := strings.Cut(version, "-"); ok {
		for _, part := range strings.Split(pre, ".") {
			if len(part) > 1 && part[0] == '0' && strings.Trim(part, "0123456789") == "" {
				return errors.New("numeric prerelease identifiers must not have leading zeroes")
			}
		}
	}
	return nil
}

func loadYAML(name string, value any) error {
	b, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, value)
}

func validate(tag string) (string, error) { return validateAt(".", tag) }

func validateAt(root, tag string) (string, error) {
	b, err := os.ReadFile(filepath.Join(root, "VERSION"))
	if err != nil {
		return "", err
	}
	version := strings.TrimSpace(string(b))
	var metadata struct {
		Name       string `yaml:"name"`
		Version    string `yaml:"version"`
		AppVersion string `yaml:"appVersion"`
	}
	if err := loadYAML(filepath.Join(root, "charts/heos-control/Chart.yaml"), &metadata); err != nil {
		return "", err
	}
	if metadata.Name != "heos-control" {
		return "", errors.New("unexpected chart name")
	}
	var api struct {
		Info struct {
			Version string `yaml:"version"`
		} `yaml:"info"`
	}
	if err := loadYAML(filepath.Join(root, "api/openapi.yaml"), &api); err != nil {
		return "", err
	}
	return version, validateVersion(tag, version, metadata.Version, metadata.AppVersion, api.Info.Version)
}

func stageChart(source, destination, image, digest, version, revision string) error {
	if !imageName.MatchString(image) || !digestValue.MatchString(digest) || !revisionValue.MatchString(revision) {
		return errors.New("chart publication needs a GHCR owner/repository, registry sha256 digest and full source commit")
	}
	if err := os.CopyFS(destination, os.DirFS(source)); err != nil {
		return err
	}
	for _, name := range []string{"values.yaml", "Chart.yaml"} {
		file := filepath.Join(destination, name)
		var value map[string]any
		if err := loadYAML(file, &value); err != nil {
			return err
		}
		if name == "values.yaml" {
			settings, ok := value["image"].(map[string]any)
			if !ok {
				return errors.New("chart has no image settings")
			}
			settings["repository"], settings["tag"], settings["digest"] = image, version, digest
		} else {
			annotations, _ := value["annotations"].(map[string]any)
			if annotations == nil {
				annotations = map[string]any{}
			}
			annotations["org.opencontainers.image.source"] = "https://github.com/" + strings.TrimPrefix(image, "ghcr.io/")
			annotations["org.opencontainers.image.revision"] = revision
			value["annotations"] = annotations
		}
		b, err := yaml.Marshal(value)
		if err != nil {
			return err
		}
		if err := os.WriteFile(file, b, 0644); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	tag := flag.String("tag", "", "published GitHub Release tag")
	destination := flag.String("destination", "", "optional staged chart directory")
	image := flag.String("image", "", "GHCR image repository")
	digest := flag.String("digest", "", "published image registry digest")
	revision := flag.String("revision", "", "source commit")
	prepare := flag.String("prepare", "", "prepare source versions: patch, minor, major or explicit")
	explicit := flag.String("version", "", "target SemVer for prepare explicit, without v prefix")
	flag.Parse()
	if *prepare != "" {
		if *tag != "" || *destination != "" || *image != "" || *digest != "" || *revision != "" {
			fmt.Fprintln(os.Stderr, "preparation cannot be combined with publication arguments")
			os.Exit(1)
		}
		version, err := prepareRelease(".", *prepare, *explicit)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(version)
		return
	}
	if *explicit != "" {
		fmt.Fprintln(os.Stderr, "version requires prepare explicit")
		os.Exit(1)
	}
	version, err := validate(*tag)
	if err == nil && *destination != "" {
		err = stageChart("charts/heos-control", *destination, *image, *digest, version, *revision)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(version)
}
