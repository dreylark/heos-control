package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/app"
)

func TestDiagnosticCLIUsage(t *testing.T) {
	for _, args := range [][]string{
		{"config"}, {"config", "unknown"}, {"doctor", "-format", "xml"},
		{"doctor", "-timeout", "0"}, {"doctor", "-timeout", "3m"},
		{"doctor", "unexpected"}, {"config", "check", "-timeout", "1s"},
	} {
		var out, errOut bytes.Buffer
		if code := runDiagnostic(context.Background(), args, &out, &errOut); code != 2 || out.Len() != 0 || errOut.Len() == 0 {
			t.Fatalf("args=%v code=%d out=%s stderr=%s", args, code, &out, &errOut)
		}
	}
}

func TestConfigCheckCLINeedsNoReachableServices(t *testing.T) {
	path := diagnosticCLIConfig(t)
	for _, format := range []string{"text", "json"} {
		var out, errOut bytes.Buffer
		if code := runDiagnostic(context.Background(), []string{"config", "check", "-config", path, "-format", format}, &out, &errOut); code != 0 {
			t.Fatalf("offline check: code=%d out=%s stderr=%s", code, &out, &errOut)
		}
		if strings.Contains(out.String(), "private-password") || strings.Contains(out.String(), filepath.Dir(path)) || strings.Contains(out.String(), `"msg":"starting"`) {
			t.Fatal("diagnostic output contains secrets or service logs", out.String())
		}
		if format == "json" {
			var report app.DiagnosticReport
			if err := json.Unmarshal(out.Bytes(), &report); err != nil || !report.OK || report.Command != "config check" || report.Version != 1 || len(report.Checks) < 4 {
				t.Fatal("not one complete JSON report", out.String(), err)
			}
		} else if !strings.Contains(out.String(), "PASS") || !strings.Contains(out.String(), "database.ca_file") {
			t.Fatal("text report omits individual checks", out.String())
		}
	}
}

func TestDiagnosticCommandEntrypointsProduceOnlyTheirReport(t *testing.T) {
	for _, command := range [][]string{{"config", "check"}, {"doctor"}} {
		t.Run(strings.Join(command, "_"), func(t *testing.T) {
			args, stdout := os.Args, os.Stdout
			t.Cleanup(func() { os.Args, os.Stdout = args, stdout })
			output, err := os.CreateTemp(t.TempDir(), "report")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = output.Close() }()
			os.Args = append([]string{"heos-control"}, command...)
			os.Args = append(os.Args, "-config", filepath.Join(t.TempDir(), "missing"), "-format", "json")
			os.Stdout = output
			if code := run(); code != 1 {
				t.Fatal(code)
			}
			body, err := os.ReadFile(output.Name())
			if err != nil {
				t.Fatal(err)
			}
			var report app.DiagnosticReport
			if err := json.Unmarshal(body, &report); err != nil || report.Command != strings.Join(command, " ") || report.OK {
				t.Fatal("entrypoint started service or mixed logs with report", string(body), err)
			}
		})
	}
}

func TestDoctorInvalidConfigurationNeverStartsService(t *testing.T) {
	var out, errOut bytes.Buffer
	path := filepath.Join(t.TempDir(), "missing-private-path.json")
	if code := runDiagnostic(context.Background(), []string{"doctor", "-config", path, "-format", "json"}, &out, &errOut); code != 1 {
		t.Fatal(code, &out, &errOut)
	}
	var report app.DiagnosticReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil || report.OK || report.Command != "doctor" || len(report.Checks) != 1 {
		t.Fatal(out.String(), err)
	}
	if strings.Contains(out.String(), path) || strings.Contains(out.String(), "starting") {
		t.Fatal("private path or startup logs escaped into report", out.String())
	}
}

func TestDoctorCancelledContextReturnsFailureReport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errOut bytes.Buffer
	code := runDiagnostic(ctx, []string{"doctor", "-config", diagnosticCLIConfig(t), "-format", "json"}, &out, &errOut)
	var report app.DiagnosticReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil || code != 1 || report.OK || !strings.Contains(out.String(), "cancelled") {
		t.Fatal(code, out.String(), err)
	}
}

func diagnosticCLIConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, data []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	cert := write("cert.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	private := write("key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	password := write("password", []byte("private-password\n"))
	body, err := json.Marshal(map[string]any{"cert_file": cert, "key_file": private, "database": map[string]any{"host": "unreachable.invalid", "name": "heos_test", "user": "runtime", "password_file": password, "ca_file": cert}})
	if err != nil {
		t.Fatal(err)
	}
	return write("config.json", body)
}
