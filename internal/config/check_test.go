package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

var checkNow = time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

func certificateMaterial(t *testing.T, alter func(*x509.Certificate)) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "offline.invalid"},
		NotBefore: checkNow.Add(-time.Hour), NotAfter: checkNow.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	if alter != nil {
		alter(cert)
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})
}

func writeCheckFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
}

func offlineFixture(t *testing.T) (Config, string) {
	t.Helper()
	dir := t.TempDir()
	cert, key := certificateMaterial(t, nil)
	ceiling := 7
	c := Config{LogLevel: "info", Listen: ":8443", CertFile: filepath.Join(dir, "secret-cert.pem"), KeyFile: filepath.Join(dir, "secret-key.pem"), ShutdownSeconds: 25,
		CredentialsFile: filepath.Join(dir, "secret-credentials.json"),
		Database:        Database{Host: "must-not-resolve.invalid", Port: 5432, Name: "secret-database", User: "secret-user", PasswordFile: filepath.Join(dir, "secret-password"), CAFile: filepath.Join(dir, "secret-ca.pem"), MaxConnections: 4, TimeoutSeconds: 5, LockTimeoutSeconds: 2},
		Players:         []Player{{Key: "room", Address: "speaker-must-not-resolve.invalid:1265", Serial: "secret-serial", FingerprintSHA256: strings.Repeat("a", 64), WritesEnabled: true, VolumeCeiling: &ceiling}}}
	writeCheckFile(t, c.CertFile, cert)
	writeCheckFile(t, c.KeyFile, key)
	writeCheckFile(t, c.Database.CAFile, cert)
	writeCheckFile(t, c.Database.PasswordFile, []byte("  never-print-this-password\n"))
	writeCheckFile(t, c.CredentialsFile, []byte(`{"version":1,"credentials":[{"principal":"never-print-principal","token_sha256":"`+strings.Repeat("e", 64)+`","scopes":["read","control"],"players":["room"]}]}`))
	return c, filepath.Join(dir, "secret-runtime-config.json")
}

func saveConfig(t *testing.T, c Config, path string) {
	t.Helper()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	writeCheckFile(t, path, data)
}

func failedChecks(checks []Check) []Check {
	var out []Check
	for _, check := range checks {
		if !check.OK {
			out = append(out, check)
		}
	}
	return out
}

func TestCheckFileValidOfflineConfiguration(t *testing.T) {
	c, path := offlineFixture(t)
	saveConfig(t, c, path)
	got, checks := CheckFile(path, checkNow)
	if got.Database.Host != c.Database.Host || len(checks) < 5 || len(failedChecks(checks)) != 0 {
		t.Fatal("valid local files failed offline validation", checks)
	}
	for _, check := range checks {
		if check.Field == "" || check.Code == "" || check.Message == "" {
			t.Fatal("incomplete check result", check)
		}
	}
	encoded, err := json.Marshal(checks)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{filepath.Dir(path), c.Database.Host, c.Database.Name, c.Database.User, c.Players[0].Address, c.Players[0].Serial, c.Players[0].FingerprintSHA256, strings.Repeat("e", 64), "never-print-this-password", "never-print-principal"} {
		if strings.Contains(string(encoded), private) {
			t.Fatal("diagnostics exposed configuration value")
		}
	}
}

func TestCheckFileReportsIndexedFieldsUsingRuntimeValidation(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		alter       func(*Config)
	}{
		{"player-address", "players[0].address", func(c *Config) { c.Players[0].Address = "sensitive-address:1255" }},
		{"player-pin", "players[0].fingerprint_sha256", func(c *Config) { c.Players[0].FingerprintSHA256 = "secret-invalid-pin" }},
		{"player-ceiling", "players[0].volume_ceiling", func(c *Config) { c.Players[0].VolumeCeiling = nil }},
		{"duplicate-player", "players[1].key", func(c *Config) { c.Players = append(c.Players, c.Players[0]) }},
		{"unknown-source-player", "sources[0].player", func(c *Config) {
			c.Sources = []Source{{Key: "music", Player: "secret-unknown-player", Name: "secret-name"}}
		}},
		{"pool-limit", "database.max_connections", func(c *Config) { c.Database.MaxConnections = 17 }},
		{"listen", "listen", func(c *Config) { c.Listen = ":80" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, path := offlineFixture(t)
			tc.alter(&c)
			saveConfig(t, c, path)
			_, checks := CheckFile(path, checkNow)
			bad := failedChecks(checks)
			if _, err := Load(path); err == nil || len(bad) != 1 || bad[0].Field != tc.field {
				t.Fatal("checker diverged from validator or lost indexed field", checks, err)
			}
		})
	}
}

func TestDatabaseHostRequiresTCPWithoutResolvingNames(t *testing.T) {
	for _, tc := range []struct {
		host string
		ok   bool
	}{
		{"database.invalid", true}, {"postgresql.database.svc.cluster.local", true}, {"localhost", true}, {"private_database", true},
		{"database.invalid.", true}, {"127.0.0.1", true}, {"2001:db8::1", true}, {"::1", true}, {"fe80::1%eth0", true},
		{"/tmp", false}, {"/var/run/postgresql", false}, {"@/tmp/postgresql", false}, {`C:\postgresql`, false},
		{"unix:/tmp", false}, {"postgres://secret-user:secret-password@database.invalid/db", false},
		{"database.invalid:5432", false}, {"[::1]", false}, {"database.invalid,other.invalid", false},
		{"database.invalid/path", false}, {"database.invalid?password=secret", false}, {"database.invalid#fragment", false},
		{" database.invalid", false}, {"database.invalid\n", false}, {"database.invalid\x00", false},
	} {
		t.Run(tc.host, func(t *testing.T) {
			c, path := offlineFixture(t)
			c.Database.Host = tc.host
			saveConfig(t, c, path)
			_, checks := CheckFile(path, checkNow)
			bad := failedChecks(checks)
			if err := c.Validate(); (err == nil) != tc.ok || (len(bad) == 0) != tc.ok {
				t.Fatal("TCP host policy differs between runtime and offline check", checks, err)
			}
			if !tc.ok && (len(bad) != 1 || bad[0].Field != "database.host") {
				t.Fatal("invalid host did not identify database.host", checks)
			}
		})
	}
}

func TestCheckFileCollectsIndependentSecretFileFailures(t *testing.T) {
	c, path := offlineFixture(t)
	writeCheckFile(t, c.CredentialsFile, []byte(`{"version":1,"credentials":[{"principal":"reader","token_sha256":"`+strings.Repeat("e", 64)+`","scopes":["read"],"players":["secret-unknown-player"]}]}`))
	writeCheckFile(t, c.CertFile, []byte("secret-invalid-certificate"))
	writeCheckFile(t, c.Database.PasswordFile, []byte(" \n\t"))
	writeCheckFile(t, c.Database.CAFile, []byte("secret-invalid-ca"))
	saveConfig(t, c, path)
	_, checks := CheckFile(path, checkNow)
	bad := failedChecks(checks)
	fields := map[string]bool{}
	for _, check := range bad {
		fields[check.Field] = true
	}
	for _, field := range []string{"credentials[0].players", "cert_file", "database.password_file", "database.ca_file"} {
		if !fields[field] {
			t.Fatal("independent failure missing", field, checks)
		}
	}
	encoded, err := json.Marshal(checks)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "secret-") || strings.Contains(string(encoded), filepath.Dir(path)) || strings.Contains(string(encoded), strings.Repeat("e", 64)) {
		t.Fatal("failed check printed a private value")
	}
}

func TestCheckFileCertificateValidation(t *testing.T) {
	for _, fault := range []string{"expired", "future", "client-only", "unknown-only-usage", "mismatched-key", "invalid-key", "invalid-chain", "expired-chain", "valid-any-usage", "valid-no-usage"} {
		t.Run(fault, func(t *testing.T) {
			c, path := offlineFixture(t)
			cert, key := certificateMaterial(t, func(cert *x509.Certificate) {
				switch fault {
				case "expired":
					cert.NotBefore, cert.NotAfter = checkNow.Add(-2*time.Hour), checkNow.Add(-time.Hour)
				case "future":
					cert.NotBefore, cert.NotAfter = checkNow.Add(time.Hour), checkNow.Add(2*time.Hour)
				case "client-only":
					cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
				case "unknown-only-usage":
					cert.ExtKeyUsage = nil
					cert.UnknownExtKeyUsage = []asn1.ObjectIdentifier{{1, 3, 6, 1, 5, 5, 7, 3, 123}}
				case "valid-any-usage":
					cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageAny}
				case "valid-no-usage":
					cert.ExtKeyUsage = nil
				}
			})
			if fault == "mismatched-key" {
				_, key = certificateMaterial(t, nil)
			}
			if fault == "invalid-key" {
				key = []byte("secret-invalid-key")
			}
			if fault == "invalid-chain" {
				cert = append(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("invalid DER")})...)
			}
			if fault == "expired-chain" {
				extra, _ := certificateMaterial(t, func(cert *x509.Certificate) {
					cert.NotBefore, cert.NotAfter = checkNow.Add(-2*time.Hour), checkNow.Add(-time.Hour)
				})
				cert = append(cert, extra...)
			}
			writeCheckFile(t, c.CertFile, cert)
			writeCheckFile(t, c.KeyFile, key)
			saveConfig(t, c, path)
			_, checks := CheckFile(path, checkNow)
			bad := failedChecks(checks)
			want := !strings.HasPrefix(fault, "valid-")
			if (len(bad) != 0) != want {
				t.Fatal("certificate validation mismatch", checks)
			}
		})
	}
}

func TestCheckFileCredentialFailuresIdentifyCredentialIndex(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		alter       func(*CredentialFile)
	}{
		{"version", "credentials_file.version", func(f *CredentialFile) { f.Version = 2 }},
		{"missing-credentials", "credentials", func(f *CredentialFile) { f.Credentials = nil }},
		{"invalid-principal", "credentials[0].principal", func(f *CredentialFile) { f.Credentials[0].Principal = "secret invalid principal" }},
		{"invalid-hash", "credentials[0].token_sha256", func(f *CredentialFile) { f.Credentials[0].TokenSHA256 = "secret invalid hash" }},
		{"unknown-scope", "credentials[0].scopes", func(f *CredentialFile) { f.Credentials[0].Scopes = []string{"secret-scope"} }},
		{"duplicate-scope", "credentials[0].scopes", func(f *CredentialFile) { f.Credentials[0].Scopes = []string{"read", "read"} }},
		{"missing-scope", "credentials[0].scopes", func(f *CredentialFile) { f.Credentials[0].Scopes = nil }},
		{"unknown-player", "credentials[0].players", func(f *CredentialFile) { f.Credentials[0].Players = []string{"secret-player"} }},
		{"duplicate-player", "credentials[0].players", func(f *CredentialFile) { f.Credentials[0].Players = []string{"room", "room"} }},
		{"missing-player", "credentials[0].players", func(f *CredentialFile) { f.Credentials[0].Players = nil }},
		{"duplicate-principal", "credentials[1].principal", func(f *CredentialFile) { f.Credentials = append(f.Credentials, f.Credentials[0]) }},
		{"duplicate-hash", "credentials[1].token_sha256", func(f *CredentialFile) {
			f.Credentials = append(f.Credentials, f.Credentials[0])
			f.Credentials[1].Principal = "reader-two"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, path := offlineFixture(t)
			f := CredentialFile{Version: 1, Credentials: []Credential{{Principal: "reader", TokenSHA256: strings.Repeat("e", 64), Scopes: []string{"read"}, Players: []string{"room"}}}}
			tc.alter(&f)
			b, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			writeCheckFile(t, c.CredentialsFile, b)
			saveConfig(t, c, path)
			_, checks := CheckFile(path, checkNow)
			bad := failedChecks(checks)
			if _, err := LoadCredentials(c.CredentialsFile, c.Players); err == nil || len(bad) != 1 || bad[0].Field != tc.field {
				t.Fatal("credential runtime/offline validation differs", checks, err)
			}
			encoded, err := json.Marshal(checks)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "secret") || strings.Contains(string(encoded), strings.Repeat("e", 64)) {
				t.Fatal("credential failure exposed a configured value")
			}
		})
	}
}

func TestCheckFileAllReferencedFilesAreBoundedRegularFiles(t *testing.T) {
	for _, field := range []string{"credentials_file", "cert_file", "key_file", "database.password_file", "database.ca_file"} {
		for _, fault := range []string{"missing", "directory", "fifo", "oversized", "symlink"} {
			t.Run(field+"/"+fault, func(t *testing.T) {
				c, path := offlineFixture(t)
				var target *string
				switch field {
				case "credentials_file":
					target = &c.CredentialsFile
				case "cert_file":
					target = &c.CertFile
				case "key_file":
					target = &c.KeyFile
				case "database.password_file":
					target = &c.Database.PasswordFile
				case "database.ca_file":
					target = &c.Database.CAFile
				}
				wantCode := "file_unreadable"
				switch fault {
				case "missing":
					*target += "-missing"
				case "directory":
					*target = filepath.Dir(path)
					wantCode = "file_not_regular"
				case "fifo":
					*target += "-fifo"
					if err := syscall.Mkfifo(*target, 0600); err != nil {
						t.Fatal(err)
					}
					wantCode = "file_not_regular"
				case "oversized":
					writeCheckFile(t, *target, []byte(strings.Repeat("x", (1<<20)+1)))
					wantCode = "file_too_large"
				case "symlink":
					link := *target + "-link"
					if err := os.Symlink(*target, link); err != nil {
						t.Fatal(err)
					}
					*target = link
				}
				saveConfig(t, c, path)
				_, checks := CheckFile(path, checkNow)
				bad := failedChecks(checks)
				if fault == "symlink" {
					if len(bad) != 0 {
						t.Fatal("projected Secret symlink was rejected", checks)
					}
				} else if len(bad) != 1 || bad[0].Field != field || bad[0].Code != wantCode {
					t.Fatal("referenced file boundary differs by file kind", checks, wantCode)
				}
			})
		}
	}
}

func TestCheckFileMatchesPasswordAndCABundleLoadingSemantics(t *testing.T) {
	c, path := offlineFixture(t)
	expiredCA, _ := certificateMaterial(t, func(cert *x509.Certificate) {
		cert.NotBefore, cert.NotAfter = checkNow.Add(-2*time.Hour), checkNow.Add(-time.Hour)
	})
	writeCheckFile(t, c.Database.CAFile, append([]byte("non-certificate text\n"), expiredCA...))
	writeCheckFile(t, c.Database.PasswordFile, []byte("\u2003secret-password\n"))
	c.Players, c.CredentialsFile = nil, ""
	saveConfig(t, c, path)
	_, checks := CheckFile(path, checkNow)
	if len(failedChecks(checks)) != 0 {
		t.Fatal("offline checker imposed trust policy beyond runtime PEM/password semantics", checks)
	}
}

func TestCheckFileRejectsStrictJSONAndNonRegularInputs(t *testing.T) {
	for _, fault := range []string{"unknown", "duplicate", "trailing", "oversized", "directory", "fifo", "missing", "symlink"} {
		t.Run(fault, func(t *testing.T) {
			c, path := offlineFixture(t)
			saveConfig(t, c, path)
			switch fault {
			case "unknown":
				writeCheckFile(t, path, []byte(`{"unknown-private-name":"secret-private-value"}`))
			case "duplicate":
				writeCheckFile(t, path, []byte(`{"listen":":8443","listen":":9443"}`))
			case "trailing":
				writeCheckFile(t, path, []byte(`{} {}`))
			case "oversized":
				writeCheckFile(t, path, []byte(strings.Repeat("x", (1<<20)+1)))
			case "directory":
				path = filepath.Dir(path)
			case "fifo":
				path = filepath.Join(filepath.Dir(path), "secret-fifo")
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "missing":
				path += "-missing"
			case "symlink":
				linked := path + "-link"
				if err := os.Symlink(path, linked); err != nil {
					t.Fatal(err)
				}
				path = linked
			}
			_, checks := CheckFile(path, checkNow)
			bad := failedChecks(checks)
			if (len(bad) != 0) != (fault != "symlink") {
				t.Fatal("unexpected file/schema acceptance", checks)
			}
			if len(bad) != 0 && bad[0].Field != "config" {
				t.Fatal("file failure did not identify config input", checks)
			}
		})
	}
}
