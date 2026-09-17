package journal

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
)

func poolTestDatabase(t *testing.T) (config.Database, []byte) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, public, private)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	dir := t.TempDir()
	d := config.Database{Host: "database.invalid", Port: 5432, User: "runtime", Name: "heos_test", PasswordFile: filepath.Join(dir, "password"),
		CAFile: filepath.Join(dir, "ca.crt"), MaxConnections: 4, TimeoutSeconds: 5, LockTimeoutSeconds: 1}
	for path, content := range map[string][]byte{d.PasswordFile: []byte("  test-password\n"), d.CAFile: caPEM} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return d, caPEM
}

func TestPoolConfigRejectsUnixSockets(t *testing.T) {
	for _, host := range []string{"/tmp", "database.invalid,/tmp"} {
		t.Run(host, func(t *testing.T) {
			d, _ := poolTestDatabase(t)
			d.Host = host
			if _, err := poolConfig(d); err == nil {
				t.Fatal("Unix socket route bypassed required TLS")
			}
		})
	}
}

func TestPoolConfigVerifiesEveryTLSRoute(t *testing.T) {
	d, caPEM := poolTestDatabase(t)
	// Public configuration allows one host. Exercise pgx's fallback shape here
	// as a defense against accidentally relaxing that boundary in the future.
	d.Host = "database.invalid,fallback.invalid"
	c, err := poolConfig(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.ConnConfig.Fallbacks) != 1 {
		t.Fatal("test did not exercise a fallback")
	}
	expected := x509.NewCertPool()
	if !expected.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid test certificate")
	}
	for _, fallback := range c.ConnConfig.Fallbacks {
		cfg := fallback.TLSConfig
		if cfg == nil || cfg.InsecureSkipVerify || cfg.ServerName != fallback.Host || cfg.RootCAs == nil || !cfg.RootCAs.Equal(expected) {
			t.Fatal("fallback lost explicit roots or hostname verification")
		}
	}
}

func TestPoolConfigExplicitVerifiedTLS(t *testing.T) {
	d, caPEM := poolTestDatabase(t)
	// The configured CA and verify-full policy win over environment defaults.
	// Client certificates are not part of the service's password-auth contract.
	t.Setenv("PGSSLMODE", "disable")
	t.Setenv("PGSSLROOTCERT", filepath.Join(t.TempDir(), "unexpected-root.crt"))
	t.Setenv("PGSSLCERT", filepath.Join(t.TempDir(), "unexpected-client.crt"))
	t.Setenv("PGSSLKEY", filepath.Join(t.TempDir(), "unexpected-client.key"))
	t.Setenv("PGHOST", "/tmp")
	c, err := poolConfig(d)
	if err != nil {
		t.Fatal(err)
	}
	expected := x509.NewCertPool()
	if !expected.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid test certificate")
	}
	configs := []*tls.Config{c.ConnConfig.TLSConfig}
	for _, fallback := range c.ConnConfig.Fallbacks {
		configs = append(configs, fallback.TLSConfig)
	}
	for _, cfg := range configs {
		if cfg == nil || cfg.InsecureSkipVerify || cfg.ServerName != d.Host || cfg.RootCAs == nil || !cfg.RootCAs.Equal(expected) || len(cfg.Certificates) != 0 {
			t.Fatal("connection configuration lost explicit roots or hostname verification")
		}
	}
	if c.ConnConfig.Password != "test-password" || c.ConnConfig.Host != d.Host {
		t.Fatal("database credentials or host changed")
	}
}

func TestPoolConfigBoundedSecretReads(t *testing.T) {
	for _, field := range []string{"password", "ca"} {
		for _, replaceLink := range []bool{false, true} {
			name := field + "/fifo"
			if replaceLink {
				name = field + "/replaced-secret-link"
			}
			t.Run(name, func(t *testing.T) {
				d, caPEM := poolTestDatabase(t)
				fifo := filepath.Join(t.TempDir(), "fifo")
				if err := syscall.Mkfifo(fifo, 0o600); err != nil {
					t.Fatal(err)
				}
				path := &d.PasswordFile
				content := []byte("test-password")
				if field == "ca" {
					path, content = &d.CAFile, caPEM
				}
				if replaceLink {
					link := filepath.Join(t.TempDir(), "secret")
					if err := os.Symlink(*path, link); err != nil {
						t.Fatal(err)
					}
					*path = link
					if _, err := poolConfig(d); err != nil {
						t.Fatalf("regular Secret symlink rejected: %v", err)
					}
					if err := os.Symlink(fifo, link+".replacement"); err != nil {
						t.Fatal(err)
					}
					if err := os.Rename(link+".replacement", link); err != nil {
						t.Fatal(err)
					}
				} else {
					*path = fifo
				}
				result := make(chan error, 1)
				go func() {
					_, err := poolConfig(d)
					result <- err
				}()
				select {
				case err := <-result:
					if err == nil {
						t.Fatal("non-regular secret accepted")
					}
				case <-time.After(500 * time.Millisecond):
					// Unblock a regressed implementation so the failing test does not
					// leave behind a goroutine stuck in a blocking FIFO open/read.
					writer, err := os.OpenFile(fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := writer.Write(content); err != nil {
						_ = writer.Close()
						t.Fatal(err)
					}
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
					select {
					case <-result:
					case <-time.After(time.Second):
						t.Fatal("blocked secret read did not finish after FIFO cleanup")
					}
					t.Fatal("secret read blocked on FIFO")
				}
			})
		}
	}
}
