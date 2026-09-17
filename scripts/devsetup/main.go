// devsetup creates disposable local credentials and TLS certificates. It never
// overwrites an existing environment; remove .local only after its DB is removed.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dreylark/heos-control/internal/config"
)

func main() {
	if err := setup(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func setup() error {
	if _, err := os.Stat(".local/initialized"); err == nil {
		if err := upgradeReads(); err != nil {
			return err
		}
		fmt.Println("Local environment already exists; credentials retained.")
		return nil
	}
	if err := os.Mkdir(".local", 0700); err != nil {
		return fmt.Errorf("create fresh .local directory: %w", err)
	}
	write := func(name string, data []byte) error { return os.WriteFile(filepath.Join(".local", name), data, 0644) }
	// Files are individually mounted into non-root containers. The host directory
	// is 0700; leaf files must be readable by the container's distinct UID.
	for _, name := range []string{"admin-password", "owner-password", "runtime-password"} {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			return err
		}
		if err := write(name, []byte(hex.EncodeToString(b))); err != nil {
			return err
		}
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "heos-control disposable development CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(1, 0, 0), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	if err = write("ca.crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})); err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost", "postgres", "heos-control"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(1, 0, 0), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	if err = write("server.crt", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})); err != nil {
		return err
	}
	if err = write("server.key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})); err != nil {
		return err
	}
	abs, err := filepath.Abs(".local")
	if err != nil {
		return err
	}
	for _, container := range []bool{false, true} {
		root, host, port, prefix := abs, "localhost", uint16(15432), ""
		if container {
			root, host, port, prefix = "/run/secrets", "postgres", 5432, "container-"
		}
		for _, test := range []bool{false, true} {
			db, dbPrefix := "heos_control", ""
			if test {
				db, dbPrefix = "heos_test", "test-"
			}
			for _, role := range []string{"owner", "runtime"} {
				c := config.Config{Listen: ":8443", CertFile: root + "/server.crt", KeyFile: root + "/server.key", ShutdownSeconds: 25, Database: config.Database{Host: host, Port: port, Name: db, User: "heos_" + role, PasswordFile: root + "/" + role + "-password", CAFile: root + "/ca.crt", MaxConnections: 4, TimeoutSeconds: 5, LockTimeoutSeconds: 2}}
				b, err := json.MarshalIndent(c, "", "  ")
				if err != nil {
					return err
				}
				if err = write(prefix+dbPrefix+role+".json", b); err != nil {
					return err
				}
			}
		}
	}
	if err = write("initialized", []byte("disposable development environment\n")); err != nil {
		return err
	}
	if err = upgradeReads(); err != nil {
		return err
	}
	fmt.Println("Created disposable credentials and certificates under ignored .local/.")
	return nil
}

// Add read-only mounts to older local environments without rotating secrets or
// overwriting operator-supplied identities or credentials.
func upgradeReads() error {
	if _, e := os.Stat(".local/credentials.json"); os.IsNotExist(e) {
		if e = os.WriteFile(".local/credentials.json", []byte("{\"version\":1,\"credentials\":[]}\n"), 0644); e != nil {
			return e
		}
	}
	paths, e := filepath.Glob(".local/*runtime.json")
	if e != nil {
		return e
	}
	root, e := filepath.Abs(".")
	if e != nil {
		return e
	}
	for _, path := range paths {
		c, e := config.Load(path)
		if e != nil {
			return e
		}
		changed := false
		if c.CredentialsFile == "" {
			c.CredentialsFile = filepath.Join(root, ".local/credentials.json")
			if strings.HasPrefix(filepath.Base(path), "container-") {
				c.CredentialsFile = "/run/secrets/api-credentials"
			}
			changed = true
		}
		if changed {
			b, e := json.MarshalIndent(c, "", "  ")
			if e != nil {
				return e
			}
			if e = os.WriteFile(path, b, 0644); e != nil {
				return e
			}
		}
	}
	return nil
}
