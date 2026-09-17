package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"strings"
	"time"
)

// Check is a value-free offline validation result suitable for machine output.
type Check struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
	OK      bool   `json:"ok"`
}

// CheckFile checks the runtime schema and referenced local files without DNS,
// network connections, listeners, migrations or device commands. A valid local
// key pair does not establish certificate hostname matching or remote trust.
func CheckFile(path string, now time.Time) (Config, []Check) {
	c, err := Load(path)
	if err != nil {
		return c, []Check{checkFailure("config", err)}
	}
	checks := []Check{checkSuccess("config", "Configuration schema and local safety policies are valid.")}
	if _, err := LoadCredentials(c.CredentialsFile, c.Players); err != nil {
		checks = append(checks, checkFailure("credentials_file", err))
	} else if c.CredentialsFile == "" {
		checks = append(checks, checkSuccess("credentials_file", "No credential file is configured; authenticated API access is disabled."))
	} else {
		checks = append(checks, checkSuccess("credentials_file", "Credential schema, scopes and player references are valid."))
	}
	checks = append(checks, checkAPITLS(c.CertFile, c.KeyFile, now)...)
	password, err := ReadReferencedFile(c.Database.PasswordFile)
	if err != nil {
		checks = append(checks, checkFailure("database.password_file", err))
	} else if strings.TrimSpace(string(password)) == "" {
		checks = append(checks, Check{Field: "database.password_file", Code: "password_empty", Message: "Database password file is empty after trimming whitespace."})
	} else {
		checks = append(checks, checkSuccess("database.password_file", "Database password file contains a non-empty value."))
	}
	ca, err := ReadReferencedFile(c.Database.CAFile)
	if err != nil {
		checks = append(checks, checkFailure("database.ca_file", err))
	} else if !x509.NewCertPool().AppendCertsFromPEM(ca) {
		checks = append(checks, Check{Field: "database.ca_file", Code: "ca_invalid", Message: "Database CA file must contain at least one parseable PEM certificate."})
	} else {
		checks = append(checks, checkSuccess("database.ca_file", "Database CA certificates can be loaded; remote identity and trust are not checked offline."))
	}
	return c, checks
}

func checkSuccess(field, message string) Check {
	return Check{Field: field, Code: "ok", Message: message, OK: true}
}

func checkFailure(field string, err error) Check {
	out := Check{Field: field, Code: "invalid_configuration", Message: "Configuration validation failed."}
	var detail *fieldError
	if errors.As(err, &detail) {
		if detail.field != "" {
			out.Field = detail.field
			if field == "credentials_file" && detail.field == "version" {
				out.Field = "credentials_file.version"
			}
		}
		out.Code, out.Message = detail.code, detail.message
	}
	return out
}

func checkAPITLS(certFile, keyFile string, now time.Time) []Check {
	certPEM, certErr := ReadReferencedFile(certFile)
	keyPEM, keyErr := ReadReferencedFile(keyFile)
	var failures []Check
	if certErr != nil {
		failures = append(failures, checkFailure("cert_file", certErr))
	}
	if keyErr != nil {
		failures = append(failures, checkFailure("key_file", keyErr))
	}
	if len(failures) > 0 {
		return failures
	}
	chain, err := parseCertificateChain(certPEM)
	if err != nil {
		return []Check{{Field: "cert_file", Code: "certificate_invalid", Message: "API certificate file must contain a parseable PEM certificate chain."}}
	}
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		failures = append(failures, Check{Field: "key_file", Code: "key_pair_invalid", Message: "API private key must be readable PEM and match the leaf certificate."})
	}
	for _, cert := range chain {
		if now.Before(cert.NotBefore) {
			failures = append(failures, Check{Field: "cert_file", Code: "certificate_not_yet_valid", Message: "API certificate chain contains a certificate that is not yet valid."})
			break
		}
		if now.After(cert.NotAfter) {
			failures = append(failures, Check{Field: "cert_file", Code: "certificate_expired", Message: "API certificate chain contains an expired certificate."})
			break
		}
	}
	if !allowsServerAuth(chain[0]) {
		failures = append(failures, Check{Field: "cert_file", Code: "certificate_usage", Message: "API leaf certificate must permit TLS server authentication."})
	}
	if len(failures) > 0 {
		return failures
	}
	return []Check{checkSuccess("api.tls", "API certificate and key match, validity and server-auth usage pass; hostname and trust are not checked offline.")}
}

func parseCertificateChain(data []byte) ([]*x509.Certificate, error) {
	var chain []*x509.Certificate
	for {
		block, rest := pem.Decode(data)
		if block == nil {
			break
		}
		data = rest
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		chain = append(chain, cert)
	}
	if len(chain) == 0 {
		return nil, errors.New("certificate chain is empty")
	}
	return chain, nil
}

func allowsServerAuth(cert *x509.Certificate) bool {
	if len(cert.ExtKeyUsage) == 0 && len(cert.UnknownExtKeyUsage) == 0 {
		return true
	}
	for _, usage := range cert.ExtKeyUsage {
		if usage == x509.ExtKeyUsageAny || usage == x509.ExtKeyUsageServerAuth {
			return true
		}
	}
	return false
}
