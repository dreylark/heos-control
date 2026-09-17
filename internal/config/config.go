// Package config loads non-secret configuration and referenced secret files.
package config

import (
	"net"
	"net/netip"
	"strconv"
	"strings"
	"unicode"
)

type Config struct {
	LogLevel        string   `json:"log_level,omitempty"`
	Listen          string   `json:"listen"`
	CertFile        string   `json:"cert_file"`
	KeyFile         string   `json:"key_file"`
	ShutdownSeconds int      `json:"shutdown_seconds"`
	Database        Database `json:"database"`
	CredentialsFile string   `json:"credentials_file,omitempty"`
	DocsEnabled     bool     `json:"docs_enabled,omitempty"`
	Players         []Player `json:"players,omitempty"`
	Sources         []Source `json:"sources,omitempty"`
}

type Database struct {
	Host               string `json:"host"`
	Port               uint16 `json:"port"`
	Name               string `json:"name"`
	User               string `json:"user"`
	PasswordFile       string `json:"password_file"`
	CAFile             string `json:"ca_file"`
	MaxConnections     int32  `json:"max_connections"`
	TimeoutSeconds     int    `json:"timeout_seconds"`
	LockTimeoutSeconds int    `json:"lock_timeout_seconds"`
}

func Load(path string) (Config, error) {
	c := Config{LogLevel: "info", Listen: ":8443", ShutdownSeconds: 25, Database: Database{Port: 5432, MaxConnections: 4, TimeoutSeconds: 5, LockTimeoutSeconds: 2}}
	if err := DecodeFile(path, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	switch c.LogLevel {
	case "", "debug", "info", "warn", "error":
	default:
		return invalidField("log_level", "invalid_value", "log_level must be debug, info, warn or error")
	}
	_, port, err := net.SplitHostPort(c.Listen)
	p, pe := strconv.Atoi(port)
	if err != nil || pe != nil || p < 1024 || p > 65535 {
		return invalidField("listen", "invalid_value", "listen must use a numeric unprivileged TCP port")
	}
	if c.CertFile == "" {
		return invalidField("cert_file", "required", "API certificate and key files are required")
	}
	if c.KeyFile == "" {
		return invalidField("key_file", "required", "API certificate and key files are required")
	}
	if c.ShutdownSeconds < 1 || c.ShutdownSeconds > 25 {
		return invalidField("shutdown_seconds", "invalid_value", "shutdown_seconds must be between 1 and 25")
	}
	d := c.Database
	for _, field := range []struct {
		name    string
		missing bool
	}{{"host", d.Host == ""}, {"port", d.Port == 0}, {"name", d.Name == ""}, {"user", d.User == ""}, {"password_file", d.PasswordFile == ""}, {"ca_file", d.CAFile == ""}} {
		if field.missing {
			return invalidField("database."+field.name, "required", "database host, port, name, user, password_file and ca_file are required")
		}
	}
	if !databaseTCPHost(d.Host) {
		return invalidField("database.host", "invalid_value", "database host must be one TCP hostname or IP address without a scheme, path or port")
	}
	if d.MaxConnections < 1 || d.MaxConnections > 16 {
		return invalidField("database.max_connections", "invalid_value", "database max_connections must be between 1 and 16")
	}
	if d.TimeoutSeconds < 1 || d.TimeoutSeconds > 30 {
		return invalidField("database.timeout_seconds", "invalid_value", "database timeout_seconds must be 1..30 and lock_timeout_seconds 1..timeout_seconds")
	}
	if d.LockTimeoutSeconds < 1 || d.LockTimeoutSeconds > d.TimeoutSeconds {
		return invalidField("database.lock_timeout_seconds", "invalid_value", "database timeout_seconds must be 1..30 and lock_timeout_seconds 1..timeout_seconds")
	}
	return c.validateReadOnly()
}

// pgx treats a filesystem host as a Unix socket and disables TLS, even with
// sslmode=verify-full. Reject non-TCP forms without resolving names or imposing
// a separate DNS policy (for example, private hostnames may use underscores).
func databaseTCPHost(host string) bool {
	if host == "" || strings.ContainsAny(host, `/\@?#,[]`) || strings.ContainsFunc(host, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return false
	}
	if strings.Contains(host, ":") {
		_, err := netip.ParseAddr(host)
		return err == nil
	}
	return true
}
