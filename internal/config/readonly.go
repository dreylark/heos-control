package config

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"unicode/utf8"
)

type Player struct {
	WritesEnabled     bool   `json:"writes_enabled"`
	Key               string `json:"key"`
	Address           string `json:"address"`
	FingerprintSHA256 string `json:"fingerprint_sha256"`
	Serial            string `json:"serial"`
	Model             string `json:"model"`
	VolumeCeiling     *int   `json:"volume_ceiling"`
}
type Source struct {
	Key    string `json:"key"`
	Player string `json:"player"`
	Name   string `json:"name"`
	// UDN is operator evidence; HEOS 1.17 does not expose it in browse responses.
	UDN string `json:"udn,omitempty"`
}
type Credential struct {
	Principal   string   `json:"principal"`
	TokenSHA256 string   `json:"token_sha256"`
	Scopes      []string `json:"scopes"`
	Players     []string `json:"players"`
}
type CredentialFile struct {
	Version     int          `json:"version"`
	Credentials []Credential `json:"credentials"`
}

var keyPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

func KeyValid(s string) bool { return keyPattern.MatchString(s) }
func validDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && s == strings.ToLower(s)
}
func (c Config) validateReadOnly() error {
	if len(c.Players) > 16 {
		return invalidField("players", "capacity_exceeded", "read-only configuration exceeds capacity")
	}
	if len(c.Sources) > 16 {
		return invalidField("sources", "capacity_exceeded", "read-only configuration exceeds capacity")
	}
	if len(c.Players) > 0 && c.CredentialsFile == "" {
		return invalidField("credentials_file", "required", "configured players require credentials_file")
	}
	keys, serials, addresses := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for i, p := range c.Players {
		field := fmt.Sprintf("players[%d].", i)
		if p.WritesEnabled && p.VolumeCeiling == nil {
			return invalidField(field+"volume_ceiling", "required", "writes_enabled requires a verified volume_ceiling")
		}
		host, port, e := net.SplitHostPort(p.Address)
		for _, check := range []struct {
			name string
			bad  bool
		}{{"key", !KeyValid(p.Key) || keys[p.Key]}, {"serial", p.Serial == "" || len(p.Serial) > 256 || serials[p.Serial]},
			{"address", addresses[p.Address] || e != nil || host == "" || port != "1265"}, {"fingerprint_sha256", !validDigest(p.FingerprintSHA256)}} {
			if check.bad {
				return invalidField(field+check.name, "invalid_value", "invalid or duplicate player identity/address/pin; pinned TLS port 1265 required")
			}
		}
		if p.VolumeCeiling != nil && (*p.VolumeCeiling < 0 || *p.VolumeCeiling > 100) {
			return invalidField(field+"volume_ceiling", "invalid_value", "volume_ceiling must be 0..100")
		}
		keys[p.Key], serials[p.Serial], addresses[p.Address] = true, true, true
	}
	sources := map[string]bool{}
	routes := map[[2]string]bool{}
	for i, s := range c.Sources {
		field := fmt.Sprintf("sources[%d].", i)
		route := [2]string{s.Player, s.Name}
		if !KeyValid(s.Key) || sources[s.Key] {
			return invalidField(field+"key", "invalid_value", "invalid source configuration")
		}
		if !keys[s.Player] {
			return invalidField(field+"player", "unknown_player", "invalid source configuration")
		}
		if routes[route] || s.Name == "" || len(s.Name) > 256 {
			return invalidField(field+"name", "invalid_value", "invalid source configuration")
		}
		sources[s.Key] = true
		routes[route] = true
	}
	return nil
}

// DecodeFile rejects unknown and duplicate keys, trailing values and oversized files.
func DecodeFile(path string, out any) error {
	b, e := ReadReferencedFile(path)
	if e != nil {
		return e
	}
	if e = UniqueJSON(b); e != nil {
		return invalidField("", "invalid_json", "invalid configuration JSON")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e = d.Decode(out); e != nil {
		return invalidField("", "invalid_json", "invalid configuration JSON")
	}
	return nil
}
func UniqueJSON(b []byte) error {
	if !utf8.Valid(b) {
		return errors.New("invalid JSON encoding")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 32 {
			return errors.New("JSON nesting exceeds bound")
		}
		tok, e := d.Token()
		if e != nil {
			return e
		}
		switch tok {
		case json.Delim('{'):
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return errors.New("duplicate JSON key")
				}
				seen[s] = true
				if e = walk(depth + 1); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		case json.Delim('['):
			for d.More() {
				if e = walk(depth + 1); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		default:
			if _, ok := tok.(json.Delim); ok {
				return errors.New("invalid JSON")
			}
			return nil
		}
	}
	if e := walk(0); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func LoadCredentials(path string, players []Player) ([]Credential, error) {
	if path == "" {
		return []Credential{}, nil
	}
	var f CredentialFile
	if e := DecodeFile(path, &f); e != nil {
		return nil, e
	}
	if f.Version != 1 {
		return nil, invalidField("version", "unsupported_version", "invalid credential file version/capacity")
	}
	if f.Credentials == nil || len(f.Credentials) > 64 {
		return nil, invalidField("credentials", "capacity_exceeded", "invalid credential file version/capacity")
	}
	allowed := map[string]bool{}
	for _, p := range players {
		allowed[p.Key] = true
	}
	principals, tokens := map[string]bool{}, map[string]bool{}
	for i, c := range f.Credentials {
		field := fmt.Sprintf("credentials[%d].", i)
		for _, check := range []struct {
			name string
			bad  bool
		}{{"principal", !KeyValid(c.Principal) || principals[c.Principal]}, {"token_sha256", !validDigest(c.TokenSHA256) || tokens[c.TokenSHA256]},
			{"scopes", len(c.Scopes) == 0 || len(c.Scopes) > 4}, {"players", len(c.Players) == 0 || len(c.Players) > 16}} {
			if check.bad {
				return nil, invalidField(field+check.name, "invalid_value", "invalid or duplicate credential")
			}
		}
		seen := map[string]bool{}
		for _, s := range c.Scopes {
			if seen[s] || (s != "read" && s != "control" && s != "alarm" && s != "operator") {
				return nil, invalidField(field+"scopes", "invalid_scope", "invalid credential scope")
			}
			seen[s] = true
		}
		seen = map[string]bool{}
		for _, p := range c.Players {
			if !allowed[p] || seen[p] {
				return nil, invalidField(field+"players", "unknown_player", "invalid credential player")
			}
			seen[p] = true
		}
		principals[c.Principal], tokens[c.TokenSHA256] = true, true
	}
	return f.Credentials, nil
}
