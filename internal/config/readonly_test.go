package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteOptInRequiresVerifiedCeilingAndRecordedSecurePort(t *testing.T) {
	c := Config{CredentialsFile: "mounted", Players: []Player{{Key: "room", Address: "speaker.invalid:1265", Serial: "serial", FingerprintSHA256: strings.Repeat("a", 64)}}}
	if e := c.validateReadOnly(); e != nil {
		t.Fatal("recorded Secure CLI port rejected", e)
	}
	c.Players[0].WritesEnabled = true
	if e := c.validateReadOnly(); e == nil {
		t.Fatal("writes enabled without ceiling")
	}
	ceiling := 40
	c.Players[0].VolumeCeiling = &ceiling
	if e := c.validateReadOnly(); e != nil {
		t.Fatal(e)
	}
	for _, port := range []string{"1255", "1256"} {
		c.Players[0].Address = "speaker.invalid:" + port
		if e := c.validateReadOnly(); e == nil {
			t.Fatal("unapproved transport port accepted", port)
		}
	}
}

func TestCredentialFileRejectsAmbiguousOrBroadAccess(t *testing.T) {
	valid := `{"version":1,"credentials":[{"principal":"reader","token_sha256":"` + strings.Repeat("a", 64) + `","scopes":["read"],"players":["room"]}]}`
	for _, body := range []string{valid, strings.Replace(valid, `"read"`, `"admin"`, 1), strings.Replace(valid, `"room"`, `"*"`, 1), strings.Replace(valid, `"version":1`, `"version":2`, 1), strings.Replace(valid, `"version":1`, `"version":1,"extra":true`, 1), strings.Replace(valid, `"version":1`, `"version":1,"version":1`, 1)} {
		path := filepath.Join(t.TempDir(), "credentials.json")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadCredentials(path, []Player{{Key: "room"}})
		if (err == nil) != (body == valid) {
			t.Fatalf("credential acceptance mismatch: %v", err)
		}
	}
}

func TestSourcesCannotAliasReferenceNamespace(t *testing.T) {
	c := Config{CredentialsFile: "mounted", Players: []Player{{Key: "room", Address: "speaker.invalid:1265", Serial: "serial", FingerprintSHA256: strings.Repeat("a", 64)}}, Sources: []Source{{Key: "a", Player: "room", Name: "Gerbera"}, {Key: "b", Player: "room", Name: "Gerbera"}}}
	if c.validateReadOnly() == nil {
		t.Fatal("logical sources alias the same HEOS reference namespace")
	}
}
