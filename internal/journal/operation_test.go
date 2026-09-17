package journal

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestRequestFingerprint(t *testing.T) {
	r := Request{Principal: "scheduler", Method: "POST", Endpoint: "/v1/players/test/alarms", Key: "morning", Player: "test", IfMatch: "epoch:1",
		Body: json.RawMessage(`{"preset":"morning","options":{"shuffle":true,"tracks":[1,2]}}`)}
	want, err := r.fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	r.Body = json.RawMessage(`{ "options": {"tracks":[1,2],"shuffle":true}, "preset":"morning" }`)
	got, err := r.fingerprint()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("key order and whitespace changed identity", err)
	}
	r.IfMatch = "epoch:2"
	got, err = r.fingerprint()
	if err != nil || bytes.Equal(got, want) {
		t.Fatal("precondition missing from fingerprint", err)
	}
	r.IfMatch = "epoch:1"
	r.Player = "other"
	got, err = r.fingerprint()
	if err != nil || bytes.Equal(got, want) {
		t.Fatal("player missing from fingerprint", err)
	}
	for _, raw := range []string{`null`, `[]`, `{"a":1,"a":2}`, `{"nested":{"a":1,"a":2}}`, `{} {}`, `{"a":`, `{"x":"\u0000"}`, `{"\u0000":1}`, "{\"x\":\"\xff\"}", strings.Repeat("[", 40) + strings.Repeat("]", 40), `{"x":"` + strings.Repeat("a", MaxJSONBytes) + `"}`} {
		t.Run(raw[:min(len(raw), 40)], func(t *testing.T) {
			if _, err := canonicalObject([]byte(raw)); err == nil {
				t.Fatal("accepted invalid/ambiguous input")
			}
		})
	}
	a, err := canonicalObject([]byte(`{"n":9007199254740992}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicalObject([]byte(`{"n":9007199254740993}`))
	if err != nil || bytes.Equal(a, b) {
		t.Fatal("integer precision was lost", err)
	}
}
