package journal

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func diagnosticPointer[T any](v T) *T { return &v }

func TestWithDiagnosticsRetainsOutcomeAndPartialEvidence(t *testing.T) {
	detected := time.Date(2026, 9, 16, 7, 0, 0, 123456000, time.FixedZone("local", 3*3600))
	d := Diagnostics{Reason: "unexpected_event", Rule: diagnosticPointer("queue_controls_changed"), Phase: "ramping", DetectedAt: &detected,
		Source: "event", ChangedFields: []string{"volume"}, Expected: &DiagnosticScalars{Volume: diagnosticPointer(10), Muted: diagnosticPointer(false)},
		Observed: &DiagnosticScalars{Volume: diagnosticPointer(13)}}
	raw, err := WithDiagnostics(json.RawMessage(`{"delivery":"not_sent","commands_confirmed":4,"custom":{"retained":true}}`), d)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Delivery    string          `json:"delivery"`
		Commands    int             `json:"commands_confirmed"`
		Custom      map[string]bool `json:"custom"`
		Diagnostics Diagnostics     `json:"diagnostics"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Delivery != "not_sent" || got.Commands != 4 || !got.Custom["retained"] || got.Diagnostics.Observed.Muted != nil || got.Diagnostics.Observed.State != nil {
		t.Fatalf("lost outcome or invented observation: %s", raw)
	}
	if got.Diagnostics.DetectedAt == nil || !got.Diagnostics.DetectedAt.Equal(detected) || !strings.Contains(string(raw), `"detected_at":"2026-09-16T04:00:00.123456Z"`) {
		t.Fatalf("timestamp not normalized: %s", raw)
	}
	if detected.Location().String() != "local" {
		t.Fatal("mutated input time")
	}
}

func TestWithDiagnosticsBoundsAndAllowlist(t *testing.T) {
	valid := func() Diagnostics { return Diagnostics{Reason: "completed", Source: "controller", Phase: "holding"} }
	for _, tt := range []struct {
		name   string
		mutate func(*Diagnostics)
	}{
		{"missing reason", func(d *Diagnostics) { d.Reason = "" }},
		{"free form reason", func(d *Diagnostics) { d.Reason = "failed at https://private.invalid" }},
		{"large reason", func(d *Diagnostics) { d.Reason = strings.Repeat("a", 129) }},
		{"invalid rule", func(d *Diagnostics) { d.Rule = diagnosticPointer("private/media/id") }},
		{"invalid phase", func(d *Diagnostics) { d.Phase = "private title" }},
		{"invalid source", func(d *Diagnostics) { d.Source = "device_url" }},
		{"unknown field", func(d *Diagnostics) { d.ChangedFields = []string{"serial"} }},
		{"duplicate field", func(d *Diagnostics) { d.ChangedFields = []string{"volume", "volume"} }},
		{"too many fields", func(d *Diagnostics) { d.ChangedFields = make([]string, 17) }},
		{"invalid state", func(d *Diagnostics) { d.Observed = &DiagnosticScalars{State: diagnosticPointer("private title")} }},
		{"invalid repeat", func(d *Diagnostics) { d.Expected = &DiagnosticScalars{Repeat: diagnosticPointer("private title")} }},
		{"unknown repeat is absent", func(d *Diagnostics) { d.Observed = &DiagnosticScalars{Repeat: diagnosticPointer("unknown")} }},
		{"negative volume", func(d *Diagnostics) { d.Observed = &DiagnosticScalars{Volume: diagnosticPointer(-1)} }},
		{"high volume", func(d *Diagnostics) { d.Expected = &DiagnosticScalars{Volume: diagnosticPointer(101)} }},
		{"invalid timestamp", func(d *Diagnostics) { d.DetectedAt = diagnosticPointer(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := valid()
			tt.mutate(&d)
			if _, err := WithDiagnostics([]byte(`{}`), d); !errors.Is(err, ErrInvalid) {
				t.Fatalf("unsafe diagnostics accepted: %v", err)
			}
		})
	}
	for _, raw := range []string{`[]`, `{"delivery":1,"delivery":2}`, strings.Repeat(" ", MaxJSONBytes) + `{}`, `{"huge":"` + strings.Repeat("x", MaxJSONBytes-20) + `"}`} {
		if _, err := WithDiagnostics([]byte(raw), valid()); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid or oversized outcome accepted: %v", err)
		}
	}
}

func TestWithDiagnosticsRecoveryDoesNotInventEvidence(t *testing.T) {
	raw, err := WithDiagnostics(nil, Diagnostics{Reason: "process_interrupted", Phase: "", Source: "recovery"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"rule":null`, `"detected_at":null`, `"changed_fields":[]`, `"phase":""`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("missing %s: %s", want, raw)
		}
	}
	for _, forbidden := range []string{`"expected"`, `"observed"`} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("invented scalar evidence: %s", raw)
		}
	}
}

func TestDiagnosticHandoffUpdate(t *testing.T) {
	now := time.Now().UTC()
	for _, tt := range []struct {
		kind          string
		uncertain     bool
		state         State
		reason, phase string
	}{
		{"stop", false, Released, "superseded", "superseded"},
		{"playback", true, Uncertain, "superseded", "superseded"},
		{"cancel", false, Running, "cancellation_requested", "cancelling"},
		{"cancel", true, Running, "cancellation_requested", "cancelling"},
	} {
		t.Run(tt.kind+"/"+string(tt.state), func(t *testing.T) {
			old := Operation{Phase: "ramping", Progress: []byte(`{"playback":{"level":7}}`), Outcome: []byte(`{"commands_confirmed":3,"delivery":"confirmed"}`)}
			u, err := handoffUpdate(old, Proposal{Kind: tt.kind, ReplaceUncertain: tt.uncertain}, now)
			if err != nil {
				t.Fatal(err)
			}
			var out struct {
				Commands    int         `json:"commands_confirmed"`
				Delivery    string      `json:"delivery"`
				Diagnostics Diagnostics `json:"diagnostics"`
			}
			if err := json.Unmarshal(u.Outcome, &out); err != nil {
				t.Fatal(err)
			}
			if u.State != tt.state || u.Phase != tt.phase || string(u.Progress) != string(old.Progress) || out.Commands != 3 || out.Diagnostics.Phase != "ramping" || out.Diagnostics.Reason != tt.reason || out.Diagnostics.DetectedAt == nil || !out.Diagnostics.DetectedAt.Equal(now) {
				t.Fatalf("invalid target handoff: %+v %s", u, u.Outcome)
			}
			if tt.uncertain && (u.ErrorCode != "command_interrupted" || out.Delivery != "unknown") {
				t.Fatalf("lost uncertainty: %+v %s", u, u.Outcome)
			}
		})
	}
}
