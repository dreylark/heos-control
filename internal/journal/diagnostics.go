package journal

import (
	"encoding/json"
	"time"
)

const maxDiagnosticBytes = 4 * 1024

// Diagnostics records bounded, public evidence for an operation's completion.
// Identifiers describe policy, never private device identity or protocol text.
type Diagnostics struct {
	Reason        string             `json:"reason"`
	Rule          *string            `json:"rule"`
	Phase         string             `json:"phase"`
	DetectedAt    *time.Time         `json:"detected_at"`
	Source        string             `json:"source"`
	ChangedFields []string           `json:"changed_fields"`
	Expected      *DiagnosticScalars `json:"expected,omitempty"`
	Observed      *DiagnosticScalars `json:"observed,omitempty"`
}

// DiagnosticScalars deliberately cannot contain media IDs, URLs or raw replies.
// Nil means evidence was absent, rather than that a requested value was observed.
type DiagnosticScalars struct {
	State   *string `json:"state,omitempty"`
	Volume  *int    `json:"volume,omitempty"`
	Muted   *bool   `json:"muted,omitempty"`
	Repeat  *string `json:"repeat,omitempty"`
	Shuffle *bool   `json:"shuffle,omitempty"`
	Grouped *bool   `json:"grouped,omitempty"`
}

// WithDiagnostics retains the outcome's other fields and enforces the same JSON
// boundary as journal writes. It does not mutate caller-owned diagnostic data.
func WithDiagnostics(outcome json.RawMessage, d Diagnostics) (json.RawMessage, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	if d.ChangedFields == nil {
		d.ChangedFields = []string{}
	}
	if d.DetectedAt != nil {
		normalized := d.DetectedAt.UTC()
		d.DetectedAt = &normalized
	}
	diagnostic, err := json.Marshal(d)
	if err != nil || len(diagnostic) > maxDiagnosticBytes {
		return nil, ErrInvalid
	}
	if outcome == nil {
		outcome = json.RawMessage(`{}`)
	}
	canonical, err := canonicalObject(outcome)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &fields); err != nil {
		return nil, ErrInvalid
	}
	fields["diagnostics"] = diagnostic
	result, err := json.Marshal(fields)
	if err != nil || len(result) > MaxJSONBytes {
		return nil, ErrInvalid
	}
	return result, nil
}

func (d Diagnostics) validate() error {
	if !diagnosticIdentifier(d.Reason, false) || !diagnosticIdentifier(d.Phase, true) ||
		(d.Rule != nil && !diagnosticIdentifier(*d.Rule, false)) || len(d.ChangedFields) > 16 {
		return ErrInvalid
	}
	switch d.Source {
	case "event", "observation", "controller", "recovery":
	default:
		return ErrInvalid
	}
	seen := make(map[string]bool, len(d.ChangedFields))
	for _, field := range d.ChangedFields {
		if seen[field] {
			return ErrInvalid
		}
		seen[field] = true
		switch field {
		case "repeat", "shuffle", "player_identity", "grouped", "state", "volume", "mute",
			"queue_items", "queue_total", "queue_next", "media_presence", "media_source", "media_id",
			"media_queue_id", "media_metadata", "queue_membership", "media", "connection_generation":
		default:
			return ErrInvalid
		}
	}
	for _, s := range []*DiagnosticScalars{d.Expected, d.Observed} {
		if s == nil {
			continue
		}
		if s.Volume != nil && (*s.Volume < 0 || *s.Volume > 100) {
			return ErrInvalid
		}
		if s.State != nil {
			switch *s.State {
			case "unknown", "play", "pause", "stop":
			default:
				return ErrInvalid
			}
		}
		if s.Repeat != nil {
			switch *s.Repeat {
			case "off", "on_all", "on_one":
			default:
				return ErrInvalid
			}
		}
	}
	return nil
}

func diagnosticIdentifier(s string, empty bool) bool {
	if len(s) > 128 || (s == "" && !empty) {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}
