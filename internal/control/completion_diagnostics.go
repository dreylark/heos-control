package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

// A captured value preserves error identity and never changes a shared sentinel.
type diagnosticError struct {
	cause    error
	evidence journal.Diagnostics
}

func (e *diagnosticError) Error() string { return e.cause.Error() }
func (e *diagnosticError) Unwrap() error { return e.cause }

// Keep the same first-cause priority for logs and persisted diagnostics.
func completionCause(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errQueueTransitionTimeout) {
		return err
	}
	var detail *ownershipError
	if cause := context.Cause(ctx); errors.As(cause, &detail) {
		return cause
	}
	return err
}

func scalarChanges(expected, observed *journal.DiagnosticScalars) []string {
	fields := []string{}
	for _, field := range []struct {
		name    string
		changed bool
	}{
		{"state", scalarChanged(expected.State, observed.State)},
		{"volume", scalarChanged(expected.Volume, observed.Volume)},
		{"mute", scalarChanged(expected.Muted, observed.Muted)},
		{"repeat", scalarChanged(expected.Repeat, observed.Repeat)},
		{"shuffle", scalarChanged(expected.Shuffle, observed.Shuffle)},
	} {
		if field.changed {
			fields = append(fields, field.name)
		}
	}
	return fields
}

func scalarChanged[T comparable](expected, observed *T) bool {
	return expected != nil && observed != nil && *expected != *observed
}

func copyScalar[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func protocolString[T ~string](v T) *string {
	s := string(v)
	return &s
}

func snapshotScalars(s heos.Snapshot) *journal.DiagnosticScalars {
	v := &journal.DiagnosticScalars{Muted: copyScalar(s.Muted), Shuffle: copyScalar(&s.Shuffle), Grouped: copyScalar(&s.Grouped)}
	if s.State.Known() {
		v.State = protocolString(s.State)
	}
	if s.Repeat.Known() {
		v.Repeat = protocolString(s.Repeat)
	}
	if s.Volume != nil && *s.Volume >= 0 && *s.Volume <= 100 {
		v.Volume = copyScalar(s.Volume)
	}
	return v
}

// Events contain partial evidence. Never fill absent fields from a full snapshot.
func eventScalars(e heos.Event) (*journal.DiagnosticScalars, []string) {
	data := e.Data()
	v := &journal.DiagnosticScalars{}
	fields := []string{}
	switch data.Kind {
	case heos.EventState:
		if data.State != heos.PlayStateAbsent {
			v.State = protocolString(data.State)
			fields = append(fields, "state")
		}
	case heos.EventVolume:
		if data.HasVolume {
			v.Volume = &data.Volume
			fields = append(fields, "volume")
		}
		if data.HasMute {
			v.Muted = &data.Muted
			fields = append(fields, "mute")
		}
	case heos.EventRepeat:
		if data.Repeat != heos.RepeatAbsent {
			v.Repeat = protocolString(data.Repeat)
			fields = append(fields, "repeat")
		}
	case heos.EventShuffle:
		if data.HasShuffle {
			v.Shuffle = &data.Shuffle
			fields = append(fields, "shuffle")
		}
	case heos.EventQueue:
		fields = append(fields, "queue_items")
	case heos.EventGroups:
		fields = append(fields, "grouped")
	}
	if v.State == nil && v.Volume == nil && v.Muted == nil && v.Repeat == nil && v.Shuffle == nil {
		return nil, fields
	}
	return v, fields
}

func evidenceFor(err error) journal.Diagnostics {
	var captured *diagnosticError
	if errors.As(err, &captured) {
		return captured.evidence
	}
	var ownership *ownershipError
	if errors.As(err, &ownership) {
		d := ownership.evidence
		d.Reason = ownership.reason
		if d.Source == "" {
			d.Source = "observation"
		}
		return d
	}
	return journal.Diagnostics{Source: "controller"}
}

// Called under execution.mu when a decision is made, before later cleanup I/O.
func (r *execution) captureDecision(err error, rule string, at time.Time) error {
	if err == nil {
		return nil
	}
	d := evidenceFor(err)
	if d.DetectedAt != nil {
		return err
	} // Already captured by the original event.
	at = at.UTC()
	d.Phase, d.DetectedAt, d.Rule = r.phase, &at, &rule
	return &diagnosticError{cause: err, evidence: d}
}

func (c *Coordinator) completionDiagnostics(r *execution, err error, code string) journal.Diagnostics {
	cause := completionCause(r.ctx, err)
	d := evidenceFor(cause)
	if d.Reason == "" {
		var device *heos.DeviceError
		switch {
		case err == nil:
			d.Reason = "completed"
		case errors.As(cause, &device):
			d.Reason = device.Reason()
		case code == "device_unavailable" && errors.Is(err, context.DeadlineExceeded):
			d.Reason = "command_timeout"
			r.mu.Lock()
			if r.unconfirmed {
				d.Reason = "confirmation_timeout"
			}
			r.mu.Unlock()
		case code != "":
			d.Reason = code
		default:
			d.Reason = "device_unavailable"
		}
	}
	if d.DetectedAt == nil {
		at := c.clock.Now().UTC()
		d.DetectedAt, d.Phase = &at, r.op.Phase
	}
	return d
}

func (c *Coordinator) targetOutcome(op journal.Operation, update journal.Update, at time.Time) json.RawMessage {
	d := journal.Diagnostics{Reason: "handoff_failed", Source: "controller", Phase: op.Phase, DetectedAt: &at}
	var prior struct {
		Diagnostics *journal.Diagnostics `json:"diagnostics"`
	}
	if json.Unmarshal(op.Outcome, &prior) == nil && prior.Diagnostics != nil && prior.Diagnostics.Reason == "cancellation_requested" {
		d.Phase = prior.Diagnostics.Phase
	}
	if update.Phase == "cancelled" {
		d.Reason = "cancelled"
		if update.State != journal.Cancelled {
			d.Reason = "cancellation_failed"
		}
	}
	// Retain the target's own command evidence; the cancellation worker owns a
	// separate operation and supplies only its delivery / may-continue result.
	fields := map[string]json.RawMessage{}
	_ = json.Unmarshal(op.Outcome, &fields)
	if fields == nil {
		fields = map[string]json.RawMessage{}
	}
	var result map[string]json.RawMessage
	_ = json.Unmarshal(update.Outcome, &result)
	for key, value := range result {
		fields[key] = value
	}
	merged, _ := json.Marshal(fields)
	return c.annotateOutcome(merged, d)
}

func (c *Coordinator) annotateOutcome(outcome json.RawMessage, diagnostic journal.Diagnostics) json.RawMessage {
	encoded, err := journal.WithDiagnostics(outcome, diagnostic)
	if err == nil {
		return encoded
	}
	// Diagnostics must not prevent terminal persistence or change device policy.
	if c.logger != nil {
		c.logger.Error("invalid completion diagnostics", "error", err)
	}
	return outcome
}

func matchesUpdate(op journal.Operation, update journal.Update) bool {
	return op.State == update.State && op.Phase == update.Phase && op.ErrorCode == update.ErrorCode &&
		equalJSON(op.Progress, update.Progress) && equalJSON(op.Outcome, update.Outcome)
}

func equalJSON(a, b json.RawMessage) bool {
	decode := func(raw json.RawMessage) (any, error) {
		if len(raw) == 0 {
			raw = json.RawMessage(`{}`)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var value any
		err := decoder.Decode(&value)
		return value, err
	}
	left, e1 := decode(a)
	right, e2 := decode(b)
	return e1 == nil && e2 == nil && reflect.DeepEqual(left, right)
}
