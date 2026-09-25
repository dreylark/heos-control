package journal

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	Retention    = 7 * 24 * time.Hour
	MaxJSONBytes = 16 * 1024
	MaxBatch     = 100
)

var (
	ErrInvalid         = errors.New("invalid journal input")
	ErrNotFound        = errors.New("operation not found")
	ErrConflict        = errors.New("idempotency conflict")
	ErrBusy            = errors.New("device reserved")
	ErrCapacity        = errors.New("journal capacity reached")
	ErrExpired         = errors.New("admission window expired")
	ErrFuture          = errors.New("admission window has not started")
	ErrRevision        = errors.New("operation revision or state conflict")
	ErrStaleEpoch      = errors.New("journal belongs to another controller epoch")
	ErrNotInitialized  = errors.New("journal recovery incomplete")
	ErrCommitUncertain = errors.New("commit outcome uncertain; resolve the original request")
)

type State string

const (
	Accepted    State = "accepted"
	Running     State = "running"
	Succeeded   State = "succeeded"
	Failed      State = "failed"
	Cancelled   State = "cancelled"
	Released    State = "released"
	Interrupted State = "interrupted"
	Uncertain   State = "uncertain"
)

func (s State) terminal() bool {
	switch s {
	case Succeeded, Failed, Cancelled, Released, Interrupted, Uncertain:
		return true
	}
	return false
}

// Request contains authenticated caller identity and schema-validated caller
// input, never resolved configuration defaults. Endpoint includes canonical path IDs.
// Authorize the principal/player before Lookup or Admit, including replays.
type Request struct {
	Principal string
	Method    string
	Endpoint  string
	Key       string
	Player    string
	IfMatch   string
	Body      json.RawMessage
}

// Proposal is constructed after Lookup misses, with device observations and
// media resolution outside the transaction. NotBefore/NotAfter come from the
// validated caller window. It contains no passwords, tokens or TLS private keys.
type Proposal struct {
	// OwnerID admits a Skip command for the active playback worker. The parent
	// keeps the sole device reservation; this child has its own durable request
	// identity and terminal result. It cannot be combined with a handoff.
	OwnerID string
	// Replace is an optimistic atomic handoff, after the old worker has joined.
	ReplaceUncertain   bool
	ReplaceID          string
	ReplaceRevision    int64
	Kind               string
	DeviceKey          string
	ConfigRevision     string
	EffectiveArguments json.RawMessage
	NotBefore          time.Time
	NotAfter           time.Time
}

type Operation struct {
	ID                 string
	Kind               string
	Player             string
	DeviceKey          string
	Principal          string
	Epoch              string
	State              State
	Revision           int64
	Phase              string
	ConfigRevision     string
	EffectiveArguments json.RawMessage
	SelectedAlbum      *string
	Progress           json.RawMessage
	Outcome            json.RawMessage
	ErrorCode          string
	ScheduledFor       time.Time
	NotAfter           time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
	StartedAt          *time.Time
	FinishedAt         *time.Time
}

type Admission struct {
	Operation Operation
	// Created is true only for the call which durably created this operation.
	// Only that call may dispatch it, once, under an application-owned context.
	Created         bool
	RecoveredCommit bool
}

// Update replaces progress/outcome snapshots, with optimistic concurrency.
// Terminal states are immutable and release the reservation atomically.
type Update struct {
	State     State
	Phase     string
	Progress  json.RawMessage
	Outcome   json.RawMessage
	ErrorCode string
}

func textWithin(s string, max int) bool {
	return s != "" && len(s) <= max && !strings.ContainsRune(s, '\x00')
}

func (r Request) fingerprint() ([]byte, error) {
	if !textWithin(r.Principal, 256) || !textWithin(r.Key, 256) || !textWithin(r.Player, 256) ||
		!textWithin(r.Endpoint, 1024) || !strings.HasPrefix(r.Endpoint, "/") || strings.ContainsAny(r.Endpoint, "?#") ||
		len(r.IfMatch) > 256 || (r.Method != "POST" && r.Method != "PUT" && r.Method != "DELETE" && r.Method != "PATCH") {
		return nil, ErrInvalid
	}
	body, err := canonicalObject(r.Body)
	if err != nil {
		return nil, err
	}
	// Arrays retain order; object keys and whitespace do not affect identity.
	// Preserve number lexemes without float rounding. Typed API validation is
	// responsible for canonical integer/time representations before this boundary.
	encoded, err := json.Marshal(struct {
		Player  string
		IfMatch string
		Body    json.RawMessage
	}{r.Player, r.IfMatch, body})
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(encoded)
	return sum[:], nil
}

func canonicalObject(raw []byte) ([]byte, error) {
	if len(raw) == 0 || len(raw) > MaxJSONBytes || !utf8.Valid(raw) {
		return nil, ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	v, err := readJSON(d, 0)
	if err != nil {
		return nil, fmt.Errorf("JSON object: %w", ErrInvalid)
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, ErrInvalid
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, ErrInvalid
	}
	result, err := json.Marshal(v)
	if err != nil || len(result) > MaxJSONBytes {
		return nil, ErrInvalid
	}
	return result, nil
}

// Reject duplicate keys and excessive nesting instead of hashing ambiguous JSON.
func readJSON(d *json.Decoder, depth int) (any, error) {
	if depth > 32 {
		return nil, ErrInvalid
	}
	token, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch token {
	case json.Delim('{'):
		m := map[string]any{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok || strings.ContainsRune(name, '\x00') {
				return nil, ErrInvalid
			}
			if _, exists := m[name]; exists {
				return nil, ErrInvalid
			}
			v, err := readJSON(d, depth+1)
			if err != nil {
				return nil, err
			}
			m[name] = v
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return nil, ErrInvalid
		}
		return m, nil
	case json.Delim('['):
		values := []any{}
		for d.More() {
			v, err := readJSON(d, depth+1)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return nil, ErrInvalid
		}
		return values, nil
	default:
		if _, delim := token.(json.Delim); delim {
			return nil, ErrInvalid
		}
		if value, ok := token.(string); ok && strings.ContainsRune(value, '\x00') {
			return nil, ErrInvalid
		}
		return token, nil
	}
}
