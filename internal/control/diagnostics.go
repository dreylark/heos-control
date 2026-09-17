package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

// Only the reason appears in Error(); structured log fields contain safe values.
// Keep ErrOwnership classification and the existing API/journal outcome unchanged.
type ownershipError struct {
	reason   string
	attrs    []slog.Attr
	evidence journal.Diagnostics
}

func (e *ownershipError) Error() string { return "ownership lost: " + e.reason }
func (e *ownershipError) Unwrap() error { return ErrOwnership }
func (e *ownershipError) LogValue() slog.Value {
	return slog.GroupValue(append([]slog.Attr{slog.String("reason", e.reason)}, e.attrs...)...)
}

func stateChanges(a, b heos.Snapshot) []string {
	fields := []string{}
	for _, field := range []struct {
		name    string
		changed bool
	}{
		{"repeat", a.Repeat != b.Repeat}, {"shuffle", a.Shuffle != b.Shuffle},
		{"player_identity", a.Player != b.Player}, {"grouped", a.Grouped != b.Grouped},
		{"state", a.State != b.State}, {"volume", !reflect.DeepEqual(a.Volume, b.Volume)},
		{"mute", !reflect.DeepEqual(a.Muted, b.Muted)},
		{"queue_items", !reflect.DeepEqual(a.Queue.Items, b.Queue.Items)},
		{"queue_total", !reflect.DeepEqual(a.Queue.Total, b.Queue.Total)},
	} {
		if field.changed {
			fields = append(fields, field.name)
		}
	}
	if (a.Media == nil) != (b.Media == nil) {
		fields = append(fields, "media_presence")
	} else if a.Media != nil && b.Media != nil {
		for _, field := range []struct {
			name    string
			changed bool
		}{
			{"media_source", a.Media.Source != b.Media.Source}, {"media_id", a.Media.ID != b.Media.ID},
			{"media_queue_id", a.Media.QueueID != b.Media.QueueID},
			{"media_metadata", a.Media.Song != b.Media.Song || a.Media.Album != b.Media.Album || a.Media.Artist != b.Media.Artist},
		} {
			if field.changed {
				fields = append(fields, field.name)
			}
		}
	}
	return fields
}

func ownershipMismatch(reason string, fields []string, a, b heos.Snapshot) *ownershipError {
	return &ownershipError{reason: reason, attrs: []slog.Attr{slog.Any("changed_fields", fields),
		slog.Any("expected", snapshotLog(a)), slog.Any("observed", snapshotLog(b))},
		evidence: journal.Diagnostics{Source: "observation", ChangedFields: append([]string{}, fields...),
			Expected: snapshotScalars(a), Observed: snapshotScalars(b)}}
}

func snapshotLog(s heos.Snapshot) slog.Value {
	// IDs, serials and media text can embed personal URLs. Fingerprints permit
	// comparison without copying that text, and queue summaries stay bounded.
	media := slog.GroupValue(slog.Bool("present", false))
	if s.Media != nil {
		media = slog.GroupValue(slog.Bool("present", true), slog.String("source", fingerprint(string(s.Media.Source))),
			slog.String("mid", fingerprint(string(s.Media.ID))), slog.String("qid", fingerprint(string(s.Media.QueueID))), slog.String("fingerprint", objectFingerprint(s.Media)))
	}
	state := "unknown"
	if s.State == "play" || s.State == "pause" || s.State == "stop" {
		state = s.State
	}
	repeat := "unknown"
	if s.Repeat == "off" || s.Repeat == "on_all" || s.Repeat == "on_one" {
		repeat = s.Repeat
	}
	return slog.GroupValue(slog.String("state", state), slog.Any("volume", s.Volume), slog.Any("muted", s.Muted),
		slog.String("repeat", repeat), slog.Bool("shuffle", s.Shuffle), slog.Bool("grouped", s.Grouped),
		slog.Any("token", s.Token), slog.Bool("connected", s.Connected), slog.Bool("stale", s.Stale),
		slog.Any("media", media), slog.String("player_identity", objectFingerprint(s.Player)),
		slog.String("queue_fingerprint", objectFingerprint(s.Queue.Items)), slog.Int("queue_items", len(s.Queue.Items)), slog.Any("queue_total", s.Queue.Total), slog.Any("queue_next", s.Queue.Next))
}

func objectFingerprint(value any) string {
	h := sha256.New()
	_ = json.NewEncoder(h).Encode(value)
	return hex.EncodeToString(h.Sum(nil)[:8])
}

func fingerprint(s string) string {
	if s == "" {
		return ""
	}
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:8])
}

func (c *Coordinator) logOwnershipLoss(r *execution, err error) {
	if c.logger == nil {
		return
	}
	var detail *ownershipError
	selected := completionCause(r.ctx, err)
	errors.As(selected, &detail)
	attrs := []slog.Attr{slog.String("operation_id", r.id), slog.String("player", r.op.Player),
		slog.String("kind", r.op.Kind), slog.String("phase", r.op.Phase)}
	r.mu.Lock()
	attrs = append(attrs, slog.Int("commands_confirmed", r.confirmed), slog.Bool("inflight", r.inflight), slog.Bool("unconfirmed", r.unconfirmed), slog.Bool("queue_owned", r.queueOwned))
	r.mu.Unlock()
	if detail != nil {
		attrs = append(attrs, slog.String("reason", detail.reason))
		attrs = append(attrs, detail.attrs...)
	} else {
		attrs = append(attrs, slog.String("reason", "unclassified"))
	}
	var captured *diagnosticError
	if errors.As(selected, &captured) && captured.evidence.Rule != nil {
		attrs = append(attrs, slog.String("rule", *captured.evidence.Rule))
	}
	c.logger.LogAttrs(context.WithoutCancel(r.ctx), slog.LevelInfo, "operation ownership lost", attrs...)
}

func (c *Coordinator) traceSnapshot(r *execution, stage string, s heos.Snapshot) {
	if c.logger != nil && c.logger.Enabled(r.ctx, slog.LevelDebug) {
		c.logger.Debug("operation observation", "operation_id", r.id, "player", r.op.Player, "phase", r.op.Phase, "stage", stage, "snapshot", snapshotLog(s))
	}
}
