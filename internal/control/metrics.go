package control

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
	"github.com/dreylark/heos-control/internal/telemetry"
)

// A completion receipt lives with one locally admitted execution and its bounded
// cancellation/orphan work. Sharing it covers lost commit acknowledgements and
// handoffs without retaining operation IDs in a process-wide deduplication map.
type operationMetrics struct {
	player    *telemetry.Player
	kind      string
	completed sync.Once
}

func (m *operationMetrics) finish(op journal.Operation) {
	if m == nil || op.FinishedAt == nil {
		return
	}
	var outcome struct {
		Diagnostics struct{ Reason string } `json:"diagnostics"`
	}
	_ = json.Unmarshal(op.Outcome, &outcome)
	reason := outcome.Diagnostics.Reason
	if reason == "" {
		reason = op.ErrorCode
		if reason == "" && op.State == journal.Succeeded {
			reason = "completed"
		}
	}
	m.finishAs(op.State, reason)
}

func (m *operationMetrics) finishAs(state journal.State, reason string) {
	if m != nil {
		m.completed.Do(func() { m.player.OperationFinished(m.kind, string(state), reason) })
	}
}

func (m *operationMetrics) phase(phase string) {
	if m != nil {
		m.player.Active(phase)
	}
}

// This interval starts at Writer.Write and includes its reply and the existing
// application confirmation. A locally rejected NotSent attempt has no sample.
func (c *Coordinator) recordConfirmation(l *lane, r *execution, kind string, began time.Time, writeErr, err error) {
	var command *heos.CommandError
	if errors.As(writeErr, &command) && command.Delivery == heos.NotSent {
		return
	}
	cause := completionCause(r.ctx, err)
	result := "uncertain"
	switch {
	case err == nil:
		result = "confirmed"
	case errors.Is(cause, ErrOwnership):
		result = "released"
	case errors.Is(cause, context.DeadlineExceeded):
		result = "timeout"
	case errors.Is(cause, heos.ErrRejected):
		result = "rejected"
	case errors.Is(cause, context.Canceled), errors.Is(cause, errSuperseded):
		result = "cancelled"
	}
	l.device.Metrics.Confirmation(kind, result, c.clock.Now().Sub(began))
}
