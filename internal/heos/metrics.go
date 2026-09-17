package heos

import (
	"context"
	"errors"
	"time"
)

// Metrics observes existing I/O paths. Implementations must be concurrency-safe,
// nonblocking and must bound labels; callbacks cannot issue device commands.
type Metrics interface {
	WireCommand(command, result string, elapsed time.Duration)
	Reconnected()
	EventGap(reason string)
	Observation(scope, trigger string)
}

type observationTriggerKey struct{}

// WithObservationTrigger identifies why an observation was requested. The metric
// sink normalizes this value to its closed label set; it never changes I/O policy.
func WithObservationTrigger(ctx context.Context, trigger string) context.Context {
	return context.WithValue(ctx, observationTriggerKey{}, trigger)
}

func defaultObservationTrigger(ctx context.Context, trigger string) context.Context {
	if _, ok := ctx.Value(observationTriggerKey{}).(string); ok {
		return ctx
	}
	return WithObservationTrigger(ctx, trigger)
}

func (o *Observer) recordObservation(ctx context.Context, scope string) {
	if o.client.cfg.Metrics == nil {
		return
	}
	trigger, ok := ctx.Value(observationTriggerKey{}).(string)
	if !ok {
		trigger = "manual"
	}
	o.client.cfg.Metrics.Observation(scope, trigger)
}

func (c *Client) recordWire(command string, sent int, err error, elapsed time.Duration) {
	if c.cfg.Metrics == nil || sent == 0 {
		return
	}
	result := "reply_success"
	if err != nil {
		result = "uncertain"
		var commandErr *CommandError
		if errors.As(err, &commandErr) && commandErr.Delivery == Rejected {
			result = "reply_rejected"
		}
	}
	c.cfg.Metrics.WireCommand(command, result, elapsed)
}

func (c *Client) recordGap(reason string) {
	if c.cfg.Metrics != nil {
		c.cfg.Metrics.EventGap(reason)
	}
}
