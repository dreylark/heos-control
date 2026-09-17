package heos

import (
	"context"
	"time"
)

const (
	IdleObservationInterval = 5 * time.Minute
	ObservationCacheTTL     = IdleObservationInterval + 10*time.Second
	observationDebounce     = 250 * time.Millisecond
	observationHeartbeat    = time.Minute
)

type observationTimer interface {
	Chan() <-chan time.Time
	Reset(time.Duration)
	Stop()
}

type wallObservationTimer struct{ timer *time.Timer }

func newObservationTimer(d time.Duration) observationTimer {
	return &wallObservationTimer{timer: time.NewTimer(d)}
}
func (t *wallObservationTimer) Chan() <-chan time.Time { return t.timer.C }
func (t *wallObservationTimer) Reset(d time.Duration)  { t.timer.Reset(d) }
func (t *wallObservationTimer) Stop()                  { t.timer.Stop() }

func (o *Observer) requestRefresh() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// Run observes at startup, after relevant events and at the backup interval.
// Its caller owns cancellation and must forward Events through Notify. The
// heartbeat (Denon 4.1.5) checks connection health without renewing state age.
// report runs synchronously after refresh or failed heartbeat and must not block.
func (o *Observer) Run(ctx context.Context, interval time.Duration, report func(error)) error {
	if interval < 100*time.Millisecond || interval > IdleObservationInterval {
		return ErrBounds
	}
	refresh := o.newTimer(interval)
	heartbeat := o.newTimer(observationHeartbeat)
	defer refresh.Stop()
	defer heartbeat.Stop()
	pending, recovering := false, false
	retry := time.Second
	reportError := func(err error) {
		if report != nil && ctx.Err() == nil {
			report(err)
		}
	}
	schedule := func(err error) {
		reportError(err)
		pending = false
		recovering = err != nil
		if err != nil {
			// Events from the failed connection cannot bypass outage backoff.
			refresh.Reset(retry)
			retry = min(2*retry, 30*time.Second)
			heartbeat.Stop()
			return
		}
		retry = time.Second
		// Partial metadata reads and event projections cannot postpone the full
		// backup indefinitely. ObservedAt advances only on a complete read.
		refresh.Reset(max(0, interval-o.now().Sub(o.Snapshot().ObservedAt)))
		heartbeat.Reset(observationHeartbeat)
	}
	schedule(o.Refresh(defaultObservationTrigger(ctx, "startup")))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-o.client.done:
			return ErrClosed
		case <-o.wake:
			if !recovering && !pending && o.Snapshot().Stale {
				pending = true
				// The first event starts the delay; a continuous burst cannot
				// keep postponing reconciliation indefinitely.
				refresh.Reset(observationDebounce)
			}
		case <-refresh.Chan():
			trigger := "audit"
			if recovering {
				trigger = "recovery"
			} else if pending {
				trigger = "event"
			}
			schedule(o.refresh(defaultObservationTrigger(ctx, trigger), !pending))
		case <-heartbeat.Chan():
			// Foreground reads and active operation checks already test the
			// connection. Idle heartbeats add no traffic while those run.
			age := o.now().Sub(o.Snapshot().ObservedAt)
			if age >= 0 && age < observationHeartbeat {
				heartbeat.Reset(observationHeartbeat - age)
				continue
			}
			beatCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := o.client.Read(beatCtx, "system/heart_beat", nil)
			cancel()
			if err != nil {
				o.mu.Lock()
				o.invalid = true
				o.signalChanged()
				o.mu.Unlock()
				schedule(err)
				continue
			}
			heartbeat.Reset(observationHeartbeat)
		}
	}
}
