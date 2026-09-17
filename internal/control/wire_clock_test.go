package control

import (
	"context"
	"sync"
	"time"
)

// Socket delivery uses real goroutines while playback deadlines use fake time.
// Do not advance that clock past a notification already in flight on the wire.
// The single Events consumer acknowledges delivery after Observer.Notify.
type wireEventDelivery struct {
	mu      sync.Mutex
	pending int
	changed chan struct{}
}

func newWireEventDelivery() *wireEventDelivery {
	return &wireEventDelivery{changed: make(chan struct{})}
}

func (d *wireEventDelivery) sending(n int) {
	d.mu.Lock()
	d.pending += n
	d.mu.Unlock()
}

func (d *wireEventDelivery) received() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pending > 0 {
		d.pending--
	}
	close(d.changed)
	d.changed = make(chan struct{})
}

func (d *wireEventDelivery) wait(ctx context.Context) error {
	for {
		d.mu.Lock()
		pending, changed := d.pending, d.changed
		d.mu.Unlock()
		if pending == 0 {
			return context.Cause(ctx)
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		}
	}
}

type playbackWireClock struct {
	advancingClock
	delivery              *wireEventDelivery
	beforeWait, afterWait func(context.Context) error
}

// Observer timestamps use the real transport clock. Let real I/O time pass
// during preparation; subsequent waits may advance the playback timeline faster.
func (c *playbackWireClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now := time.Now(); now.After(c.now) {
		c.now = now
	}
	return c.now
}

func (c *playbackWireClock) Wait(ctx context.Context, duration time.Duration, wake <-chan struct{}) error {
	if c.beforeWait != nil {
		if err := c.beforeWait(ctx); err != nil {
			return err
		}
	}
	if err := c.delivery.wait(ctx); err != nil {
		return err
	}
	if err := c.advancingClock.Wait(ctx, duration, wake); err != nil {
		return err
	}
	if c.afterWait != nil {
		return c.afterWait(ctx)
	}
	return context.Cause(ctx)
}
