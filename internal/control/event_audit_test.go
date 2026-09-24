package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

type auditDevice struct{ *modeEventDevice }

func (d auditDevice) Reconcile(ctx context.Context) error { return context.Cause(ctx) }

func TestNewBackgroundAuditCannotBeHiddenByScalarProjection(t *testing.T) {
	for _, field := range []string{"queue", "volume"} {
		t.Run(field, func(t *testing.T) {
			c, r, base, clock := modeEventFixture(t)
			d := auditDevice{base}
			l := c.lanes["room"]
			l.device.Observer, l.device.Client, l.writer = d, d, d
			total := 1
			base.s.State = heos.PlayStatePlay
			base.s.Media = &heos.Media{Source: "1024", ID: "owned", QueueID: "1"}
			base.s.Queue = heos.QueuePage{Total: &total, Items: []heos.Media{*base.s.Media}}
			r.expected = base.s
			r.confirmed, r.queueOwned, r.bounded, r.automating = 1, true, true, true
			r.playbackDeadline = clock.Now().Add(time.Minute)
			clock.now = clock.now.Add(time.Second)
			base.s.ObservedAt = clock.Now()
			base.s.EventUpdated = true // An unrelated scalar event followed the full audit.
			if field == "queue" {
				base.s.Queue.Items = []heos.Media{{ID: "foreign", QueueID: "1"}}
			} else {
				level := 19
				base.s.Volume = &level
			}
			var err error
			if field == "queue" {
				err = c.checkOwnership(l, r)
			} else {
				err = c.write(l, r, heos.Mutation{Kind: heos.MutationKindVolume, Level: 21})
			}
			if !errors.Is(err, ErrOwnership) || len(base.writes) != 0 {
				t.Fatalf("new audit was ignored: error=%v setters=%v", err, base.writes)
			}
		})
	}
}
