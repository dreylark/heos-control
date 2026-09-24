package control

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type rejectionRecoveryDevice struct {
	*fakeDevice
	writeErr error
	recover  func(context.Context) error
	reads    int
}

func (d *rejectionRecoveryDevice) Write(_ context.Context, m heos.Mutation, _ heos.Guard) (heos.Response, error) {
	d.mu.Lock()
	d.writes = append(d.writes, m)
	d.s.Stale, d.s.Verified = true, false
	d.mu.Unlock()
	return heos.Response{}, d.writeErr
}

func (d *rejectionRecoveryDevice) Refresh(ctx context.Context) error {
	d.mu.Lock()
	if len(d.writes) == 0 {
		d.mu.Unlock()
		return d.fakeDevice.Refresh(ctx)
	}
	d.reads++
	d.mu.Unlock()
	if d.recover != nil {
		if err := d.recover(ctx); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	d.s.Stale, d.s.Verified = false, true
	d.mu.Unlock()
	return nil
}

func installRejectionRecoveryDevice(c *Coordinator, base *fakeDevice, err error) *rejectionRecoveryDevice {
	d := &rejectionRecoveryDevice{fakeDevice: base, writeErr: err}
	l := c.lanes["room"]
	l.writer, l.device.Observer = d, d
	c.reads.devices[0].Observer = d
	return d
}

func TestRejectedSetterRecoversObservationWithoutReplay(t *testing.T) {
	for _, tc := range []struct {
		name, delivery string
		err            error
		state          journal.State
		recoveryReads  int
	}{
		{
			name: "wrapped rejection", delivery: "rejected", state: journal.Failed, recoveryReads: 1,
			err: fmt.Errorf("native setter: %w", &heos.CommandError{Delivery: heos.Rejected,
				Cause: &heos.DeviceError{Command: "player/set_volume", Code: 9}}),
		},
		{
			name: "not sent", delivery: "not_sent", state: journal.Failed,
			err: &heos.CommandError{Delivery: heos.NotSent, Cause: heos.ErrStale},
		},
		{
			name: "uncertain", delivery: "unknown", state: journal.Uncertain,
			err: &heos.CommandError{Delivery: heos.Uncertain, Cause: heos.ErrOffline},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, db, base, req := fixtureCoordinator(t)
			d := installRejectionRecoveryDevice(c, base, tc.err)
			a, err := c.Submit(context.Background(), req, Command{Kind: CommandKindVolume, Level: 10})
			if err != nil {
				t.Fatal(err)
			}
			o := awaitOperation(t, db, a.ID)
			var outcome struct{ Delivery string }
			if err := json.Unmarshal(o.Outcome, &outcome); err != nil {
				t.Fatal(err)
			}
			if o.State != tc.state || outcome.Delivery != tc.delivery {
				t.Fatalf("state=%s delivery=%s; want %s/%s", o.State, outcome.Delivery, tc.state, tc.delivery)
			}
			d.mu.Lock()
			reads, writes := d.reads, len(d.writes)
			d.mu.Unlock()
			if reads != tc.recoveryReads || writes != 1 {
				t.Fatalf("recovery reads=%d writes=%d; want %d/1", reads, writes, tc.recoveryReads)
			}
			if tc.recoveryReads > 0 {
				if o.ErrorCode != "device_rejected" {
					t.Fatalf("lost rejection code: %s", o.ErrorCode)
				}
				if err := safety(c.lanes["room"], d.Snapshot()); err != nil {
					t.Fatalf("successful recovery left controls unavailable: %v", err)
				}
			}
		})
	}
}

func TestRejectedSetterRecoveryHonorsCancellationAndKeepsEvidence(t *testing.T) {
	for _, reason := range []string{"cancelled", "event gap"} {
		t.Run(reason, func(t *testing.T) {
			c, db, base, req := fixtureCoordinator(t)
			d := installRejectionRecoveryDevice(c, base, &heos.CommandError{Delivery: heos.Rejected,
				Cause: &heos.DeviceError{Command: "player/set_volume", Code: 9}})
			entered := make(chan context.Context, 1)
			d.recover = func(ctx context.Context) error {
				entered <- ctx
				<-ctx.Done()
				return ctx.Err()
			}
			a, err := c.Submit(context.Background(), req, Command{Kind: CommandKindVolume, Level: 10})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case ctx := <-entered:
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 5*time.Second {
					t.Fatal("recovery has no five-second bound")
				}
			case <-time.After(time.Second):
				t.Fatal("rejected setter did not attempt observation recovery")
			}
			l := c.lanes["room"]
			l.mu.Lock()
			r := l.run
			l.mu.Unlock()
			if reason == "event gap" {
				r.event(heos.Event{Gap: true})
			} else {
				r.cancel(context.Canceled)
			}
			o := awaitOperation(t, db, a.ID)
			var outcome struct {
				Delivery    string
				Diagnostics journal.Diagnostics
			}
			if err := json.Unmarshal(o.Outcome, &outcome); err != nil {
				t.Fatal(err)
			}
			if o.State != journal.Failed || o.ErrorCode != "device_rejected" || outcome.Delivery != "rejected" || outcome.Diagnostics.Reason != "parameter_out_of_range" {
				t.Fatalf("recovery changed rejection evidence: state=%s code=%s outcome=%s", o.State, o.ErrorCode, o.Outcome)
			}
			if err := safety(l, d.Snapshot()); err == nil {
				t.Fatal("cancelled recovery made the observation safe for writes")
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			if d.reads != 1 || len(d.writes) != 1 {
				t.Fatalf("recovery reads=%d writes=%d; want 1/1", d.reads, len(d.writes))
			}
		})
	}
}

func TestRejectedReadDoesNotAddWriteRecovery(t *testing.T) {
	for _, phase := range []string{"prewrite", "scalar fallback"} {
		t.Run(phase, func(t *testing.T) {
			c, r, d, clock := modeEventFixture(t)
			began := clock.Now()
			readErr := &heos.CommandError{Delivery: heos.Rejected,
				Cause: &heos.DeviceError{Command: "player/get_volume", Code: 4}}
			wantWrites, wantFallback, wantElapsed, wantState := 0, 0, time.Duration(0), journal.Failed
			if phase == "prewrite" {
				d.onRead = func() error { return readErr }
			} else {
				d.onScalarRead = func(heos.MutationKind) error { return readErr }
				wantWrites, wantFallback, wantElapsed, wantState = 1, 1, 11*time.Second, journal.Uncertain
			}
			c.execute(c.lanes["room"], r, Command{Kind: CommandKindVolume, Level: 21}, heos.Item{})
			if len(d.reads) != 1 || len(d.scalarReads) != wantFallback || len(d.writes) != wantWrites || clock.Now().Sub(began) != wantElapsed {
				t.Fatalf("rejected read changed IO budget: full=%d scalar=%d writes=%d elapsed=%s; want 1/%d/%d/%s",
					len(d.reads), len(d.scalarReads), len(d.writes), clock.Now().Sub(began), wantFallback, wantWrites, wantElapsed)
			}
			o, err := c.db.Get(context.Background(), r.op.ID)
			if err != nil || o.State != wantState || o.FinishedAt == nil {
				t.Fatalf("read failure completion: state=%s finished=%v error=%v", o.State, o.FinishedAt, err)
			}
		})
	}
}
