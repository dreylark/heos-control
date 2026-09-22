package control

import (
	"context"
	"errors"
	"testing"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func TestSkipUnknownStateIsNotSkippable(t *testing.T) {
	for _, takeover := range []bool{false, true} {
		name := "initial observation"
		if takeover {
			name = "takeover refresh"
		}
		t.Run(name, func(t *testing.T) {
			c, j, d, req := skipFixture(t, "", 0, 1)
			if takeover {
				c.lanes["room"].device.Observer = changingObservation{d.fakeDevice, func() error {
					d.s.State = "unknown"
					return nil
				}}
			} else {
				d.s.State = "unknown"
			}
			_, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next", Takeover: takeover})
			if !errors.Is(err, ErrNotSkippable) {
				t.Fatalf("unknown playback state: got %v, want %v", err, ErrNotSkippable)
			}
			if len(d.writes) != 0 || len(j.ops) != 0 {
				t.Fatal("rejected skip had side effects", d.writes, j.ops)
			}
		})
	}
}

func TestSkipUnknownStatePreservesDeviceSafety(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*lane, *heos.Snapshot)
		want error
	}{
		{name: "writes disabled", edit: func(l *lane, _ *heos.Snapshot) { l.device.Config.WritesEnabled = false }, want: heos.ErrReadOnly},
		{name: "unverified", edit: func(_ *lane, s *heos.Snapshot) { s.Verified = false }, want: ErrUnavailable},
		{name: "stale", edit: func(_ *lane, s *heos.Snapshot) { s.Stale = true }, want: ErrUnavailable},
		{name: "disconnected", edit: func(_ *lane, s *heos.Snapshot) { s.Connected = false }, want: ErrUnavailable},
		{name: "missing player", edit: func(_ *lane, s *heos.Snapshot) { s.Player.ID = "" }, want: ErrUnavailable},
		{name: "different identity", edit: func(_ *lane, s *heos.Snapshot) { s.Player.Serial = "different" }, want: ErrUnavailable},
		{name: "missing volume", edit: func(_ *lane, s *heos.Snapshot) { s.Volume = nil }, want: ErrUnavailable},
		{name: "missing mute", edit: func(_ *lane, s *heos.Snapshot) { s.Muted = nil }, want: ErrUnavailable},
		{name: "grouped", edit: func(_ *lane, s *heos.Snapshot) { s.Grouped = true }, want: ErrGrouped},
		{name: "missing ceiling", edit: func(l *lane, _ *heos.Snapshot) { l.device.Config.VolumeCeiling = nil }, want: heos.ErrReadOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, j, d, req := skipFixture(t, "", 0, 1)
			d.s.State = "unknown"
			tc.edit(c.lanes["room"], &d.s)
			_, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
			if !errors.Is(err, tc.want) {
				t.Fatalf("unsafe observation: got %v, want %v", err, tc.want)
			}
			if len(d.writes) != 0 || len(j.ops) != 0 {
				t.Fatal("rejected skip had side effects", d.writes, j.ops)
			}
		})
	}
}

func TestSkipUnknownStateBeforeSendingIsNotSkippable(t *testing.T) {
	c, j, d, req := skipFixture(t, "", 0, 1)
	c.lanes["room"].device.Observer = changingObservation{d.fakeDevice, func() error {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.s.State = "unknown"
		return nil
	}}
	a, err := c.Submit(context.Background(), skipRequest(c, req), Command{Kind: "skip", Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	if o.State != journal.Failed || o.ErrorCode != "not_skippable" || len(d.writes) != 0 {
		t.Fatalf("unknown prewrite state: operation=%+v writes=%v", o, d.writes)
	}
}
