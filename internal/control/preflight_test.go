package control

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dreylark/heos-control/internal/heos"
)

func TestPlaybackPreflightChecksConcreteRequestWithoutWrites(t *testing.T) {
	for _, name := range []string{"ready", "short playback", "playing", "paused", "takeover", "disabled", "ceiling missing", "ceiling exceeded", "stale", "identity", "grouped", "unknown volume", "unknown state", "stale reference", "overlap", "empty album"} {
		t.Run(name, func(t *testing.T) {
			c, j, d, _, cmd := albumFixture(t)
			warning := ""
			var wantErr error
			switch name {
			case "short playback":
				cmd.Automation = nil
				d.s.State = "play"
			case "playing":
				d.s.State = "play"
				warning = "player_busy"
			case "paused":
				d.s.State = "pause"
			case "takeover":
				d.s.State = "play"
				cmd.Takeover = true
			case "disabled":
				c.reads.devices[0].Config.WritesEnabled = false
				warning = "writes_disabled"
			case "ceiling missing":
				c.reads.devices[0].Config.VolumeCeiling = nil
				warning = "volume_policy_unverified"
			case "ceiling exceeded":
				cmd.Automation.TargetLevel = 41
				warning = "volume_policy_exceeded"
			case "stale":
				d.s.Stale = true
				warning = "state_unavailable"
			case "identity":
				d.s.Player.Serial = "foreign"
				warning = "state_unavailable"
			case "grouped":
				d.s.Grouped = true
				warning = "grouped_target"
			case "unknown state":
				d.s.State = "unknown"
				warning = "state_unavailable"
			case "unknown volume":
				d.s.Volume = nil
				warning = "state_unavailable"
			case "stale reference":
				c.reads.devices[0].Catalogs["music"] = playbackCatalog{}
				cmd.ItemRef = "expired"
				wantErr = heos.ErrStaleReference
			case "overlap":
				cmd.Automation.RampSeconds = 1200
				wantErr = heos.ErrBounds
			case "empty album":
				d.tracks = nil
				wantErr = heos.ErrBounds
			}
			got, err := c.reads.Preflight(context.Background(), "room", cmd)
			if !errors.Is(err, wantErr) {
				t.Fatalf("error=%v want %v", err, wantErr)
			}
			if wantErr == nil && (got.Ready != (warning == "") || warning != "" && !slices.Contains(got.Warnings, warning)) {
				t.Fatalf("%+v", got)
			}
			if len(d.writes) != 0 || len(j.ops) != 0 || len(j.keys) != 0 {
				t.Fatal("preflight mutated device or journal")
			}
		})
	}
}

type preflightObservation struct {
	*fakeDevice
	err error
}

func (d preflightObservation) Refresh(context.Context) error { return d.err }

type preflightCatalog struct {
	albumCatalog
	after func()
}

func (c preflightCatalog) Resolve(sid heos.ID, ref string) (heos.Item, error) {
	c.after()
	return c.albumCatalog.Resolve(sid, ref)
}

func TestPreflightNeverAuthorizesFailedRefreshOrChangedState(t *testing.T) {
	for _, changed := range []bool{false, true} {
		c, _, d, _, cmd := albumFixture(t)
		warning := "state_unavailable"
		if changed {
			warning = "state_changed"
			c.reads.devices[0].Catalogs["music"] = preflightCatalog{after: func() { d.s.Token.Revision++ }}
		} else {
			c.reads.devices[0].Observer = preflightObservation{d.fakeDevice, errors.New("refresh failed")}
		}
		got, err := c.reads.Preflight(context.Background(), "room", cmd)
		if err != nil || got.Ready || !slices.Contains(got.Warnings, warning) {
			t.Fatal(got, err)
		}
		if len(d.writes) != 0 {
			t.Fatal("preflight wrote device")
		}
	}
}
