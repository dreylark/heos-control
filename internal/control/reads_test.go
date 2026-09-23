package control

import (
	"context"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
)

type observationStub struct{ value heos.Snapshot }

func (s *observationStub) Snapshot() heos.Snapshot       { return s.value }
func (s *observationStub) Refresh(context.Context) error { return nil }

func TestPlayerIncludesVolumeCeiling(t *testing.T) {
	ceiling := 40
	s := &observationStub{value: heos.Snapshot{Key: "room", State: "stop", ObservedAt: time.Now(), Connected: true, Verified: true}}
	with, err := NewReads("epoch", []Device{{Config: config.Player{Key: "room", Serial: "serial", VolumeCeiling: &ceiling}, Observer: s}}, nil).Player("room")
	if err != nil || with.VolumeCeiling == nil || *with.VolumeCeiling != 40 || with.VolumeCeiling == &ceiling {
		t.Fatalf("%+v %v", with, err)
	}
	without, err := NewReads("epoch", []Device{{Config: config.Player{Key: "room", Serial: "serial"}, Observer: s}}, nil).Player("room")
	if err != nil || without.VolumeCeiling != nil {
		t.Fatalf("%+v %v", without, err)
	}
}

func TestUnknownPlayerRevision(t *testing.T) {
	p := config.Player{Key: "room", Serial: "serial"}
	s := &observationStub{value: heos.Snapshot{Key: "room", State: "unknown", Stale: true}}
	a := NewReads("epoch-a", []Device{{Config: p, Observer: s}}, nil)
	b := NewReads("epoch-b", []Device{{Config: p, Observer: s}}, nil)
	av, _ := a.Player("room")
	bv, _ := b.Player("room")
	if av.Revision == bv.Revision || av.Volume.Level != nil || av.ObservedAt != nil || av.PlaybackState != "unknown" || !av.Stale {
		t.Fatalf("unknown state fabricated: %+v", av)
	}
	old := av.Revision
	s.value = heos.Snapshot{Key: "room", State: "pause", ObservedAt: time.Now(), Connected: true, Verified: true}
	av, _ = a.Player("room")
	if av.Revision == old {
		t.Fatal("freshness did not fence revision")
	}
}
