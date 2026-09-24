package control

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func TestScalarReadyAtPlaybackDeadlineCannotConfirmLateStep(t *testing.T) {
	c, r, d, clock := modeEventFixture(t)
	began := clock.Now()
	r.playbackDeadline = began.Add(4 * time.Second)
	clock.events = []modeClockEvent{{r.playbackDeadline, func() {
		r.event(modeEvent("shuffle", "on"))
	}}}
	err := c.write(c.lanes["room"], r, heos.Mutation{Kind: heos.MutationKindMode, Repeat: heos.RepeatOff, Shuffle: true})
	if !errors.Is(err, context.DeadlineExceeded) || r.confirmed != 0 || len(d.writes) != 1 || len(d.scalarReads) != 0 || clock.Now() != r.playbackDeadline {
		t.Fatalf("late ready state escaped playback bound: err=%v confirmed=%d writes=%d fallback=%d elapsed=%s", err, r.confirmed, len(d.writes), len(d.scalarReads), clock.Now().Sub(began))
	}
}

type scalarSlowJournal struct {
	*memoryJournal
	beforeTransition func()
}

func (j scalarSlowJournal) Transition(ctx context.Context, id string, rev int64, u journal.Update) (journal.Operation, error) {
	j.beforeTransition()
	return j.memoryJournal.Transition(ctx, id, rev, u)
}

func TestScalarJournalDelayCannotSendPositiveLevelAfterPlaybackDeadline(t *testing.T) {
	c, r, d, clock := modeEventFixture(t)
	began := clock.Now()
	r.automating, r.confirmed = true, 1
	r.playbackDeadline = began.Add(time.Second)
	c.db = scalarSlowJournal{c.db.(*memoryJournal), func() { clock.now = r.playbackDeadline.Add(time.Millisecond) }}
	d.onWrite = func(heos.Mutation) error {
		r.event(heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"21"}, "mute": {"off"}}})
		return nil
	}
	err := c.write(c.lanes["room"], r, heos.Mutation{Kind: heos.MutationKindVolume, Level: 21})
	if !errors.Is(err, errPlaybackTimelineChanged) || len(d.writes) != 0 || len(d.scalarReads) != 0 {
		t.Fatalf("journal delay allowed late positive volume: err=%v writes=%+v fallback=%d", err, d.writes, len(d.scalarReads))
	}
}

func TestScalarPositiveWireGuardEndsAtPlaybackDeadline(t *testing.T) {
	c, r, d, clock := modeEventFixture(t)
	r.automating, r.confirmed = true, 1
	r.playbackDeadline = clock.Now().Add(time.Second)
	d.onWrite = func(heos.Mutation) error {
		r.event(heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"21"}, "mute": {"off"}}})
		return nil
	}
	before := time.Now()
	if err := c.write(c.lanes["room"], r, heos.Mutation{Kind: heos.MutationKindVolume, Level: 21}); err != nil {
		t.Fatal(err)
	}
	if d.lastGuard.ExpiresAt.Before(before.Add(900*time.Millisecond)) || d.lastGuard.ExpiresAt.After(time.Now().Add(time.Second)) {
		t.Fatal("mailbox guard may outlive positive step's playback window", d.lastGuard.ExpiresAt.Sub(before))
	}
}

type scalarPublicationSpy struct {
	*modeEventDevice
	published int
	resolved  int
}

func (d *scalarPublicationSpy) ConfirmScalars(heos.Snapshot) error {
	d.published++
	return nil
}
func (d *scalarPublicationSpy) Reconcile(context.Context) error {
	d.resolved++
	return nil
}

// Client.publish calls the ownership callback before the ordinary cache consumer.
// Publishing the newer token here would cause Notify to drop the queued media
// event as already covered, leaving old metadata eligible for another write.
func TestScalarProjectionCannotSkipUnconsumedMetadataEvent(t *testing.T) {
	for _, change := range []string{"media", "stop"} {
		t.Run(change, func(t *testing.T) {
			c, r, base, clock := modeEventFixture(t)
			d := &scalarPublicationSpy{modeEventDevice: base}
			l := c.lanes["room"]
			l.device.Observer, l.device.Client, l.writer = d, d, d
			r.expected.State, d.s.State = heos.PlayStatePlay, "play"
			r.queueOwned, r.automating, r.confirmed = true, true, 1
			r.playbackDeadline = clock.Now().Add(30 * time.Second)
			m := heos.Mutation{Kind: heos.MutationKindVolume, Level: 21}
			r.expect(m)
			r.unconfirmed = true // The successful setter reply has been received.
			r.event(heos.Event{Command: "event/player_volume_changed", Params: url.Values{"pid": {"1"}, "level": {"21"}, "mute": {"off"}}})
			event := heos.Event{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"1"}}}
			if change == "stop" {
				event.Command = "event/player_state_changed"
				event.Params.Set("state", "stop")
			}
			r.event(event)
			if !r.observationPending() {
				t.Fatal("test did not leave unresolved event history")
			}
			if _, err := c.confirmScalar(l, r, m); err != nil {
				t.Fatal(err)
			}
			if d.published != 0 || !r.observationPending() {
				t.Fatalf("scalar publication consumed metadata/state history: published=%d pending=%t", d.published, r.observationPending())
			}
			if _, err := c.prewriteObservation(l, r, heos.Mutation{Kind: heos.MutationKindVolume, Level: 22}); err != nil {
				t.Fatal(err)
			}
			if d.resolved != 1 {
				t.Fatal("next step did not require state/media reconciliation")
			}
		})
	}
}

// Model a duplicate notification arriving between capturing ownership state and
// reading the transport revision. Reconciliation must consume the event, not GET.
type scalarPrewriteRace struct {
	*modeEventDevice
	once       func()
	reconciled int
}

func (d *scalarPrewriteRace) PlayerView(pid heos.ID) heos.View {
	if f := d.once; f != nil {
		d.once = nil
		f()
	}
	return d.modeEventDevice.PlayerView(pid)
}
func (d *scalarPrewriteRace) Reconcile(ctx context.Context) error {
	d.reconciled++
	return context.Cause(ctx)
}
func TestScalarDuplicateRacingPrewriteDoesNotForceFullRead(t *testing.T) {
	c, r, base, _ := modeEventFixture(t)
	d := &scalarPrewriteRace{modeEventDevice: base}
	l := c.lanes["room"]
	l.device.Observer, l.device.Client, l.writer = d, d, d
	r.confirmed = 1
	d.once = func() {
		base.s.Token.Player++
		base.s.EventUpdated = true
		r.event(heos.Event{Command: "event/player_volume_changed", Token: base.s.Token,
			Params: url.Values{"pid": {"1"}, "level": {"20"}, "mute": {"off"}}})
	}
	snapshot, err := c.prewriteObservation(l, r, heos.Mutation{Kind: heos.MutationKindVolume, Level: 21})
	if err != nil || len(base.reads) != 0 || d.reconciled != 1 || snapshot.Token != base.s.Token {
		t.Fatalf("duplicate required a full read: err=%v reads=%d reconciliations=%d token=%+v", err, len(base.reads), d.reconciled, snapshot.Token)
	}
}
