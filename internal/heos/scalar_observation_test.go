package heos

import (
	"context"
	"errors"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestScalarWriteEventsKeepObservationContinuous(t *testing.T) {
	for _, timing := range []string{"before-reply", "after-reply", "missing-event", "unrelated-event"} {
		t.Run(timing, func(t *testing.T) {
			var reads atomic.Int32
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				if commandName(u) == "player/set_volume" {
					if timing == "before-reply" {
						sendEvent(c, "event/player_volume_changed", "pid=9007199254740993&level=20&mute=off")
					}
					sendReply(c, u, nil, nil)
					if timing == "after-reply" {
						sendEvent(c, "event/player_volume_changed", "pid=9007199254740993&level=20&mute=off")
					}
					if timing == "unrelated-event" {
						sendEvent(c, "event/player_state_changed", "pid=9007199254740993&state=play")
					}
					return
				}
				reads.Add(1)
				observationReply(c, u, "serial-A")
			})
			c, err := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin, EnableWrites: true})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			before, count := o.Snapshot(), reads.Load()
			guard := Guard{Token: before.Token, ExpiresAt: time.Now().Add(time.Second)}
			if _, err := c.Write(context.Background(), Mutation{Kind: MutationKindVolume, Player: before.Player.ID, Level: 20}, guard); err != nil {
				t.Fatal(err)
			}
			if timing != "missing-event" {
				select {
				case e := <-c.Events():
					o.Notify(e)
				case <-time.After(time.Second):
					t.Fatal("missing fixture event")
				}
			}
			s := o.Snapshot()
			if timing == "missing-event" || timing == "unrelated-event" {
				if !s.Stale || *s.Volume != 12 {
					t.Fatal("unconfirmed scalar write fabricated fresh target state", s)
				}
			} else if s.Stale || *s.Volume != 20 || s.Token != c.PlayerView(before.Player.ID).Token || s.ObservedAt != before.ObservedAt {
				t.Fatal("matching event did not confirm continuous observation", s)
			}
			if reads.Load() != count || len(o.wake) != 0 {
				t.Fatal("scalar write scheduled hidden full read", reads.Load(), len(o.wake))
			}
			if _, err := c.Write(context.Background(), Mutation{Kind: MutationKindVolume, Player: before.Player.ID, Level: 21}, guard); !errors.Is(err, ErrStale) {
				t.Fatal("consumed prewrite guard remained reusable", err)
			}
		})
	}
}

func TestProgressAndDuplicateEventsDuringScalarPublication(t *testing.T) {
	for _, timing := range []string{"before-reply", "after-reply", "before-publication", "after-publication"} {
		t.Run(timing, func(t *testing.T) {
			var reads atomic.Int32
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				if commandName(u) == "player/set_volume" {
					// Real Home 150 trace: progress arrived with the new command's
					// Write revision before its first volume event. Progress does
					// not advance the Player revision (Denon 5.6).
					sendEvent(c, "event/player_now_playing_progress", "pid=9007199254740993&cur_pos=1000&duration=100000")
					if timing == "before-reply" {
						sendEvent(c, "event/player_volume_changed", "pid=9007199254740993&level=20&mute=off")
						sendEvent(c, "event/player_volume_changed", "pid=9007199254740993&level=20&mute=off")
					}
					sendReply(c, u, nil, nil)
					if timing != "before-reply" {
						sendEvent(c, "event/player_volume_changed", "pid=9007199254740993&level=20&mute=off")
					}
					return
				}
				reads.Add(1)
				observationReply(c, u, "serial-A")
			})
			c, err := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin, EnableWrites: true})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			confirmed, count := o.Snapshot(), reads.Load()
			if _, err := c.Write(context.Background(), Mutation{Kind: MutationKindVolume, Player: confirmed.Player.ID, Level: 20}, Guard{Token: confirmed.Token, ExpiresAt: time.Now().Add(time.Second)}); err != nil {
				t.Fatal(err)
			}
			next := func() Event {
				t.Helper()
				select {
				case e := <-c.Events():
					return e
				case <-time.After(time.Second):
					t.Fatal("missing fixture event")
					return Event{}
				}
			}
			progress, target := next(), next()
			if progress.Command != "event/player_now_playing_progress" || progress.Token.Player != confirmed.Token.Player || progress.Token.Write == confirmed.Token.Write {
				t.Fatal("fixture did not reproduce progress between command and volume event", progress)
			}
			o.Notify(progress)
			o.Notify(target)
			level := 20
			confirmed.Volume, confirmed.Token = &level, target.Token
			duplicate := func() Event {
				if timing != "before-reply" {
					c.event(Response{Command: "event/player_volume_changed", Params: url.Values{"pid": {"9007199254740993"}, "level": {"20"}, "mute": {"off"}}})
				}
				return next()
			}
			switch timing {
			case "before-reply", "after-reply":
				e := duplicate()
				o.Notify(e)
				confirmed.Token = e.Token
			case "before-publication":
				e := duplicate()
				confirmed.Token = e.Token
				if err := o.ConfirmScalars(confirmed); err != nil {
					t.Fatal("synchronous confirmation lost the buffered duplicate", err)
				}
				o.Notify(e)
			case "after-publication":
				if err := o.ConfirmScalars(confirmed); err != nil {
					t.Fatal("confirmation before late duplicate failed", err)
				}
				o.Notify(duplicate())
				confirmed.Token = c.PlayerView(confirmed.Player.ID).Token
			}
			if err := o.ConfirmScalars(confirmed); err != nil {
				t.Fatal("progress/duplicate invalidated scalar confirmation", err)
			}
			if s := o.Snapshot(); s.Stale || *s.Volume != 20 || len(o.wake) != 0 || reads.Load() != count {
				t.Fatal("progress/duplicate scheduled hidden full read", s, len(o.wake), reads.Load()-count)
			}
		})
	}
}

func TestModeEventsConfirmOnlyChangedFields(t *testing.T) {
	for _, scenario := range []string{"repeat", "shuffle", "both", "unchanged", "transport"} {
		t.Run(scenario, func(t *testing.T) {
			var reads atomic.Int32
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				if commandName(u) == "player/set_play_mode" || commandName(u) == "player/set_play_state" {
					sendReply(c, u, nil, nil)
					return
				}
				reads.Add(1)
				observationReply(c, u, "serial-A")
			})
			c, err := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin, EnableWrites: true})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			before, count := o.Snapshot(), reads.Load()
			m := Mutation{Kind: MutationKindMode, Player: before.Player.ID, Repeat: RepeatOff, Shuffle: true}
			if scenario == "repeat" || scenario == "both" {
				m.Repeat = RepeatOnAll
			}
			if scenario == "shuffle" || scenario == "both" {
				m.Shuffle = false
			}
			if scenario == "transport" {
				m.Kind, m.State = "transport", "stop"
			}
			if _, err := c.Write(context.Background(), m, Guard{Token: before.Token, ExpiresAt: time.Now().Add(time.Second)}); err != nil {
				t.Fatal(err)
			}
			if scenario == "repeat" || scenario == "both" {
				deliverObservationEvent(c, o, "event/repeat_mode_changed", "pid=9007199254740993&repeat=on_all")
			}
			if scenario == "both" {
				deliverObservationEvent(c, o, "event/repeat_mode_changed", "pid=9007199254740993&repeat=on_all")
				if !o.Snapshot().Stale || len(o.wake) != 0 {
					t.Fatal("partial/duplicate mode notification confirmed remaining field or scheduled GET")
				}
			}
			if scenario == "shuffle" || scenario == "both" {
				deliverObservationEvent(c, o, "event/shuffle_mode_changed", "pid=9007199254740993&shuffle=off")
			}
			if scenario == "unchanged" || scenario == "transport" {
				deliverObservationEvent(c, o, "event/player_state_changed", "pid=9007199254740993&state=stop")
			}
			s := o.Snapshot()
			if s.Stale != (scenario == "transport") || reads.Load() != count || len(o.wake) != 0 || s.ObservedAt != before.ObservedAt {
				t.Fatal("wrong scalar write footprint or hidden GET", s, reads.Load()-count)
			}
		})
	}
}

func TestScalarFallbackReadsOnlyChangedControls(t *testing.T) {
	for _, kind := range []MutationKind{MutationKindVolume, MutationKindMute, MutationKindMode} {
		t.Run(string(kind), func(t *testing.T) {
			var changed atomic.Bool
			var reads atomic.Int32
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				switch commandName(u) {
				case "player/set_volume", "player/set_mute", "player/set_play_mode":
					changed.Store(true)
					sendReply(c, u, nil, nil) // Missing Denon event; success alone is insufficient.
					return
				}
				reads.Add(1)
				if changed.Load() {
					switch commandName(u) {
					case "player/get_volume":
						sendReply(c, u, url.Values{"level": {"20"}}, nil)
						return
					case "player/get_mute":
						sendReply(c, u, url.Values{"state": {"on"}}, nil)
						return
					case "player/get_play_mode":
						sendReply(c, u, url.Values{"repeat": {"on_all"}, "shuffle": {"off"}}, nil)
						return
					}
				}
				observationReply(c, u, "serial-A")
			})
			c, err := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin, EnableWrites: true})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			before, count := o.Snapshot(), reads.Load()
			if _, err := c.Write(context.Background(), Mutation{Kind: kind, Player: before.Player.ID, Level: 20, Muted: true, Repeat: RepeatOnAll}, Guard{Token: before.Token, ExpiresAt: time.Now().Add(time.Second)}); err != nil {
				t.Fatal(err)
			}
			if !o.Snapshot().Stale {
				t.Fatal("missing event confirmed a write")
			}
			if err := o.RefreshScalars(context.Background(), kind); err != nil {
				t.Fatal(err)
			}
			s := o.Snapshot()
			want := int32(2)
			if kind == "mode" {
				want = 1
				if s.Repeat != RepeatOnAll || s.Shuffle || *s.Volume != *before.Volume {
					t.Fatal("mode read changed unrelated volume", s)
				}
			} else if *s.Volume != 20 || !*s.Muted || s.Repeat != before.Repeat || s.Shuffle != before.Shuffle {
				t.Fatal("volume read changed unrelated mode", s)
			}
			if s.Stale || s.ObservedAt != before.ObservedAt || !s.EventUpdated || reads.Load() != count+want || *s.Media != *before.Media || len(s.Queue.Items) != len(before.Queue.Items) {
				t.Fatal("scalar fallback performed full read or renewed unrelated state", s, reads.Load()-count)
			}
		})
	}
}

func TestScalarFallbackCannotRepairMissingHistory(t *testing.T) {
	for _, fault := range []string{"missing-event", "gap", "expired", "media-pending", "invalid", "disconnected", "wrong-kind"} {
		t.Run(fault, func(t *testing.T) {
			var reads atomic.Int32
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				reads.Add(1)
				observationReply(c, u, "serial-A")
			})
			c := fakeClient(t, server)
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			now := time.Now()
			o.now = func() time.Time { return now }
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			count, kind := reads.Load(), MutationKindVolume
			switch fault {
			case "missing-event":
				c.event(Response{Command: "event/player_state_changed", Params: url.Values{"pid": {"9007199254740993"}, "state": {"stop"}}})
				<-c.Events()
			case "gap":
				o.Notify(Event{Gap: true, Token: c.PlayerView("9007199254740993").Token})
			case "expired":
				now = now.Add(ObservationCacheTTL)
			case "media-pending":
				deliverObservationEvent(c, o, "event/player_now_playing_changed", "pid=9007199254740993")
			case "invalid":
				o.invalid = true
			case "disconnected":
				c.Interrupt()
			case "wrong-kind":
				kind = "queue"
			}
			if err := o.RefreshScalars(context.Background(), kind); err == nil || reads.Load() != count {
				t.Fatal("scalar fallback hid invalid history or sent GETs", err, reads.Load()-count)
			}
		})
	}
}

func TestConfirmedScalarSnapshotPrecedesBufferedEvent(t *testing.T) {
	var reads atomic.Int32
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		reads.Add(1)
		observationReply(c, u, "serial-A")
	})
	c := fakeClient(t, server)
	o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, count := o.Snapshot(), reads.Load()
	c.event(Response{Command: "event/player_volume_changed", Params: url.Values{"pid": {"9007199254740993"}, "level": {"20"}, "mute": {"off"}}})
	e := <-c.Events()
	confirmed := before
	level := 20
	confirmed.Volume, confirmed.Token = &level, e.Token
	if err := o.ConfirmScalars(confirmed); err != nil {
		t.Fatal(err)
	}
	o.Notify(e)
	if s := o.Snapshot(); s.Stale || *s.Volume != 20 || s.ObservedAt != before.ObservedAt || len(o.wake) != 0 || reads.Load() != count {
		t.Fatal("late already-confirmed event invalidated state", s)
	}
	if err := o.ConfirmScalars(before); !errors.Is(err, ErrStale) {
		t.Fatal("old confirmed snapshot replaced newer event state", err)
	}
}

func TestConfirmedScalarsCannotOverwriteNewerFullAudit(t *testing.T) {
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) { observationReply(c, u, "serial-A") })
	c := fakeClient(t, server)
	o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	now := time.Now()
	o.now = func() time.Time { return now }
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	confirmed := o.Snapshot()
	level := 20
	confirmed.Volume = &level
	now = now.Add(time.Second)
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if confirmed.Token != o.Snapshot().Token {
		t.Fatal("fixture expected audit without a new event token")
	}
	if err := o.ConfirmScalars(confirmed); !errors.Is(err, ErrStale) || *o.Snapshot().Volume != 12 || o.Snapshot().EventUpdated {
		t.Fatal("old scalar proof erased newer full audit evidence", err, o.Snapshot())
	}
}

func TestUnchangedScalarWriteCanUseExplicitConfirmation(t *testing.T) {
	var reads atomic.Int32
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		if commandName(u) == "player/set_volume" {
			sendReply(c, u, nil, nil)
			return
		}
		reads.Add(1)
		observationReply(c, u, "serial-A")
	})
	c, err := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin, EnableWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	confirmed, count := o.Snapshot(), reads.Load()
	r, err := c.Write(context.Background(), Mutation{Kind: MutationKindVolume, Player: confirmed.Player.ID, Level: *confirmed.Volume}, Guard{Token: confirmed.Token, ExpiresAt: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if !o.Snapshot().Stale {
		t.Fatal("command echo was implicitly treated as confirmation")
	}
	confirmed.Token = r.Token
	if err := o.ConfirmScalars(confirmed); err != nil || o.Snapshot().Stale || reads.Load() != count || len(o.wake) != 0 {
		t.Fatal("explicit unchanged-state proof caused a read or stayed invalid", err, o.Snapshot())
	}
}

func TestScalarFallbackCannotOverwriteConcurrentEvents(t *testing.T) {
	for _, fault := range []string{"volume-event", "queue-event", "invalid-volume", "invalid-mute", "duplicate-volume", "wrong-token"} {
		t.Run(fault, func(t *testing.T) {
			var fallback atomic.Bool
			var reads atomic.Int32
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				reads.Add(1)
				if fallback.Load() && commandName(u) == "player/get_volume" {
					switch fault {
					case "volume-event":
						sendEvent(c, "event/player_volume_changed", "pid=9007199254740993&level=21&mute=off")
					case "queue-event":
						sendEvent(c, "event/player_queue_changed", "pid=9007199254740993")
					case "invalid-volume":
						sendReply(c, u, url.Values{"level": {"101"}}, nil)
						return
					case "duplicate-volume":
						sendReply(c, u, url.Values{"level": {"12", "20"}}, nil)
						return
					case "wrong-token":
						sendEvent(c, "event/groups_changed", "")
					}
				}
				if fallback.Load() && commandName(u) == "player/get_mute" && fault == "invalid-mute" {
					sendReply(c, u, url.Values{"state": {"unknown"}}, nil)
					return
				}
				observationReply(c, u, "serial-A")
			})
			c := fakeClient(t, server)
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			c.SetEventHandler(o.Notify)
			before, count := o.Snapshot(), reads.Load()
			fallback.Store(true)
			if err := o.RefreshScalars(context.Background(), MutationKindVolume); err == nil {
				t.Fatal("inconsistent scalar read succeeded")
			}
			s := o.Snapshot()
			if s.ObservedAt != before.ObservedAt || reads.Load()-count > 2 {
				t.Fatal("failed scalar read renewed full state or expanded into full GETs", s, reads.Load()-count)
			}
			if fault == "volume-event" && *s.Volume != 21 {
				t.Fatal("older scalar read replaced newer event", s)
			}
		})
	}
}

func TestReconcileReusesEventsAndOnlyReadsRequiredMetadata(t *testing.T) {
	for _, scenario := range []string{"scalar-event", "metadata-event", "consumer-lag", "gap", "audit", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			var reads atomic.Int32
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				reads.Add(1)
				observationReply(c, u, "serial-A")
			})
			c := fakeClient(t, server)
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			now := time.Now()
			o.now = func() time.Time { return now }
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			before, count := o.Snapshot(), reads.Load()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want, wantErr := int32(0), false
			switch scenario {
			case "scalar-event":
				deliverObservationEvent(c, o, "event/player_volume_changed", "pid=9007199254740993&level=20&mute=off")
			case "metadata-event":
				deliverObservationEvent(c, o, "event/player_now_playing_changed", "pid=9007199254740993")
				want = 1
			case "consumer-lag":
				c.event(Response{Command: "event/player_state_changed", Params: url.Values{"pid": {"9007199254740993"}, "state": {"play"}}})
				cancel() // No event consumer exists in this scenario; the wait is bounded.
				wantErr = true
			case "gap":
				o.Notify(Event{Gap: true, Token: before.Token})
				want = 8
			case "audit":
				now = now.Add(IdleObservationInterval)
				want = 8
			case "canceled":
				cancel()
				wantErr = true
			}
			if err := o.Reconcile(ctx); (err != nil) != wantErr || reads.Load()-count != want {
				t.Fatal("unexpected reconciliation result/GET budget", err, reads.Load()-count, want)
			}
			if want < 8 && o.Snapshot().ObservedAt != before.ObservedAt {
				t.Fatal("partial reconciliation renewed full audit deadline")
			}
		})
	}
}

func TestReconcileWaitsForBufferedMetadataEvent(t *testing.T) {
	var reads atomic.Int32
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		reads.Add(1)
		observationReply(c, u, "serial-A")
	})
	c := fakeClient(t, server)
	o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	count := reads.Load()
	c.event(Response{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {"9007199254740993"}}})
	e := <-c.Events()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- o.Reconcile(ctx) }()
	select {
	case err := <-done:
		t.Fatal("consumer lag was treated as lost ownership", err)
	case <-time.After(20 * time.Millisecond):
	}
	if reads.Load() != count {
		t.Fatal("consumer lag caused GET before event delivery")
	}
	o.Notify(e)
	if err := <-done; err != nil || reads.Load() != count+1 {
		t.Fatal("metadata was not reconciled after consumer caught up", err, reads.Load()-count)
	}
}

func TestConcurrentReconcileSharesOneMetadataRead(t *testing.T) {
	var reads atomic.Int32
	var block atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
	server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		reads.Add(1)
		if commandName(u) == "player/get_now_playing_media" && block.CompareAndSwap(true, false) {
			close(started)
			<-release
		}
		observationReply(c, u, "serial-A")
	})
	c := fakeClient(t, server)
	o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
	if err := o.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	count := reads.Load()
	deliverObservationEvent(c, o, "event/player_now_playing_changed", "pid=9007199254740993")
	block.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 4)
	for range 4 {
		go func() { done <- o.Reconcile(ctx) }()
	}
	<-started
	close(release)
	for range 4 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if reads.Load() != count+1 {
		t.Fatal("concurrent callers duplicated metadata read", reads.Load()-count)
	}
}

func TestPlaybackFallbackPreservesConfirmedQueueAndAuditAge(t *testing.T) {
	for _, scenario := range []string{"missing-play-event", "pending-media", "queue-changed", "write-pending", "gap", "concurrent-event", "expired"} {
		t.Run(scenario, func(t *testing.T) {
			var fallback atomic.Bool
			var reads atomic.Int32
			server := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				reads.Add(1)
				if fallback.Load() {
					switch commandName(u) {
					case "player/get_play_state":
						sendReply(c, u, url.Values{"state": {"play"}}, nil)
						return
					case "player/get_now_playing_media":
						if scenario == "concurrent-event" {
							sendEvent(c, "event/player_volume_changed", "pid=9007199254740993&level=21&mute=off")
						}
						sendReply(c, u, nil, map[string]any{"sid": 1024, "mid": "track-1", "qid": 1})
						return
					}
				}
				observationReply(c, u, "serial-A")
			})
			c := fakeClient(t, server)
			o, _ := NewObserver(c, Identity{Key: "room", Serial: "serial-A"}, ObservationCacheTTL)
			now := time.Now()
			o.now = func() time.Time { return now }
			if err := o.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			before, count := o.Snapshot(), reads.Load()
			c.SetEventHandler(o.Notify)
			fallback.Store(true)
			want, wantErr := int32(2), false
			switch scenario {
			case "pending-media":
				c.event(Response{Command: "event/player_now_playing_changed", Params: url.Values{"pid": {string(before.Player.ID)}}})
			case "queue-changed":
				c.event(Response{Command: "event/player_queue_changed", Params: url.Values{"pid": {string(before.Player.ID)}}})
				want, wantErr = 0, true
			case "write-pending":
				c.mu.Lock()
				c.writes[before.Player.ID] = playerWrite{revision: 1, mutation: Mutation{Kind: MutationKindVolume}}
				c.mu.Unlock()
				want, wantErr = 0, true
			case "gap":
				o.Notify(Event{Gap: true, Token: before.Token})
				want, wantErr = 0, true
			case "concurrent-event":
				wantErr = true
			case "expired":
				now = now.Add(ObservationCacheTTL)
				want = 8 // The normal full audit repairs expired state first.
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "write-pending" {
				cancel() // No command completion will arrive in this fixture.
			}
			if err := o.RefreshPlayback(ctx); (err != nil) != wantErr || reads.Load()-count != want {
				t.Fatal("wrong playback fallback result/budget", err, reads.Load()-count, want)
			}
			s := o.Snapshot()
			if (want < 8 && s.ObservedAt != before.ObservedAt) || len(s.Queue.Items) != len(before.Queue.Items) || s.Queue.Items[0] != before.Queue.Items[0] {
				t.Fatal("playback fallback renewed queue/full audit evidence", s)
			}
			if !wantErr && (s.Stale || s.EventUpdated != (want < 8) || s.State != PlayStatePlay || s.Media.ID != "track-1") {
				t.Fatal("playback fallback did not supply state and metadata", s)
			}
		})
	}
}
