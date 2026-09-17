package control

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
	"github.com/dreylark/heos-control/internal/telemetry"
)

// A stateful TLS fixture implements Denon 4.2.4/7/11/14, 4.4.11 and 5.4–5.11.
// Setter replies and own events are both exercised, including confirmation of modes.
func TestPlaybackWireOrderReadbackAndReplay(t *testing.T) {
	for _, tc := range []struct {
		name                                          string
		ignoreVolume, bounded, multi, retainedQueue   bool
		duplicateVolume, duplicateMedia, delayedState bool
		paused                                        bool
		loadingStops                                  int
		unknownState                                  bool
		volumeUnmutes                                 bool
		playingInitially                              bool
		delayedMode                                   bool
		requestBudget, replyFirst, longHold           bool
		constantVolume, changedModeOnly               bool
		missingVolumeEvent, failedVolumeReply         bool
		volumeIntervention                            bool
		hybridMedia                                   bool
		loadingMID                                    string
	}{
		{name: "plain"},
		{name: "missing-volume-event", missingVolumeEvent: true},
		{name: "missing-volume-event-auto-unmute", missingVolumeEvent: true, volumeUnmutes: true},
		{name: "missing-volume-event-unchanged", missingVolumeEvent: true, ignoreVolume: true},
		{name: "target-event-before-failed-reply", failedVolumeReply: true},
		{name: "target-event-then-manual-volume", volumeIntervention: true},
		{name: "request-budget-ramp-fade", bounded: true, requestBudget: true},
		{name: "request-budget-hold", bounded: true, requestBudget: true, longHold: true},
		{name: "request-budget-constant-volume", bounded: true, requestBudget: true, constantVolume: true},
		{name: "request-budget-duplicate-events", bounded: true, requestBudget: true, duplicateVolume: true},
		{name: "request-budget-reply-first", bounded: true, requestBudget: true, replyFirst: true},
		{name: "request-budget-reply-first-duplicate-events", bounded: true, requestBudget: true, replyFirst: true, duplicateVolume: true},
		{name: "request-budget-changed-mode-event-only", bounded: true, requestBudget: true, changedModeOnly: true},
		{name: "paused-delayed-mode", bounded: true, retainedQueue: true, paused: true, delayedMode: true},
		{name: "playing-takeover", bounded: true, retainedQueue: true, duplicateVolume: true, duplicateMedia: true, delayedState: true, loadingStops: 2, unknownState: true, playingInitially: true},
		{name: "ignored-volume", ignoreVolume: true},
		{name: "bounded", bounded: true},
		{name: "track-transitions", bounded: true, multi: true},
		{name: "play-only-hybrid-transition", bounded: true, multi: true, hybridMedia: true, requestBudget: true},
		{name: "retained-queue", retainedQueue: true},
		{name: "bounded-retained-queue", bounded: true, retainedQueue: true},
		{name: "duplicate-volume", bounded: true, retainedQueue: true, duplicateVolume: true},
		{name: "duplicate-media", bounded: true, retainedQueue: true, duplicateVolume: true, duplicateMedia: true},
		{name: "delayed-state", bounded: true, retainedQueue: true, duplicateVolume: true, duplicateMedia: true, delayedState: true},
		{name: "paused-delayed-state", bounded: true, retainedQueue: true, duplicateVolume: true, duplicateMedia: true, delayedState: true, paused: true},
		{name: "paused-delayed-state-unbounded", retainedQueue: true, delayedState: true, paused: true},
		{name: "paused-duplicate-loading-stop", bounded: true, retainedQueue: true, duplicateVolume: true, duplicateMedia: true, delayedState: true, paused: true, loadingStops: 2},
		{name: "stopped-repeated-loading-stop", bounded: true, retainedQueue: true, delayedState: true, loadingStops: 5},
		{name: "paused-duplicate-loading-stop-unbounded", retainedQueue: true, delayedState: true, paused: true, loadingStops: 2},
		{name: "paused-album-unknown-loading", bounded: true, retainedQueue: true, duplicateVolume: true, duplicateMedia: true, delayedState: true, paused: true, loadingStops: 2, unknownState: true},
		{name: "muted-paused-album", bounded: true, retainedQueue: true, duplicateVolume: true, duplicateMedia: true, delayedState: true, paused: true, loadingStops: 2, unknownState: true, volumeUnmutes: true},
		{name: "unresolved-loading-mid", bounded: true, retainedQueue: true, delayedState: true, unknownState: true, loadingMID: "loading-mid"},
		{name: "paused-unresolved-loading-mid", bounded: true, retainedQueue: true, delayedState: true, unknownState: true, paused: true, loadingMID: "loading-mid"},
		{name: "unresolved-loading-mid-background", bounded: true, retainedQueue: true, delayedState: true, unknownState: true, loadingMID: "loading-mid", requestBudget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ignoreVolume := tc.ignoreVolume
			seed := httptest.NewTLSServer(http.NotFoundHandler())
			cert := seed.TLS.Certificates[0]
			seed.Close()
			listener, e := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}})
			if e != nil {
				t.Fatal(e)
			}
			var mu sync.Mutex
			var wg sync.WaitGroup
			var conns []net.Conn
			var writes []string
			var requests []string
			var requestTimes []time.Time
			level, muted, state, repeat, shuffle := 70, "on", "stop", "off", "off"
			if tc.retainedQueue {
				level, muted = 10, "off"
			}
			if tc.volumeUnmutes {
				muted = "on"
			}
			if tc.paused {
				state = "pause"
			}
			if tc.playingInitially {
				state = "play"
			}
			playing := tc.playingInitially
			pendingState, stateReads := "", 0
			var pendingMode net.Conn
			loadingStopPending := false
			delivery := newWireEventDelivery()
			clock := &playbackWireClock{advancingClock: advancingClock{now: time.Now()}, delivery: delivery}
			var playBegan time.Time
			track, transitions := 0, 0
			hybridPending, hybridReads := false, 0
			media := func(index int) map[string]any {
				mid := "track"
				if index == 1 {
					mid = "track-next"
				}
				return map[string]any{"sid": 1024, "mid": mid, "qid": index + 1, "album": "Green"}
			}
			// Denon 4.2.5/4.2.15 expose media and queue separately from play
			// state. A stopped player can retain the previous album and track.
			previous := []map[string]any{}
			for i := range 8 {
				previous = append(previous, map[string]any{"sid": 1024, "mid": fmt.Sprintf("previous-%d", i), "qid": i + 1, "album": "Previous album"})
			}
			emit := func(ctx context.Context, conn net.Conn, events ...map[string]string) error {
				delivery.sending(len(events))
				for _, event := range events {
					b, _ := json.Marshal(map[string]any{"heos": event})
					if _, err := conn.Write(append(b, '\r', '\n')); err != nil {
						return err
					}
				}
				return delivery.wait(ctx)
			}
			clock.beforeWait = func(ctx context.Context) error {
				mu.Lock()
				conn := pendingMode
				if conn != nil {
					shuffle, pendingMode = "on", nil
				}
				mu.Unlock()
				if conn != nil {
					return emit(ctx, conn, map[string]string{"command": "event/shuffle_mode_changed", "message": "pid=1&shuffle=on"})
				}
				return nil
			}
			clock.afterWait = func(ctx context.Context) error {
				mu.Lock()
				var conn net.Conn
				if tc.multi && playing && transitions == 0 && clock.Now().Sub(playBegan) >= 750*time.Millisecond {
					track, transitions, conn = 1, 1, conns[0]
					hybridPending = tc.hybridMedia
				} else if hybridPending && hybridReads > 0 {
					hybridPending, conn = false, conns[0]
				}
				mu.Unlock()
				if conn != nil {
					if tc.hybridMedia {
						// Natural advancement supplies only 5.5, without Stop/Play.
						return emit(ctx, conn, map[string]string{"command": "event/player_now_playing_changed", "message": "pid=1"})
					}
					return emit(ctx, conn,
						map[string]string{"command": "event/player_now_playing_changed", "message": "pid=1"},
						map[string]string{"command": "event/player_state_changed", "message": "pid=1&state=play"})
				}
				return nil
			}
			wg.Go(func() {
				for {
					conn, e := listener.Accept()
					if e != nil {
						return
					}
					mu.Lock()
					conns = append(conns, conn)
					mu.Unlock()
					wg.Go(func() {
						defer func() { _ = conn.Close() }()
						reader := bufio.NewReader(conn)
						for {
							line, e := reader.ReadString('\n')
							if e != nil {
								return
							}
							u, e := url.Parse(strings.TrimSpace(line))
							if e != nil {
								return
							}
							name := u.Host + u.Path
							params := u.Query()
							var payload any = []any{}
							events := []map[string]string{}
							pendingReply := false
							mu.Lock()
							requests = append(requests, name)
							requestTimes = append(requestTimes, clock.Now())
							switch name {
							case "system/register_for_change_events":
							case "player/get_players":
								payload = []map[string]any{{"pid": 1, "serial": "synthetic", "name": "Room", "model": "test"}}
							case "group/get_groups":
							case "player/get_play_state":
								if loadingStopPending && tc.loadingStops == 0 {
									// Emit the loading stop during readback, after the
									// queue setter's successful reply, as in the trace.
									events = append(events, map[string]string{"command": "event/player_state_changed", "message": "pid=1&state=stop"})
									loadingStopPending = false
								}
								if stateReads > 0 {
									stateReads--
									if tc.unknownState && pendingState == "play" && stateReads > 0 {
										state = "unknown" // Observed between queue replacement and play.
									}
									if stateReads == 0 {
										state = pendingState
										events = append(events, map[string]string{"command": "event/player_state_changed", "message": "pid=1&state=" + state})
									}
								}
								params.Set("state", state)
							case "player/get_volume":
								params.Set("level", strconv.Itoa(level))
							case "player/get_mute":
								params.Set("state", muted)
							case "player/get_play_mode":
								params.Set("repeat", repeat)
								params.Set("shuffle", shuffle)
							case "player/get_now_playing_media":
								payload = map[string]any{}
								if tc.retainedQueue && !playing {
									payload = previous[3]
								}
								if playing && (!tc.multi || state != "stop") {
									payload = media(track)
								}
								if hybridPending {
									// Incident trace: old MID, new QID, state still Play.
									m := media(1)
									m["mid"] = "track"
									payload = m
									hybridReads++
								}
								if tc.unknownState && state == "unknown" {
									// Home 150 can retain the previous MID while QID
									// and other metadata already describe the new queue.
									oldMID := "previous-3"
									if tc.playingInitially {
										oldMID = "track"
									}
									if tc.loadingMID != "" {
										oldMID = tc.loadingMID
									}
									payload = map[string]any{"sid": 1024, "mid": oldMID, "qid": 1, "album": "Green"}
								}
							case "player/get_queue":
								if loadingStopPending && tc.loadingStops > 0 {
									// The real trace has duplicate stop events while a
									// post-setter queue read is pending (Denon 5.4).
									pendingReply, loadingStopPending = true, false
									for range tc.loadingStops {
										events = append(events, map[string]string{"command": "event/player_state_changed", "message": "pid=1&state=stop"})
									}
								}
								params.Set("count", "0")
								params.Set("returned", "0")
								if tc.retainedQueue && !playing {
									payload = previous
									params.Set("count", "8")
									params.Set("returned", "8")
								}
								if playing {
									payload = []map[string]any{{"sid": 1024, "mid": "track", "qid": 1, "album": "Green"}}
									params.Set("count", "1")
									params.Set("returned", "1")
									if tc.multi {
										payload = []map[string]any{media(0), media(1)}
										params.Set("count", "2")
										params.Set("returned", "2")
									}
								}
							case "browse/browse":
								params.Set("count", "1")
								params.Set("returned", "1")
								if params.Get("sid") == "1024" {
									payload = []map[string]any{{"sid": 900, "name": "Gerbera", "type": "heos_server"}}
								} else {
									payload = []map[string]any{{"cid": "green", "name": "Green", "container": "yes", "playable": "yes"}}
									if params.Get("cid") == "green" {
										payload = []map[string]any{{"mid": "track", "name": "Song", "type": "song", "container": "no", "playable": "yes"}}
										if tc.multi {
											payload = append(payload.([]map[string]any), map[string]any{"mid": "track-next", "name": "Next song", "type": "song", "container": "no", "playable": "yes"})
											params.Set("count", "2")
											params.Set("returned", "2")
										}
									}
								}
							case "player/set_volume":
								if hybridPending {
									t.Error("volume write before MID/QID settled")
								}
								writes = append(writes, name)
								if tc.volumeUnmutes {
									muted = "off"
								}
								if !ignoreVolume {
									level, _ = strconv.Atoi(params.Get("level"))
								}
								if tc.multi && transitions == 1 && level == 9 {
									// Manual Previous during an in-flight fade setter.
									track, transitions = 0, 2
									events = append(events, map[string]string{"command": "event/player_now_playing_changed", "message": "pid=1"})
								}
								events = append(events, map[string]string{"command": "event/player_volume_changed", "message": fmt.Sprintf("pid=1&level=%d&mute=%s", level, muted)})
								if tc.missingVolumeEvent {
									events = nil
								}
								if tc.volumeIntervention {
									level++
									events = append(events, map[string]string{"command": "event/player_volume_changed", "message": fmt.Sprintf("pid=1&level=%d&mute=%s", level, muted)})
								}
							case "player/set_play_state":
								writes = append(writes, name)
								if tc.delayedState {
									pendingState, stateReads = params.Get("state"), 3
									break
								}
								state = params.Get("state")
								events = append(events, map[string]string{"command": "event/player_state_changed", "message": "pid=1&state=" + state})
							case "player/set_play_mode":
								writes = append(writes, name)
								repeat, shuffle = params.Get("repeat"), params.Get("shuffle")
								events = append(events, map[string]string{"command": "event/repeat_mode_changed", "message": "pid=1&repeat=" + repeat}, map[string]string{"command": "event/shuffle_mode_changed", "message": "pid=1&shuffle=" + shuffle})
								if tc.changedModeOnly {
									events = events[1:] // Repeat already equals off; only shuffle changes.
								}
								if tc.delayedMode {
									// Success echoes the request before application.
									// The event arrives once the controller waits,
									// independently of whether it sends any GET.
									shuffle, pendingMode = "off", conn
									events = nil
								}
							case "player/set_mute":
								writes = append(writes, name)
								if level != 10 {
									t.Error("unmute before initial volume confirmation")
								}
								muted = params.Get("state")
								events = append(events, map[string]string{"command": "event/player_volume_changed", "message": fmt.Sprintf("pid=1&level=%d&mute=%s", level, muted)})
							case "browse/add_to_queue":
								writes = append(writes, name)
								if level != 10 || params.Get("aid") != "4" || params.Get("cid") != "green" || params.Get("sid") != "900" {
									t.Error("unsafe/wrong native queue replacement", params)
								}
								playing, state = true, "play"
								playBegan = clock.Now()
								if tc.duplicateMedia {
									// Home 150 reports initial media metadata and then
									// another now-playing notification when audio starts.
									events = append(events, map[string]string{"command": "event/player_now_playing_changed", "message": "pid=1"})
								}
								events = append(events, map[string]string{"command": "event/player_queue_changed", "message": "pid=1"}, map[string]string{"command": "event/player_now_playing_changed", "message": "pid=1"}, map[string]string{"command": "event/player_state_changed", "message": "pid=1&state=play"})
								if tc.delayedState {
									// A successful queue reply precedes actual playback.
									// Home 150 reports stop while replacing a stopped or
									// paused queue (observed firmware behavior, Denon 5.4).
									state, pendingState, stateReads = "stop", "play", 3
									events[len(events)-1]["message"] = "pid=1&state=stop"
									if tc.paused || tc.loadingStops > 0 {
										loadingStopPending = true
										events = events[:len(events)-1]
									}
								}
							default:
								t.Error("unexpected wire command", name)
							}
							pendingEvents := len(events)
							if tc.duplicateVolume {
								for _, event := range events {
									if event["command"] == "event/player_volume_changed" {
										pendingEvents++
									}
								}
							}
							delivery.sending(pendingEvents)
							mu.Unlock()
							if pendingReply {
								b, _ := json.Marshal(map[string]any{"heos": map[string]string{"command": name, "result": "success", "message": "command under process&" + params.Encode()}})
								if _, err := conn.Write(append(b, '\r', '\n')); err != nil {
									return
								}
							}
							result := "success"
							if tc.failedVolumeReply && name == "player/set_volume" {
								// Denon 6.1/6.2: receiving an expected value does
								// not turn an explicitly rejected SET into success.
								result = "fail"
								params.Set("eid", "7")
								params.Set("text", "Command not executed.")
							}
							reply, _ := json.Marshal(map[string]any{"heos": map[string]string{"command": name, "result": result, "message": params.Encode()}, "payload": payload})
							if tc.replyFirst {
								if _, e = conn.Write(append(reply, '\r', '\n')); e != nil {
									return
								}
							}
							for _, event := range events {
								b, _ := json.Marshal(map[string]any{"heos": event})
								if tc.duplicateVolume && event["command"] == "event/player_volume_changed" {
									// Home 150 emits identical Denon 5.9 notifications
									// for one setter; both precede readback confirmation.
									b = append(append(append([]byte(nil), b...), '\r', '\n'), b...)
								}
								if _, e = conn.Write(append(b, '\r', '\n')); e != nil {
									return
								}
							}
							if !tc.replyFirst {
								if _, e = conn.Write(append(reply, '\r', '\n')); e != nil {
									return
								}
							}
						}
					})
				}
			})
			defer func() {
				_ = listener.Close()
				mu.Lock()
				for _, conn := range conns {
					_ = conn.Close()
				}
				mu.Unlock()
				wg.Wait()
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			metrics := telemetry.NewPlayer("room")
			client, e := heos.New(ctx, heos.Config{Address: listener.Addr().String(), Fingerprint: fmt.Sprintf("%x", sha256.Sum256(cert.Certificate[0])), EnableWrites: true, Metrics: metrics})
			if e != nil {
				t.Fatal(e)
			}
			defer client.Close()
			observer, e := heos.NewObserver(client, heos.Identity{Key: "room", Serial: "synthetic"}, heos.ObservationCacheTTL)
			if e != nil {
				t.Fatal(e)
			}
			// Match production: one channel consumer feeds the observer,
			// independently of the synchronous ownership callback.
			observationCtx, observationCancel := context.WithCancel(ctx)
			var observationWG sync.WaitGroup
			observationWG.Go(func() {
				for {
					select {
					case <-observationCtx.Done():
						return
					case event, ok := <-client.Events():
						if !ok {
							return
						}
						observer.Notify(event)
						delivery.received()
					}
				}
			})
			defer func() { observationCancel(); observationWG.Wait() }()
			if tc.requestBudget {
				// Include background reconciliation in the request budget.
				ready := make(chan error, 1)
				observationWG.Go(func() {
					_ = observer.Run(observationCtx, heos.IdleObservationInterval, func(err error) {
						select {
						case ready <- err:
						default:
						}
					})
				})
				select {
				case e = <-ready:
					if e != nil {
						t.Fatal(e)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			} else if e = observer.Refresh(ctx); e != nil {
				t.Fatal(e)
			}
			catalog, e := heos.NewCatalog(client, 32, time.Minute)
			if e != nil {
				t.Fatal(e)
			}
			page, e := catalog.Browse(ctx, "900", "", "", 50)
			if e != nil {
				t.Fatal(e)
			}
			ceiling := 40
			reads := NewReads("wire", []Device{{Config: config.Player{Key: "room", Serial: "synthetic", WritesEnabled: true, VolumeCeiling: &ceiling}, Observer: observer, Client: client, Metrics: metrics, Catalogs: map[string]Browser{"music": catalog}}}, []config.Source{{Key: "music", Player: "room", Name: "Gerbera"}})
			db := &memoryJournal{ops: map[string]journal.Operation{}, keys: map[string]string{}}
			c := NewCoordinator(ctx, reads, db, nil, nil)
			c.clock = clock
			defer c.Close()
			p, _ := reads.Player("room")
			request := journal.Request{Principal: "operator", Player: "room", Key: "play", Method: "POST", Endpoint: "/v1/players/room/playback", IfMatch: fmt.Sprintf("%q", p.Revision), Body: json.RawMessage(`{"initial_volume":{"unit":"heos","level":10}}`)}
			cmd := Command{Kind: "playback", Level: 10, ItemRef: page.Items[0].Ref, Shuffle: true, Repeat: "off", Takeover: tc.playingInitially}
			if tc.bounded {
				cmd.Automation = &Automation{TargetLevel: 12, RampSeconds: 1, DurationSeconds: 3, FadeSeconds: 1}
				if tc.longHold {
					cmd.Automation.DurationSeconds = 15
				}
				if tc.constantVolume {
					cmd.Automation = &Automation{TargetLevel: 10, DurationSeconds: 15}
				}
			}
			a, e := c.Submit(ctx, request, cmd)
			if e != nil {
				t.Fatal(e)
			}
			o := awaitOperation(t, db, a.ID)
			wantFailure := ignoreVolume || tc.failedVolumeReply || tc.volumeIntervention
			if !wantFailure && o.State != journal.Succeeded {
				mu.Lock()
				failedRequests := append([]string(nil), requests...)
				mu.Unlock()
				t.Fatalf("operation failed: %+v\nwire requests: %v", o, failedRequests)
			}
			if wantFailure && o.State == journal.Succeeded {
				t.Fatal("false success after ignored/rejected/interrupted volume setter")
			}
			if _, e = c.Submit(ctx, request, cmd); e != nil {
				t.Fatal(e)
			}
			mu.Lock()
			defer mu.Unlock()
			want := 1
			if !wantFailure {
				want = 4
			}
			if tc.bounded {
				want = 11 // four preparation, two ramp, four fade, one stop
				finalLevel := 0
				if tc.constantVolume {
					want, finalLevel = 5, 10 // four preparation and one stop
				}
				if state != "stop" || level != finalLevel {
					t.Fatal("bounded playback did not stop", state, level)
				}
			}
			if tc.playingInitially {
				want++
				if len(writes) == 0 || writes[0] != "player/set_play_state" {
					t.Fatal("takeover did not confirm stop first", writes)
				}
			}
			if len(writes) != want {
				t.Fatal("unexpected write sequence", writes, o)
			}
			if tc.multi && transitions != 2 {
				t.Fatal("wire transitions not exercised", transitions)
			}
			if tc.loadingMID != "" {
				assertQueueLoadingWireBudget(t, requests)
			}
			if tc.requestBudget && tc.loadingMID == "" {
				if tc.hybridMedia {
					if hybridReads != 1 {
						t.Fatal("hybrid read was skipped or polled repeatedly", hybridReads)
					}
					assertPlaybackWireRequestBudget(t, requests, "player/get_now_playing_media", "player/get_play_state", "player/get_now_playing_media", "player/get_now_playing_media")
				} else {
					assertPlaybackWireRequestBudget(t, requests)
				}
			}
			if tc.missingVolumeEvent {
				assertWireScalarFallback(t, requests, requestTimes)
			}
			if tc.failedVolumeReply || tc.volumeIntervention {
				assertWireScalarNotConfirmed(t, requests, o)
			}
		})
	}
}

// Count actual protocol requests, not Observer.Refresh invocations. The last
// queue page completes preparation; subsequent volume steps and the constant
// hold need no GET when contiguous Denon 5.9 events confirm every setter. The
// final transport Stop retains its separate queue/state confirmation contract.
func assertPlaybackWireRequestBudget(t *testing.T, requests []string, transitionReads ...string) {
	t.Helper()
	queueSent, prepared, lastVolume, stop := false, -1, -1, -1
	initialVolume, mode, unmute := -1, -1, -1
	for i, request := range requests {
		if !queueSent {
			switch request {
			case "player/set_volume":
				initialVolume = i
			case "player/set_play_mode":
				mode = i
			case "player/set_mute":
				unmute = i
			}
		}
		if request == "browse/add_to_queue" {
			queueSent = true
		}
		if queueSent && prepared < 0 && request == "player/get_queue" {
			prepared = i
		}
		if queueSent && request == "player/set_volume" {
			lastVolume = i
		}
		if queueSent && request == "player/set_play_state" {
			stop = i
		}
	}
	if prepared < 0 || stop <= prepared {
		t.Fatal("fixture did not exercise confirmed playback and final stop", requests)
	}
	if initialVolume < 0 || mode <= initialVolume || unmute <= mode {
		t.Fatal("fixture did not exercise scalar preparation sequence", requests)
	}
	for _, request := range requests[initialVolume+1 : unmute] {
		if strings.Contains(request, "/get_") {
			t.Errorf("event-confirmed initial volume/mode required unexpected preparation GET: %s", request)
		}
	}
	if lastVolume < 0 {
		lastVolume = prepared // Constant volume has no active setter.
	}
	var reads []string
	for _, request := range requests[prepared+1 : lastVolume+1] {
		if strings.Contains(request, "/get_") {
			reads = append(reads, request)
		}
	}
	if !slices.Equal(reads, transitionReads) {
		t.Errorf("confirmed ramp/hold/fade GET requests = %v; want only transition reads %v", reads, transitionReads)
	}
	reads = nil
	for _, request := range requests[lastVolume+1 : stop] {
		if strings.Contains(request, "/get_") {
			reads = append(reads, request)
		}
	}
	if len(reads) > 8 {
		t.Errorf("hold/final volume confirmation sent %d GET requests before Stop; allow only its eight-command prewrite check: %v", len(reads), reads)
	}
}
