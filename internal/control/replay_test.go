package control

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func TestReplayIncidents(t *testing.T) {
	for _, fixture := range loadReplayFixtures(t) {
		t.Run(fixture.Name, func(t *testing.T) {
			result := runReplayFixture(t, fixture)
			if err := checkReplayResult(fixture, result); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type replayWrite struct {
	At       time.Duration
	Mutation heos.Mutation
}
type replayResult struct {
	Operation              journal.Operation
	Writes                 []replayWrite
	Elapsed                time.Duration
	FullReads, ScalarReads int
	Problem                error
}

func TestReplayCheckerRejectsWrongOutcomeAndBudgets(t *testing.T) {
	f := namedReplayFixture(t, "confirmation-fallback")
	r := runReplayFixture(t, f)
	if err := checkReplayResult(f, r); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"outcome", "writes", "reads", "unconsumed", "identity", "unknown-command"} {
		t.Run(kind, func(t *testing.T) {
			broken := r
			switch kind {
			case "outcome":
				broken.Operation.State = journal.Accepted
			case "writes":
				broken.Writes = append(slices.Clone(r.Writes), replayWrite{Mutation: heos.Mutation{Kind: heos.MutationKindQueue}})
			case "reads":
				broken.FullReads = f.Expect.MaxFullReads + 1
			case "unconsumed":
				broken.Problem = fmt.Errorf("required step not consumed")
			case "identity", "unknown-command":
				broken.Writes = slices.Clone(r.Writes)
				if kind == "identity" {
					broken.Writes[0].Mutation.Player = "other-player"
				} else {
					broken.Writes[0].Mutation.Kind = "unexpected"
				}
			}
			if err := checkReplayResult(f, broken); err == nil {
				t.Fatal("broken replay passed")
			}
		})
	}
}

func TestReplayRejectsMismatchedAndUnconsumedScripts(t *testing.T) {
	for _, kind := range []string{"arguments", "anchor", "future-step"} {
		t.Run(kind, func(t *testing.T) {
			f := namedReplayFixture(t, "confirmation-fallback")
			want := "unconsumed"
			switch kind {
			case "arguments":
				f.Scripts[0].Args["level"] = "999"
				want = "level="
			case "anchor":
				f.Scripts[0].Occurrence = 100
			case "future-step":
				f.Scripts[0].Steps = append(f.Scripts[0].Steps, replayStep{AtMS: 60000, Action: "event", Event: &replayEvent{Command: "event/player_state_changed", Params: map[string]string{"pid": "1", "state": "pause"}}})
			}
			result := runReplayFixture(t, f)
			if result.Problem == nil || !strings.Contains(result.Problem.Error(), want) {
				t.Fatalf("invalid script was not reported: %v", result.Problem)
			}
			if err := checkReplayResult(f, result); err == nil {
				t.Fatal("invalid script passed result verification")
			}
		})
	}
}

func TestReplayRejectsEarlyFadeAndStop(t *testing.T) {
	for _, f := range loadReplayFixtures(t) {
		if f.Name != "navigation-full-envelope" {
			continue
		}
		r := runReplayFixture(t, f)
		if err := checkReplayResult(f, r); err != nil {
			t.Fatal(err)
		}
		for _, kind := range []string{"fade", "stop"} {
			t.Run(kind, func(t *testing.T) {
				broken := r
				broken.Writes = slices.Clone(r.Writes)
				for i, w := range broken.Writes {
					if kind == "fade" && w.Mutation.Kind == heos.MutationKindVolume && w.Mutation.Level == 9 || kind == "stop" && w.Mutation.Kind == heos.MutationKindTransport && w.Mutation.State == heos.PlayStateStop {
						broken.Writes[i].At = time.Second
					}
				}
				if err := checkReplayResult(f, broken); err == nil {
					t.Fatal("premature fade/Stop passed with an unchanged persisted envelope")
				}
			})
		}
		return
	}
	t.Fatal("required full-envelope fixture missing")
}

func runReplayFixture(t *testing.T, f replayFixture) replayResult {
	t.Helper()
	c, store, base, request, command := albumFixture(t)
	clock := &modeEventClock{now: time.Now()}
	c.clock = clock
	base.tracks = replaySelectedQueue()
	base.s.State, base.s.Repeat, base.s.Shuffle = heos.PlayState(f.Initial.State), heos.Repeat(f.Initial.Repeat), f.Initial.Shuffle
	volume, muted := f.Initial.Volume, f.Initial.Muted
	base.s.Volume, base.s.Muted, base.s.ObservedAt = &volume, &muted, clock.Now()
	applyReplayPatch(&base.s, replayPatch{Queue: "old", Media: &replayMedia{MID: "previous-track", QID: "1", Source: "1024"}})
	device := &replayDevice{albumDevice: base, clock: clock, start: clock.Now(), fixture: f, occurrences: map[string]int{}, used: make([]bool, len(f.Scripts))}
	l := c.lanes["room"]
	l.device.Observer, l.device.Client, l.writer = device, device, device
	c.reads.devices[0] = l.device
	base.now = clock.Now
	command.Automation = &f.Automation
	player, err := c.reads.Player("room")
	if err != nil {
		t.Fatal(err)
	}
	request.IfMatch = fmt.Sprintf("%q", player.Revision)
	request.Body, err = json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	op, err := c.Submit(context.Background(), request, command)
	if err != nil {
		t.Fatal(err)
	}
	finished := awaitOperation(t, store, op.ID)
	c.Close() // Joins the owner before reading its test timeline/counters.
	if device.problem == nil {
		for i, used := range device.used {
			if !used {
				device.problem = fmt.Errorf("unconsumed script %d (%s)", i, f.Scripts[i].On)
				break
			}
		}
		if len(clock.events) > 0 {
			device.problem = fmt.Errorf("%d unconsumed required steps", len(clock.events))
		}
	}
	result := replayResult{Operation: finished, Writes: device.recorded, Elapsed: clock.Now().Sub(device.start), FullReads: device.fullReads, ScalarReads: device.scalarReads, Problem: device.problem}
	t.Logf("elapsed=%s full=%d scalar=%d writes=%d", result.Elapsed, result.FullReads, result.ScalarReads, len(result.Writes))
	return result
}

func checkReplayResult(f replayFixture, r replayResult) error {
	if r.Problem != nil {
		return fmt.Errorf("%s: %w", f.Name, r.Problem)
	}
	if string(r.Operation.State) != f.Expect.State {
		return fmt.Errorf("%s: state=%s want=%s", f.Name, r.Operation.State, f.Expect.State)
	}
	var outcome struct{ Diagnostics struct{ Reason string } }
	if err := json.Unmarshal(r.Operation.Outcome, &outcome); err != nil || outcome.Diagnostics.Reason != f.Expect.Reason {
		return fmt.Errorf("%s: outcome=%s want reason=%s", f.Name, r.Operation.Outcome, f.Expect.Reason)
	}
	if f.Expect.EndMS != nil && r.Elapsed != time.Duration(*f.Expect.EndMS)*time.Millisecond {
		return fmt.Errorf("%s: elapsed=%s want=%dms", f.Name, r.Elapsed, *f.Expect.EndMS)
	}
	var levels []int
	queues := 0
	for _, w := range r.Writes {
		name, args := replayMutationRequest(w.Mutation)
		if name == "" || args["pid"] != "1" || w.Mutation.Kind == heos.MutationKindQueue && (args["sid"] != "900" || args["cid"] != "green") {
			return fmt.Errorf("%s: unexpected mutation or identity: %+v", f.Name, w.Mutation)
		}
		if w.Mutation.Kind == heos.MutationKindVolume {
			if len(f.Expect.VolumeAtMS) > 0 && (len(levels) >= len(f.Expect.VolumeAtMS) || w.At != time.Duration(f.Expect.VolumeAtMS[len(levels)])*time.Millisecond) {
				return fmt.Errorf("%s: volume %d written at %s outside its envelope", f.Name, w.Mutation.Level, w.At)
			}
			levels = append(levels, w.Mutation.Level)
		}
		if w.Mutation.Kind == heos.MutationKindTransport && w.Mutation.State == heos.PlayStateStop && f.Expect.StopAtMS != nil && w.At != time.Duration(*f.Expect.StopAtMS)*time.Millisecond {
			return fmt.Errorf("%s: Stop written at %s want %dms", f.Name, w.At, *f.Expect.StopAtMS)
		}
		if w.Mutation.Kind == heos.MutationKindQueue {
			queues++
		}
		for _, window := range f.Expect.NoWrites {
			if w.At >= time.Duration(window.FromMS)*time.Millisecond && w.At < time.Duration(window.UntilMS)*time.Millisecond {
				return fmt.Errorf("%s: write during forbidden interval: %+v", f.Name, w)
			}
		}
	}
	if !slices.Equal(levels, f.Expect.VolumeLevels) || queues != f.Expect.QueueWrites || len(r.Writes) > f.Expect.MaxWireWrites {
		return fmt.Errorf("%s: writes=%+v; want levels %v and %d queue writes", f.Name, r.Writes, f.Expect.VolumeLevels, f.Expect.QueueWrites)
	}
	if r.FullReads > f.Expect.MaxFullReads || r.ScalarReads != f.Expect.MaxScalarReads {
		return fmt.Errorf("%s: reads full=%d (max %d), scalar=%d (want %d)", f.Name, r.FullReads, f.Expect.MaxFullReads, r.ScalarReads, f.Expect.MaxScalarReads)
	}
	if r.Operation.State == journal.Succeeded {
		var progress PlaybackProgress
		if err := json.Unmarshal(r.Operation.Progress, &progress); err != nil || progress.StopAt.Sub(progress.PlaybackStartedAt) != time.Duration(f.Automation.DurationSeconds)*time.Second || progress.Level != 0 {
			return fmt.Errorf("%s: missing or extended playback envelope: %s", f.Name, r.Operation.Progress)
		}
		if len(r.Writes) == 0 {
			return fmt.Errorf("%s: successful run sent no commands", f.Name)
		}
		last := r.Writes[len(r.Writes)-1].Mutation
		if last.Kind != heos.MutationKindTransport || last.State != heos.PlayStateStop {
			return fmt.Errorf("%s: successful run did not end with Stop", f.Name)
		}
	}
	return nil
}

type replayDevice struct {
	*albumDevice
	clock                  *modeEventClock
	start                  time.Time
	fixture                replayFixture
	occurrences            map[string]int
	used                   []bool
	recorded               []replayWrite
	fullReads, scalarReads int
	problem                error
}

func (d *replayDevice) Refresh(ctx context.Context) error {
	d.fullReads++
	return d.albumDevice.Refresh(ctx)
}
func (d *replayDevice) RefreshScalars(ctx context.Context, _ heos.MutationKind) error {
	d.scalarReads++
	return context.Cause(ctx)
}
func (d *replayDevice) Write(ctx context.Context, m heos.Mutation, g heos.Guard) (heos.Response, error) {
	d.recorded = append(d.recorded, replayWrite{At: d.clock.Now().Sub(d.start), Mutation: m})
	name, args := replayMutationRequest(m)
	d.occurrences[name]++
	for i, script := range d.fixture.Scripts {
		if script.On != name || script.Occurrence != d.occurrences[name] {
			continue
		}
		d.used[i] = true
		for key, want := range script.Args {
			if args[key] != want {
				d.problem = fmt.Errorf("script %d request %s: %s=%q want=%q", i, name, key, args[key], want)
				return heos.Response{}, d.problem
			}
		}
		return d.scriptedWrite(ctx, i, script)
	}
	return d.albumDevice.Write(ctx, m, g)
}

func (d *replayDevice) scriptedWrite(ctx context.Context, index int, script replayScript) (heos.Response, error) {
	at := d.clock.Now()
	replied := false
	var replyErr error
	for stepIndex, step := range script.Steps {
		d.clock.events = append(d.clock.events, modeClockEvent{at: at.Add(time.Duration(step.AtMS) * time.Millisecond), fn: func() {
			switch step.Action {
			case "apply":
				d.mu.Lock()
				applyReplayPatch(&d.s, *step.Patch)
				d.mu.Unlock()
			case "event":
				params := url.Values{}
				for k, v := range step.Event.Params {
					params.Set(k, v)
				}
				d.handler((heos.Event{Command: step.Event.Command, Params: params, Gap: step.Event.Gap}).Decode())
			case "reply":
				replied = true
				if step.Result == "rejected" {
					replyErr = &heos.CommandError{Delivery: heos.Rejected, Cause: &heos.DeviceError{Code: 14}}
				}
			case "disconnect":
				d.mu.Lock()
				d.s.Connected = false
				d.mu.Unlock()
				d.handler(heos.Event{Gap: true, GapReason: "connection_closed"})
				replied = true
				replyErr = &heos.CommandError{Delivery: heos.Uncertain, Cause: heos.ErrClosed}
			default:
				d.problem = fmt.Errorf("script %d step %d: unsupported action %q", index, stepIndex, step.Action)
			}
		}})
	}
	slices.SortStableFunc(d.clock.events, func(a, b modeClockEvent) int { return a.at.Compare(b.at) })
	for !replied {
		if len(d.clock.events) == 0 {
			return heos.Response{}, fmt.Errorf("script %d missing reply", index)
		}
		if err := d.clock.Wait(ctx, d.clock.events[0].at.Sub(d.clock.Now()), nil); err != nil {
			if replyErr != nil {
				return heos.Response{}, replyErr
			}
			return heos.Response{}, err
		}
	}
	return heos.Response{}, replyErr
}
