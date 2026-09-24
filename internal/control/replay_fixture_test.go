package control

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dreylark/heos-control/internal/heos"
)

// These fixtures describe device behavior, not executable controller policies.
// Denon 3.1/3.2 and 5.4–5.11 define requests, replies and independent events.
type replayFixture struct {
	Version    int               `json:"version"`
	Name       string            `json:"name"`
	Family     string            `json:"family"`
	Provenance string            `json:"provenance"`
	Source     string            `json:"source"`
	Model      string            `json:"model"`
	Firmware   *string           `json:"firmware"`
	Notes      []string          `json:"notes"`
	Initial    replayInitial     `json:"initial"`
	Automation Automation        `json:"automation"`
	Scripts    []replayScript    `json:"scripts"`
	Expect     replayExpectation `json:"expect"`
}
type replayInitial struct {
	State   string `json:"state"`
	Volume  int    `json:"volume"`
	Muted   bool   `json:"muted"`
	Repeat  string `json:"repeat"`
	Shuffle bool   `json:"shuffle"`
}
type replayScript struct {
	On         string            `json:"on"`
	Occurrence int               `json:"occurrence"`
	Args       map[string]string `json:"args"`
	Steps      []replayStep      `json:"steps"`
}
type replayStep struct {
	AtMS   int          `json:"at_ms"`
	Action string       `json:"action"`
	Patch  *replayPatch `json:"patch,omitempty"`
	Event  *replayEvent `json:"event,omitempty"`
	Result string       `json:"result,omitempty"`
}
type replayPatch struct {
	State   *string      `json:"state,omitempty"`
	Volume  *int         `json:"volume,omitempty"`
	Muted   *bool        `json:"muted,omitempty"`
	Repeat  *string      `json:"repeat,omitempty"`
	Shuffle *bool        `json:"shuffle,omitempty"`
	Grouped *bool        `json:"grouped,omitempty"`
	Queue   string       `json:"queue,omitempty"`
	Media   *replayMedia `json:"media,omitempty"`
}
type replayMedia struct {
	MID    string `json:"mid"`
	QID    string `json:"qid"`
	Source string `json:"source"`
}
type replayEvent struct {
	Command string            `json:"command"`
	Params  map[string]string `json:"params"`
	Gap     bool              `json:"gap,omitempty"`
}
type replayWindow struct {
	FromMS  int `json:"from_ms"`
	UntilMS int `json:"until_ms"`
}
type replayExpectation struct {
	QueueWrites    int            `json:"queue_writes"`
	State          string         `json:"state"`
	Reason         string         `json:"reason"`
	VolumeLevels   []int          `json:"volume_levels"`
	VolumeAtMS     []int          `json:"volume_at_ms,omitempty"`
	StopAtMS       *int           `json:"stop_at_ms,omitempty"`
	MaxFullReads   int            `json:"max_full_reads"`
	MaxScalarReads int            `json:"max_scalar_reads"`
	MaxWireGets    int            `json:"max_wire_gets"`
	MaxWireWrites  int            `json:"max_wire_writes"`
	EndMS          *int           `json:"end_ms,omitempty"`
	NoWrites       []replayWindow `json:"no_writes,omitempty"`
}

func loadReplayFixtures(t *testing.T) []replayFixture {
	t.Helper()
	paths, err := filepath.Glob("testdata/replay/*.json")
	if err != nil || len(paths) == 0 {
		t.Fatal("replay corpus missing", err)
	}
	var out []replayFixture
	names := map[string]bool{}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		fixture, err := decodeReplayFixture(data)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if names[fixture.Name] {
			t.Fatal("duplicate replay name", fixture.Name)
		}
		names[fixture.Name] = true
		out = append(out, fixture)
	}
	return out
}

func namedReplayFixture(t *testing.T, name string) replayFixture {
	t.Helper()
	for _, f := range loadReplayFixtures(t) {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("required replay fixture %q missing", name)
	return replayFixture{}
}

func TestReplayFixtureValidation(t *testing.T) {
	valid := []byte(`{"version":1,"name":"test","family":"mode","provenance":"synthetic","source":"docs/DEVICE_COMPATIBILITY.md","model":"synthetic","notes":["Synthetic baseline"],"initial":{"state":"stop","volume":20,"repeat":"off"},"automation":{"target_level":12,"ramp_seconds":1,"duration_seconds":3,"fade_seconds":1},"scripts":[{"on":"player/set_play_mode","occurrence":1,"steps":[{"at_ms":0,"action":"reply","result":"success"}]}],"expect":{"state":"succeeded","reason":"completed","volume_levels":[10,11,12,9,6,3,0],"max_full_reads":10,"max_wire_gets":100,"max_wire_writes":11}}`)
	if _, err := decodeReplayFixture(valid); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func([]byte) []byte
	}{
		{"unknown-field", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1,"address":"live-device"`), 1)
		}},
		{"version", func(b []byte) []byte { return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":2`), 1) }},
		{"negative-time", func(b []byte) []byte { return bytes.Replace(b, []byte(`"at_ms":0`), []byte(`"at_ms":-1`), 1) }},
		{"action", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"action":"reply"`), []byte(`"action":"eval"`), 1)
		}},
		{"no-reply", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"action":"reply","result":"success"`), []byte(`"action":"apply","patch":{"state":"play"}`), 1)
		}},
		{"oversize", func(b []byte) []byte { return append(b, bytes.Repeat([]byte(" "), 256*1024)...) }},
		{"trailing-data", func(b []byte) []byte { return append(b, []byte(` {}`)...) }},
		{"missing-provenance", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"provenance":"synthetic"`), []byte(`"provenance":""`), 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeReplayFixture(tc.change(slices.Clone(valid))); err == nil {
				t.Fatal("invalid replay accepted")
			}
		})
	}
}

func TestReplayRejectsInvalidTimeline(t *testing.T) {
	for _, kind := range []string{"duplicate-anchor", "missing-steps", "two-replies", "too-many-steps", "backwards-time", "invalid-queue", "incomplete-media", "invalid-window"} {
		t.Run(kind, func(t *testing.T) {
			f := namedReplayFixture(t, "confirmation-fallback")
			s := &f.Scripts[0]
			switch kind {
			case "duplicate-anchor":
				f.Scripts = append(f.Scripts, *s)
			case "missing-steps":
				s.Steps = nil
			case "two-replies":
				s.Steps = append(s.Steps, replayStep{Action: "reply", Result: "success"})
			case "too-many-steps":
				for range 256 {
					s.Steps = append(s.Steps, replayStep{Action: "apply", Patch: &replayPatch{Queue: "selected"}})
				}
			case "backwards-time":
				s.Steps = append(s.Steps, replayStep{AtMS: 10, Action: "apply", Patch: &replayPatch{Queue: "selected"}}, replayStep{AtMS: 9, Action: "apply", Patch: &replayPatch{Queue: "selected"}})
			case "invalid-queue":
				s.Steps[0].Patch = &replayPatch{Queue: "unrecognized"}
			case "incomplete-media":
				s.Steps[0].Patch = &replayPatch{Media: &replayMedia{MID: "track-1"}}
			case "invalid-window":
				f.Expect.NoWrites = []replayWindow{{FromMS: 10, UntilMS: 9}}
			}
			data, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeReplayFixture(data); err == nil {
				t.Fatal("invalid replay accepted")
			}
		})
	}
}

func replaySelectedQueue() []heos.Media {
	return []heos.Media{{Source: "1024", ID: "track-1", QueueID: "1", Album: "Synthetic album"}, {Source: "1024", ID: "track-2", QueueID: "2", Album: "Synthetic album"}}
}

func TestReplayMatchesActualMutationIdentity(t *testing.T) {
	name, args := replayMutationRequest(heos.Mutation{Kind: heos.MutationKindQueue, Player: "other-player", Item: heos.Item{Source: "other-server", ContainerID: "other-album"}})
	if name != "browse/add_to_queue" || args["pid"] != "other-player" || args["sid"] != "other-server" || args["cid"] != "other-album" {
		t.Fatal("replay concealed the actual mutation identity", name, args)
	}
}

func applyReplayPatch(s *heos.Snapshot, p replayPatch) {
	if p.State != nil {
		s.State = heos.PlayState(*p.State)
	}
	if p.Volume != nil {
		v := *p.Volume
		s.Volume = &v
	}
	if p.Muted != nil {
		v := *p.Muted
		s.Muted = &v
	}
	if p.Repeat != nil {
		s.Repeat = heos.Repeat(*p.Repeat)
	}
	if p.Shuffle != nil {
		s.Shuffle = *p.Shuffle
	}
	if p.Grouped != nil {
		s.Grouped = *p.Grouped
	}
	if p.Queue != "" {
		items := replaySelectedQueue()
		if p.Queue == "old" {
			items = []heos.Media{{Source: "1024", ID: "previous-track", QueueID: "1"}}
		}
		n := len(items)
		s.Queue = heos.QueuePage{Items: items, Total: &n}
	}
	if p.Media != nil {
		s.Media = &heos.Media{Source: heos.ID(p.Media.Source), ID: heos.ID(p.Media.MID), QueueID: heos.ID(p.Media.QID)}
	}
}

// Keep request matching independent of the production encoder and policies.
func replayMutationRequest(m heos.Mutation) (string, map[string]string) {
	args := map[string]string{"pid": string(m.Player)}
	switch m.Kind {
	case heos.MutationKindVolume:
		args["level"] = fmt.Sprint(m.Level)
		return "player/set_volume", args
	case heos.MutationKindMute:
		args["state"] = "off"
		if m.Muted {
			args["state"] = "on"
		}
		return "player/set_mute", args
	case heos.MutationKindMode:
		args["repeat"] = string(m.Repeat)
		args["shuffle"] = "off"
		if m.Shuffle {
			args["shuffle"] = "on"
		}
		return "player/set_play_mode", args
	case heos.MutationKindTransport:
		args["state"] = string(m.State)
		return "player/set_play_state", args
	case heos.MutationKindQueue:
		args["sid"], args["cid"], args["aid"] = string(m.Item.Source), string(m.Item.ContainerID), "4"
		return "browse/add_to_queue", args
	default:
		return "", nil
	}
}

// Keep input bounded and steps causal; a fixture is data, not a script language.
func decodeReplayFixture(data []byte) (replayFixture, error) {
	var f replayFixture
	if len(data) > 256*1024 {
		return f, fmt.Errorf("fixture exceeds 256 KiB")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&f); err != nil {
		return f, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return f, fmt.Errorf("trailing fixture data")
	}
	if f.Version != 1 || f.Name == "" || f.Family == "" || f.Model == "" || f.Source == "" || len(f.Notes) == 0 || !slices.Contains([]string{"captured", "reconstructed", "synthetic"}, f.Provenance) {
		return f, fmt.Errorf("missing or unsupported fixture metadata")
	}
	if !slices.Contains([]string{"stop", "pause"}, f.Initial.State) || f.Initial.Volume < 0 || f.Initial.Volume > 40 || f.Initial.Repeat != string(heos.RepeatOff) {
		return f, fmt.Errorf("invalid initial state")
	}
	ceiling := 40
	if err := f.Automation.validate(Command{Kind: CommandKindPlayback, Level: 10, Repeat: heos.RepeatOff}, &ceiling); err != nil {
		return f, err
	}
	if f.Expect.State == "" || f.Expect.Reason == "" || f.Expect.QueueWrites < 0 || f.Expect.QueueWrites > 1 || f.Expect.MaxFullReads < 0 || f.Expect.MaxScalarReads < 0 || f.Expect.MaxWireGets < 0 || f.Expect.MaxWireWrites < 1 {
		return f, fmt.Errorf("missing or invalid expectations")
	}
	if len(f.Expect.VolumeAtMS) != 0 && len(f.Expect.VolumeAtMS) != len(f.Expect.VolumeLevels) {
		return f, fmt.Errorf("volume timestamps must match expected levels")
	}
	for i, at := range f.Expect.VolumeAtMS {
		if at < 0 || i > 0 && at < f.Expect.VolumeAtMS[i-1] {
			return f, fmt.Errorf("invalid volume timestamp order")
		}
	}
	if f.Expect.StopAtMS != nil && *f.Expect.StopAtMS < 0 || f.Expect.EndMS != nil && *f.Expect.EndMS < 0 {
		return f, fmt.Errorf("negative expected stop/completion time")
	}
	for _, w := range f.Expect.NoWrites {
		if w.FromMS < 0 || w.UntilMS <= w.FromMS {
			return f, fmt.Errorf("invalid write exclusion window")
		}
	}
	count := 0
	seen := map[string]bool{}
	for _, s := range f.Scripts {
		key := fmt.Sprintf("%s/%d", s.On, s.Occurrence)
		if !slices.Contains([]string{"player/set_volume", "player/set_mute", "player/set_play_mode", "player/set_play_state", "browse/add_to_queue"}, s.On) || s.Occurrence < 1 || seen[key] {
			return f, fmt.Errorf("invalid or duplicate script anchor %s", key)
		}
		seen[key] = true
		last, terminal := -1, 0
		for _, step := range s.Steps {
			count++
			if count > 256 || step.AtMS < last || step.AtMS < 0 || step.AtMS > 7200000 {
				return f, fmt.Errorf("invalid step count/time")
			}
			last = step.AtMS
			switch step.Action {
			case "apply":
				if step.Patch == nil || step.Event != nil || step.Result != "" {
					return f, fmt.Errorf("invalid apply step")
				}
				p := step.Patch
				if p.Queue != "" && p.Queue != "old" && p.Queue != "selected" || p.State != nil && !slices.Contains([]string{"stop", "pause", "play", "unknown"}, *p.State) || p.Volume != nil && (*p.Volume < 0 || *p.Volume > 100) {
					return f, fmt.Errorf("invalid state patch")
				}
				if p.Media != nil && (p.Media.MID == "" || p.Media.QID == "" || p.Media.Source == "") {
					return f, fmt.Errorf("incomplete media patch")
				}
			case "event":
				if step.Event == nil || step.Patch != nil || step.Result != "" || !step.Event.Gap && !strings.HasPrefix(step.Event.Command, "event/") {
					return f, fmt.Errorf("invalid event step")
				}
			case "reply":
				if !slices.Contains([]string{"success", "rejected"}, step.Result) || step.Patch != nil || step.Event != nil {
					return f, fmt.Errorf("invalid reply step")
				}
				terminal++
			case "disconnect":
				terminal++
				if step.Patch != nil || step.Event != nil || step.Result != "" {
					return f, fmt.Errorf("invalid disconnect")
				}
			default:
				return f, fmt.Errorf("unknown replay action %q", step.Action)
			}
		}
		if terminal != 1 {
			return f, fmt.Errorf("script requires exactly one reply or disconnect")
		}
	}
	return f, nil
}
