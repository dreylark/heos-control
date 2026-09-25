package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func testBuffer() *BufferedPlayback {
	return &BufferedPlayback{PartTracks: []int{3, 2, 3}, RefillThreshold: 1, MaxQueueTracks: 8, RetainPrevious: 1, MaxSessionSeconds: 60}
}

func TestBufferedCaptureDefersFutureContent(t *testing.T) {
	c, _, d, _, cmd := multipartFixture(t)
	cmd.Buffered = testBuffer()
	cmd.ItemRefs[2] = "later"
	cat := dCatalog(c)
	cat.items["later"] = heos.Item{Source: "900", ContainerID: "later", Container: "yes"}
	// Not imported yet: only the descriptor must exist at admission.
	d.contents["later"] = nil
	_, members, parts, err := c.reads.resolvePlayback(context.Background(), c.lanes["room"].device, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(d.browsed, []heos.ID{"a", "b"}) {
		t.Fatalf("eager future traversal: %v", d.browsed)
	}
	if len(parts) != 3 || len(parts[2].IDs) != 0 || parts[2].Count != 3 || !members["three"] {
		t.Fatalf("parts=%+v members=%v", parts, members)
	}
	if len(d.writes) != 0 {
		t.Fatal("capture wrote to the device")
	}
	cmd.Buffered.PartTracks[2] = 1
	if parts[2].Count != 3 {
		t.Fatal("captured manifest aliases caller input")
	}
}

func dCatalog(c *Coordinator) multipartCatalog {
	return c.lanes["room"].device.Catalogs["music"].(multipartCatalog)
}

func TestBufferedSelectionBounds(t *testing.T) {
	for _, scenario := range []string{"missing_refs", "counts", "empty_part", "shuffle", "repeat", "capacity", "threshold", "history", "lifetime", "too_many_parts", "too_many_tracks"} {
		t.Run(scenario, func(t *testing.T) {
			c, _, d, _, cmd := multipartFixture(t)
			cmd.Buffered = testBuffer()
			switch scenario {
			case "missing_refs":
				cmd.ItemRefs = nil
				cmd.ItemRef = "a"
			case "counts":
				cmd.Buffered.PartTracks = []int{3}
			case "empty_part":
				cmd.Buffered.PartTracks[2] = 0
			case "shuffle":
				cmd.Shuffle = true
			case "repeat":
				cmd.Repeat = heos.Repeat("on_all")
			case "capacity":
				cmd.Buffered.MaxQueueTracks = 4
			case "threshold":
				cmd.Buffered.RefillThreshold = 0
			case "history":
				cmd.Buffered.RetainPrevious = -1
			case "lifetime":
				cmd.Buffered.MaxSessionSeconds = 86401
			case "too_many_parts":
				cmd.ItemRefs = make([]string, 257)
				cmd.Buffered.PartTracks = make([]int, 257)
				for i := range cmd.ItemRefs {
					cmd.ItemRefs[i] = "a"
					cmd.Buffered.PartTracks[i] = 3
				}
			case "too_many_tracks":
				cmd.ItemRefs = make([]string, 256)
				cmd.Buffered.PartTracks = make([]int, 256)
				cmd.Buffered.MaxQueueTracks = 100
				for i := range cmd.ItemRefs {
					cmd.ItemRefs[i] = "a"
					cmd.Buffered.PartTracks[i] = 50
				}
			}
			if _, _, _, err := c.reads.resolvePlayback(context.Background(), c.lanes["room"].device, cmd); !errors.Is(err, heos.ErrBounds) {
				t.Fatalf("err=%v", err)
			}
			if len(d.writes) != 0 {
				t.Fatal(d.writes)
			}
		})
	}
}

func TestBufferedPositionUsesOccurrenceAndConfirmedQueue(t *testing.T) {
	ids := []heos.ID{"same", "different", "same"}
	total := 3
	q := heos.QueuePage{Items: []heos.Media{
		{Source: "1024", ID: "same", QueueID: "entry-a"},
		{Source: "1024", ID: "different", QueueID: "entry-b"},
		{Source: "1024", ID: "same", QueueID: "entry-c"},
	}, Total: &total}
	s := heos.Snapshot{State: heos.PlayStatePlay, Queue: q, Media: &q.Items[2]}
	if pos, err := bufferedPosition(s, ids); err != nil || pos != 2 {
		t.Fatalf("pos=%d err=%v", pos, err)
	}
	s.State = heos.PlayStateUnknown
	if _, err := bufferedPosition(s, ids); err == nil {
		t.Fatal("unknown state accepted")
	}
	s.State = heos.PlayStatePlay
	foreign := heos.Media{Source: "1024", ID: "same", QueueID: "outside"}
	s.Media = &foreign
	if _, err := bufferedPosition(s, ids); err == nil {
		t.Fatal("foreign occurrence accepted")
	}
}

func TestBufferedTenThousandTrackManifestFitsDurableBounds(t *testing.T) {
	c, j, d, req, cmd := multipartFixture(t)
	cmd.Buffered = &BufferedPlayback{RefillThreshold: 10, MaxQueueTracks: 100, RetainPrevious: 5, MaxSessionSeconds: 1}
	cmd.ItemRefs = nil
	cat := dCatalog(c)
	for i := range 201 {
		ref := fmt.Sprintf("ref_%027d", i)
		count := 50
		if i == 0 {
			count = 30
		}
		if i == 200 {
			count = 20
		}
		cmd.ItemRefs = append(cmd.ItemRefs, ref)
		cmd.Buffered.PartTracks = append(cmd.Buffered.PartTracks, count)
		cid := heos.ID(fmt.Sprintf("part-%d", i))
		cat.items[ref] = heos.Item{Source: "900", ContainerID: cid, Container: "yes"}
		if i < 2 {
			for range count {
				d.contents[cid] = append(d.contents[cid], heos.Item{Source: "900", ContainerID: cid, MediaID: "repeated", Type: "song", Playable: "yes"})
			}
		}
	}
	req.Body, _ = json.Marshal(cmd)
	if len(req.Body) > journal.MaxJSONBytes {
		t.Fatalf("request bytes=%d", len(req.Body))
	}
	admitted, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, admitted.ID)
	c.Close()
	if len(o.EffectiveArguments) > journal.MaxJSONBytes {
		t.Fatalf("durable bytes=%d", len(o.EffectiveArguments))
	}
	progress := ProjectOperation(o).QueueLoading
	if o.State != journal.Released || o.ErrorCode != "session_expired" || progress.TotalTracks != 10000 || progress.TotalParts != 201 || progress.ConfirmedTracks != 30 {
		t.Fatalf("%s %s %+v", o.State, o.ErrorCode, progress)
	}
	if len(d.browsed) != 2 {
		t.Fatalf("future enumeration: %v", d.browsed)
	}
}

func TestBufferedPreparationRejectsChangedCatalogAndMissingFutureContent(t *testing.T) {
	for _, scenario := range []string{"missing", "count", "unplayable", "catalog", "connection"} {
		t.Run(scenario, func(t *testing.T) {
			c, _, d, _, cmd := multipartFixture(t)
			cmd.Buffered = testBuffer()
			_, _, parts, err := c.reads.resolvePlayback(context.Background(), c.lanes["room"].device, cmd)
			if err != nil {
				t.Fatal(err)
			}
			parts[2].IDs = nil
			device := c.lanes["room"].device
			client := &bufferedViewDevice{multipartDevice: d, view: d.View()}
			device.Client = client
			switch scenario {
			case "missing":
				d.contents["a"] = nil
			case "count":
				d.contents["a"] = d.contents["a"][:2]
			case "unplayable":
				d.contents["a"][0].Playable = "no"
			case "catalog":
				client.view.Token.Catalog++
			case "connection":
				client.view.Connected = false
			}
			if err := prepareBufferedPart(context.Background(), device, &parts[2]); err == nil {
				t.Fatal("unsafe future content accepted")
			}
			if len(d.writes) != 0 {
				t.Fatal("preparation changed playback")
			}
		})
	}
}

func TestBufferedFutureFailureLeavesConfirmedAudioPlaying(t *testing.T) {
	c, j, d, req, cmd, clock := bufferedFixture(t)
	d.contents["a"] = append([]heos.Item(nil), d.contents["a"]...)
	clock.events = []modeClockEvent{
		{at: clock.Now().Add(time.Second), fn: func() { bufferedMove(d, 1) }},
		{at: clock.Now().Add(2 * time.Second), fn: func() { d.contents["a"] = nil; bufferedMove(d, 3) }},
	}
	a, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	o := awaitOperation(t, j, a.ID)
	c.Close()
	if o.State != journal.Failed || len(d.s.Queue.Items) != 5 || d.s.State != heos.PlayStatePlay || !strings.Contains(string(o.Outcome), `"playback_may_continue":true`) {
		t.Fatalf("%s queue=%d state=%s %s", o.State, len(d.s.Queue.Items), d.s.State, o.Outcome)
	}
}

type bufferedViewDevice struct {
	*multipartDevice
	view heos.View
}

func (d *bufferedViewDevice) View() heos.View { return d.view }
