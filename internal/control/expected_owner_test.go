package control

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func expectedOwnerFixture(t *testing.T) (*Coordinator, *memoryJournal, *fakeDevice, journal.Request, context.Context) {
	t.Helper()
	c, j, d, request := fixtureCoordinator(t)
	owner := journal.Operation{ID: "op_current_owner", Kind: "playback", Player: request.Player, Principal: request.Principal, DeviceKey: "serial", State: journal.Running, Revision: 1}
	j.ops[owner.ID] = owner
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct{})
	close(done)
	c.lanes[request.Player].run = &execution{id: owner.ID, op: owner, ctx: ctx, cancel: cancel, done: done}
	request.Key = "fenced-takeover"
	request.Endpoint = "/v1/players/room/mute"
	request.Body = json.RawMessage(`{"muted":true,"takeover":true,"expected_owner":"op_current_owner"}`)
	return c, j, d, request, ctx
}

func TestExpectedOwnerMismatchDoesNotCancelCurrentOwner(t *testing.T) {
	c, j, d, request, ownerCtx := expectedOwnerFixture(t)
	// The owner changed without a device-state change, so the same player
	// If-Match still succeeds. The independent owner fence must stop takeover.
	request.Body = json.RawMessage(`{"muted":true,"takeover":true,"expected_owner":"op_previous_owner"}`)
	_, err := c.Submit(context.Background(), request, Command{Kind: CommandKindMute, Muted: true, Takeover: true, ExpectedOwner: "op_previous_owner"})
	if !errors.Is(err, ErrPrecondition) {
		t.Fatalf("owner replacement accepted: %v", err)
	}
	if context.Cause(ownerCtx) != nil {
		t.Fatal("mismatched takeover cancelled the current worker")
	}
	d.mu.Lock()
	sent := len(d.writes)
	d.mu.Unlock()
	j.mu.Lock()
	count := len(j.ops)
	active := j.ops["op_current_owner"]
	j.mu.Unlock()
	if sent != 0 || count != 1 || active.FinishedAt != nil {
		t.Fatalf("owner fence mutated work: sent=%d operations=%d owner=%+v", sent, count, active)
	}
}

func TestExpectedOwnerMatchingTakeoverAndReplay(t *testing.T) {
	c, j, d, request, ownerCtx := expectedOwnerFixture(t)
	cmd := Command{Kind: CommandKindMute, Muted: true, Takeover: true, ExpectedOwner: "op_current_owner"}
	admitted, err := c.Submit(context.Background(), request, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if op := awaitOperation(t, j, admitted.ID); op.State != journal.Succeeded {
		t.Fatal(op)
	}
	if !errors.Is(context.Cause(ownerCtx), errSuperseded) {
		t.Fatal("matching takeover did not revoke old worker")
	}
	old, _ := j.Get(context.Background(), "op_current_owner")
	if old.State != journal.Released {
		t.Fatal(old)
	}
	var args struct {
		Command Command `json:"command"`
	}
	if json.Unmarshal(admitted.EffectiveArguments, &args) != nil || args.Command.ExpectedOwner != cmd.ExpectedOwner {
		t.Fatal("effective command lost owner fence")
	}
	d.mu.Lock()
	before := len(d.writes)
	d.mu.Unlock()
	// The expected owner no longer exists, and the revision is now stale. The
	// original idempotent request still returns its admitted result before both.
	replay, err := c.Submit(context.Background(), request, cmd)
	if err != nil || replay.ID != admitted.ID {
		t.Fatal(replay, err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != before {
		t.Fatal("takeover replay sent another mutation")
	}
}

func TestExpectedOwnerWithoutActiveOperationDoesNotWrite(t *testing.T) {
	c, j, d, request := fixtureCoordinator(t)
	_, err := c.Submit(context.Background(), request, Command{Kind: CommandKindVolume, Level: 10, Takeover: true, ExpectedOwner: "op_expired"})
	if !errors.Is(err, ErrPrecondition) {
		t.Fatal("missing owner accepted", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != 0 || len(j.ops) != 0 {
		t.Fatal("missing-owner takeover mutated work")
	}
}

func TestExpectedOwnerRequiresTakeoverAndBoundedSupportedInput(t *testing.T) {
	for _, cmd := range []Command{
		{Kind: CommandKindMute, Muted: true, ExpectedOwner: "op_owner"},
		{Kind: CommandKindMute, Muted: true, Takeover: true, ExpectedOwner: strings.Repeat("x", 129)},
		{Kind: CommandKindSkip, Direction: "next", Takeover: true, ExpectedOwner: "op_owner"},
	} {
		c, _, d, request := fixtureCoordinator(t)
		if _, err := c.Submit(context.Background(), request, cmd); !errors.Is(err, heos.ErrBounds) {
			t.Fatal("invalid owner fence accepted", err)
		}
		d.mu.Lock()
		sent := len(d.writes)
		d.mu.Unlock()
		if sent != 0 {
			t.Fatal("invalid owner fence sent a command")
		}
	}
}

func TestBufferedPolicyRejectedForDirectControl(t *testing.T) {
	c, j, d, request := fixtureCoordinator(t)
	_, err := c.Submit(context.Background(), request, Command{Kind: CommandKindMute, Muted: true, Buffered: &BufferedPlayback{}})
	if !errors.Is(err, heos.ErrBounds) {
		t.Fatal("buffer policy accepted for direct control", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.writes) != 0 || len(j.ops) != 0 {
		t.Fatal("invalid command reached admission or device")
	}
}
