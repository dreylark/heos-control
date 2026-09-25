package control

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

func ownedSkipFixture(t *testing.T) (*Coordinator, *memoryJournal, *skipDevice, journal.Request, *execution) {
	t.Helper()
	c, j, d, req := skipFixture(t, "", 0, 1)
	l := c.lanes["room"]
	ownerReq := req
	ownerReq.Key = "owner"
	a, err := j.Admit(context.Background(), ownerReq, journal.Proposal{Kind: "playback", DeviceKey: l.device.Config.Serial, EffectiveArguments: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(c.ctx)
	r := &execution{id: a.Operation.ID, ctx: ctx, cancel: cancel, done: make(chan struct{}), op: a.Operation,
		expected: d.Snapshot(), events: map[string][]map[string]string{}, bounded: true, queueOwned: true,
		buffered: &bufferedPlayback{}, automating: true, now: c.clock.Now, wake: make(chan struct{}, 1),
		metrics: &operationMetrics{player: l.device.Metrics, kind: "playback"}}
	l.run = r
	t.Cleanup(func() { c.closeBufferedSkips(r); cancel(nil) })
	return c, j, d, skipRequest(c, req), r
}

func TestBufferedSkipRetainsOwnerAndHasSeparateReplayableResult(t *testing.T) {
	c, j, d, req, parent := ownedSkipFixture(t)
	cmd := Command{Kind: CommandKindSkip, Direction: "next"}
	child, err := c.Submit(context.Background(), req, cmd)
	if err != nil || child.ID == parent.id {
		t.Fatal(child, err)
	}
	if len(d.writes) != 0 {
		t.Fatal("admission sent a device command", d.writes)
	}
	active, err := j.Active(context.Background(), "room")
	if err != nil || active.ID != parent.id {
		t.Fatal(active, err)
	}
	handled, err := c.serviceBufferedSkip(c.lanes["room"], parent)
	if err != nil || !handled {
		t.Fatal(handled, err)
	}
	result := awaitOperation(t, j, child.ID)
	if result.State != journal.Succeeded || len(d.writes) != 1 || d.writes[0].Kind != heos.MutationKindSkip {
		t.Fatal(result, d.writes)
	}
	var evidence struct {
		Diagnostics struct {
			Reason string `json:"reason"`
		} `json:"diagnostics"`
	}
	if err = json.Unmarshal(result.Outcome, &evidence); err != nil || evidence.Diagnostics.Reason != "completed" {
		t.Fatal("child completion carried incorrect diagnostics", evidence, err)
	}
	active, err = j.Active(context.Background(), "room")
	if err != nil || active.ID != parent.id || active.FinishedAt != nil {
		t.Fatal(active, err)
	}
	replay, err := c.Submit(context.Background(), req, cmd)
	if err != nil || replay.ID != child.ID || len(d.writes) != 1 {
		t.Fatal(replay, err, d.writes)
	}
	if context.Cause(parent.ctx) != nil || c.lanes["room"].run != parent {
		t.Fatal("skip revoked playback owner")
	}
}

func TestBufferedSkipBoundsPendingWorkAndPrincipal(t *testing.T) {
	c, j, d, req, parent := ownedSkipFixture(t)
	cmd := Command{Kind: CommandKindSkip, Direction: "next"}
	other := req
	other.Principal = "another-caller"
	if _, err := c.Submit(context.Background(), other, cmd); !errors.Is(err, journal.ErrBusy) {
		t.Fatal(err)
	}
	child, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	req.Key = "next-child"
	if _, err = c.Submit(context.Background(), req, cmd); !errors.Is(err, heos.ErrQueueFull) {
		t.Fatal(err)
	}
	c.closeBufferedSkips(parent)
	result := awaitOperation(t, j, child.ID)
	if result.State != journal.Released || len(d.writes) != 0 {
		t.Fatal(result, d.writes)
	}
	var outcome struct {
		Delivery string `json:"delivery"`
	}
	if err = json.Unmarshal(result.Outcome, &outcome); err != nil || outcome.Delivery != "not_sent" {
		t.Fatal(outcome, err)
	}
}

type closeDuringOwnedAdmission struct {
	*memoryJournal
	closed func()
}

func (j closeDuringOwnedAdmission) Admit(ctx context.Context, r journal.Request, p journal.Proposal) (journal.Admission, error) {
	a, err := j.memoryJournal.Admit(ctx, r, p)
	if p.OwnerID != "" {
		j.closed()
	}
	return a, err
}

func TestBufferedSkipParentClosesDuringAdmissionCommit(t *testing.T) {
	c, j, d, req, parent := ownedSkipFixture(t)
	c.db = closeDuringOwnedAdmission{memoryJournal: j, closed: func() { parent.cancel(errSuperseded); c.closeBufferedSkips(parent) }}
	child, err := c.Submit(context.Background(), req, Command{Kind: CommandKindSkip, Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitOperation(t, j, child.ID)
	if result.State != journal.Released || len(d.writes) != 0 {
		t.Fatal(result, d.writes)
	}
	if handled, err := c.serviceBufferedSkip(c.lanes["room"], parent); handled || err != nil {
		t.Fatal(handled, err)
	}
}

type uncertainOwnedSkipWriter struct{ *skipDevice }

func (w uncertainOwnedSkipWriter) Write(_ context.Context, m heos.Mutation, _ heos.Guard) (heos.Response, error) {
	w.mu.Lock()
	w.writes = append(w.writes, m)
	w.mu.Unlock()
	return heos.Response{}, &heos.CommandError{Delivery: heos.Uncertain, Cause: io.ErrUnexpectedEOF}
}

func TestBufferedSkipUncertainSendIsNeverReplayed(t *testing.T) {
	c, j, d, req, parent := ownedSkipFixture(t)
	c.lanes["room"].writer = uncertainOwnedSkipWriter{d}
	cmd := Command{Kind: CommandKindSkip, Direction: "next"}
	child, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := c.serviceBufferedSkip(c.lanes["room"], parent); !handled || err == nil {
		t.Fatal(handled, err)
	}
	result := awaitOperation(t, j, child.ID)
	if result.State != journal.Uncertain || len(d.writes) != 1 {
		t.Fatal(result, d.writes)
	}
	c.closeBufferedSkips(parent)
	replay, err := c.Submit(context.Background(), req, cmd)
	if err != nil || replay.ID != child.ID || len(d.writes) != 1 {
		t.Fatal(replay, err, d.writes)
	}
}

func TestBufferedSkipCancellationBeforeDispatchDoesNotSend(t *testing.T) {
	c, j, d, req, parent := ownedSkipFixture(t)
	child, err := c.Submit(context.Background(), req, Command{Kind: CommandKindSkip, Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	parent.cancel(errSuperseded)
	if handled, err := c.serviceBufferedSkip(c.lanes["room"], parent); !handled || !errors.Is(err, errSuperseded) {
		t.Fatal(handled, err)
	}
	result := awaitOperation(t, j, child.ID)
	if result.State != journal.Released || len(d.writes) != 0 {
		t.Fatal(result, d.writes)
	}
}

type rejectOwnedAdmission struct{ *memoryJournal }

func (j rejectOwnedAdmission) Admit(context.Context, journal.Request, journal.Proposal) (journal.Admission, error) {
	return journal.Admission{}, journal.ErrCapacity
}

func TestBufferedSkipFailedAdmissionReleasesPendingSlot(t *testing.T) {
	c, j, d, req, parent := ownedSkipFixture(t)
	c.db = rejectOwnedAdmission{j}
	cmd := Command{Kind: CommandKindSkip, Direction: "next"}
	if _, err := c.Submit(context.Background(), req, cmd); !errors.Is(err, journal.ErrCapacity) {
		t.Fatal(err)
	}
	parent.mu.Lock()
	pending := parent.bufferedSkip
	parent.mu.Unlock()
	if pending != nil || len(d.writes) != 0 {
		t.Fatal("failed admission retained pending work", pending, d.writes)
	}
	c.db = j
	child, err := c.Submit(context.Background(), req, cmd)
	if err != nil {
		t.Fatal(err)
	}
	c.closeBufferedSkips(parent)
	if result := awaitOperation(t, j, child.ID); result.State != journal.Released {
		t.Fatal(result)
	}
}

func TestBufferedSkipAtNewBoundaryOnlyFailsChild(t *testing.T) {
	c, j, d, req, parent := ownedSkipFixture(t)
	child, err := c.Submit(context.Background(), req, Command{Kind: CommandKindSkip, Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	// Natural progress can reach the resident boundary after admission but
	// before this worker gets its turn; it must not skip outside its buffer.
	d.mu.Lock()
	last := d.tracks[len(d.tracks)-1]
	d.s.Media = &last
	d.mu.Unlock()
	handled, err := c.serviceBufferedSkip(c.lanes["room"], parent)
	if !handled || err != nil {
		t.Fatal(handled, err)
	}
	result := awaitOperation(t, j, child.ID)
	if result.State != journal.Failed || result.ErrorCode != "not_skippable" || len(d.writes) != 0 {
		t.Fatal(result, d.writes)
	}
	if context.Cause(parent.ctx) != nil {
		t.Fatal("boundary revoked parent", context.Cause(parent.ctx))
	}
	parent.mu.Lock()
	observed := parent.expected.Media.QueueID
	parent.mu.Unlock()
	if observed != last.QueueID {
		t.Fatal("boundary retained stale position and could delay refill", observed, last.QueueID)
	}
}

type blockedOwnedAdmission struct {
	*memoryJournal
	committed chan struct{}
	proceed   chan struct{}
}

func (j blockedOwnedAdmission) Admit(ctx context.Context, r journal.Request, p journal.Proposal) (journal.Admission, error) {
	a, err := j.memoryJournal.Admit(ctx, r, p)
	if p.OwnerID != "" {
		close(j.committed)
		<-j.proceed
	}
	return a, err
}

func TestBufferedSkipShutdownWaitsForCommittedAdmission(t *testing.T) {
	c, j, d, req, _ := ownedSkipFixture(t)
	admission := blockedOwnedAdmission{memoryJournal: j, committed: make(chan struct{}), proceed: make(chan struct{})}
	c.db = admission
	release := sync.OnceFunc(func() { close(admission.proceed) })
	defer release()
	type result struct {
		operation journal.Operation
		err       error
	}
	finished := make(chan result, 1)
	go func() {
		op, err := c.Submit(context.Background(), req, Command{Kind: CommandKindSkip, Direction: "next"})
		finished <- result{op, err}
	}()
	select {
	case <-admission.committed:
	case <-time.After(time.Second):
		t.Fatal("child admission did not commit")
	}
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	select {
	case <-c.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel ownership")
	}
	select {
	case <-closed:
		t.Fatal("shutdown returned while a committed child was still unpublished")
	default:
	}
	release()
	var got result
	select {
	case got = <-finished:
	case <-time.After(time.Second):
		t.Fatal("child admission did not finish")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not join child admission")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	op, err := j.Get(context.Background(), got.operation.ID)
	if err != nil || op.FinishedAt == nil || op.State != journal.Released || len(d.writes) != 0 {
		t.Fatal(op, err, d.writes)
	}
}
