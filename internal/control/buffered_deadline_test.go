package control

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

type bufferedDeadlineJournal struct {
	*memoryJournal
	ready      func()
	transition func(string, journal.Update)
}

func (j bufferedDeadlineJournal) Ready(ctx context.Context) error {
	if j.ready != nil {
		j.ready()
	}
	return j.memoryJournal.Ready(ctx)
}

func (j bufferedDeadlineJournal) Transition(ctx context.Context, id string, revision int64, update journal.Update) (journal.Operation, error) {
	op, err := j.memoryJournal.Transition(ctx, id, revision, update)
	if err == nil && j.transition != nil {
		j.transition(id, update)
	}
	return op, err
}

func TestBufferedSkipCannotSendPastDeadlineAfterJournalIO(t *testing.T) {
	for _, stage := range []string{"health", "sending-phase"} {
		t.Run(stage, func(t *testing.T) {
			c, j, d, req, parent := ownedSkipFixture(t)
			clock := &advancingClock{now: time.Now()}
			c.clock = clock
			parent.playbackDeadline = clock.Now().Add(time.Second)
			parent.buffered.expires = parent.playbackDeadline
			child, err := c.Submit(context.Background(), req, Command{Kind: CommandKindSkip, Direction: "next"})
			if err != nil {
				t.Fatal(err)
			}
			advance := func() {
				if err := clock.Wait(context.Background(), 2*time.Second, nil); err != nil {
					t.Error(err)
				}
			}
			wrapped := bufferedDeadlineJournal{memoryJournal: j}
			if stage == "health" {
				wrapped.ready = advance
			} else {
				wrapped.transition = func(id string, update journal.Update) {
					if id == parent.id && update.Phase == "sending_skip" {
						advance()
					}
				}
			}
			c.db = wrapped
			handled, err := c.serviceBufferedSkip(c.lanes["room"], parent)
			if err != nil || !handled {
				t.Fatal(handled, err)
			}
			result := awaitOperation(t, j, child.ID)
			var outcome struct {
				Delivery string `json:"delivery"`
			}
			if err = json.Unmarshal(result.Outcome, &outcome); err != nil {
				t.Fatal(err)
			}
			if result.State != journal.Released || outcome.Delivery != "not_sent" || len(d.writes) != 0 {
				t.Fatal("skip escaped playback deadline", result, outcome, d.writes)
			}
			if context.Cause(parent.ctx) != nil {
				t.Fatal("unsent expired navigation cancelled original Stop", context.Cause(parent.ctx))
			}
		})
	}
}

func TestBufferedSkipConfirmationCannotOutlivePlaybackDeadline(t *testing.T) {
	c, j, d, req, parent := ownedSkipFixture(t)
	clock := &advancingClock{now: time.Now()}
	c.clock = clock
	parent.playbackDeadline = clock.Now().Add(time.Second)
	parent.buffered.expires = parent.playbackDeadline
	// The native command is acknowledged, but now-playing never settles.
	d.before = nil
	child, err := c.Submit(context.Background(), req, Command{Kind: CommandKindSkip, Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	handled, err := c.serviceBufferedSkip(c.lanes["room"], parent)
	if !handled || err == nil {
		t.Fatal(handled, err)
	}
	result := awaitOperation(t, j, child.ID)
	if result.State != journal.Uncertain || len(d.writes) != 1 {
		t.Fatal(result, d.writes)
	}
	if clock.Now().After(parent.playbackDeadline) {
		t.Fatal("skip confirmation extended original playback window", clock.Now(), parent.playbackDeadline)
	}
}

func TestBufferedSkipSendGuardExpiresWithPlaybackWindow(t *testing.T) {
	c, j, d, req, parent := ownedSkipFixture(t)
	parent.playbackDeadline = c.clock.Now().Add(time.Second)
	parent.buffered.expires = parent.playbackDeadline
	writer := &bufferedSkipGuardWriter{Writer: d}
	c.lanes["room"].writer = writer
	child, err := c.Submit(context.Background(), req, Command{Kind: CommandKindSkip, Direction: "next"})
	if err != nil {
		t.Fatal(err)
	}
	if handled, err := c.serviceBufferedSkip(c.lanes["room"], parent); !handled || err != nil {
		t.Fatal(handled, err)
	}
	if result := awaitOperation(t, j, child.ID); result.State != journal.Succeeded {
		t.Fatal(result)
	}
	if writer.guard.ExpiresAt.After(parent.playbackDeadline.Add(10 * time.Millisecond)) {
		t.Fatal("Skip retained the generic five-second guard", writer.guard.ExpiresAt, parent.playbackDeadline)
	}
}

type bufferedSkipGuardWriter struct {
	Writer
	guard heos.Guard
}

func (w *bufferedSkipGuardWriter) Write(ctx context.Context, mutation heos.Mutation, guard heos.Guard) (heos.Response, error) {
	w.guard = guard
	return w.Writer.Write(ctx, mutation, guard)
}
