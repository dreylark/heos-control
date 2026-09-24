package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

var (
	ErrPrecondition         = errors.New("player revision changed")
	ErrPreconditionRequired = errors.New("If-Match required")
	ErrGrouped              = errors.New("grouped target")
	ErrOwnership            = errors.New("ownership lost")
	errSuperseded           = errors.New("superseded by admitted control")
	errJournalUnavailable   = errors.New("journal unavailable")
)

type Writer interface {
	Write(context.Context, heos.Mutation, heos.Guard) (heos.Response, error)
	BeginPriority() func()
	SetEventHandler(func(heos.Event))
}
type OperationJournal interface {
	Ready(context.Context) error
	Lookup(context.Context, journal.Request) (journal.Operation, error)
	Admit(context.Context, journal.Request, journal.Proposal) (journal.Admission, error)
	Get(context.Context, string) (journal.Operation, error)
	Active(context.Context, string) (journal.Operation, error)
	Transition(context.Context, string, int64, journal.Update) (journal.Operation, error)
}

// CommandKind is a coordinator command. It is not a HEOS write: playback, stop
// and cancel have no MutationKind. Absent is the zero value and is not a command.
type CommandKind string

const (
	CommandKindAbsent    CommandKind = ""
	CommandKindVolume    CommandKind = "volume"
	CommandKindMute      CommandKind = "mute"
	CommandKindTransport CommandKind = "transport"
	CommandKindSkip      CommandKind = "skip"
	CommandKindPlayback  CommandKind = "playback"
	CommandKindStop      CommandKind = "stop"
	CommandKindCancel    CommandKind = "cancel"
)

// Command is validated policy input; wire identifiers are resolved internally.
type Command struct {
	Kind        CommandKind    `json:"kind"`
	Level       int            `json:"level"`
	Muted       bool           `json:"muted"`
	State       heos.PlayState `json:"state"`
	Direction   string         `json:"direction,omitempty"`
	Takeover    bool           `json:"takeover"`
	ItemRef     string         `json:"item_ref"`
	Shuffle     bool           `json:"shuffle"`
	Repeat      heos.Repeat    `json:"repeat"`
	Mode        string         `json:"mode"`
	FadeSeconds int            `json:"fade_seconds"`
	Target      string         `json:"target"`
	Automation  *Automation    `json:"automation,omitempty"`
}
type controlClock interface {
	Now() time.Time
	Wait(context.Context, time.Duration, <-chan struct{}) error
}
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) Wait(ctx context.Context, d time.Duration, wake <-chan struct{}) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	case <-wake:
		return context.Cause(ctx)
	}
}

type lane struct {
	priorityGate chan struct{}
	priority     int
	admitting    context.CancelFunc
	device       Device
	writer       Writer
	gate         chan struct{}
	mu           sync.Mutex
	run          *execution
}
type execution struct {
	metrics          *operationMetrics // shared with journal-only handoff reconciliation
	targetMetrics    *operationMetrics // cancellation target has its own completion
	id               string
	ctx              context.Context
	cancel           context.CancelCauseFunc
	done             chan struct{}
	mu               sync.Mutex
	op               journal.Operation
	phase            string // protected by mu; event-time phase without reading worker-owned op
	expected         heos.Snapshot
	events           map[string][]map[string]string
	inflight         bool
	uncertain        bool
	unconfirmed      bool
	rejectedWrite    bool // worker-owned; read failures do not trigger write recovery
	members          map[heos.ID]bool
	confirmed        int
	playbackDeadline time.Time           // worker-owned monotonic end of the envelope
	progress         *PlaybackProgress   // worker-owned; persisted with every phase change
	bounded          bool                // immutable; complete queue observations for bounded playback/cancellation
	queueOwned       bool                // protected by mu; enabled only after native queue readback
	queueStart       queueStartPhase     // protected by mu; pending queue command lifecycle
	scalar           *scalarConfirmation // protected by mu; pending scalar observation, distinct from command reply
	queueWait        time.Time           // protected by mu; zero outside an active track transition
	automating       bool                // protected by mu; only the bounded playback worker may wait for Next
	keepExpectations bool                // protected by mu; skip retains transitional state events until readback
	now              func() time.Time    // immutable operation clock
	wake             chan struct{}       // immutable, coalesces observation hints from the existing callback
	eventRevision    uint64              // protected by mu; relevant events only
	observedRevision uint64              // protected by mu; revision at the start of the last successful read
	lastReadAttempt  time.Time           // worker-owned; bounds polling even after a stale/failed observation
	lastRead         time.Time           // worker-owned monotonic time of the last complete observation
	observedAt       time.Time           // worker-owned timestamp of that snapshot, never renewed by events
}
type Coordinator struct {
	logger    *slog.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	reads     *Reads
	db        OperationJournal
	lanes     map[string]*lane
	clock     controlClock
	publish   func(journal.Operation)
	lifecycle sync.Mutex
	closing   bool
	wg        sync.WaitGroup
}

func NewCoordinator(parent context.Context, reads *Reads, db OperationJournal, publish func(journal.Operation), logger *slog.Logger) *Coordinator {
	ctx, cancel := context.WithCancel(parent)
	c := &Coordinator{ctx: ctx, cancel: cancel, reads: reads, db: db, lanes: map[string]*lane{}, clock: realClock{}, publish: publish, logger: logger}
	for _, d := range reads.Devices() {
		w, ok := d.Client.(Writer)
		if !ok {
			continue
		}
		l := &lane{device: d, writer: w, gate: make(chan struct{}, 1), priorityGate: make(chan struct{}, 1)}
		c.lanes[d.Config.Key] = l
		w.SetEventHandler(func(e heos.Event) {
			l.mu.Lock()
			r := l.run
			l.mu.Unlock()
			if r != nil {
				r.event(e)
			}
		})
	}
	return c
}
func (c *Coordinator) Close() {
	c.lifecycle.Lock()
	c.closing = true
	c.cancel()
	c.lifecycle.Unlock()
	c.wg.Wait()
	for _, l := range c.lanes {
		l.writer.SetEventHandler(nil)
	}
}
func (c *Coordinator) emit(o journal.Operation) {
	if c.publish != nil {
		c.publish(o)
	}
}
func safety(l *lane, s heos.Snapshot) error {
	return deviceSafety(l.device.Config, s)
}

func mutationSafety(l *lane, s heos.Snapshot, skip bool) error {
	if !skip || s.State != heos.PlayStateUnknown {
		return safety(l, s)
	}
	// A verified unknown playback state is a skip-domain rejection, while
	// unavailable observations and device safety failures retain their errors.
	if err := deviceObservationSafety(l.device.Config, s); err != nil {
		return err
	}
	return ErrNotSkippable
}

func deviceSafety(p config.Player, s heos.Snapshot) error {
	if err := deviceObservationSafety(p, s); err != nil {
		return err
	}
	if !s.State.Writable() {
		return ErrUnavailable
	}
	return nil
}

// Waiting for an acknowledged command is read-only: an unknown playback state
// cannot authorize a write, but does not invalidate the other observed controls.
func deviceObservationSafety(p config.Player, s heos.Snapshot) error {
	if !p.WritesEnabled {
		return heos.ErrReadOnly
	}
	if !s.Verified || s.Stale || !s.Connected || s.Player.ID == "" || string(s.Player.Serial) != p.Serial || s.Volume == nil || s.Muted == nil {
		return ErrUnavailable
	}
	if s.Grouped {
		return ErrGrouped
	}
	if p.VolumeCeiling == nil {
		return heos.ErrReadOnly
	}
	return nil
}
func sameState(a, b heos.Snapshot) bool {
	return len(stateChanges(a, b)) == 0
}

// Submit follows authorization in the HTTP boundary. Matching retries precede
// freshness checks, and only confirmed newly committed admission starts a worker.
func (c *Coordinator) Submit(ctx context.Context, request journal.Request, cmd Command) (journal.Operation, error) {
	if o, e := c.db.Lookup(ctx, request); !errors.Is(e, journal.ErrNotFound) {
		return o, e
	}
	l := c.lanes[request.Player]
	if l == nil {
		return journal.Operation{}, ErrUnavailable
	}
	priority := cmd.Kind == CommandKindStop || cmd.Kind == CommandKindCancel
	release := func() {}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	if priority {
		if !l.device.Config.WritesEnabled {
			return journal.Operation{}, heos.ErrReadOnly
		}
		select {
		case l.priorityGate <- struct{}{}:
			defer func() { <-l.priorityGate }()
		default:
			return journal.Operation{}, heos.ErrQueueFull
		}
		l.mu.Lock()
		l.priority++
		if l.admitting != nil {
			l.admitting()
		}
		l.mu.Unlock()
		wireRelease := func() {}
		if cmd.Kind == CommandKindStop {
			wireRelease = l.writer.BeginPriority()
			ctx = heos.Priority(ctx)
		}
		release = sync.OnceFunc(func() { wireRelease(); l.mu.Lock(); l.priority--; l.mu.Unlock() })
		select {
		case l.gate <- struct{}{}:
			defer func() { <-l.gate }()
		case <-ctx.Done():
			return journal.Operation{}, ctx.Err()
		}
	} else {
		select {
		case l.gate <- struct{}{}:
			defer func() { <-l.gate }()
		default:
			return journal.Operation{}, heos.ErrQueueFull
		}
		l.mu.Lock()
		if l.priority > 0 && !cmd.Takeover {
			l.mu.Unlock()
			return journal.Operation{}, heos.ErrQueueFull
		}
		if l.priority > 0 && cmd.Takeover {
			ctx = heos.Priority(ctx)
		}
		var stop context.CancelFunc
		ctx, stop = context.WithCancel(ctx)
		l.admitting = stop
		l.mu.Unlock()
		defer func() { stop(); l.mu.Lock(); l.admitting = nil; l.mu.Unlock() }()
	}
	if o, e := c.db.Lookup(ctx, request); !errors.Is(e, journal.ErrNotFound) {
		return o, e
	}
	if e := c.ctx.Err(); e != nil {
		return journal.Operation{}, e
	}
	if cmd.FadeSeconds < 0 || cmd.FadeSeconds > 60 || cmd.Level < 0 || cmd.Level > 100 ||
		(cmd.Kind == CommandKindSkip && cmd.Direction != "next" && cmd.Direction != "previous") {
		return journal.Operation{}, heos.ErrBounds
	}
	// Freeze caller-owned optional settings before admitting asynchronous work.
	if cmd.Automation != nil {
		settings := *cmd.Automation
		cmd.Automation = &settings
		if e := settings.validate(cmd, l.device.Config.VolumeCeiling); e != nil {
			return journal.Operation{}, e
		}
	}
	if !priority {
		if request.IfMatch == "" {
			return journal.Operation{}, ErrPreconditionRequired
		}
		p, e := c.reads.Player(request.Player)
		if e != nil {
			return journal.Operation{}, e
		}
		if request.IfMatch != fmt.Sprintf("%q", p.Revision) {
			return journal.Operation{}, ErrPrecondition
		}
	}
	s := l.device.Observer.Snapshot()
	if !priority {
		if e := mutationSafety(l, s, cmd.Kind == CommandKindSkip); e != nil {
			return journal.Operation{}, e
		}
	}
	if cmd.Automation != nil && !cmd.Takeover && s.State == heos.PlayStatePlay {
		return journal.Operation{}, journal.ErrBusy
	}
	if (cmd.Kind == CommandKindVolume || cmd.Kind == CommandKindPlayback) && (l.device.Config.VolumeCeiling == nil || cmd.Level > *l.device.Config.VolumeCeiling) {
		return journal.Operation{}, heos.ErrBounds
	}
	var item heos.Item
	members := map[heos.ID]bool{}
	if cmd.Kind == CommandKindPlayback {
		var e error
		item, e = c.reads.resolveItem(ctx, l.device, cmd.ItemRef)
		if e != nil {
			return journal.Operation{}, e
		}
		members, e = playableMembers(ctx, l.device, item)
		if e != nil {
			return journal.Operation{}, e
		}
	}
	old, e := c.db.Active(ctx, request.Player)
	if e != nil && !errors.Is(e, journal.ErrNotFound) {
		return journal.Operation{}, e
	}
	if e == nil && !priority && !cmd.Takeover {
		return journal.Operation{}, journal.ErrBusy
	}
	if cmd.Kind == CommandKindCancel && (old.ID != cmd.Target || old.Phase == "cancelling") {
		return journal.Operation{}, ErrOwnership
	}
	l.mu.Lock()
	previous := l.run
	l.mu.Unlock()
	if previous != nil && old.ID == previous.id {
		previous.cancel(errSuperseded)
		select {
		case <-previous.done:
		case <-ctx.Done():
			return journal.Operation{}, ctx.Err()
		}
	}
	// A revoked worker is never restarted if subsequent admission fails.
	replaced := false
	defer func() {
		if old.ID != "" && previous != nil && !replaced {
			c.finishOrphan(old.ID, previous.metrics)
		}
	}()
	if old.ID != "" {
		old, e = c.db.Get(ctx, old.ID)
		if e != nil {
			return journal.Operation{}, e
		}
		if old.FinishedAt != nil {
			old = journal.Operation{}
		}
	}
	if cmd.Kind == CommandKindCancel {
		if previous == nil {
			return journal.Operation{}, ErrOwnership
		}
		previous.mu.Lock()
		s = previous.expected
		cause := context.Cause(previous.ctx)
		lost := previous.uncertain || previous.unconfirmed || (cause != nil && !errors.Is(cause, errSuperseded))
		previous.mu.Unlock()
		if cmd.Mode == "stop_owned" && lost {
			return journal.Operation{}, ErrOwnership
		}
	}
	if cmd.Kind != CommandKindCancel || cmd.Mode != "release" {
		if priority || cmd.Takeover { // Reads only; reconnect can briefly be in cooldown after fencing.
			deadline := time.Now().Add(3 * time.Second)
			for {
				e = l.device.Observer.Refresh(heos.WithObservationTrigger(ctx, "admission"))
				if e == nil || time.Now().After(deadline) || ctx.Err() != nil {
					break
				}
				if wait := c.clock.Wait(ctx, 50*time.Millisecond, nil); wait != nil {
					break
				}
			}
			if e != nil {
				return journal.Operation{}, e
			}
		}
		fresh := l.device.Observer.Snapshot()
		if e = mutationSafety(l, fresh, cmd.Kind == CommandKindSkip); e != nil {
			return journal.Operation{}, e
		}
		if cmd.Kind == CommandKindCancel {
			if previous.bounded {
				fresh, e = completeQueue(ctx, l, fresh)
				if e != nil {
					return journal.Operation{}, e
				}
			}
			previous.mu.Lock()
			same := previous.ownedState(s, fresh)
			previous.mu.Unlock()
			if !same {
				return journal.Operation{}, ErrOwnership
			}
		}
		s = fresh
		if cmd.Automation != nil && !cmd.Takeover && s.State == heos.PlayStatePlay {
			return journal.Operation{}, journal.ErrBusy
		}
		if ((cmd.Kind == CommandKindTransport && cmd.State == heos.PlayStatePlay) || (cmd.Kind == CommandKindMute && !cmd.Muted)) && *s.Volume > *l.device.Config.VolumeCeiling {
			return journal.Operation{}, heos.ErrBounds
		}
	}
	if !priority && !cmd.Takeover {
		p, _ := c.reads.Player(request.Player)
		if request.IfMatch != fmt.Sprintf("%q", p.Revision) {
			return journal.Operation{}, ErrPrecondition
		}
	}
	if cmd.Kind == CommandKindSkip {
		readctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		s, e = completeQueue(readctx, l, s)
		cancel()
		if e != nil {
			return journal.Operation{}, e
		}
		if e = skipAdmissible(l.device.Config.VolumeCeiling, cmd.Direction, s); e != nil {
			return journal.Operation{}, e
		}
	}
	ids := make([]string, 0, len(members))
	if cmd.Automation != nil {
		readctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		s, e = completeQueue(readctx, l, s)
		cancel()
		if e != nil {
			return journal.Operation{}, e
		}
	}
	for id := range members {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	memberBytes, _ := json.Marshal(ids)
	args, _ := json.Marshal(struct {
		MembersSHA256 string    `json:"members_sha256"`
		Command       Command   `json:"command"`
		Item          heos.Item `json:"resolved_item"`
		VolumeCeiling *int      `json:"volume_ceiling"`
	}{fmt.Sprintf("%x", sha256.Sum256(memberBytes)), cmd, item, l.device.Config.VolumeCeiling})
	if len(args) > journal.MaxJSONBytes {
		return journal.Operation{}, heos.ErrBounds
	}
	configuration, _ := json.Marshal(l.device.Config)
	now := c.clock.Now()
	proposal := journal.Proposal{Kind: string(cmd.Kind), DeviceKey: l.device.Config.Serial, ConfigRevision: fmt.Sprintf("direct:%x", sha256.Sum256(configuration)), EffectiveArguments: args, NotBefore: now, NotAfter: now.Add(time.Minute), ReplaceID: old.ID, ReplaceRevision: old.Revision}
	if previous != nil {
		previous.mu.Lock()
		proposal.ReplaceUncertain = previous.uncertain || previous.unconfirmed
		previous.mu.Unlock()
	}
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.closing || ctx.Err() != nil {
		return journal.Operation{}, ErrUnavailable
	}
	a, e := c.db.Admit(ctx, request, proposal)
	if e != nil {
		return journal.Operation{}, e
	}
	if !a.Created {
		return a.Operation, nil
	}
	replaced = true
	if old.ID != "" && previous != nil && old.ID == previous.id && cmd.Kind != CommandKindCancel {
		// Admit commits this terminal handoff atomically with the new operation.
		state := journal.Released
		if proposal.ReplaceUncertain {
			state = journal.Uncertain
		}
		previous.metrics.finishAs(state, "superseded")
	}
	runctx, cancel := context.WithCancelCause(c.ctx)
	if cmd.Kind == CommandKindStop {
		runctx = heos.Priority(runctx)
	}
	run := &execution{ctx: runctx, cancel: cancel, done: make(chan struct{}), op: a.Operation, phase: a.Operation.Phase, id: a.Operation.ID, expected: s, members: members, events: map[string][]map[string]string{}, now: c.clock.Now, wake: make(chan struct{}, 1)}
	run.metrics = &operationMetrics{player: l.device.Metrics, kind: a.Operation.Kind}
	if cmd.Kind == CommandKindCancel && previous != nil {
		run.targetMetrics = previous.metrics
	}
	run.bounded = cmd.Automation != nil
	if cmd.Kind == CommandKindCancel && cmd.Mode == "stop_owned" && previous != nil {
		run.bounded = previous.bounded
		previous.mu.Lock()
		run.queueOwned = previous.queueOwned
		previous.mu.Unlock()
	}
	l.mu.Lock()
	l.run = run
	l.mu.Unlock()
	run.metrics.phase("accepted")
	transferred = true
	c.emit(a.Operation)
	c.wg.Go(func() {
		defer close(run.done)
		defer release()
		defer cancel(nil)
		c.execute(l, run, cmd, item)
		l.mu.Lock()
		if l.run == run {
			l.run = nil
			run.metrics.phase("")
		}
		l.mu.Unlock()
	})
	return a.Operation, nil
}
func (c *Coordinator) finishOrphan(id string, metrics *operationMetrics) {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.closing {
		return
	}
	c.wg.Go(func() {
		c.persistByID(id, journal.Update{State: journal.Uncertain, Phase: "handoff_failed", ErrorCode: "ownership_revoked", Outcome: json.RawMessage(`{"delivery":"unknown","playback_may_continue":true}`)}, metrics)
	})
}

// Journal-only retries are safe: they cannot recreate a queue or replay a setter.
func (c *Coordinator) persistByID(id string, update journal.Update, metrics *operationMetrics) {
	prepared := false
	at := c.clock.Now().UTC()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		o, e := c.db.Get(ctx, id)
		if e == nil && o.FinishedAt != nil {
			metrics.finish(o)
			if prepared && !matchesUpdate(o, update) && c.logger != nil {
				c.logger.Error("operation reconciliation found a different terminal result", "operation_id", id)
			}
			cancel()
			return
		}
		if e == nil {
			if !prepared {
				update.Outcome = c.targetOutcome(o, update, at)
				prepared = true
			}
			update.Progress = o.Progress
			o, e = c.db.Transition(ctx, id, o.Revision, update)
			if e == nil {
				metrics.finish(o)
				c.emit(o)
			}
		}
		cancel()
		if e == nil || errors.Is(e, journal.ErrStaleEpoch) || c.ctx.Err() != nil {
			return
		}
		if c.clock.Wait(c.ctx, time.Second, nil) != nil {
			return
		}
	}
}
