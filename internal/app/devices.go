package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/dreylark/heos-control/internal/api"
	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/control"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
	"github.com/dreylark/heos-control/internal/telemetry"
)

type devices struct {
	reads   *control.Reads
	events  *api.Events
	clients []*heos.Client
	cancel  context.CancelFunc
	ctx     context.Context
	wg      sync.WaitGroup
}

func openDevices(parent context.Context, cfg config.Config, logger *slog.Logger) (*devices, error) {
	ctx, cancel := context.WithCancel(parent)
	epoch := make([]byte, 16)
	_, _ = rand.Read(epoch)
	result := &devices{cancel: cancel, ctx: ctx, events: api.NewEvents(hex.EncodeToString(epoch), 256, 32, 64)}
	ds := []control.Device{}
	for _, p := range cfg.Players {
		metrics := telemetry.NewPlayer(p.Key)
		c, e := heos.New(ctx, heos.Config{Address: p.Address, Fingerprint: p.FingerprintSHA256, EnableWrites: p.WritesEnabled, Logger: logger.With("player", p.Key), Metrics: metrics})
		if e != nil {
			result.Close()
			return nil, e
		}
		result.clients = append(result.clients, c)
		o, e := heos.NewObserver(c, heos.Identity{Key: p.Key, Serial: p.Serial, Model: p.Model}, heos.ObservationCacheTTL)
		if e != nil {
			result.Close()
			return nil, e
		}
		// Isolate opaque refs by configured source key, even if firmware later
		// reuses a numeric SID. Share one bounded slot budget per connection.
		sourceCount := 0
		for _, source := range cfg.Sources {
			if source.Player == p.Key {
				sourceCount++
			}
		}
		catalogs := map[string]control.Browser{}
		for _, source := range cfg.Sources {
			if source.Player == p.Key {
				catalog, e := heos.NewCatalog(c, 4096/max(1, sourceCount), 2*time.Minute)
				if e != nil {
					result.Close()
					return nil, e
				}
				catalogs[source.Key] = catalog
			}
		}
		ds = append(ds, control.Device{Config: p, Observer: o, Client: c, Catalogs: catalogs, Metrics: metrics})
		result.wg.Go(func() { _ = o.Run(ctx, heos.IdleObservationInterval, observationReporter(logger, p.Key, time.Now)) })
		result.wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case e, ok := <-c.Events():
					if !ok {
						return
					}
					o.Notify(e)
					if e.Gap {
						result.events.Publish(api.Event{Kind: "snapshot_required", Player: p.Key})
					}
				}
			}
		})
	}
	result.reads = control.NewReads(hex.EncodeToString(epoch), ds, cfg.Sources)
	result.wg.Go(func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		last := map[string]string{}
		for {
			for _, d := range ds {
				p, _ := result.reads.Player(d.Config.Key)
				if last[p.Key] != p.Revision {
					last[p.Key] = p.Revision
					result.events.Publish(api.Event{Kind: "player_changed", Player: p.Key})
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	return result, nil
}
func (d *devices) Close() {
	d.cancel()
	for _, c := range d.clients {
		c.Close()
	}
	d.wg.Wait()
	d.events.Close()
}

// Follow active reservations and their final transitions, without retaining an
// unbounded history or exposing creator-only operation IDs to other readers.
func (d *devices) watchOperations(db api.JournalReader) {
	ctx := d.ctx
	d.wg.Go(func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		last := map[string]journal.Operation{}
		for {
			for _, device := range d.reads.Devices() {
				key := device.Config.Key
				op, e := db.Active(ctx, key)
				old, exists := last[key]
				if errors.Is(e, journal.ErrNotFound) && exists {
					op, e = db.Get(ctx, old.ID)
				}
				if e != nil {
					continue
				}
				if !exists || old.ID != op.ID || old.Revision != op.Revision {
					d.events.Publish(api.Event{Kind: "operation_changed", Player: key, Operation: op.ID, Principal: op.Principal})
					last[key] = op
				}
				if op.FinishedAt != nil {
					delete(last, key)
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
}

// Called serially by the observer; repeated failures are bounded per player.
func observationReporter(logger *slog.Logger, player string, now func() time.Time) func(error) {
	var failed bool
	var lastLog time.Time
	return func(err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		if err == nil {
			if failed {
				logger.Info("HEOS observation recovered", "player", player)
			}
			failed = false
			return
		}
		at := now()
		if !failed || at.Sub(lastLog) >= time.Minute {
			logger.Warn("HEOS observation failed", "player", player, "error", err)
			lastLog = at
		}
		failed = true
	}
}
