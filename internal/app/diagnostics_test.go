package app

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dreylark/heos-control/internal/config"
)

func TestDiagnosticProbesKeepOrderAndIndependentFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	gate := make(chan struct{})
	probes := []diagnosticProbe{
		{field: "database", run: func(ctx context.Context) []config.Check {
			select {
			case <-gate:
			case <-ctx.Done():
				t.Error("independent probe was blocked")
			}
			return []config.Check{{Field: "database.schema", Code: "schema", Message: "schema incompatible"}}
		}},
		{field: "players[0]", run: func(context.Context) []config.Check {
			close(gate)
			return []config.Check{{Field: "players[0]", Code: "ok", Message: "device reachable", OK: true}}
		}},
	}
	checks := runDiagnosticProbes(ctx, probes)
	if len(checks) != 2 || checks[0].Field != "database.schema" || checks[0].OK || checks[1].Field != "players[0]" || !checks[1].OK {
		t.Fatal("failure cancelled another check or changed report order", checks)
	}
}

func TestDiagnosticProbesBoundConcurrencyAndJoinOnCancellation(t *testing.T) {
	var active, maximum, ended atomic.Int32
	started := make(chan struct{}, 4)
	probes := make([]diagnosticProbe, 17)
	for i := range probes {
		field := fmt.Sprintf("players[%d]", i)
		probes[i] = diagnosticProbe{field: field, run: func(ctx context.Context) []config.Check {
			n := active.Add(1)
			for old := maximum.Load(); n > old; old = maximum.Load() {
				if maximum.CompareAndSwap(old, n) {
					break
				}
			}
			started <- struct{}{}
			<-ctx.Done()
			active.Add(-1)
			ended.Add(1)
			return []config.Check{{Field: field, Code: "cancelled", Message: "check cancelled"}}
		}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan []config.Check, 1)
	go func() { done <- runDiagnosticProbes(ctx, probes) }()
	for range 4 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("four independent probes did not start")
		}
	}
	cancel()
	select {
	case checks := <-done:
		if len(checks) != len(probes) || maximum.Load() != 4 || active.Load() != 0 || ended.Load() != 4 {
			t.Fatalf("checks=%d max=%d active=%d joined=%d", len(checks), maximum.Load(), active.Load(), ended.Load())
		}
		for i, c := range checks {
			if c.OK || c.Field != probes[i].field {
				t.Fatal("missing cancellation result", c)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("diagnostic workers did not exit")
	}
}

func TestExpiredDiagnosticDeadlineSkipsNetwork(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	checks := runDiagnosticProbes(ctx, []diagnosticProbe{{field: "database", run: func(context.Context) []config.Check {
		t.Error("expired probe called")
		return nil
	}}})
	if len(checks) != 1 || checks[0].OK || checks[0].Code != "timeout" {
		t.Fatal(checks)
	}
}

func TestCancelledDiagnosticsNeverStartNetworkProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	checks := runDiagnosticProbes(ctx, []diagnosticProbe{{field: "database", run: func(context.Context) []config.Check {
		t.Error("cancelled probe called")
		return nil
	}}})
	if len(checks) != 1 || checks[0].OK || checks[0].Code != "cancelled" {
		t.Fatal(checks)
	}
}
