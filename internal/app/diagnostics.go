package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
	"github.com/dreylark/heos-control/internal/journal"
)

const MaxDiagnosticDuration = 2 * time.Minute

// DiagnosticReport is a bounded, redacted CLI result, not a service API resource.
type DiagnosticReport struct {
	Version int            `json:"version"`
	Command string         `json:"command"`
	OK      bool           `json:"ok"`
	Checks  []config.Check `json:"checks"`
}

func diagnosticReport(command string, checks []config.Check) DiagnosticReport {
	ok := len(checks) != 0
	for _, check := range checks {
		ok = ok && check.OK
	}
	return DiagnosticReport{Version: 1, Command: command, OK: ok, Checks: checks}
}

// CheckConfiguration performs no DNS, network, database or listener operations.
func CheckConfiguration(path string) DiagnosticReport {
	_, checks := config.CheckFile(path, time.Now())
	return diagnosticReport("config check", checks)
}

// Doctor uses short-lived read-only clients. It deliberately does not call Run,
// OpenDevices, journal recovery, or any control coordinator entrypoint.
func Doctor(parent context.Context, path string) DiagnosticReport {
	ctx, cancel := context.WithTimeout(parent, MaxDiagnosticDuration)
	defer cancel()
	cfg, checks := config.CheckFile(path, time.Now())
	if report := diagnosticReport("doctor", checks); !report.OK {
		return report
	}
	probes := []diagnosticProbe{{field: "database", run: func(ctx context.Context) []config.Check {
		var result []config.Check
		for _, c := range journal.Diagnose(ctx, cfg.Database) {
			result = append(result, config.Check{Field: "database." + c.Name, Code: c.Code, Message: c.Message, OK: c.OK})
		}
		return result
	}}}
	for i, player := range cfg.Players {
		field := fmt.Sprintf("players[%d]", i)
		probes = append(probes, diagnosticProbe{field: field, run: func(ctx context.Context) []config.Check {
			result := heos.Probe(ctx, heos.Config{Address: player.Address, Fingerprint: player.FingerprintSHA256}, heos.Identity{Key: player.Key, Serial: player.Serial, Model: player.Model})
			return []config.Check{{Field: field, Code: result.Code, Message: result.Message, OK: result.OK}}
		}})
	}
	checks = append(checks, runDiagnosticProbes(ctx, probes)...)
	return diagnosticReport("doctor", checks)
}

type diagnosticProbe struct {
	field string
	run   func(context.Context) []config.Check
}

// Each probe owns and closes its connections before returning. Context expiry
// cancels in-flight work; join it rather than abandoning network goroutines.
func runDiagnosticProbes(ctx context.Context, probes []diagnosticProbe) []config.Check {
	results := make([][]config.Check, len(probes))
	permits := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for i, probe := range probes {
		wg.Go(func() {
			select {
			case permits <- struct{}{}:
				defer func() { <-permits }()
			case <-ctx.Done():
			}
			// A select may acquire a permit after cancellation. Never start a
			// new connection merely because the two cases were both ready.
			if ctx.Err() != nil {
				code, message := "cancelled", "Diagnostic cancelled before this check started."
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					code, message = "timeout", "Diagnostic deadline expired before this check started."
				}
				results[i] = []config.Check{{Field: probe.field, Code: code, Message: message}}
				return
			}
			results[i] = probe.run(ctx)
		})
	}
	wg.Wait()
	var checks []config.Check
	for _, result := range results {
		checks = append(checks, result...)
	}
	return checks
}
