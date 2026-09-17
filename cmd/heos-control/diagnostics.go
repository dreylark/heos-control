package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/dreylark/heos-control/internal/app"
)

func runDiagnostic(ctx context.Context, args []string, out, errOut io.Writer) int {
	command := args[0]
	args = args[1:]
	if command == "config" {
		if len(args) == 0 || args[0] != "check" {
			_, _ = fmt.Fprintln(errOut, "usage: heos-control config check -config PATH [-format text|json]")
			return 2
		}
		command, args = "config check", args[1:]
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard) // Never echo arbitrary flag values into a report.
	path := flags.String("config", "/etc/heos-control/config.json", "JSON configuration file")
	format := flags.String("format", "text", "text or json")
	var timeout time.Duration
	if command == "doctor" {
		flags.DurationVar(&timeout, "timeout", 30*time.Second, "overall network diagnostic deadline (1s..2m)")
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = fmt.Fprintf(errOut, "usage: heos-control %s -config PATH [-format text|json]", command)
			if command == "doctor" {
				_, _ = fmt.Fprint(errOut, " [-timeout 30s]")
			}
			_, _ = fmt.Fprintln(errOut)
			return 0
		}
		_, _ = fmt.Fprintln(errOut, "invalid diagnostic arguments; use -help for usage")
		return 2
	}
	if flags.NArg() != 0 || (*format != "text" && *format != "json") || (command == "doctor" && (timeout < time.Second || timeout > app.MaxDiagnosticDuration)) {
		_, _ = fmt.Fprintln(errOut, "invalid diagnostic arguments; format must be text or json; doctor timeout must be 1s..2m")
		return 2
	}
	var report app.DiagnosticReport
	if command == "doctor" {
		bounded, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		report = app.Doctor(bounded, *path)
	} else {
		report = app.CheckConfiguration(*path)
	}
	if err := writeDiagnosticReport(out, *format, report); err != nil {
		_, _ = fmt.Fprintln(errOut, "cannot write diagnostic report")
		return 1
	}
	if !report.OK {
		return 1
	}
	return 0
}

func writeDiagnosticReport(out io.Writer, format string, report app.DiagnosticReport) error {
	if format == "json" {
		return json.NewEncoder(out).Encode(report)
	}
	for _, check := range report.Checks {
		status := "FAIL"
		if check.OK {
			status = "PASS"
		}
		if _, err := fmt.Fprintf(out, "%s %s [%s]: %s\n", status, check.Field, check.Code, check.Message); err != nil {
			return err
		}
	}
	return nil
}
