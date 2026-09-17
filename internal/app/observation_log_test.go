package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestObservationLogThrottleAndRecovery(t *testing.T) {
	var output bytes.Buffer
	now := time.Unix(100, 0)
	report := observationReporter(slog.New(slog.NewJSONHandler(&output, nil)), "room", func() time.Time { return now })
	report(nil)
	report(context.Canceled)
	if output.Len() != 0 {
		t.Fatal("healthy/shutdown observations logged")
	}
	failure := errors.New("queue read rejected")
	report(failure)
	report(failure)
	now = now.Add(59 * time.Second)
	report(failure)
	if strings.Count(output.String(), "HEOS observation failed") != 1 {
		t.Fatal(output.String())
	}
	now = now.Add(time.Second)
	report(failure)
	report(nil)
	report(nil)
	report(failure)
	if strings.Count(output.String(), "HEOS observation failed") != 3 || strings.Count(output.String(), "HEOS observation recovered") != 1 {
		t.Fatal(output.String())
	}
	if !strings.Contains(output.String(), `"player":"room"`) || !strings.Contains(output.String(), `"error":"queue read rejected"`) {
		t.Fatal(output.String())
	}
}
