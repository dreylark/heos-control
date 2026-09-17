package heos

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"testing"
)

func TestDebugWireLogsAreOptInAndRedacted(t *testing.T) {
	for _, level := range []slog.Level{slog.LevelInfo, slog.LevelDebug} {
		t.Run(level.String(), func(t *testing.T) {
			var output bytes.Buffer
			s := newFakeHEOS(t, func(conn net.Conn, u *url.URL, _ int64) {
				// Denon 5.9 and 6.1 fields must not disclose arbitrary text.
				sendEvent(conn, "event/player_volume_changed", "pid=1&level=10&mute=off&text=private-event")
				sendReply(conn, u, url.Values{"level": {"10"}, "text": {"private-response"}, "un": {"private-user"}}, map[string]string{"song": "private-song", "url": "https://private-url/?token=private-token"})
			})
			c, err := New(context.Background(), Config{Address: s.listener.Addr().String(), Fingerprint: s.pin, Logger: slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: level}))})
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Read(context.Background(), "player/get_volume", url.Values{"pid": {"1"}})
			c.Close()
			if err != nil {
				t.Fatal(err)
			}
			logs := output.String()
			if strings.Contains(logs, "private-") {
				t.Fatal("private protocol data leaked", logs)
			}
			if level == slog.LevelInfo && logs != "" {
				t.Fatal("wire logging enabled at INFO", logs)
			}
			if level == slog.LevelDebug {
				for _, want := range []string{"HEOS request", "HEOS response", "HEOS event", `"level":10`, "payload_bytes", "elapsed_ms"} {
					if !strings.Contains(logs, want) {
						t.Fatalf("missing %q: %s", want, logs)
					}
				}
			}
		})
	}
}

func TestDiagnosticAllowlistRejectsUntrustedValues(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	logger.Info("event", "event", Event{Command: "event/private-command", Params: url.Values{"state": {"private-state"}, "level": {"private-level"}, "pid": {"private-player"}, "error": {"private-error"}}})
	if strings.Contains(output.String(), "private-") {
		t.Fatal(output.String())
	}
}

func TestDebugCommandFailureNamesDeviceReason(t *testing.T) {
	var output bytes.Buffer
	server := newFakeHEOS(t, func(conn net.Conn, u *url.URL, _ int64) {
		sendFailure(conn, u, "eid=12&syserrno=-1063&text=private-account")
	})
	c, err := New(context.Background(), Config{Address: server.listener.Addr().String(), Fingerprint: server.pin,
		Logger: slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Read(context.Background(), "player/get_play_state", url.Values{"pid": {"1"}})
	c.Close()
	if err == nil {
		t.Fatal("device rejection was lost")
	}
	found := false
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var record struct {
			Msg       string `json:"msg"`
			Delivery  string `json:"delivery"`
			ErrorKind string `json:"error_kind"`
			Device    *struct {
				Code       int    `json:"eid"`
				Reason     string `json:"reason"`
				SystemCode int    `json:"syserrno"`
			} `json:"device_error"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if record.Msg == "HEOS command completed" && record.Device != nil {
			found = true
			if record.Delivery != "rejected" || record.ErrorKind != "rejected" || record.Device.Code != 12 || record.Device.Reason != "system_error" || record.Device.SystemCode != -1063 {
				t.Fatal(line)
			}
		}
	}
	if !found || strings.Contains(output.String(), "private-account") {
		t.Fatal("missing safe device reason or leaked device text", output.String())
	}
}
