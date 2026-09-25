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

func TestDiagnosticQueueSelectionIsCorrelatableAndRedacted(t *testing.T) {
	for _, tc := range []struct {
		name             string
		params           url.Values
		wantAid          float64
		wantCID, wantMID bool
	}{
		{name: "container", params: url.Values{"aid": {"4"}, "cid": {"private-container"}}, wantAid: 4, wantCID: true},
		{name: "track", params: url.Values{"aid": {"4"}, "cid": {"private-container"}, "mid": {"private-track"}}, wantAid: 4, wantCID: true, wantMID: true},
		{name: "untrusted", params: url.Values{"aid": {"private-action"}, "cid": {"private-first", "private-second"}, "mid": {strings.Repeat("private-", 1024)}}},
		{name: "out of range", params: url.Values{"aid": {"5"}}},
		{name: "duplicate action", params: url.Values{"aid": {"4", "1"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			logger.Info("params", "params", diagnosticParams(tc.params))
			var record struct {
				Params map[string]any `json:"params"`
			}
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if tc.wantAid != 0 && record.Params["aid"] != tc.wantAid {
				t.Fatalf("missing queue action: %s", output.String())
			}
			if tc.wantAid == 0 && record.Params["aid"] != nil {
				t.Fatalf("invalid queue action: %s", output.String())
			}
			for _, field := range []struct {
				key, digest string
				present     bool
			}{
				{"cid_fingerprint", "c0b535550c0050ff", tc.wantCID},
				{"mid_fingerprint", "0830331fb94e30b5", tc.wantMID},
			} {
				value, _ := record.Params[field.key].(string)
				if field.present && value != field.digest || !field.present && value != "" {
					t.Fatalf("%s has wrong identity or presence: %s", field.key, output.String())
				}
			}
			if strings.Contains(output.String(), "private-") {
				t.Fatal("private selection leaked", output.String())
			}
		})
	}
}

func TestDebugBrowseCapabilityOptionsAreBoundedAndRedacted(t *testing.T) {
	for _, tc := range []struct {
		name, options, state string
		playable             bool
	}{
		{name: "absent", state: "absent"},
		{name: "null", options: `null`, state: "absent"},
		{name: "too large", options: `[{"browse":[{"id":21,"name":"` + strings.Repeat("private-", 3000) + `"}]}]`, state: "invalid"},
		{name: "too many groups", options: `[` + strings.Repeat(`{},`, 64) + `{}]`, state: "invalid"},
		{name: "play all", options: `[{"browse":[{"id":21,"name":"private-option","scid":"private-search"}]}]`, state: "valid", playable: true},
		{name: "other context", options: `[{"play":[{"id":21,"name":"private-option"}]}]`, state: "valid"},
		{name: "other option", options: `[{"browse":[{"id":19,"name":"private-option"}]}]`, state: "valid"},
		{name: "invalid id", options: `[{"browse":[{"id":"private-id"}]}]`, state: "invalid"},
		{name: "invalid shape", options: `{"browse":[{"id":21}]}`, state: "invalid"},
		{name: "too many options", options: `[{"browse":[` + strings.Repeat(`{"id":21},`, 64) + `{"id":21}]}]`, state: "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := `{"heos":{"command":"browse/browse","result":"success"},"payload":[]`
			if tc.options != "" {
				frame += `,"options":` + tc.options
			}
			frame += `}`
			response, err := decodeResponse([]byte(frame))
			if err != nil {
				t.Fatal("diagnostic metadata changed protocol acceptance", err)
			}
			var output bytes.Buffer
			c := &Client{ctx: context.Background(), cfg: Config{Logger: slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))}}
			c.traceFrame(response, nil)
			var record struct {
				Options struct {
					State    string `json:"state"`
					Playable bool   `json:"playable_container"`
				} `json:"browse_options"`
			}
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if record.Options.State != tc.state || record.Options.Playable != tc.playable {
				t.Fatalf("capability=%+v want=%s/%t", record.Options, tc.state, tc.playable)
			}
			if strings.Contains(output.String(), "private-") {
				t.Fatal("private options leaked", output.String())
			}
		})
	}
}
