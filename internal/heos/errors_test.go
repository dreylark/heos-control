package heos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// Denon 6.2 defines these codes independently of the device's free-form text.
func TestDeviceErrorReasons(t *testing.T) {
	tests := []struct {
		code   int
		reason string
	}{
		{1, "unrecognized_command"},
		{2, "invalid_id"},
		{3, "invalid_arguments"},
		{4, "data_unavailable"},
		{5, "resource_unavailable"},
		{6, "invalid_credentials"},
		{7, "command_not_executed"},
		{8, "user_not_logged_in"},
		{9, "parameter_out_of_range"},
		{10, "user_not_found"},
		{11, "internal_error"},
		{12, "system_error"},
		{13, "device_busy"},
		{14, "cannot_play"},
		{15, "option_not_supported"},
		{16, "device_queue_full"},
		{17, "skip_limit_reached"},
		{18, "unknown"},
		{999, "unknown"},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.code), func(t *testing.T) {
			const privateText = "private-account / secret-title\nreason=forged"
			err := fmt.Errorf("reading device: %w", rejection(Response{
				Command: "player/get_play_state",
				Params:  url.Values{"eid": {strconv.Itoa(tt.code)}, "text": {privateText}},
			}))
			var device *DeviceError
			var command *CommandError
			if !errors.Is(err, ErrRejected) || !errors.As(err, &device) || !errors.As(err, &command) || command.Delivery != Rejected {
				t.Fatalf("lost rejection classification: %v", err)
			}
			if device.Code != tt.code || device.Text != privateText || device.SystemCode != nil {
				t.Fatalf("lost device details: %+v", device)
			}
			if got := device.Reason(); got != tt.reason {
				t.Fatalf("Reason() = %q, want %q", got, tt.reason)
			}
			want := fmt.Sprintf("HEOS device rejected player/get_play_state (eid=%d, reason=%s)", tt.code, tt.reason)
			if got := device.Error(); got != want {
				t.Fatalf("Error() = %q, want %q", got, want)
			}
			if strings.Contains(err.Error(), "private-account") || strings.Contains(err.Error(), "secret-title") || strings.Contains(err.Error(), "forged") {
				t.Fatal("device text leaked into wrapped error")
			}
		})
	}
}

func TestNamedRejectionsDoNotReplayCommands(t *testing.T) {
	for _, code := range []int{13, 14, 16} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
				sendFailure(c, u, "eid="+strconv.Itoa(code)+"&text=Rejected")
			})
			c := fakeClient(t, s)
			_, err := c.Read(context.Background(), "system/heart_beat", nil)
			var device *DeviceError
			if !errors.Is(err, ErrRejected) || !errors.As(err, &device) || device.Code != code || device.Reason() == "unknown" {
				t.Fatalf("unexpected rejection: %v", err)
			}
			if s.commands.Load() != 2 || s.connections.Load() != 1 || !c.View().Connected {
				t.Fatalf("rejection replayed or disconnected: commands=%d connections=%d connected=%t", s.commands.Load(), s.connections.Load(), c.View().Connected)
			}
		})
	}
}

func sendFailure(c net.Conn, u *url.URL, message string) {
	raw, _ := json.Marshal(map[string]any{"heos": map[string]string{
		"command": commandName(u), "result": "fail", "message": message,
	}})
	_, _ = c.Write(append(raw, '\r', '\n'))
}

// Denon 6.1–6.2: preserve codes without leaking server text into default logs.
func TestTypedDeviceErrors(t *testing.T) {
	s := newFakeHEOS(t, func(c net.Conn, u *url.URL, _ int64) {
		sendFailure(c, u, "eid=12&text=Account%20A%26B&syserrno=-1063")
	})
	c := fakeClient(t, s)
	calls := []func() error{
		func() error { _, err := c.Sources(context.Background()); return err },
		func() error { _, err := c.BrowsePage(context.Background(), "server", "album", 0, 100); return err },
		func() error { _, err := c.Queue(context.Background(), "1", 0, 100); return err },
	}
	for _, call := range calls {
		err := call()
		var device *DeviceError
		var command *CommandError
		if !errors.Is(err, ErrRejected) || !errors.As(err, &device) || !errors.As(err, &command) || command.Delivery != Rejected {
			t.Fatal(err)
		}
		if device.Code != 12 || device.Reason() != "system_error" || device.Text != "Account A&B" || device.SystemCode == nil || *device.SystemCode != -1063 || device.Command == "" {
			t.Fatalf("lost device error details: %+v", device)
		}
		if strings.Contains(err.Error(), "Account") {
			t.Fatal("device text leaked into default error string")
		}
	}
	if s.connections.Load() != 1 {
		t.Fatal("definite command rejection discarded healthy connection")
	}
}

func TestRejectedRegistrationDiscardsConnection(t *testing.T) {
	s := newFakeHEOSWithRegistration(t,
		func(c net.Conn, u *url.URL, _ int64) { sendReply(c, u, nil, nil) },
		func(c net.Conn, u *url.URL) { sendFailure(c, u, "eid=7&text=Cannot%20subscribe") },
	)
	c := fakeClient(t, s)
	_, err := c.Read(context.Background(), "system/heart_beat", nil)
	var command *CommandError
	if !errors.As(err, &command) || command.Delivery != NotSent {
		t.Fatal(err)
	}
	if c.View().Connected {
		t.Fatal("failed registration left usable connection")
	}
	if s.commands.Load() != 1 {
		t.Fatal("user command sent without event subscription")
	}
}
