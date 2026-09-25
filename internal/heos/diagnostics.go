package heos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Diagnostic fields follow Denon 3.2, 5 and 6.1. Use an allowlist, including
// value validation: error text, account names and browse IDs can contain secrets.
func diagnosticParams(params url.Values) slog.Value {
	attrs := []slog.Attr{}
	for _, key := range []string{"pid", "gid", "sid", "qid", "level", "eid", "syserrno", "SEQUENCE", "count", "returned", "cur_pos", "duration", "a"} {
		values := params[key]
		if len(values) != 1 {
			continue
		}
		if n, err := strconv.ParseInt(values[0], 10, 64); err == nil {
			attrs = append(attrs, slog.Int64(key, n))
		}
	}
	for _, field := range []struct {
		key    string
		values []string
	}{
		{"state", []string{string(PlayStatePlay), string(PlayStatePause), string(PlayStateStop), string(PlayStateUnknown)}},
		{"mute", []string{"on", "off"}}, {"shuffle", []string{"on", "off"}},
		{"repeat", []string{string(RepeatOff), string(RepeatOnAll), string(RepeatOnOne)}}, {"enable", []string{"on", "off"}},
	} {
		if len(params[field.key]) != 1 {
			continue
		}
		for _, value := range field.values {
			if params.Get(field.key) == value {
				attrs = append(attrs, slog.String(field.key, value))
				break
			}
		}
	}
	if values := params["aid"]; len(values) == 1 {
		if aid, err := strconv.Atoi(values[0]); err == nil && aid >= 1 && aid <= 4 {
			attrs = append(attrs, slog.Int("aid", aid))
		}
	}
	for _, key := range []string{"cid", "mid"} {
		values := params[key]
		if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > 4096 || !utf8.ValidString(values[0]) || strings.ContainsRune(values[0], '\x00') {
			continue
		}
		digest := sha256.Sum256([]byte(values[0]))
		attrs = append(attrs, slog.String(key+"_fingerprint", hex.EncodeToString(digest[:8])))
	}
	if start, end, err := parseRange(params.Get("range")); err == nil {
		attrs = append(attrs, slog.Int("range_start", start), slog.Int("range_end", end))
	}
	return slog.GroupValue(attrs...)
}

func diagnosticCommand(command string) string {
	if readCommand(command) {
		return command
	}
	switch command {
	case "system/register_for_change_events", "player/set_volume", "player/set_mute", "player/set_play_state", "player/play_next", "player/play_previous", "player/set_play_mode", "browse/add_to_queue",
		"event/sources_changed", "event/players_changed", "event/groups_changed", "event/player_state_changed", "event/player_now_playing_changed", "event/player_now_playing_progress", "event/player_playback_error", "event/player_queue_changed", "event/player_volume_changed", "event/repeat_mode_changed", "event/shuffle_mode_changed", "event/group_volume_changed", "event/user_changed":
		return command
	}
	return "unknown"
}

// LogValue deliberately excludes free-form fields and raw payloads at all levels.
func (e Event) LogValue() slog.Value {
	reason := "unknown"
	if e.GapReason == "connection_closed" || e.GapReason == "event_buffer_overflow" {
		reason = e.GapReason
	}
	return slog.GroupValue(slog.String("command", diagnosticCommand(e.Command)),
		slog.Any("params", diagnosticParams(e.Params)), slog.Bool("gap", e.Gap),
		slog.String("gap_reason", reason), slog.Any("token", e.Token))
}

func (c *Client) traceFrame(r Response, err error) {
	if c.cfg.Logger == nil || !c.cfg.Logger.Enabled(c.ctx, slog.LevelDebug) {
		return
	}
	if err != nil {
		c.cfg.Logger.Debug("HEOS frame rejected", "error_kind", diagnosticError(err))
		return
	}
	name := "HEOS response"
	if r.Event {
		name = "HEOS event"
	}
	result := "unknown"
	if r.Result == "success" || r.Result == "fail" {
		result = r.Result
	}
	attrs := []any{"command", diagnosticCommand(r.Command), "params", diagnosticParams(r.Params),
		"result", result, "pending", r.Pending, "payload_bytes", len(r.Payload), "generation", c.View().Token.Generation}
	if r.Command == "browse/browse" && !r.Pending {
		attrs = append(attrs, "browse_options", diagnosticBrowseOptions(r.Options))
	}
	c.cfg.Logger.Debug(name, attrs...)
}

func diagnosticError(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, ErrProtocol):
		return "protocol"
	case errors.Is(err, ErrRejected):
		return "rejected"
	case errors.Is(err, ErrStale):
		return "stale"
	case errors.Is(err, ErrClosed):
		return "closed"
	case errors.Is(err, ErrInterrupted):
		return "interrupted"
	default:
		return "transport"
	}
}

func diagnosticDeviceError(err error) slog.Attr {
	var device *DeviceError
	if !errors.As(err, &device) {
		return slog.Attr{}
	}
	attrs := []slog.Attr{slog.Int("eid", device.Code), slog.String("reason", device.Reason())}
	if device.SystemCode != nil {
		attrs = append(attrs, slog.Int("syserrno", *device.SystemCode))
	}
	return slog.Attr{Key: "device_error", Value: slog.GroupValue(attrs...)}
}

// Denon 4.4.4 option 21 describes the current browsed container, not its children.
// Malformed/large optional metadata affects diagnostics only, never read acceptance.
func diagnosticBrowseOptions(raw json.RawMessage) slog.Value {
	result := func(state string, playable bool) slog.Value {
		return slog.GroupValue(slog.String("state", state), slog.Bool("playable_container", playable))
	}
	if len(raw) == 0 || string(raw) == "null" {
		return result("absent", false)
	}
	if len(raw) > 16*1024 {
		return result("invalid", false)
	}
	var groups []struct {
		Browse []struct {
			ID int `json:"id"`
		} `json:"browse"`
	}
	if err := json.Unmarshal(raw, &groups); err != nil || len(groups) > 64 {
		return result("invalid", false)
	}
	count, playable := 0, false
	for _, group := range groups {
		count += len(group.Browse)
		if count > 64 {
			return result("invalid", false)
		}
		for _, option := range group.Browse {
			playable = playable || option.ID == 21
		}
	}
	return result("valid", playable)
}
