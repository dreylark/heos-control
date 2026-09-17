package heos

import (
	"fmt"
	"strconv"
)

// DeviceError preserves Denon 6.1–6.2 details for explicit classification.
// Text may contain private media/account data; Error deliberately omits it.
type DeviceError struct {
	Command    string
	Code       int
	Text       string
	SystemCode *int
}

func (e *DeviceError) Error() string {
	return fmt.Sprintf("HEOS device rejected %s (eid=%d, reason=%s)", e.Command, e.Code, e.Reason())
}
func (e *DeviceError) Unwrap() error { return ErrRejected }

// Reason names Denon 6.2 error codes without exposing free-form device text.
// Unknown codes retain their numeric value in Code; names imply no retry policy.
func (e *DeviceError) Reason() string {
	switch e.Code {
	case 1:
		return "unrecognized_command"
	case 2:
		return "invalid_id"
	case 3:
		return "invalid_arguments"
	case 4:
		return "data_unavailable"
	case 5:
		return "resource_unavailable"
	case 6:
		return "invalid_credentials"
	case 7:
		return "command_not_executed"
	case 8:
		return "user_not_logged_in"
	case 9:
		return "parameter_out_of_range"
	case 10:
		return "user_not_found"
	case 11:
		return "internal_error"
	case 12:
		return "system_error"
	case 13:
		return "device_busy"
	case 14:
		return "cannot_play"
	case 15:
		return "option_not_supported"
	case 16:
		return "device_queue_full"
	case 17:
		return "skip_limit_reached"
	default:
		return "unknown"
	}
}

func rejection(r Response) error {
	code, err := strconv.Atoi(r.Params.Get("eid"))
	if err != nil || code <= 0 {
		return &CommandError{Uncertain, ErrProtocol}
	}
	device := &DeviceError{Command: r.Command, Code: code, Text: r.Params.Get("text")}
	if r.Params.Has("syserrno") {
		n, err := strconv.Atoi(r.Params.Get("syserrno"))
		if err != nil {
			return &CommandError{Uncertain, ErrProtocol}
		}
		device.SystemCode = &n
	}
	return &CommandError{Rejected, device}
}
