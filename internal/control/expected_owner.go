package control

import "github.com/dreylark/heos-control/internal/heos"

// ExpectedOwner fences a takeover against a captured reservation independently
// of the player revision. Absence preserves ordinary explicit takeover behavior.
func (cmd Command) validateExpectedOwner() error {
	if cmd.ExpectedOwner == "" {
		return nil
	}
	if !cmd.Takeover || len(cmd.ExpectedOwner) > 128 {
		return heos.ErrBounds
	}
	switch cmd.Kind {
	case CommandKindPlayback, CommandKindVolume, CommandKindMute, CommandKindTransport:
		return nil
	default:
		return heos.ErrBounds
	}
}
