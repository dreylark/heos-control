package control

import (
	"context"
	"errors"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

type Preflight struct {
	Ready    bool     `json:"ready"`
	Player   Player   `json:"player"`
	Warnings []string `json:"warnings"`
}

// Preflight checks explicit playback settings using the same read-only media and
// device policy helpers as admission. It never reserves or takes over a device.
func (s *Reads) Preflight(ctx context.Context, key string, cmd Command) (Preflight, error) {
	d, err := s.device(key)
	if err != nil {
		return Preflight{}, err
	}
	if cmd.Kind != CommandKindPlayback || validatePlaybackSelection(cmd) != nil || cmd.validateExpectedOwner() != nil || cmd.Level < 0 || cmd.Level > 100 {
		return Preflight{}, heos.ErrBounds
	}
	// Malformed automation is an input error; the configured ceiling is a separate
	// readiness warning, including players whose ceiling is still unverified.
	maximum := 100
	if cmd.Automation != nil {
		if err := cmd.Automation.validate(cmd, &maximum); err != nil {
			return Preflight{}, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	refreshErr := d.Observer.Refresh(heos.WithObservationTrigger(ctx, "preflight"))
	before := d.Observer.Snapshot()
	player, _ := s.Player(key)
	result := Preflight{Player: player, Warnings: []string{}}
	if refreshErr != nil {
		result.Warnings = append(result.Warnings, "state_unavailable")
	}
	switch err := deviceSafety(d.Config, before); {
	case errors.Is(err, heos.ErrReadOnly):
		if !d.Config.WritesEnabled {
			result.Warnings = append(result.Warnings, "writes_disabled")
		}
		if d.Config.VolumeCeiling == nil {
			result.Warnings = append(result.Warnings, "volume_policy_unverified")
		}
	case errors.Is(err, ErrGrouped):
		result.Warnings = append(result.Warnings, "grouped_target")
	case err != nil && refreshErr == nil:
		result.Warnings = append(result.Warnings, "state_unavailable")
	}
	if ceiling := d.Config.VolumeCeiling; ceiling != nil && (cmd.Level > *ceiling || cmd.Automation != nil && cmd.Automation.TargetLevel > *ceiling) {
		result.Warnings = append(result.Warnings, "volume_policy_exceeded")
	}
	if cmd.Automation != nil && !cmd.Takeover && before.State == heos.PlayStatePlay {
		result.Warnings = append(result.Warnings, "player_busy")
	}
	// Skip catalog I/O when the device cannot currently accept this request.
	if len(result.Warnings) > 0 {
		return result, nil
	}
	if _, _, _, err := s.resolvePlayback(ctx, d, cmd); err != nil {
		return Preflight{}, err
	}
	current, _ := s.Player(key)
	if current.Revision != player.Revision || !sameState(before, d.Observer.Snapshot()) || ctx.Err() != nil {
		result.Warnings = append(result.Warnings, "state_changed")
	}
	result.Ready = len(result.Warnings) == 0
	return result, nil
}
