package control

import (
	"encoding/json"
	"github.com/dreylark/heos-control/internal/journal"
	"time"
)

// Operation is the public projection; principal and internal journal arguments
// are deliberately kept behind the authorization boundary.
type Operation struct {
	Playback       *PlaybackProgress `json:"playback"`
	Outcome        json.RawMessage   `json:"outcome"`
	ID             string            `json:"id"`
	Kind           string            `json:"kind"`
	Player         string            `json:"player"`
	State          string            `json:"state"`
	Phase          string            `json:"phase"`
	Revision       int64             `json:"revision"`
	ConfigRevision string            `json:"config_revision"`
	SelectedAlbum  *string           `json:"selected_album"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	StartedAt      *time.Time        `json:"started_at"`
	FinishedAt     *time.Time        `json:"finished_at"`
	ErrorCode      *string           `json:"error_code"`
}

func ProjectOperation(op journal.Operation) Operation {
	var code *string
	if op.ErrorCode != "" {
		code = &op.ErrorCode
	}
	outcome := op.Outcome
	if len(outcome) == 0 {
		outcome = json.RawMessage(`{}`)
	}
	var playback *PlaybackProgress
	var progress PlaybackProgress
	if json.Unmarshal(op.Progress, &progress) == nil && !progress.PlaybackStartedAt.IsZero() {
		playback = &progress
	}
	return Operation{Playback: playback, ID: op.ID, Kind: op.Kind, Player: op.Player, State: string(op.State), Phase: op.Phase, Revision: op.Revision, ConfigRevision: op.ConfigRevision, SelectedAlbum: op.SelectedAlbum, CreatedAt: op.CreatedAt, UpdatedAt: op.UpdatedAt, StartedAt: op.StartedAt, FinishedAt: op.FinishedAt, ErrorCode: code, Outcome: outcome}
}
