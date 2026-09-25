package control

import (
	"encoding/json"
	"github.com/dreylark/heos-control/internal/journal"
	"time"
)

// Operation is the public projection; principal and internal journal arguments
// are deliberately kept behind the authorization boundary.
type Operation struct {
	QueueLoading   *QueueLoadingProgress `json:"queue_loading"`
	Playback       *PlaybackProgress     `json:"playback"`
	Outcome        json.RawMessage       `json:"outcome"`
	ID             string                `json:"id"`
	Kind           string                `json:"kind"`
	Player         string                `json:"player"`
	State          string                `json:"state"`
	Phase          string                `json:"phase"`
	Revision       int64                 `json:"revision"`
	ConfigRevision string                `json:"config_revision"`
	SelectedAlbum  *string               `json:"selected_album"`
	CreatedAt      time.Time             `json:"created_at"`
	UpdatedAt      time.Time             `json:"updated_at"`
	StartedAt      *time.Time            `json:"started_at"`
	FinishedAt     *time.Time            `json:"finished_at"`
	ErrorCode      *string               `json:"error_code"`
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
	var progress operationProgress
	if json.Unmarshal(op.Progress, &progress) == nil && progress.PlaybackProgress != nil && !progress.PlaybackStartedAt.IsZero() {
		playback = progress.PlaybackProgress
	}
	// Admission stores the frozen plan atomically. Before the first worker
	// transition (including recovery there), no part is durably confirmed.
	if progress.QueueLoading == nil {
		var args struct {
			Plan *playbackPlanIdentity `json:"ordered_plan"`
		}
		if json.Unmarshal(op.EffectiveArguments, &args) == nil && args.Plan != nil && len(args.Plan.PartTracks) > 0 {
			progress.QueueLoading = &QueueLoadingProgress{TotalParts: len(args.Plan.PartTracks)}
			for _, count := range args.Plan.PartTracks {
				progress.QueueLoading.TotalTracks += count
			}
		}
	}
	return Operation{QueueLoading: progress.QueueLoading, Playback: playback, ID: op.ID, Kind: op.Kind, Player: op.Player, State: string(op.State), Phase: op.Phase, Revision: op.Revision, ConfigRevision: op.ConfigRevision, SelectedAlbum: op.SelectedAlbum, CreatedAt: op.CreatedAt, UpdatedAt: op.UpdatedAt, StartedAt: op.StartedAt, FinishedAt: op.FinishedAt, ErrorCode: code, Outcome: outcome}
}
