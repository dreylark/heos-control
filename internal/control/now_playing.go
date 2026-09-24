package control

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
)

// NowPlaying is the last device-reported media, independent of queue ownership.
// Stale media may be retained for display while its replacement is verified.
type NowPlaying struct {
	Song     string              `json:"song"`
	Album    string              `json:"album"`
	Artist   string              `json:"artist"`
	QueueID  string              `json:"queue_id,omitempty"`
	MediaID  string              `json:"media_id,omitempty"`
	Stale    bool                `json:"stale"`
	Progress *NowPlayingProgress `json:"progress,omitempty"`
}

// NowPlayingProgress is a last-sampled playhead in milliseconds. It is display
// data and is excluded from player revision. DurationMS is omitted when the
// device reports an unknown duration.
type NowPlayingProgress struct {
	PositionMS int64     `json:"position_ms"`
	DurationMS *int64    `json:"duration_ms,omitempty"`
	SampledAt  time.Time `json:"sampled_at"`
}

func (s *Reads) nowPlaying(player config.Player, v heos.Snapshot) *NowPlaying {
	if !v.Connected || v.MediaUnavailable || v.ObservedAt.IsZero() || v.Media == nil ||
		!v.State.Active() || v.Player.ID == "" || player.Serial == "" ||
		string(v.Player.Serial) != player.Serial || (player.Model != "" && string(v.Player.Model) != player.Model) {
		return nil
	}
	media := &NowPlaying{
		Song: string(v.Media.Song), Album: string(v.Media.Album), Artist: string(v.Media.Artist),
		QueueID: string(v.Media.QueueID), Stale: v.Stale || v.MediaStale,
	}
	if v.Media.ID != "" {
		// Native MIDs can contain private URLs. This bounded, process-scoped
		// identity supports equality without exposing a reversible native value.
		tuple, _ := json.Marshal([4]string{s.epoch, player.Key, string(v.Media.Source), string(v.Media.ID)})
		media.MediaID = fmt.Sprintf("%x", sha256.Sum256(tuple))
	}
	if !media.Stale {
		media.Progress = playheadProgress(v)
	}
	return media
}

func playheadProgress(v heos.Snapshot) *NowPlayingProgress {
	sample := v.Playhead
	if sample == nil || v.Media == nil || sample.Source != v.Media.Source || sample.Media != v.Media.ID || sample.Queue != v.Media.QueueID {
		return nil
	}
	out := &NowPlayingProgress{PositionMS: sample.PositionMS, SampledAt: sample.At.UTC()}
	if sample.DurationMS > 0 {
		duration := sample.DurationMS
		out.DurationMS = &duration
	}
	return out
}

// revisionNowPlaying omits the playhead so a moving sample cannot change
// Player.revision, the detail ETag, or player_changed.
func revisionNowPlaying(now *NowPlaying) *NowPlaying {
	if now == nil || now.Progress == nil {
		return now
	}
	copy := *now
	copy.Progress = nil
	return &copy
}
