package control

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/heos"
)

// NowPlaying is the last device-reported media, independent of queue ownership.
// Stale media may be retained for display while its replacement is verified.
type NowPlaying struct {
	Song    string `json:"song"`
	Album   string `json:"album"`
	Artist  string `json:"artist"`
	QueueID string `json:"queue_id,omitempty"`
	MediaID string `json:"media_id,omitempty"`
	Stale   bool   `json:"stale"`
}

func (s *Reads) nowPlaying(player config.Player, v heos.Snapshot) *NowPlaying {
	if !v.Connected || v.MediaUnavailable || v.ObservedAt.IsZero() || v.Media == nil ||
		(v.State != "play" && v.State != "pause") || v.Player.ID == "" || player.Serial == "" ||
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
	return media
}
