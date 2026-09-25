package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

const maxPlaybackParts = 32

type playbackPart struct {
	Item         heos.Item  `json:"item"`
	IDs          []heos.ID  `json:"ids"`
	Count        int        `json:"count,omitempty"`
	CatalogToken heos.Token `json:"-"`
}

// Durable identity retains declared part counts and a digest of descriptors
// plus prepared content. Buffered future content is deliberately not enumerated.
// Recovery never reconstructs or resumes device commands.
type playbackPlanIdentity struct {
	SHA256     string `json:"sha256"`
	PartTracks []int  `json:"part_tracks"`
}

func planIdentity(parts []playbackPart) *playbackPlanIdentity {
	if len(parts) == 0 {
		return nil
	}
	b, _ := json.Marshal(parts)
	p := &playbackPlanIdentity{SHA256: fmt.Sprintf("%x", sha256.Sum256(b))}
	for _, part := range parts {
		p.PartTracks = append(p.PartTracks, part.trackCount())
	}
	return p
}

func validatePlaybackSelection(cmd Command) error {
	if !cmd.Repeat.Known() || (cmd.ItemRef != "") == (cmd.ItemRefs != nil) || len(cmd.ItemRef) > 256 {
		return heos.ErrBounds
	}
	if cmd.ItemRefs == nil {
		if cmd.Buffered != nil {
			return heos.ErrBounds
		}
		return nil
	}
	limit := maxPlaybackParts
	if cmd.Buffered != nil {
		if err := cmd.Buffered.validate(cmd); err != nil {
			return err
		}
		limit = maxBufferedParts
	}
	if len(cmd.ItemRefs) == 0 || len(cmd.ItemRefs) > limit || cmd.Shuffle {
		return heos.ErrBounds
	}
	for _, ref := range cmd.ItemRefs {
		if len(ref) == 0 || len(ref) > 256 {
			return heos.ErrBounds
		}
	}
	return nil
}

func (s *Reads) resolvePlayback(ctx context.Context, d Device, cmd Command) (heos.Item, map[heos.ID]bool, []playbackPart, error) {
	if err := validatePlaybackSelection(cmd); err != nil {
		return heos.Item{}, nil, nil, err
	}
	if cmd.Buffered != nil {
		return s.resolveBufferedPlayback(ctx, d, cmd)
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if cmd.ItemRefs == nil {
		item, err := s.resolveItem(ctx, d, cmd.ItemRef)
		if err != nil {
			return item, nil, nil, err
		}
		members, err := playableMembers(ctx, d, item)
		return item, members, nil, err
	}
	token := d.Client.View().Token
	var parts []playbackPart
	var sid heos.ID
	count := 0
	for _, ref := range cmd.ItemRefs {
		item, err := s.resolveItem(ctx, d, ref)
		if err != nil {
			return heos.Item{}, nil, nil, err
		}
		if item.Source == "" || (sid != "" && sid != item.Source) {
			return heos.Item{}, nil, nil, ErrAmbiguous
		}
		sid = item.Source
		tracks := []heos.Item{item}
		if item.Container == "yes" {
			tracks, err = d.Client.BrowseAll(ctx, item.Source, item.ContainerID)
			if err != nil {
				return heos.Item{}, nil, nil, err
			}
		}
		count += len(tracks)
		if len(tracks) == 0 || count > heos.MaxBrowseItems {
			return heos.Item{}, nil, nil, heos.ErrBounds
		}
		part := playbackPart{Item: item}
		for _, track := range tracks {
			if track.Source != "" && track.Source != sid {
				return heos.Item{}, nil, nil, ErrAmbiguous
			}
			if track.Container == "yes" || track.Playable != "yes" || track.MediaID == "" || (track.Type != "song" && track.Type != "track") {
				return heos.Item{}, nil, nil, heos.ErrBounds
			}
			part.IDs = append(part.IDs, track.MediaID)
		}
		parts = append(parts, part)
	}
	view := d.Client.View()
	if err := ctx.Err(); err != nil {
		return heos.Item{}, nil, nil, err
	}
	if !view.Connected || view.Token.Generation != token.Generation || view.Token.Catalog != token.Catalog {
		return heos.Item{}, nil, nil, heos.ErrStaleReference
	}
	return parts[0].Item, idMembership(parts[0].IDs), parts, nil
}
func idMembership(ids []heos.ID) map[heos.ID]bool {
	m := make(map[heos.ID]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}
