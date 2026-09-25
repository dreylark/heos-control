package control

import (
	"context"
	"time"

	"github.com/dreylark/heos-control/internal/heos"
)

const maxBufferedParts = 256

// BufferedPlayback keeps a finite ordered manifest while bounding the native
// queue. Future part contents are validated when prepared, not at admission.
type BufferedPlayback struct {
	PartTracks        []int `json:"part_tracks"`
	RefillThreshold   int   `json:"refill_threshold"`
	MaxQueueTracks    int   `json:"max_queue_tracks"`
	RetainPrevious    int   `json:"retain_previous"`
	MaxSessionSeconds int   `json:"max_session_seconds"`
}

type bufferedPlayback struct {
	policy  BufferedPlayback
	expires time.Time
	pruned  int
}

func (b BufferedPlayback) validate(cmd Command) error {
	if cmd.Kind != CommandKindPlayback || cmd.ItemRefs == nil || cmd.Repeat != heos.RepeatOff || cmd.Shuffle ||
		len(b.PartTracks) != len(cmd.ItemRefs) || b.RefillThreshold < 1 || b.RefillThreshold > 100 ||
		b.MaxQueueTracks < 2 || b.MaxQueueTracks > 1000 || b.RetainPrevious < 0 || b.RetainPrevious > 100 ||
		b.MaxSessionSeconds < 1 || b.MaxSessionSeconds > 86400 {
		return heos.ErrBounds
	}
	if cmd.Automation != nil && cmd.Automation.DurationSeconds > b.MaxSessionSeconds {
		return heos.ErrBounds
	}
	total := 0
	for _, n := range b.PartTracks {
		if n < 1 || n > b.MaxQueueTracks-b.RefillThreshold-b.RetainPrevious-1 {
			return heos.ErrBounds
		}
		total += n
		if total > heos.MaxBrowseItems {
			return heos.ErrBounds
		}
	}
	return nil
}

func (p playbackPart) trackCount() int {
	if p.Count > 0 {
		return p.Count
	}
	return len(p.IDs)
}

func sameCatalog(a, b heos.Token) bool {
	return a.Generation == b.Generation && a.Catalog == b.Catalog
}

// Capture each reference locally after resolving the configured source once.
// Operation-owned descriptors outlive opaque HTTP refs, but never a catalog or
// connection generation change. No native IDs are accepted from the caller.
func (s *Reads) resolveBufferedPlayback(ctx context.Context, d Device, cmd Command) (heos.Item, map[heos.ID]bool, []playbackPart, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	token := d.Client.View().Token
	var sourceErr error
	for _, source := range s.sources {
		if source.Player != d.Config.Key {
			continue
		}
		_, _, sid, err := s.resolveSource(ctx, source.Key)
		if err != nil {
			sourceErr = err
			continue
		}
		resolver, ok := d.Catalogs[source.Key].(interface {
			Resolve(heos.ID, string) (heos.Item, error)
		})
		if !ok {
			continue
		}
		parts := make([]playbackPart, 0, len(cmd.ItemRefs))
		var actualSource heos.ID
		for i, ref := range cmd.ItemRefs {
			item, err := resolver.Resolve(sid, ref)
			if err != nil {
				parts = nil
				break
			}
			if item.ContainerID == "" || item.Source == "" || (actualSource != "" && item.Source != actualSource) {
				return heos.Item{}, nil, nil, heos.ErrBounds
			}
			actualSource = item.Source
			if item.Container != "yes" && (item.Playable != "yes" || item.MediaID == "" || (item.Type != "song" && item.Type != "track") || cmd.Buffered.PartTracks[i] != 1) {
				return heos.Item{}, nil, nil, heos.ErrBounds
			}
			parts = append(parts, playbackPart{Item: item, Count: cmd.Buffered.PartTracks[i], CatalogToken: token})
		}
		if len(parts) != len(cmd.ItemRefs) {
			continue
		}
		for i := 0; i < min(2, len(parts)); i++ {
			if err := prepareBufferedPart(ctx, d, &parts[i]); err != nil {
				return heos.Item{}, nil, nil, err
			}
		}
		view := d.Client.View()
		if err := ctx.Err(); err != nil {
			return heos.Item{}, nil, nil, err
		}
		if !view.Connected || !sameCatalog(token, view.Token) {
			return heos.Item{}, nil, nil, heos.ErrStaleReference
		}
		return parts[0].Item, idMembership(parts[0].IDs), parts, nil
	}
	if sourceErr != nil {
		return heos.Item{}, nil, nil, sourceErr
	}
	return heos.Item{}, nil, nil, heos.ErrStaleReference
}

func prepareBufferedPart(ctx context.Context, d Device, part *playbackPart) error {
	view := d.Client.View()
	if !view.Connected || !sameCatalog(part.CatalogToken, view.Token) {
		return heos.ErrStaleReference
	}
	if part.IDs != nil {
		return context.Cause(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tracks := []heos.Item{part.Item}
	if part.Item.Container == "yes" {
		var err error
		tracks, err = d.Client.BrowseAll(ctx, part.Item.Source, part.Item.ContainerID)
		if err != nil {
			return err
		}
	}
	if len(tracks) != part.Count {
		return heos.ErrBounds
	}
	ids := make([]heos.ID, 0, len(tracks))
	for _, track := range tracks {
		if (track.Source != "" && track.Source != part.Item.Source) || track.Container == "yes" || track.Playable != "yes" || track.MediaID == "" || (track.Type != "song" && track.Type != "track") {
			return heos.ErrBounds
		}
		ids = append(ids, track.MediaID)
	}
	view = d.Client.View()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !view.Connected || !sameCatalog(part.CatalogToken, view.Token) {
		return heos.ErrStaleReference
	}
	part.IDs = ids
	return nil
}

// Locate an occurrence using confirmed MID/QID evidence, including repeats.
func bufferedPosition(s heos.Snapshot, ids []heos.ID) (int, error) {
	if s.State != heos.PlayStatePlay || !orderedQueue(s.Queue, ids) || !queuedMedia(s.Queue, s.Media) {
		return 0, ErrOwnership
	}
	for i, m := range s.Queue.Items {
		if m.QueueID == s.Media.QueueID && m.ID == s.Media.ID {
			return i, nil
		}
	}
	return 0, ErrOwnership
}
