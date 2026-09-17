package control

import (
	"context"
	"github.com/dreylark/heos-control/internal/heos"
)

func (s *Reads) resolveItem(ctx context.Context, d Device, ref string) (heos.Item, error) {
	var sourceErr error
	for _, source := range s.sources {
		if source.Player != d.Config.Key {
			continue
		}
		_, _, sid, e := s.resolveSource(ctx, source.Key)
		if e != nil {
			sourceErr = e
			continue
		}
		resolver, ok := d.Catalogs[source.Key].(interface {
			Resolve(heos.ID, string) (heos.Item, error)
		})
		if !ok {
			continue
		}
		item, e := resolver.Resolve(sid, ref)
		if e == nil {
			if item.Playable != "yes" || item.ContainerID == "" || (item.Container != "yes" && (item.MediaID == "" || (item.Type != "song" && item.Type != "track"))) {
				return item, heos.ErrBounds
			}
			return item, nil
		}
	}
	if sourceErr != nil {
		return heos.Item{}, sourceErr
	}
	return heos.Item{}, heos.ErrStaleReference
}

// Resolve bounded local container membership before any device mutation. Album
// labels are not media identity; this also supports nested playable containers.
func playableMembers(ctx context.Context, d Device, item heos.Item) (map[heos.ID]bool, error) {
	members := map[heos.ID]bool{}
	if item.Container != "yes" {
		members[item.MediaID] = true
		return members, nil
	}
	type node struct {
		cid   heos.ID
		depth int
	}
	todo := []node{{item.ContainerID, 0}}
	seen := map[heos.ID]bool{}
	count := 0
	token := d.Client.View().Token
	for len(todo) > 0 {
		n := todo[0]
		todo = todo[1:]
		if seen[n.cid] || n.depth > 8 || len(seen) >= heos.MaxBrowsePages {
			return nil, heos.ErrBounds
		}
		seen[n.cid] = true
		children, e := d.Client.BrowseAll(ctx, item.Source, n.cid)
		if e != nil {
			return nil, e
		}
		count += len(children)
		if count > heos.MaxBrowseItems {
			return nil, heos.ErrBounds
		}
		for _, child := range children {
			if child.Source != "" && child.Source != item.Source {
				return nil, ErrAmbiguous
			}
			if child.Container == "yes" {
				if child.ContainerID == "" {
					return nil, heos.ErrBounds
				}
				todo = append(todo, node{child.ContainerID, n.depth + 1})
				continue
			}
			if child.Playable != "yes" || child.MediaID == "" || (child.Type != "song" && child.Type != "track") {
				return nil, heos.ErrBounds
			}
			members[child.MediaID] = true
		}
	}
	view := d.Client.View()
	if !view.Connected || view.Token.Generation != token.Generation || view.Token.Catalog != token.Catalog {
		return nil, heos.ErrStaleReference
	}
	if len(members) == 0 {
		return nil, heos.ErrBounds
	}
	return members, nil
}
