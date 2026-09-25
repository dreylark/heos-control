package heos

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// HEOS CLI Protocol Specification 1.17, 4.2.17 accepts comma-separated queue
// IDs. The list and frame bounds are application policy, not firmware limits.
// Membership, current-entry exclusion and post-write confirmation belong to
// control; the transport does not infer queue positions from these opaque IDs.
func validateQueueRemoval(ids []string) error {
	if len(ids) == 0 || len(ids) > 1000 {
		return ErrBounds
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" || len(id) > 4096 || !utf8.ValidString(id) || strings.ContainsRune(id, ',') || strings.ContainsFunc(id, unicode.IsControl) {
			return ErrBounds
		}
		if _, exists := seen[id]; exists {
			return ErrBounds
		}
		seen[id] = struct{}{}
	}
	return nil
}
