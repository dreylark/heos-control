package journal

import (
	"context"
	"database/sql"
	"time"

	"github.com/pressly/goose/v3/lock"
)

// Goose detaches cleanup from the migration context. Its unlock retry settings
// bound retries, but each database call still needs its own cancellation deadline.
type boundedSessionLocker struct {
	lock.SessionLocker
}

func (l boundedSessionLocker) SessionUnlock(ctx context.Context, conn *sql.Conn) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return l.SessionLocker.SessionUnlock(ctx, conn)
}
