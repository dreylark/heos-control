-- name: LockJournal :one
SELECT epoch, ready FROM heos.journal_control WHERE singleton = true FOR UPDATE;

-- name: ReadJournal :one
SELECT epoch, ready FROM heos.journal_control WHERE singleton = true FOR SHARE;

-- name: SetJournalEpoch :exec
UPDATE heos.journal_control SET epoch = $1, ready = $2 WHERE singleton = true;

-- name: LookupRequest :one
SELECT i.operation_id, i.request_hash FROM heos.idempotency_records i
WHERE i.principal = $1 AND i.method = $2 AND i.endpoint = $3 AND i.key = $4;

-- name: GetOperation :one
SELECT * FROM heos.operations WHERE id = $1;

-- name: ActiveOperation :one
SELECT o.* FROM heos.operations o JOIN heos.device_reservations r ON r.operation_id = o.id
WHERE o.player = $1;

-- name: CountOperations :one
SELECT count(*) FROM (SELECT id FROM heos.operations LIMIT $1) bounded;

-- name: InsertOperation :one
INSERT INTO heos.operations (id, kind, player, device_key, principal, epoch, state,
    config_revision, effective_arguments, created_at, updated_at, scheduled_for, not_after)
VALUES ($1, $2, $3, $4, $5, $6, 'accepted', $7, $8, $9, $9, $10, $11) RETURNING *;

-- name: InsertRequest :exec
INSERT INTO heos.idempotency_records (principal, method, endpoint, key, request_hash, operation_id, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: ReserveDevice :exec
INSERT INTO heos.device_reservations (device_key, operation_id, epoch) VALUES ($1, $2, $3);

-- name: TransitionOperation :one
UPDATE heos.operations SET state = $2, revision = revision + 1, phase = $3,
    progress = $4, outcome = $5, error_code = $6, updated_at = $7,
    started_at = CASE WHEN $2 = 'running' THEN COALESCE(started_at, $7) ELSE started_at END,
    finished_at = CASE WHEN $2 IN ('accepted', 'running') THEN NULL ELSE $7 END
WHERE id = $1 AND revision = $8 AND epoch = $9 AND finished_at IS NULL RETURNING *;

-- name: SelectAlbum :one
UPDATE heos.operations SET selected_album = $2, revision = revision + 1, updated_at = $3
WHERE id = $1 AND revision = $4 AND epoch = $5 AND selected_album IS NULL AND finished_at IS NULL RETURNING *;

-- name: ReleaseReservations :exec
DELETE FROM heos.device_reservations WHERE operation_id = ANY($1::text[]);

-- name: ExtendRetention :exec
UPDATE heos.idempotency_records SET expires_at = GREATEST(expires_at, $2)
WHERE operation_id = ANY($1::text[]);

-- name: InterruptBatch :many
UPDATE heos.operations SET state = 'uncertain', phase = 'restart',
    error_code = 'process_interrupted', outcome = jsonb_build_object(
        'device_state', 'unknown', 'playback_may_continue', true,
        'diagnostics', jsonb_build_object('reason', 'process_interrupted', 'rule', NULL,
            'phase', phase, 'detected_at', NULL, 'source', 'recovery', 'changed_fields', '[]'::jsonb)),
    revision = revision + 1, updated_at = $2, finished_at = $2
WHERE id IN (SELECT old.id FROM heos.operations old WHERE old.epoch <> $1 AND old.finished_at IS NULL
    ORDER BY old.id LIMIT $3 FOR UPDATE) RETURNING id;

-- name: PruneOperations :many
DELETE FROM heos.operations WHERE id IN (
    SELECT o.id FROM heos.operations o
    WHERE o.finished_at <= $1 AND NOT EXISTS (
        SELECT 1 FROM heos.idempotency_records i WHERE i.operation_id = o.id AND i.expires_at > $2)
    AND NOT EXISTS (SELECT 1 FROM heos.device_reservations r WHERE r.operation_id = o.id)
    ORDER BY o.finished_at, o.id LIMIT $3 FOR UPDATE OF o
) RETURNING id;
