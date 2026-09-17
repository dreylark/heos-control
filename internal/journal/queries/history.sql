-- name: ListOperationHistory :many
SELECT id, kind, player, state, revision, phase, config_revision, selected_album,
    progress, outcome, error_code, created_at, updated_at, started_at, finished_at
FROM heos.operations
WHERE player = ANY(sqlc.arg(players)::text[])
    AND (sqlc.arg(is_operator)::boolean OR principal = sqlc.arg(principal)::text)
    AND (sqlc.arg(player_filter)::text = '' OR player = sqlc.arg(player_filter)::text)
    AND (sqlc.arg(state_filter)::text = '' OR state = sqlc.arg(state_filter)::text)
    AND (sqlc.arg(kind_filter)::text = '' OR kind = sqlc.arg(kind_filter)::text)
    AND (sqlc.arg(delivery_filter)::text = '' OR outcome->>'delivery' = sqlc.arg(delivery_filter)::text)
    AND (sqlc.narg(created_from)::timestamptz IS NULL OR created_at >= sqlc.narg(created_from)::timestamptz)
    AND (sqlc.narg(created_before)::timestamptz IS NULL OR created_at < sqlc.narg(created_before)::timestamptz)
    AND (sqlc.narg(before_time)::timestamptz IS NULL OR (created_at, id) < (sqlc.narg(before_time)::timestamptz, sqlc.arg(before_id)::text))
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_limit)::integer;
