-- name: ListMigrations :many
SELECT version, checksum, min_runtime
FROM heos.schema_migrations
ORDER BY version;
