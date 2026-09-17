-- name: InspectRuntimePrivileges :one
-- Privilege lists passed to has_table_privilege mean ANY, not ALL. Keep one
-- required privilege per row so bool_and verifies every runtime DML grant.
WITH required AS (
    SELECT 'operations'::text AS table_name, 'SELECT'::text AS privilege
    UNION ALL SELECT 'operations', 'INSERT'
    UNION ALL SELECT 'operations', 'UPDATE'
    UNION ALL SELECT 'operations', 'DELETE'
    UNION ALL SELECT 'idempotency_records', 'SELECT'
    UNION ALL SELECT 'idempotency_records', 'INSERT'
    UNION ALL SELECT 'idempotency_records', 'UPDATE'
    UNION ALL SELECT 'idempotency_records', 'DELETE'
    UNION ALL SELECT 'device_reservations', 'SELECT'
    UNION ALL SELECT 'device_reservations', 'INSERT'
    UNION ALL SELECT 'device_reservations', 'UPDATE'
    UNION ALL SELECT 'device_reservations', 'DELETE'
    UNION ALL SELECT 'journal_control', 'SELECT'
    UNION ALL SELECT 'journal_control', 'UPDATE'
    UNION ALL SELECT 'schema_migrations', 'SELECT'
), objects AS (
    SELECT required.privilege, namespace.oid AS schema_id, relation.oid AS table_id
    FROM required
    LEFT JOIN pg_catalog.pg_namespace AS namespace ON namespace.nspname = 'heos'
    LEFT JOIN pg_catalog.pg_class AS relation
        ON relation.relnamespace = namespace.oid
        AND relation.relname = required.table_name
        AND relation.relkind IN ('r', 'p')
)
SELECT
    bool_and(schema_id IS NOT NULL)::boolean AS schema_present,
    bool_and(table_id IS NOT NULL)::boolean AS tables_present,
    coalesce(bool_and(
        schema_id IS NOT NULL AND table_id IS NOT NULL
        AND pg_catalog.has_schema_privilege(schema_id, 'USAGE')
        AND pg_catalog.has_table_privilege(table_id, privilege)
    ), false)::boolean AS permissions
FROM objects;
