# PostgreSQL migrations

Migrations use embedded, forward-only SQL through goose 3.28.0. Runtime queries
use native pgx; the pgx database/sql adapter is confined to migration execution.
Ordinary service startup checks compatibility and recovers the journal but
performs no DDL.

PostgreSQL 18 is the local test baseline; Compose pins 18.6. Other major versions
are not covered by those checks. Runtime enforces schema compatibility rather
than a hard-coded PostgreSQL major.

## Provisioning and execution

Provision the database, a schema-owner login and the privilege role
`heos_runtime` before migration. A separate runtime login can inherit that role.
Use verified TLS and separate owner/runtime password Secrets.

The initial migration creates the application schema/tables and grants:

| Object | Runtime privileges |
| --- | --- |
| Database | CONNECT, provisioned externally |
| Schema `heos` | USAGE |
| `operations`, `idempotency_records`, `device_reservations` | SELECT, INSERT, UPDATE, DELETE |
| `journal_control` | SELECT, UPDATE |
| `schema_migrations` | SELECT |

Runtime must not own the database/schema, receive CREATE/TEMP rights or modify
migration metadata. The service does not provision databases, roles or Secrets.

For local/non-Helm use, run the application entrypoint with owner configuration:

```sh
heos-control migrate -config /etc/heos-control/migration.json
```

This uses the same database TLS and timeout settings as runtime. For the
disposable development database, `make db-up migrate` initializes local files,
starts PostgreSQL and applies migrations using `.local/owner.json`.

The Helm chart always renders its migration Job, ConfigMap and ServiceAccount;
NetworkPolicy follows `networkPolicy.enabled`. Set `migration.user`,
`migration.credentialsSecret` and `migration.passwordKey` to owner credentials.
The Job uses the same image as runtime, mounts no player/API credentials, and
has only DNS/database egress. Runtime never mounts the owner password.

Default `migration.hooks: helm` runs before every install/upgrade. Supporting
resources have hook weight -2 and the Job weight -1. Provision owner-password,
database-CA and image-pull Secrets before hooks execute. Job failure blocks
rollout; its default active deadline is 300 seconds with no Pod retries.
Successful Jobs are removed; failed Jobs remain for diagnosis and are replaced
on the next attempt. Supporting hook resources can remain after uninstall;
remove them only when no migration Job is running. The external database and
Secrets remain operator-owned.

For an external delivery controller, set `migration.hooks: external` and supply
`migration.jobAnnotations`, `migration.resourceAnnotations` and appropriate
`deploymentAnnotations`. The operator must order dependencies, run the Job for
each release and wait for success before starting the new application. There is
no migration-disable switch or bundled delivery controller.

## Upgrade and rollback contract

Each release's schema changes must preserve the previous application's reads
and writes on every committed intermediate and final schema, including data
written by the new release.

1. Apply migrations while the previous application can remain active.
2. If a file fails, block the new rollout. Earlier successfully committed files
   remain applied and must still support the previous application.
3. After success, replace the application with one controller using `Recreate`.
4. If the new application is defective, the previous image can run on the
   upgraded schema within that verified compatibility window.

This is schema/data compatibility, not a guarantee against database outages or
DDL lock delays. Keep statement and lock waits bounded. It does not authorize
overlapping controllers or `RollingUpdate`.

There are no Down, force or reset commands. Application rollback retains the
database. Backup restoration is separate recovery work and may lose idempotency
history; follow [the restore procedure](OPERATIONS.md#backup-and-restore).

## Versions, checksums and compatibility

Files are embedded from
[internal/journal/migrations](../internal/journal/migrations).
Names are consecutive five-digit versions. Every file declares:

```sql
-- +goose Up
-- heos:min-runtime=1
```

`min-runtime` is a migration baseline, not application SemVer. A binary's
baseline is its highest embedded migration. Declarations are explicit,
nondecreasing and cannot exceed their own migration version.

The runtime requires its complete embedded prefix with matching checksums.
It accepts a contiguous newer tail only when every declared minimum permits
that binary's baseline. For example, a binary requiring migration 2 can run on
migrations 3 and 4 if both declare minimum 2. It rejects minimum 3. A binary
requiring migration 4 rejects a database ending at migration 3.

An old binary cannot verify the contents of future files it does not embed;
their declarations are schema-owner assertions backed by compatibility tests.
Sharing a baseline does not extend the tested predecessor compatibility window.
A release without schema changes need not advance its baseline.

The migration command is stricter: it rejects unknown future versions and checks
agreement between the application ledger `heos.schema_migrations` and goose's
`heos.goose_db_version`. The former records checksums and compatibility floors;
goose owns its native version records. SQL and both metadata records commit
together, once per migration file.

An explicitly configured advisory session lock serializes migrators. Lock,
statement, command and cleanup deadlines are bounded. A lost COMMIT
acknowledgement may leave a committed file: inspect durable metadata and rerun
after resolving the cause. The migrator reconciles history before applying
pending files; do not edit markers or blindly replay SQL.

## Author a change

Applied SQL files are immutable. Correct their effects with a later migration.
A never-applied broken file can be corrected before release; a later file cannot
skip it. Do not hide unexpected state with indiscriminate `IF NOT EXISTS`.

For an incompatible change, use expand/contract across releases: add structures,
move reads/writes, then remove obsolete structures after the supported
predecessor no longer depends on them. Review constraints, defaults and stored
data semantics as well as columns.

Raise the minimum baseline only after the supported previous release has
adopted it. If that preparation requires no DDL, an explicit compatibility
preparation migration can establish the next baseline before a later removal.
Never lower a declaration merely to make readiness pass.

Use transactional Up SQL only: no Down section, `NO TRANSACTION`, out-of-order
application or standalone DDL. Goose `StatementBegin`/`StatementEnd` blocks
are available when SQL grouping needs them.

After changing migration/schema inputs:

```sh
make generate
make generate-check
make db-up
make coverage
```

Run coverage with exclusive use of the disposable database. Tests must cover
fresh/no-op application, concurrent migrators, partial-batch failure, rollback of
SQL and metadata together, cancellation, lost acknowledgements and checksum/floor
rejection. Prove the predecessor's actual reads and writes against intermediate
and final schemas; metadata comparisons alone are insufficient.

Migration or checksum errors are never repaired by service startup or
`doctor`. There is no automatic schema reset or legacy-ledger adoption.
