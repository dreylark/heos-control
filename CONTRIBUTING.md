# Contributing

Start with [architecture](docs/ARCHITECTURE.md), [API usage](docs/API_USAGE.md) and
[device compatibility](docs/DEVICE_COMPATIBILITY.md). The service uses Go,
`net/http`, PostgreSQL/pgx, sqlc and an OpenAPI contract. Scheduling and client
media-selection policy belong outside this repository.

For a local environment, follow [getting started](docs/GETTING_STARTED.md).
Use the pinned tool versions. Tests live beside Go implementations; they use
synthetic devices and a separate disposable PostgreSQL database.

Before a pull request:

```sh
make verify
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(cat .golangci-lint-version) run ./...
```

Run the additional [area-specific checks](docs/TESTING.md) for SQL, chart,
workflow, container or persistence changes. Do not run tests against a production
database or physical speaker. Real device tests require an agreed device, volume
ceiling and test window.

Keep fixes focused and include a failing behavioral regression when changing
protocol, timing, ownership or persistence. Preserve uncertainty rather than
replaying a possibly delivered command. Explain the problem, resulting behavior,
validation and limitations in the PR. AI-assisted contributions follow the same
review and evidence requirements as any other change.

Treat `api/openapi.yaml` and SQL in `internal/journal/queries` as generator inputs.
Run `make generate` after changing them; do not edit generated Go by hand.
Migration authoring follows [the forward-only contract](docs/MIGRATIONS.md).

Never commit credentials, device serials/pins, private hosts, real media lists or
raw personal device captures. Fixtures must be synthetic or reviewed/redacted and
must describe the scope of their evidence. Manufacturer documentation is not
redistributed here; identify the reference by title, version and section.

[Release preparation](docs/RELEASING.md) is a maintainer workflow. A successful
local build does not authorize deployment or publication. Report vulnerabilities
through [the security process](SECURITY.md), without disclosing secrets in an issue.
