# Getting started

Start with a local development instance. It creates its own database roles,
passwords and TLS certificates; do not use these generated credentials in a
production deployment. For Kubernetes packaging, see [operations](OPERATIONS.md).

## 1. Start the database and service

Install the Go version declared in `go.mod`, Docker with Compose, curl, OpenSSL
and jq. Run from the repository root:

```sh
make db-up
make migrate
make run
```

`make db-up` initializes `.local/` and starts PostgreSQL. `make migrate` uses the
schema-owner connection; `make run` uses the restricted runtime connection.
The generated configuration points at the checkout's absolute `.local/` path.
No speaker is contacted until you configure a player.

In another terminal:

```sh
curl --fail --cacert .local/ca.crt https://localhost:8443/livez
curl --fail --cacert .local/ca.crt https://localhost:8443/readyz
```

Both should return a status of `live` or `ready` respectively. PostgreSQL is
published on loopback port 15432. Host `make run` uses `:8443`; change `listen`
in `.local/runtime.json` to `127.0.0.1:8443` if it must only accept local clients.
The containerized API publishes only on loopback.

The development CA private key is discarded; certificates last one year.
Subsequent `make dev-init` calls retain the existing credentials. Do not regenerate
passwords while retaining a database initialized with the old ones.

## 2. Configure a player with writes disabled

Stop `make run` with Ctrl-C before editing configuration. Add a player to
`.local/runtime.json` following [player identity and TLS](CONFIGURATION.md#players-and-device-trust).
Use your verified serial, model, address and certificate fingerprint; leave
`writes_enabled` false. A placeholder identity cannot pass device diagnostics.
Do not run a second controller against a player already managed by another
instance.

The generated `.local/container-runtime.json` is a separate configuration used
by Compose. If you switch to a container, copy the player/source settings there
as well; retain its container-specific file paths and database hostname.

## 3. Create a client token

This example is for a fresh development environment with an empty credential
file. `room-speaker` must already exist in the runtime configuration. For an
existing environment, add a new entry instead of replacing the credential file.

```sh
umask 077
HEOS_TOKEN=$(openssl rand -hex 32)
HEOS_TOKEN_SHA256=$(printf '%s' "$HEOS_TOKEN" | openssl dgst -sha256 -r | cut -d' ' -f1)
printf '%s' "$HEOS_TOKEN" > .local/client-token
jq -n --arg digest "$HEOS_TOKEN_SHA256" '{
  version: 1,
  credentials: [{
    principal: "local-client",
    token_sha256: $digest,
    scopes: ["read"],
    players: ["room-speaker"]
  }]
}' > .local/credentials.json
unset HEOS_TOKEN_SHA256
```

The server stores only the hash. The caller sends the original token; a hash
cannot be used as the bearer token. Keep `.local/client-token` private. The
`.local/` directory is ignored by Git and excluded from the Docker build.
For Compose, its distinct non-root UID must be able to read mounted secret leaf
files; see [file permissions](CONFIGURATION.md#files-and-restarts).

## 4. Check and start

```sh
go run ./cmd/heos-control config check -config .local/runtime.json
go run ./cmd/heos-control doctor -config .local/runtime.json -format json
make run
```

The first command is offline. `doctor` checks PostgreSQL and the configured
speaker without changing playback or running migrations. It verifies connectivity
and identity, not permission to start music. Diagnose a failure before enabling
writes; see [diagnostics](OPERATIONS.md).

In another terminal:

```sh
HEOS_TOKEN=$(cat .local/client-token)
curl --fail-with-body --cacert .local/ca.crt \
  --header "Authorization: Bearer $HEOS_TOKEN" \
  https://localhost:8443/v1/players
```

Check `availability`, `stale` and the read capability. A configured player can
appear in this list while unavailable; a successful HTTP response alone does not
establish a working device connection.

## 5. Explore the API and playback

Set `docs_enabled` to true in runtime configuration and restart to open
`https://localhost:8443/docs`. Browsers must trust your development CA; the
renderer is fetched from a CDN. Enter the bearer token in the reference UI.

Before playback, set a verified per-player volume ceiling, opt into writes,
configure a local source, and grant the caller the required scopes. Follow the
[complete playback workflow](API_USAGE.md); preflight never starts music.

## Container alternative and cleanup

After database initialization:

```sh
make image
docker compose --profile tools run --rm migrate
docker compose --profile app up -d heos-control
docker compose logs -f heos-control
```

Stop any host instance first: both use port 8443 and must not share a live
controller journal. `make down` removes the local containers/network and retains
the database volume. To deliberately discard this development environment:

```sh
docker compose --profile app --profile tools --profile tests down -v
rm -r .local
```

This removes all local development/test data and credentials. Do not use the
reset procedure against a production database.
