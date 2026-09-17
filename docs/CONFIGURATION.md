# Configuration

The service reads strict JSON with `heos-control serve -config PATH`; the default
path is `/etc/heos-control/config.json`. Unknown/duplicate fields, invalid bounds
and inconsistent references are rejected. Helm renders this same configuration
from chart values; there is no environment-variable configuration layer.

## Runtime configuration

This is a template: replace the host, file paths and role details before use.
For a working disposable environment, run `make dev-init` instead.

```json
{
  "listen": ":8443",
  "log_level": "info",
  "cert_file": "/etc/heos-control/api-tls/tls.crt",
  "key_file": "/etc/heos-control/api-tls/tls.key",
  "shutdown_seconds": 25,
  "docs_enabled": false,
  "credentials_file": "/etc/heos-control/credentials/credentials.json",
  "database": {
    "host": "postgres.example.invalid",
    "port": 5432,
    "name": "heos_control",
    "user": "heos_runtime",
    "password_file": "/etc/heos-control/database/password",
    "ca_file": "/etc/heos-control/database-ca/ca.crt",
    "max_connections": 4,
    "timeout_seconds": 5,
    "lock_timeout_seconds": 2
  },
  "players": [],
  "sources": []
}
```

| Setting | Default / constraint |
| --- | --- |
| `listen` | `:8443`; explicit numeric TCP port 1024..65535 |
| `log_level` | `info`; `debug`, `info`, `warn`, `error` |
| `cert_file`, `key_file` | Required server certificate chain and matching key |
| `shutdown_seconds` | 25; range 1..25 |
| `docs_enabled` | false; exposes `/docs` and embedded OpenAPI when true |
| `credentials_file` | Required when any player is configured; otherwise omission disables API credential access |
| `database.port` | 5432 |
| `database.max_connections` | 4; range 1..16 |
| `database.timeout_seconds` | 5; range 1..30 |
| `database.lock_timeout_seconds` | 2; range 1..`timeout_seconds` |
| `players`, `sources` | Empty by default; at most 16 of each |

PostgreSQL uses verified TLS, including hostname validation. `database.host` is
one TCP hostname or bare IP, not a URL, socket path or host:port pair. Provision a
trusted CA and a matching server certificate. Keep `PGSERVICE` unset; connection
settings come from the application configuration. Runtime and migration
connections use separate credentials; see [migrations](MIGRATIONS.md).

## Players and device trust

```json
{
  "key": "room-speaker",
  "address": "speaker.example.invalid:1265",
  "fingerprint_sha256": "<64 lowercase hexadecimal SHA-256 digits>",
  "serial": "<verified device serial>",
  "model": "<verified model>",
  "writes_enabled": false,
  "volume_ceiling": null
}
```

Keys, addresses and serials must be unique. Keys use the service's lowercase
identifier format: start with a lowercase letter or digit, followed by lowercase letters, digits or `-`,
up to 64 characters. The serial identifies the physical device; a friendly room
name is insufficient. A nonempty `model` adds an exact model check.

A certificate pin is the SHA-256 fingerprint of the device's DER leaf
certificate. For initial provisioning on a trusted local network, inspect the
candidate certificate and calculate its fingerprint:

```sh
openssl s_client -connect speaker.example.invalid:1265 \
  -servername speaker.example.invalid </dev/null 2>/dev/null \
  | openssl x509 -noout -fingerprint -sha256
```

Use the 64 hexadecimal digits after `=`, without colons and in lowercase, once
you have independently confirmed the address/device association. A fingerprint
read from an untrusted endpoint does not itself verify the device. This command
performs a TLS handshake only; it does not authenticate or identify the player
by serial. Verify the serial/model from the physical device or trusted device
information, then use `doctor` to check the configured match.

The service requires pinned TLS on port 1265 and never falls back to plaintext.
A changed pin requires operator investigation and an explicit configuration
update. See [device compatibility](DEVICE_COMPATIBILITY.md).

`volume_ceiling` is an integer from 0 to 100 in HEOS units, or null/omitted while
unverified. `writes_enabled` defaults false; enabling it requires an explicit
ceiling. HEOS units are not dB or a guarantee of safe acoustic output. Verify a
limit for each device and listening environment. Fresh identity/state, write
permission, an ungrouped target and caller authorization are still checked at
admission.

## Local music sources

```json
{
  "key": "music-library",
  "player": "room-speaker",
  "name": "Gerbera"
}
```

The player must be configured. `name` selects one uniquely named local music
server as seen through that HEOS player; it is not a filesystem path or server
URL. The service browses HEOS Local Music and resolves the discovered source.
Duplicate player/name mappings and ambiguous results are rejected. Optional
`udn` is operator metadata; the documented browse response cannot verify it.

Clients browse this source to obtain opaque container/item references. The
service/chart contains no album lists or presets. `sources: []` disables these
configured catalog roots; direct player reads/control do not require a source.
Playback through `/playback` requires a current reference from an allowed source.

## Credentials and scopes

```json
{
  "version": 1,
  "credentials": [{
    "principal": "automation-client",
    "token_sha256": "<64 lowercase hexadecimal SHA-256 digits>",
    "scopes": ["read", "operator"],
    "players": ["room-speaker"]
  }]
}
```

Generate the token from at least 32 random bytes; send its original representation
in `Authorization: Bearer <token>`. Store only its SHA-256 hash in the server's
credential file. See [token generation](GETTING_STARTED.md#3-create-a-client-token).
The bearer representation must be 32..256 characters without whitespace.

| Scope | Permission |
| --- | --- |
| `read` | Player/catalog/history reads, preflight, SSE and authorized metrics |
| `control` | Volume, mute, transport and cancellation of permitted operations |
| `operator` | Playback, operator stop, explicit takeover, and visibility of other creators' operations for granted players |
| `alarm` | Reserved; grants no implemented endpoint |

Scopes are additive: `operator` does not imply `read` or `control`. Every
credential needs at least one configured player; there are no wildcard grants.
Operation reads/cancellation also require creator identity or the appropriate
operator grant. Adding a credential before its player is configured is an error.

At most 64 credentials are accepted, with unique principals and token hashes.
Principals use the same key format as players. Scope/player duplicates and
unknown scopes/player keys are rejected. `{"version":1,"credentials":[]}`
explicitly disables access. Health probes remain public; documentation resources
are public only when enabled. API and metrics retain their authorization.

## Files and restarts

Configuration and referenced files must be regular files, at most 1 MiB each.
Secret-volume symlinks are supported. Keep private keys, passwords, hashes and
raw client tokens out of source control. Mount credentials read-only and ensure
UID/GID 10001 can read the files in the runtime container.

For disposable local Compose, `.local/` is mode 0700 and generated mounted leaf
files are readable by the container UID. A newly created token file should remain
0600 and is never mounted into the server. If an editor replaces
`.local/credentials.json` with a 0600 file, adjust that mounted file's permissions
for the container while keeping the parent directory private. Production Secret
permissions are supplied by the deployment platform.

API certificates and credentials load at startup; restart to apply rotation or
revocation. Existing operations are not resumed after restart. Use `config check`
before replacing files and review [operating diagnostics](OPERATIONS.md).
