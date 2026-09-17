# heos-control

An HTTP API for HEOS players, with confirmed playback operations, volume ramps
and automatic fade-out. Use it from a scheduler, home automation system or your
own scripts while keeping the native HEOS app usable.

An external client starts playback by calling the API at the desired time.
heos-control has no scheduler and does not accept future start times. It executes
the requested playback and optional volume ramp, fade and stop, tracks the result,
and stops making changes when it loses control of the player.

## What it does

- Reads player state and queues; browses configured local music servers.
- Controls playback, volume and mute through an authenticated API.
- Plays a selected album with an optional ramp, duration, fade and final stop.
- Records operation results and reasons for interrupted automation in PostgreSQL.
- Provides SSE notifications, Prometheus metrics and read-only diagnostics.

Requests are idempotent. Accepted work survives the HTTP client's disconnect;
it does **not** resume after a service restart. Manual pause or a conflicting
change cancels automation, including its pending stop. Next/Previous within the
verified unchanged queue keeps the original timeline.

## Requirements and compatibility

- A supported HEOS device reachable over pinned TLS on TCP 1265.
- External PostgreSQL with verified TLS and separate runtime/migration roles.
- One active controller process. The Helm chart uses one replica and `Recreate`.

Hardware testing covers Denon Home 150; other models need verification. See
[device compatibility](docs/DEVICE_COMPATIBILITY.md) for limitations and firmware
behavior. Grouped targets are rejected. Scheduling, random album selection and
presets belong to clients. Receiver power/input controls are not implemented.

## Try it locally

With Go (version in [go.mod](go.mod)) and Docker Compose installed:

```sh
git clone https://github.com/dreylark/heos-control.git
cd heos-control
make db-up
make migrate
make run
```

In another terminal:

```sh
curl --cacert .local/ca.crt https://localhost:8443/readyz
```

This creates disposable development credentials and starts with no players or
API access. Follow [getting started](docs/GETTING_STARTED.md) to configure a
player, create a token and make an authenticated request. These are local
commands; they do not deploy to a cluster.

## API

The contract is [OpenAPI 3.1](api/openapi.yaml). Enable `docs_enabled` in runtime
configuration, or `api.docs.enabled` in Helm, to serve a Scalar reference at
`/docs`. It defaults off and loads its renderer from a pinned CDN.

A playback request can include this automation envelope:

```json
"automation": {
  "target_volume": {"unit": "heos", "level": 20},
  "ramp_seconds": 60,
  "duration_seconds": 600,
  "fade_seconds": 30
}
```

This ramps within a ten-minute playback window, fades during its last 30 seconds
and stops while ownership remains valid. The level must fit the configured
ceiling. See the [complete request and retry workflow](docs/API_USAGE.md).
**HTTP 202 means durably accepted, not confirmed playback.**

## Documentation

| Guide | Purpose |
| --- | --- |
| [Getting started](docs/GETTING_STARTED.md) | Local setup and first authenticated read |
| [Configuration](docs/CONFIGURATION.md) | Players, TLS, tokens, permissions and sources |
| [API usage](docs/API_USAGE.md) | Playback, retries, history and events |
| [Operations](docs/OPERATIONS.md) | Helm, diagnostics, metrics, upgrades and recovery |
| [Architecture](docs/ARCHITECTURE.md) | Packages, execution and device ownership |
| [Testing](docs/TESTING.md) | Checks, replay, fuzzing and coverage |
| [Contributing](CONTRIBUTING.md) | Development conventions and review |

The project is versioned below 1.0. Review release notes and the embedded API
contract before upgrading; publication is not a claim of compatibility with all
HEOS hardware. This is an independent project, not affiliated with Denon.

## License

[MIT](LICENSE). Distributed dependency notices are in
[third_party](third_party/README.md).
