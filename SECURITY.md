# Security

Use a maintained release and review its release notes before upgrading. The
project does not currently promise security backports for older releases or a
fixed response time.

For a suspected vulnerability, use GitHub's private vulnerability reporting under
this repository's **Security** tab when it is enabled. If it is unavailable,
open an issue asking for a private reporting channel without including exploit
details, credentials or sensitive logs. Do not post secrets in public issues.

Include the affected version, deployment assumptions, impact and a minimal
reproduction using synthetic data. Ordinary configuration/connectivity failures
can be reported as bugs after removing tokens, private keys, passwords, device
identities, media identifiers and private network details.

The service requires verified PostgreSQL TLS and pinned device TLS, explicit
player grants and write opt-in. These boundaries do not make an untrusted network
or a second concurrent controller safe. The container and chart use one
controller process; restart recovery cannot fence an isolated old device writer.
See [operations](docs/OPERATIONS.md) and [configuration](docs/CONFIGURATION.md).
