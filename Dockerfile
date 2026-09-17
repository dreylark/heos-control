# The runner contains source and the pinned toolchain, never compilation caches.
FROM golang:1.27.1-trixie@sha256:9baa6b4187bbb98d240372a8a235ac0bb6b5ddd52bba1431dc2f7c0705862728 AS runner
WORKDIR /src
COPY . .

# Local builds retain Go caches in BuildKit mounts across source changes.
# CI runs these same commands in runner with Actions-backed bind mounts.
FROM runner AS test-check
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    make verify && mkdir -p /out && \
    go test -race -tags=integration -c -o /out/journal.test ./internal/journal

FROM debian:trixie-slim@sha256:d7e12182ce18b85b93007c1dedf31f2d29e01ccf3182cc4017c709b6259bc132 AS integration-base
WORKDIR /tests
USER 10001:10001
ENTRYPOINT ["/tests/journal.test"]
CMD ["-test.count=1", "-test.timeout=5m"]

# Local all-in-one check; the resulting image contains only integration tests.
FROM integration-base AS test
COPY --from=test-check /out/journal.test /tests/journal.test

# CI supplies binaries compiled and tested in the pinned runner above.
FROM integration-base AS integration-prebuilt
COPY --from=integration-binaries /journal.test /tests/journal.test

FROM runner AS build
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags="-s -w -X main.version=$VERSION -X main.commit=$COMMIT" \
    -o /out/heos-control ./cmd/heos-control

FROM scratch AS runtime-base
ARG VERSION=dev
ARG COMMIT=unknown
ARG SOURCE=""
LABEL org.opencontainers.image.title="heos-control" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$COMMIT \
      org.opencontainers.image.source=$SOURCE
COPY --from=runner /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY LICENSE /usr/share/licenses/heos-control/LICENSE
COPY third_party /usr/share/licenses/heos-control/third_party
COPY --from=runner /usr/share/doc/ca-certificates/copyright /usr/share/licenses/heos-control/ca-certificates-copyright
COPY --from=runner /usr/share/common-licenses/MPL-2.0 /usr/share/licenses/heos-control/MPL-2.0
USER 10001:10001
EXPOSE 8443
ENTRYPOINT ["/heos-control"]

FROM runtime-base AS runtime-prebuilt
COPY --from=runtime-binaries /heos-control /heos-control

# Default/local runtime build remains independent of the test stage.
FROM runtime-base AS runtime
COPY --from=build /out/heos-control /heos-control
