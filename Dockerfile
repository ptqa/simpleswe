# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.26.5-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags='-s -w' -o /out/simpleswe ./cmd/simpleswe

# OPENCODE_BINARY must name a prebuilt binary inside the build context.
# docker build --target worker --build-arg OPENCODE_BINARY=bin/opencode \
#   -t simpleswe-worker .
FROM debian:bookworm-slim AS worker
ARG OPENCODE_BINARY=_prebuilt_opencode_binary_must_be_provided
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates git openssh-client \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/simpleswe /usr/local/bin/simpleswe
COPY ${OPENCODE_BINARY} /usr/local/bin/opencode
RUN chmod 0755 /usr/local/bin/opencode \
    && mkdir -p /workspace \
    && chown 65532:65532 /workspace
USER 65532:65532
WORKDIR /workspace
ENTRYPOINT ["/usr/local/bin/simpleswe"]

# Keep the dependency-free controller as the default build target.
FROM gcr.io/distroless/static-debian12:nonroot AS controller
COPY --from=build /out/simpleswe /usr/local/bin/simpleswe
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/simpleswe"]
