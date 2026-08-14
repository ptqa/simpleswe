# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.26.6-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags='-s -w' -o /out/simpleswe ./cmd/simpleswe

FROM ghcr.io/anomalyco/opencode:1.18.15@sha256:59b2582fb5a10b7022d8b3347a9d9c60710d526ad6bfd02eb17a2c8582b35809 AS worker
RUN apk add --no-cache ca-certificates github-cli git openssh-client
COPY --from=build /out/simpleswe /usr/local/bin/simpleswe
RUN mkdir -p /workspace
WORKDIR /workspace
ENTRYPOINT ["/usr/local/bin/simpleswe"]

# Hermes owns Slack ingress and calls the controller over Pod localhost.
FROM docker.io/nousresearch/hermes-agent:v2026.8.3@sha256:16788311e2fa3035456bdc1bafb8ec2b1777db64ebf020af9bb7eb73c3712c9e AS hermes
COPY --from=build /out/simpleswe /usr/local/bin/simpleswe
ENV HERMES_HOME=/opt/data \
    HERMES_DISABLE_LAZY_INSTALLS=1
USER 10000:10000
WORKDIR /opt/data
ENTRYPOINT ["hermes", "gateway"]

# Keep the dependency-free controller as the default build target.
FROM gcr.io/distroless/static-debian12:nonroot AS controller
COPY --from=build /out/simpleswe /usr/local/bin/simpleswe
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/simpleswe"]
