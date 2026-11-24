# Build on the host platform and cross-compile, so multi-arch builds do not run under emulation.
FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /kube-mirror ./cmd

FROM gcr.io/distroless/static:nonroot
COPY --from=build /kube-mirror /kube-mirror
USER 65532:65532
ENTRYPOINT ["/kube-mirror"]
