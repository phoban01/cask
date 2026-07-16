# Builds one image carrying both binaries: the cask node and the
# cask-apiserver (fleet.cask.dev extension server). Build from the repo root:
#   docker build -f demo/kind/Dockerfile -t cask-demo:latest .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/cask ./cmd/cask && \
    CGO_ENABLED=0 go build -o /out/cask-apiserver ./cmd/cask-apiserver

FROM alpine:3.20
COPY --from=build /out/cask /usr/local/bin/cask
COPY --from=build /out/cask-apiserver /usr/local/bin/cask-apiserver
ENTRYPOINT ["/usr/local/bin/cask"]
