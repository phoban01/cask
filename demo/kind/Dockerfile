# The demo image is a single binary: cask-apiserver, with cask embedded —
# each apiserver carries its own consensus acceptor, so the demo needs no
# separate cask image or processes. Build from the repo root:
#   docker build -f demo/kind/Dockerfile -t cask-demo:latest .
#
# cmd/cask-apiserver is its own Go module. Its go.mod replaces the core
# module with the repo root, so the build context must be the repo root.
FROM golang:1.26-alpine AS build
WORKDIR /src
# A local go.work must not change the build.
ENV GOWORK=off
COPY go.mod go.sum ./
COPY cmd/cask-apiserver/go.mod cmd/cask-apiserver/go.sum ./cmd/cask-apiserver/
RUN go -C cmd/cask-apiserver mod download
COPY . .
RUN CGO_ENABLED=0 go -C cmd/cask-apiserver build -o /out/cask-apiserver .

FROM alpine:3.20
COPY --from=build /out/cask-apiserver /usr/local/bin/cask-apiserver
ENTRYPOINT ["/usr/local/bin/cask-apiserver"]
