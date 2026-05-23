# syntax=docker/dockerfile:1.6
# robot-dh-exporter Dockerfile：multi-stage build，运行时使用 distroless 静态镜像。

ARG GO_VERSION=1.22.5
ARG GO_PROXY=https://goproxy.cn,direct

FROM golang:${GO_VERSION}-bookworm AS build
ARG GO_PROXY
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPROXY=${GO_PROXY}
ENV CGO_ENABLED=0
ENV GOFLAGS=-buildvcs=false

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/robot-dh-exporter ./

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/robot-dh-exporter /robot-dh-exporter

EXPOSE 9108
USER nonroot:nonroot
ENTRYPOINT ["/robot-dh-exporter"]
