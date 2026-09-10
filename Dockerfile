# syntax=docker/dockerfile:1

# ---------- build stage ----------
FROM golang:1.26-alpine AS build

WORKDIR /src

# Кэшируем зависимости отдельно от исходников.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Собираем статические бинарники под Linux (в образе — linux/arm64 или amd64
# в зависимости от платформы сборки). CGO не нужен (kafka-go, pgx — чистый Go).
ARG TARGETOS=linux
ARG TARGETARCH
ENV CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH}

RUN go build -trimpath -ldflags="-s -w" -o /out/api      ./cmd/api && \
    go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer && \
    go build -trimpath -ldflags="-s -w" -o /out/results  ./cmd/results && \
    go build -trimpath -ldflags="-s -w" -o /out/loadtest ./loadtest

# ---------- runtime stage ----------
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata wget

WORKDIR /app
COPY --from=build /out/ /app/bin/

# По умолчанию — сервис результатов; переопределяется в docker-compose.
ENTRYPOINT ["/app/bin/results"]
