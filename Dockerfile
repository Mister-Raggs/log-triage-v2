# Multi-stage build for minimal final image.
# Stage 1: Build both binaries.
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Cache dependency downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build server and generator as static binaries.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /generator ./cmd/generator

# Stage 2: Minimal runtime image.
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

# Non-root user for security.
RUN adduser -D -u 1000 appuser
USER appuser

COPY --from=builder /server /usr/local/bin/server
COPY --from=builder /generator /usr/local/bin/generator

# Log output directory.
WORKDIR /data
VOLUME ["/data"]

EXPOSE 8080

ENTRYPOINT ["server"]
