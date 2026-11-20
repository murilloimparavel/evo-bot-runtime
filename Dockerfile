FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o bin/evo-bot-runtime ./cmd/server

FROM alpine:3.20

WORKDIR /app
COPY --from=builder /app/bin/evo-bot-runtime .
ARG GIT_SHA=unknown
RUN printf '%s\n' "$GIT_SHA" > /app/.git_sha
LABEL org.opencontainers.image.revision=$GIT_SHA

EXPOSE 8080

CMD ["./evo-bot-runtime"]
