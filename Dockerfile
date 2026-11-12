FROM golang:1.26.11-alpine3.24 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o bin/evo-bot-runtime ./cmd/server

FROM alpine:3.24

WORKDIR /app
COPY --from=builder /app/bin/evo-bot-runtime .

EXPOSE 8080

CMD ["./evo-bot-runtime"]
