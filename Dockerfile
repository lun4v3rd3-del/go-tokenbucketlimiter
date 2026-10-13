FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd/main/main.go

FROM alpine:latest

WORKDIR /

COPY --from=builder /main /main

ENTRYPOINT ["/main"]
