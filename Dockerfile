FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .

RUN go build -o gcache .

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/gcache .

# RUN adduser -u 1001 -g 1001 -D gcache
# USER gcache

EXPOSE 8080
CMD ["./gcache"]
