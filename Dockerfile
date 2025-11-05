# Build Artifact-Registry executable
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY . .

RUN go mod download && CGO_ENABLED=0 GOOS=linux go build -o /app/artifact-registry .

# Create a minimal runtime image
FROM alpine:3.22

RUN apk --no-cache add ca-certificates
WORKDIR /app

COPY --from=builder /app/artifact-registry .

EXPOSE 8080

CMD ["./artifact-registry"]