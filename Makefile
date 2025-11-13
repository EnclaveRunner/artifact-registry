.PHONY: clean verify fmt lint build test help proto

# Default target
all: test

# Format code
fmt:
	golangci-lint fmt

# Lint code (requires golangci-lint to be installed)
lint:
	golangci-lint run --fix

# Clean test cache
clean:
	go clean -testcache

build:
	go build

proto:
	protoc --go_out=. --go-grpc_out=. registry.proto

test:
	docker compose -f docker-compose.test.yml down
	docker compose -f docker-compose.test.yml up -d
	sleep 3
	go test ./registry/...
	go test ./orm/...
	go test ./config/...
	go test main.go

# Simulate CI tests
verify:
	@echo "Running CI tests..."
	make lint
	make build
	make test
	make clean
	@echo "✅ CI Test will pass, you are ready to commit / open the PR! Thank you for your contribution :)"
# Show help
help:
	@echo "Available targets:"
	@echo "  build         - Build the application"
	@echo "  test          - Run tests"
	@echo "  fmt           - Format code"
	@echo "  lint          - Lint and fix code"
	@echo "  clean         - Clean test cache"
	@echo "  verify        - Simulate CI Checks before opening a PR"
	@echo "  help          - Show this help"
	@echo "  proto         - Generate Go code from protobuf definitions"
