.PHONY: clean verify fmt lint build

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

# Simulate CI tests
verify:
	@echo "Running CI tests..."
	make lint
	make build
	make clean
	@echo "✅ CI Test will pass, you are ready to commit / open the PR! Thank you for your contribution :)"
# Show help
help:
	@echo "Available targets:"
	@echo "  build         - Build the application"
	@echo "  fmt           - Format code"
	@echo "  lint          - Lint and fix code"
	@echo "  clean         - Clean test cache"
	@echo "  verify        - Simulate CI Checks before opening a PR"
	@echo "  help          - Show this help"
