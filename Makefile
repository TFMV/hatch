.PHONY: build run test clean docker-build docker-run size compress optimize

# Build variables
BINARY_NAME=porter
DOCKER_IMAGE=porter
DOCKER_TAG=latest

# Detect OS
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    # macOS specific flags
    LDFLAGS=-w -s
    CGO_ENABLED=1
    IS_MACOS=1
else
    # Linux specific flags
    LDFLAGS=-w -s -extldflags "-static"
    CGO_ENABLED=1
    IS_MACOS=0
endif

# Common flags
GOFLAGS=-trimpath -ldflags="-s -w"

# Default target
all: build

# Build the application with size optimizations
build:
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(shell go env GOOS) GOARCH=$(shell go env GOARCH) go build \
		-ldflags "$(LDFLAGS)" \
		-gcflags="-l=4" \
		-o $(BINARY_NAME) ./cmd/server

# Optimize binary size
optimize: build
	@echo "Optimizing binary size..."
	@if [ "$(IS_MACOS)" = "1" ]; then \
		echo "Running macOS-specific optimizations..."; \
		strip -x $(BINARY_NAME); \
	else \
		echo "Running Linux-specific optimizations..."; \
		if command -v upx >/dev/null 2>&1; then \
			echo "Compressing with UPX..."; \
			upx --best --lzma $(BINARY_NAME); \
		else \
			echo "UPX not found. Install it with: apt-get install upx"; \
		fi; \
	fi

# Show binary size and dependencies
size: build
	@echo "Binary size:"
	@ls -lh $(BINARY_NAME)
	@echo "\nDependencies:"
	@go tool nm $(BINARY_NAME) | grep -v "runtime\|internal" | sort -k3
	@echo "\nTop 10 largest symbols:"
	@go tool nm -size $(BINARY_NAME) | sort -n -r | head -n 10
	@echo "\nGo module dependencies:"
	@go list -f '{{.Path}} {{.Size}}' -m all | sort -k2 -nr | head -n 10

# Run the application locally
run: build
	./$(BINARY_NAME) --config config/config.yaml

# Run tests
test:
       go test -v ./...

# Run microbenchmarks
bench:
       go test -run=^$ -bench=. ./bench -benchmem

# Clean build artifacts
clean:
	rm -f $(BINARY_NAME)
	go clean

# Build Docker image
docker-build:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

# Run Docker container
docker-run:
	docker run -p 32010:32010 $(DOCKER_IMAGE):$(DOCKER_TAG)

# Help target
help:
	@echo "Available targets:"
	@echo "  build        - Build the application"
	@echo "  optimize     - Build and optimize binary size"
	@echo "  size         - Show binary size and dependencies"
	@echo "  run          - Run the application locally"
	@echo "  test         - Run tests"
	@echo "  clean        - Clean build artifacts"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run Docker container"
	@echo "  help         - Show this help message" 