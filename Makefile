.PHONY: help run build test cover lint fmt tidy clean hooks
BINARY := bin/server
help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-8s %s\n", $$1, $$2}'

run:
	@if [ -f .env ]; then set -a; . ./.env; set +a; fi; go run ./cmd/server

build:
	go build -o $(BINARY) ./cmd/server

test:
	go test -race -count=1 ./...

cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

lint:
	golangci-lint run ./...

fmt:
	golangci-lint fmt ./...

tidy:
	go mod tidy

clean:
	rm -rf bin coverage.out output

hooks:
	git config core.hooksPath .githooks
	chmod +x .githooks/*
	@echo "git hooks installed"