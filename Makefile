.DEFAULT_GOAL := help

# ---- variables --------------------------------------------------------------

BIN        := ./tmp/main
PKG        := ./...
DOCKER_IMG := zonyx-core

# ---- targets ----------------------------------------------------------------

.PHONY: help
help: ## Show available targets.
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: dev
dev: ## Run with hot reload (requires `air`).
	air

.PHONY: build
build: ## Build the binary into ./tmp/main.
	go build -o $(BIN) ./cmd

.PHONY: simulator
simulator: ## Build the QUUPPA UDP simulator into ./tmp/quuppa-simulator.
	go build -o ./tmp/quuppa-simulator ./cmd/quuppa-simulator

.PHONY: smoke
smoke: ## Run end-to-end smoke tests (embedded stack, no external services). ~30s.
	go test -tags smoke -v -timeout 120s ./internal/smoketest/...

.PHONY: run
run: build ## Build and run, sourcing .env.
	@set -a && . ./.env && set +a && $(BIN)

.PHONY: test
test: ## Run the test suite with coverage.
	go test -cover -v $(PKG)

.PHONY: vet
vet: ## Run go vet.
	go vet $(PKG)

.PHONY: tidy
tidy: ## Tidy go.mod / go.sum.
	go mod tidy

.PHONY: docker
docker: ## Build the Docker image (requires ssh-agent with GitHub key loaded).
	docker build \
		--ssh default \
		-f build/package/Dockerfile \
		-t $(DOCKER_IMG) .

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf tmp/
