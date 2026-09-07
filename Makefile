.PHONY: integration-test conformance-test release-check build test lint fmt clean help install dev benchmark

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install package with dev dependencies
	pip install -e ".[dev]"

build: ## Build the package
	python -m build

test: ## Run tests
	pytest tests/

lint: ## Run linting
	ruff check .
	mypy

fmt: ## Format code
	ruff format .

clean: ## Clean build artifacts
	rm -rf dist/ build/ *.egg-info .pytest_cache .mypy_cache

benchmark: ## Run benchmarks
	pytest benchmarks/ --benchmark-group-by=group --benchmark-sort=fullname

dev: install ## Set up development environment
	pre-commit install 2>/dev/null || true

integration-test: ## Run integration tests (requires Docker)
	docker compose -f docker-compose.test.yml up -d
	@status=0; \
	echo "Waiting for Streamline server..."; \
	for i in $$(seq 1 30); do \
		if curl -sf http://localhost:9094/health; then \
			echo "Server ready"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then status=1; fi; \
		sleep 2; \
	done; \
	if [ $$status -eq 0 ]; then \
		STREAMLINE_INTEGRATION=1 pytest tests/ -m integration --timeout=60 \
			|| status=$$?; \
	fi; \
	docker compose -f docker-compose.test.yml down -v; \
	exit $$status

conformance-test: ## Run required conformance tests (requires Docker)
	docker compose -f docker-compose.test.yml up -d
	@status=0; \
	echo "Waiting for Streamline server..."; \
	for i in $$(seq 1 30); do \
		if curl -sf http://localhost:9094/health; then \
			echo "Server ready"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then status=1; fi; \
		sleep 2; \
	done; \
	if [ $$status -eq 0 ]; then \
		CONFORMANCE=1 STREAMLINE_REQUIRE_CONFORMANCE=1 \
			pytest tests/conformance -m conformance --timeout=60 -rs \
			|| status=$$?; \
	fi; \
	docker compose -f docker-compose.test.yml down -v; \
	exit $$status

release-check: ## Validate all packages without publishing
	python -m build
	twine check dist/*
	python -m build testcontainers
	twine check testcontainers/dist/*
	cargo test --manifest-path streamline_embedded/Cargo.toml
	cargo package --manifest-path streamline_embedded/Cargo.toml --allow-dirty --no-verify
	maturin build --release --manifest-path streamline_embedded/Cargo.toml
	twine check streamline_embedded/target/wheels/*
