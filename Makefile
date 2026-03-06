.PHONY: integration-test build test lint fmt clean help install dev benchmark

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
	@echo "Waiting for Streamline server..."
	@for i in $$(seq 1 30); do \
		if curl -sf http://localhost:9094/health/live > /dev/null 2>&1; then \
			echo "Server ready"; \
			break; \
		fi; \
		sleep 2; \
	done
	pytest tests/ -m integration --timeout=60 || true
	docker compose -f docker-compose.test.yml down -v
