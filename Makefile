.PHONY: help install-dev check test docker-infra docker-up clean

# Use local uv cache to avoid system permission issues
export UV_CACHE_DIR := $(PWD)/.uv_cache_fix

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

install-dev: ## Install dependencies using uv
	@echo "Installing dependencies..."
	uv sync

format:
	@echo "Running ruff format..."
	uv run ruff format src tests dags

lint:
	@echo "Running ruff check..."
	uv run ruff check src tests dags --fix

check: format lint type-check ## Run all checks (format, lint, type-checking)

type-check:
	@echo "Running mypy..."
	uv run mypy src tests dags

test: ## Run tests
	@echo "Running tests..."
	uv run pytest tests/

test-cov: ## Run tests with coverage
	@echo "Running tests with coverage..."
	uv run pytest tests/ --cov=src/f1_pipeline --cov-report=html

docker-up: ## Start all Docker services (Airflow + Infra)
	docker-compose up -d

docker-infra: ## Start only data infrastructure (MinIO, ClickHouse, Postgres)
	docker-compose up -d minio clickhouse postgres

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-nuke:
	docker-compose down -v

docker-ps:
	docker-compose ps

docker-shell-clickhouse:
	docker-compose exec clickhouse clickhouse-client

docker-shell-airflow:
	docker-compose exec airflow-scheduler bash

requirements:
	@echo "Exporting requirements.txt..."
	uv pip compile pyproject.toml -o requirements.txt
	@echo "requirements.txt updated."

clean: ## Clean up local cache and temporary files
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Cleanup complete."
