.PHONY: help install-dev format lint test clean docker-up docker-down requirements

# Use local uv cache to avoid system permission issues
export UV_CACHE_DIR := $(PWD)/.uv_cache

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install-dev: ## Install dependencies using uv
	@echo "Installing dependencies..."
	uv sync

format: ## Run code formatters (ruff)
	@echo "Running ruff format..."
	uv run ruff format src tests dags

lint: ## Run linters (ruff)
	@echo "Running ruff check..."
	uv run ruff check src tests dags --fix

test: ## Run tests (pytest)
	@echo "Running tests..."
	uv run pytest tests/ -v

type-check: ## Run type checking (mypy)
	@echo "Running mypy..."
	uv run mypy src tests

test-cov: ## Run tests with coverage
	@echo "Running tests with coverage..."
	uv run pytest tests/ --cov=src/f1_pipeline --cov-report=html

docker-up: ## Start Docker services
	docker-compose up -d

docker-down: ## Stop Docker services
	docker-compose down

requirements: ## Export requirements.txt from pyproject.toml
	@echo "Exporting requirements.txt..."
	uv pip compile pyproject.toml -o requirements.txt
	@echo "requirements.txt updated."

clean: ## Clean up cache and temporary files
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf htmlcov
	rm -rf .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
