.DEFAULT_GOAL := help

.PHONY: help install run lint test compose-up compose-down hooks

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS=":.*?## "}; {printf "%-14s %s\n", $$1, $$2}'

install: ## Install runtime + dev dependencies (editable)
	python -m pip install --upgrade pip
	pip install -r requirements.txt
	pip install -e ".[dev]"

run: ## Run the app locally with autoreload
	uvicorn main:app --reload --host 0.0.0.0 --port 8000

lint: ## Ruff check + format check
	ruff check src tests
	ruff format --check src tests

test: ## Run the unit test suite (coverage reported, not gated)
	pytest tests/unit/

compose-up: ## Start local backing services (Postgres, Valkey, MinIO, ElasticMQ) + app
	docker compose up -d

compose-down: ## Stop local backing services
	docker compose down

hooks: ## Install pre-commit hooks (Ruff + Ruff-format + Spectral)
	pre-commit install
