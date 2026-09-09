.DEFAULT_GOAL := help

.PHONY: help install run lint typecheck test loadtest migrate compose-up compose-down seed seed-reset hooks gen-alembic-env relay bus-setup s3-setup image-worker cache-worker cart-consumer reaper

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS=":.*?## "}; {printf "%-14s %s\n", $$1, $$2}'

install: ## Install runtime + dev dependencies (editable)
	python -m pip install --upgrade pip
	pip install -r requirements.txt
	pip install -e ".[dev]"

run: ## Run the app locally with autoreload
	uvicorn main:app --reload --host 0.0.0.0 --port 8000

lint: ## Ruff check + format check + import-linter (module boundaries)
	ruff check src tests
	ruff format --check src tests
	lint-imports

typecheck: ## Run basedpyright + ty (advisory only — both carry pre-existing baseline errors; NOT wired into lint/CI)
	basedpyright src
	ty check src

test: ## Run the unit test suite (coverage reported, not gated)
	pytest tests/unit/

loadtest: ## Run the k6 checkout load test against a running stack (fails when checkout p95 >= 300 ms)
	k6 run loadtest/checkout.js

migrate: ## Run every module's independent Alembic chain to head (portable: one line per module)
	python -m alembic -c src/identity/alembic.ini upgrade head
	python -m alembic -c src/catalog/alembic.ini upgrade head
	python -m alembic -c src/inventory/alembic.ini upgrade head
	python -m alembic -c src/orders/alembic.ini upgrade head
	python -m alembic -c src/payments/alembic.ini upgrade head

relay: ## Run the transactional-outbox relay worker (service role; outbox → SNS)
	python -m src.shared.bus.relay

bus-setup: ## Create local SNS topics + consumer queues/DLQs/subscriptions on LocalStack
	python -m scripts.bus_bootstrap

s3-setup: ## Create local S3 bucket + image-uploads queue/DLQ + ObjectCreated→SQS notification on LocalStack
	python -m scripts.s3_bootstrap

image-worker: ## Run the image worker (service role; S3 ObjectCreated → sniff/re-encode/thumbnails)
	python -m src.catalog.adapters.image_worker

cache-worker: ## Run the catalog cache-invalidation worker (service role; ProductUpdated/Deleted → evict)
	python -m src.catalog.adapters.cache_worker

cart-consumer: ## Run the cart product-event worker (service role; ProductUpdated/Deleted → refresh/prune carts)
	python -m src.cart.adapters.cart_consumer

reaper: ## Run the reservation reaper (service role; releases expired stock holds)
	python -m src.inventory.adapters.reaper

gen-alembic-env: ## Regenerate each module's env.py from scripts/alembic_env.py.tmpl
	python scripts/generate_alembic_env.py

compose-up: ## Start local backing services (Postgres, Valkey, LocalStack S3/SNS/SQS, ElasticMQ) + app + workers
	docker compose up -d

seed: ## Seed the demo state into the running stack (idempotent: 4 users, 11 products, images, stock)
	docker compose run --rm catalog-seed

seed-reset: ## Wipe + re-seed the known demo state (demo users get fresh subs; products re-anchor)
	docker compose run --rm catalog-seed python -m scripts.catalog_seed --reset

compose-down: ## Stop local backing services
	docker compose down

hooks: ## Install pre-commit hooks (Ruff + Ruff-format + Spectral)
	pre-commit install
