.DEFAULT_GOAL := help
.PHONY: help lint test jars build up-core down logs beeline

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

lint: ## ruff check + ruff format --check
	uv run ruff check .
	uv run ruff format --check .

test: ## Unit tests (pytest)
	uv run pytest

jars: ## Download pinned jars into jars/ and check their Maven sha1
	set -a && . ./.env && set +a && scripts/fetch_jars.sh

build: jars ## Build the Spark image
	docker compose build

up-core: build ## Start the core services and wait until healthy
	docker compose up -d --wait

down: ## Stop all services (keeps volumes)
	docker compose down

logs: ## Follow one service's logs: make logs s=minio
	docker compose logs -f $(s)

beeline: ## SQL shell on the Spark Thrift Server
	docker compose run --rm --no-deps spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://spark-thrift:10000
