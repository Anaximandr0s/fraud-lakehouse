.DEFAULT_GOAL := help
.PHONY: help lint test jars data build up-core down logs beeline reset-producer

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

lint: ## ruff check + ruff format --check
	uv run ruff check .
	uv run ruff format --check .

test: ## Unit tests (pytest)
	uv run pytest

jars: ## Download pinned jars into jars/ and check their Maven sha1
	set -a && . ./.env && set +a && scripts/fetch_jars.sh

DATA_ZIP := data/raw/fraud-detection.zip

data: $(DATA_ZIP) ## Download the Kaggle dataset and merge it into data/fraud_all.csv
	uv run python scripts/prepare_data.py $(DATA_ZIP) data/fraud_all.csv

$(DATA_ZIP):
	mkdir -p $(@D)
	curl -fsSL --retry 5 --retry-all-errors -C - -o $@.part \
		https://www.kaggle.com/api/v1/datasets/download/kartik2112/fraud-detection
	mv $@.part $@

build: jars ## Build the Spark and producer images
	docker compose build

up-core: build data ## Start the core services and wait until healthy
	docker compose up -d --wait

down: ## Stop all services (keeps volumes)
	docker compose down

logs: ## Follow one service's logs: make logs s=minio
	docker compose logs -f $(s)

beeline: ## SQL shell on the Spark Thrift Server
	docker compose run --rm --no-deps spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://spark-thrift:10000

reset-producer: ## Stop the producer and make it replay the CSV from the start
	docker compose stop producer
	docker compose run --rm --no-deps --entrypoint rm producer -f /state/position
