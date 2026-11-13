.DEFAULT_GOAL := help
.PHONY: help lint test jars up-core down logs spike

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

lint: ## ruff check + ruff format --check
	uv run ruff check .
	uv run ruff format --check .

test: ## Unit tests (pytest)
	uv run pytest

jars: ## Download pinned jars into jars/ (skips existing ones)
	set -a && . ./.env && set +a && scripts/fetch_jars.sh

up-core: jars ## Start the core services and wait until healthy
	docker compose up -d --wait

down: ## Stop all services (keeps volumes)
	docker compose down

logs: ## Follow one service's logs: make logs s=minio
	docker compose logs -f $(s)

spike: ## Create and query an Iceberg table on MinIO through the Hive Metastore
	set -a && . ./.env && set +a && docker run --rm --network fraud-lakehouse_default \
	  -e AWS_ACCESS_KEY_ID="$$MINIO_ROOT_USER" -e AWS_SECRET_ACCESS_KEY="$$MINIO_ROOT_PASSWORD" \
	  -e AWS_REGION=us-east-1 \
	  -v "$(CURDIR)/jars/spark:/opt/extra-jars:ro" \
	  -v "$(CURDIR)/scripts/spike_iceberg_hms.sql:/tmp/spike.sql:ro" \
	  "$$SPARK_IMAGE" /opt/spark/bin/spark-sql \
	  --jars "$$(ls jars/spark/*.jar | sed 's|jars/spark|/opt/extra-jars|' | paste -sd,)" \
	  --conf spark.driver.memory=512m \
	  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
	  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
	  --conf spark.sql.catalog.lakehouse.type=hive \
	  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
	  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
	  --conf spark.hadoop.fs.s3a.path.style.access=true \
	  -f /tmp/spike.sql
