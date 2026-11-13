.DEFAULT_GOAL := help
.PHONY: help lint test

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

lint: ## ruff check + ruff format --check
	uv run ruff check .
	uv run ruff format --check .

test: ## Unit tests (pytest)
	uv run pytest
