INTERPRETER ?= uv run python

.PHONY: run
run:
	$(INTERPRETER) -m src.kafka_key_breakdown

.PHONY: format
format:
	uvx ruff format

.PHONY: lint
lint:
	uvx ruff check
