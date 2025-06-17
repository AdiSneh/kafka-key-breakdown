INTERPRETER ?= uv run python

.PHONY: run
run:
	$(INTERPRETER) -m src.kafka_key_breakdown