.PHONY: install
install:
	uv sync

.PHONY: format
format:
	uvx ruff format

.PHONY: lint
lint:
	uvx ruff check

.PHONY: dev
dev:
	docker-compose up --force-recreate

.PHONY: dev-down
dev-down:
	docker-compose down
