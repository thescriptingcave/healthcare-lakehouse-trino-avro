.PHONY: help up down down-clean status check logs seed produce produce-json docker-produce migrate validate superset-setup tutorial test test-unit test-integration lint lint-fix typecheck clean setup

# Use uv to run Python commands against the project virtualenv
PYTHON := uv run python

SHELL := /bin/bash

# Default target
help: ## Show this help message
	@echo "Healthcare Lakehouse - Available Targets:"
	@echo "========================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start all services
	docker compose up -d
	@echo ""
	@echo "Waiting for services to become healthy..."
	@sleep 10
	@$(MAKE) status

down: ## Stop all services
	docker compose down

down-clean: ## Stop all services and remove volumes
	docker compose down -v

status: ## Show service status
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

check: ## Run health checks
	$(PYTHON) check_stock.py

logs: ## View logs (optionally: make logs SERVICE=kafka)
	@if [ -n "$(SERVICE)" ]; then \
		docker compose logs -f $(SERVICE); \
	else \
		docker compose logs -f; \
	fi

seed: ## Seed MySQL with healthcare data + apply V2 enrichment
	@echo "Seeding MySQL database..."
	@for f in sql/HEALTHCARE_*.sql; do \
		echo "  Loading $$f..."; \
		docker exec -i db mysql -u root -p$${MYSQL_ROOT_PASSWORD:-rootpassword} healthcare < $$f 2>/dev/null; \
	done
	@echo "  Applying V2__enrich_patients_for_kafka.sql (patient_id column for the Kafka join)..."
	@docker exec -i db mysql -u root -p$${MYSQL_ROOT_PASSWORD:-rootpassword} healthcare < scripts/migration/V2__enrich_patients_for_kafka.sql 2>/dev/null
	@echo "Seeding complete."

produce: ## Start the Avro vitals producer (local, telemetry.vitals topic)
	$(PYTHON) produce_vitals.py

produce-json: ## Start the JSON vitals producer (alternative, vitals topic)
	$(PYTHON) producer.py

docker-produce: ## Start the Avro vitals producer against the compose stack
	KAFKA_BOOTSTRAP_SERVERS=localhost:9092 SCHEMA_REGISTRY_URL=http://localhost:8081 $(PYTHON) docker_produce_vitals.py

migrate: ## Run Trino migration scripts
	@echo "Running migrations..."
	@for f in scripts/migration/0[1-9]_*.sql; do \
		echo "  Running $$f..."; \
		docker exec -i trino trino --catalog nessie --schema healthcare < $$f; \
	done
	@echo "Migrations complete."

validate: ## Run data validation queries
	@echo "Running validation..."
	@docker exec -i trino trino < scripts/migration/03_validation.sql

superset-setup: ## Provision Superset databases, datasets, charts, dashboard
	$(PYTHON) scripts/setup_superset.py

tutorial: ## Run a SQL tutorial (usage: make tutorial TUT=01)
	@if [ -z "$(TUT)" ]; then \
		echo "Usage: make tutorial TUT=01  (01..04)"; \
		exit 1; \
	fi
	docker exec -i trino trino < tutorials/$(TUT)*.sql

test: ## Run unit tests
	$(PYTHON) -m pytest tests/ -v

test-unit: ## Run unit tests only
	$(PYTHON) -m pytest tests/ -v -m unit

test-integration: ## Run integration tests only
	$(PYTHON) -m pytest tests/ -v -m integration

lint: ## Run linter and formatter
	ruff check .
	ruff format --check .

lint-fix: ## Auto-fix linting issues
	ruff check --fix .
	ruff format .

typecheck: ## Run type checker
	mypy .

clean: ## Remove containers, volumes, and generated data
	docker compose down -v --remove-orphans
	rm -rf mysql_data/ minio-data/ superset_home/ trino/var/
	rm -f trino.log
	@echo "Clean complete."

setup: ## Initial setup: copy .env, install deps
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Created .env from .env.example"; \
	fi
	uv sync
	@echo "Setup complete."
