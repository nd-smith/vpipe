.PHONY: help dummy xact-ingest xact-download xact-upload xact-results \
        claimx-ingest claimx-enrich claimx-download claimx-upload claimx-results \
        claimx-entities kafka-up kafka-down

help:
	@echo "Development Worker Commands"
	@echo ""
	@echo "Data Generation:"
	@echo "  make dummy              - Start dummy data source (generates test events)"
	@echo ""
	@echo "XACT Workers:"
	@echo "  make xact-ingest        - XACT event ingester"
	@echo "  make xact-download      - XACT download worker"
	@echo "  make xact-upload        - XACT upload worker"
	@echo "  make xact-results       - XACT result processor"
	@echo ""
	@echo "ClaimX Workers:"
	@echo "  make claimx-ingest      - ClaimX event ingester"
	@echo "  make claimx-enrich      - ClaimX enrichment worker"
	@echo "  make claimx-download    - ClaimX download worker"
	@echo "  make claimx-upload      - ClaimX upload worker"
	@echo "  make claimx-results     - ClaimX result processor"
	@echo "  make claimx-entities    - ClaimX entity writer"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make kafka-up           - Start local Kafka cluster"
	@echo "  make kafka-down         - Stop local Kafka cluster"
	@echo ""
	@echo "Typical workflow:"
	@echo "  1. make kafka-up        (start Kafka)"
	@echo "  2. make dummy           (in one terminal - generates data)"
	@echo "  3. make xact-download   (in another terminal)"
	@echo "  4. make xact-upload     (in another terminal)"
	@echo "  etc..."

# Dummy data source
dummy:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker dummy-source --dev --no-delta

# XACT workers
xact-ingest:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker xact-event-ingester --dev --no-delta

xact-download:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker xact-download --dev --no-delta

xact-upload:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker xact-upload --dev --no-delta

xact-results:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker xact-result-processor --dev --no-delta

xact-delta:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker xact-delta-writer --dev

# ClaimX workers
claimx-ingest:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker claimx-ingester --dev --no-delta

claimx-enrich:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker claimx-enricher --dev --no-delta

claimx-download:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker claimx-downloader --dev --no-delta

claimx-upload:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker claimx-uploader --dev --no-delta

claimx-results:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker claimx-result-processor --dev --no-delta

claimx-entities:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker claimx-entity-writer --dev --no-delta

claimx-delta:
	cd src && ENABLE_TELEMETRY=false .venv/bin/python -m kafka_pipeline --worker claimx-delta-writer --dev

# Infrastructure
kafka-up:
	docker compose -f scripts/docker-compose.kafka.yml up -d

kafka-down:
	docker compose -f scripts/docker-compose.kafka.yml down
