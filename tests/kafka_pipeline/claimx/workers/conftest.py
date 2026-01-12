"""Fixtures for ClaimX worker tests."""

import os
from datetime import datetime, timezone

import pytest

from kafka_pipeline.config import KafkaConfig


# Set environment variables for all worker tests
# These must be set before workers are instantiated

# OneLake base path for worker testing
if "ONELAKE_BASE_PATH" not in os.environ:
    os.environ["ONELAKE_BASE_PATH"] = "abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse"

# Test mode to prevent audit logger from creating /var/log/verisk_pipeline
if "TEST_MODE" not in os.environ:
    os.environ["TEST_MODE"] = "true"

# Audit log path for testing
if "AUDIT_LOG_PATH" not in os.environ:
    os.environ["AUDIT_LOG_PATH"] = "/tmp/test_audit.log"

# ClaimX API test settings
if "CLAIMX_API_URL" not in os.environ:
    os.environ["CLAIMX_API_URL"] = "https://test.claimxperience.com/api"

if "CLAIMX_API_USERNAME" not in os.environ:
    os.environ["CLAIMX_API_USERNAME"] = "test_user"

if "CLAIMX_API_PASSWORD" not in os.environ:
    os.environ["CLAIMX_API_PASSWORD"] = "test_password"

# ClaimX API token for worker tests
if "CLAIMX_API_TOKEN" not in os.environ:
    os.environ["CLAIMX_API_TOKEN"] = "test-api-token"


@pytest.fixture
def base_kafka_config():
    """Create base Kafka configuration for ClaimX workers."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="PLAINTEXT",
        claimx={
            "topics": {
                "events": "test.claimx.events.raw",
                "enrichment_pending": "test.claimx.enrichment.pending",
                "downloads_pending": "test.claimx.downloads.pending",
                "downloads_cached": "test.claimx.downloads.cached",
                "downloads_results": "test.claimx.downloads.results",
                "entities_rows": "test.claimx.entities.rows",
            },
            "consumer_group_prefix": "test-claimx",
            "retry_delays": [300, 600, 1200],
            "event_ingester": {
                "consumer": {"max_poll_records": 100},
                "processing": {"health_port": 8084},
            },
            "enrichment_worker": {
                "consumer": {"max_poll_records": 100},
                "processing": {"health_port": 8081, "max_poll_records": 100},
            },
            "download_worker": {
                "consumer": {"max_poll_records": 20},
                "processing": {
                    "concurrency": 10,
                    "batch_size": 20,
                    "health_port": 8082,
                },
            },
            "upload_worker": {
                "consumer": {"max_poll_records": 20},
                "processing": {
                    "concurrency": 10,
                    "batch_size": 20,
                    "health_port": 8083,
                },
            },
            "entity_delta_writer": {
                "consumer": {"max_poll_records": 100},
                "processing": {
                    "batch_size": 50,
                    "batch_timeout_seconds": 30.0,
                    "max_retries": 3,
                },
            },
        },
        onelake_base_path="abfss://test@onelake.dfs.fabric.microsoft.com/lakehouse",
        onelake_domain_paths={
            "claimx": "abfss://claimx@onelake.dfs.fabric.microsoft.com/lakehouse"
        },
        claimx_api_url="https://test.claimxperience.com/api",
        claimx_api_token="test-token",
        claimx_api_timeout_seconds=30,
        claimx_api_concurrency=20,
    )


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create temporary cache directory for downloaded files."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return cache_dir


@pytest.fixture
def temp_download_dir(tmp_path):
    """Create temporary download directory."""
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    return download_dir
