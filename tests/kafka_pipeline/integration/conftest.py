"""
Pytest fixtures for end-to-end integration tests.

Provides fixtures for:
- All worker instances (event ingester, download worker, result processor)
- Mock OneLake client (in-memory file storage for testing)
- Mock Delta Lake writers (in-memory verification)
- Worker lifecycle management
- Test environment validation

Note: Shared fixtures (mock classes, worker fixtures) are defined in the parent
conftest.py (tests/kafka_pipeline/conftest.py) to be available to both
integration and performance tests.
"""

import os

# Set test mode environment variables at module import time
# This prevents audit logger from trying to create /var/log/verisk_pipeline
if "TEST_MODE" not in os.environ:
    os.environ["TEST_MODE"] = "true"
if "AUDIT_LOG_PATH" not in os.environ:
    os.environ["AUDIT_LOG_PATH"] = "/tmp/test_audit.log"
if "ALLOWED_ATTACHMENT_DOMAINS" not in os.environ:
    os.environ["ALLOWED_ATTACHMENT_DOMAINS"] = "example.com,claimxperience.com,www.claimxperience.com,claimxperience.s3.amazonaws.com,claimxperience.s3.us-east-1.amazonaws.com"
if "ONELAKE_BASE_PATH" not in os.environ:
    os.environ["ONELAKE_BASE_PATH"] = "abfss://test@test.dfs.core.windows.net/Files"

# Import shared fixtures from parent conftest.py
# These are automatically available via pytest's fixture discovery,
# but we re-export the class names for any direct imports in tests
from tests.kafka_pipeline.conftest import (
    MockOneLakeClient,
    MockDeltaEventsWriter,
    MockDeltaInventoryWriter,
)
