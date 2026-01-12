"""
Pytest fixtures for Delta writer tests.

Sets up environment for testing without requiring /var/log access.
"""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """
    Setup test environment before any imports.

    Configures environment variables and mocks to avoid permission issues
    with audit logging and other system resources.
    """
    # Create temporary directory for test logs
    temp_log_dir = tempfile.mkdtemp(prefix="verisk_test_logs_")

    # Set environment variables for tests
    os.environ["TEST_MODE"] = "true"
    os.environ["AUDIT_LOG_PATH"] = f"{temp_log_dir}/audit.log"
    os.environ["LAKEHOUSE_ABFSS_PATH"] = "abfss://test@onelake/lakehouse/Tables"

    yield

    # Cleanup is handled by tempfile cleanup
