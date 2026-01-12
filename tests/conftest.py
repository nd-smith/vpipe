"""
pytest configuration for core library tests.

Adds src directory to Python path for imports and sets up test environment.
"""

import os
import sys
from pathlib import Path

# Set test environment variables BEFORE any imports
os.environ.setdefault("TEST_MODE", "true")
os.environ.setdefault("AUDIT_LOG_PATH", "/tmp/test_audit.log")

# Add src directory to Python path
src_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_dir))

# Configure test environment for audit logging
# This needs to happen before any application modules are imported
def pytest_configure(config):
    """Configure pytest environment for testing."""
    # Audit logging is configured via AUDIT_LOG_PATH environment variable
    # which is already set above to /tmp/test_audit.log
    pass
