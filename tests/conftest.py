"""
pytest configuration for core library tests.

Adds src directory to Python path for imports and sets up test environment.
"""

import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

# Set test environment variables BEFORE any imports
os.environ.setdefault("TEST_MODE", "true")
os.environ.setdefault("AUDIT_LOG_PATH", "/tmp/test_audit.log")

# Add src directory to Python path
src_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_dir))

# Mock polars if not installed so test files can be collected.
# Tests that actually use polars DataFrames will fail at runtime
# and should be skipped with @pytest.mark.skipif.
if "polars" not in sys.modules:
    try:
        import polars  # noqa: F401
    except ImportError:
        _polars = MagicMock()
        _polars.__name__ = "polars"
        _polars.__path__ = []
        _polars.__file__ = "mock"
        _polars.__spec__ = None
        sys.modules["polars"] = _polars
        for _sub in ("testing", "datatypes", "selectors", "exceptions"):
            sys.modules[f"polars.{_sub}"] = MagicMock()


# Configure test environment for audit logging
# This needs to happen before any application modules are imported
def pytest_configure(config):
    """Configure pytest environment for testing."""
    # Audit logging is configured via AUDIT_LOG_PATH environment variable
    # which is already set above to /tmp/test_audit.log
    pass
