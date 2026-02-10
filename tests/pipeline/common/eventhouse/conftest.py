"""Conftest for eventhouse tests.

Pre-mocks modules that require Python 3.12+ (PEP 695 syntax) or
unavailable dependencies (aiokafka) so that the poller module can be
imported on Python 3.11 test environments.
"""

import sys
import types

# -- pipeline.common.retry needs aiokafka -----------------------------------
_mock_retry = types.ModuleType("pipeline.common.retry")
_mock_retry.RetryConfig = type("RetryConfig", (), {"__init__": lambda self, **kw: None})
_mock_retry.with_retry = lambda *a, **k: lambda f: f
sys.modules.setdefault("pipeline.common.retry", _mock_retry)
sys.modules.setdefault(
    "pipeline.common.retry.delta_handler",
    types.ModuleType("pipeline.common.retry.delta_handler"),
)
sys.modules.setdefault(
    "pipeline.common.retry.unified_scheduler",
    types.ModuleType("pipeline.common.retry.unified_scheduler"),
)
