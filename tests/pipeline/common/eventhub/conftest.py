"""Shared fixtures for EventHub test modules.

Mocks Azure SDK modules so tests can run without azure-eventhub installed.
"""

import sys
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def mock_azure_modules():
    """Pre-populate sys.modules with mock Azure SDK modules.

    This allows importing pipeline.common.eventhub modules without
    the actual Azure packages installed.
    """
    mock_eventhub = MagicMock()
    mock_eventhub.TransportType = MagicMock()
    mock_eventhub.TransportType.AmqpOverWebsocket = "AmqpOverWebsocket"
    mock_eventhub.EventData = MagicMock()

    mock_eventhub_aio = MagicMock()
    mock_storage_blob = MagicMock()
    mock_storage_blob_aio = MagicMock()

    modules_to_mock = {
        "azure": MagicMock(),
        "azure.eventhub": mock_eventhub,
        "azure.eventhub.aio": mock_eventhub_aio,
        "azure.eventhub.extensions": MagicMock(),
        "azure.eventhub.extensions.checkpointstoreblobaio": MagicMock(),
        "azure.storage": MagicMock(),
        "azure.storage.blob": mock_storage_blob,
        "azure.storage.blob.aio": mock_storage_blob_aio,
    }

    originals = {k: sys.modules.get(k) for k in modules_to_mock}
    sys.modules.update(modules_to_mock)

    yield {
        "eventhub": mock_eventhub,
        "eventhub_aio": mock_eventhub_aio,
        "storage_blob_aio": mock_storage_blob_aio,
    }

    for k, v in originals.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v
