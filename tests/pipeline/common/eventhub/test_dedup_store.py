"""Tests for pipeline.common.eventhub.dedup_store module.

Covers the singleton factory (get_dedup_store), configuration loading,
close_dedup_store, and reset_dedup_store.
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import pipeline.common.eventhub.dedup_store as dedup_module


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset dedup store singleton before and after each test."""
    dedup_module.reset_dedup_store()
    yield
    dedup_module.reset_dedup_store()


@pytest.fixture(autouse=True)
def clear_env_vars():
    """Clear dedup-related env vars before each test."""
    env_vars = [
        "EVENTHUB_DEDUP_STORE_TYPE",
        "EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING",
        "EVENTHUB_DEDUP_CONTAINER_NAME",
        "EVENTHUB_DEDUP_JSON_PATH",
        "EVENTHUB_DEDUP_TTL_SECONDS",
    ]
    originals = {}
    for var in env_vars:
        originals[var] = os.environ.get(var)
        os.environ.pop(var, None)

    yield

    for var, val in originals.items():
        if val is not None:
            os.environ[var] = val
        else:
            os.environ.pop(var, None)


# =============================================================================
# _load_dedup_config
# =============================================================================


class TestLoadDedupConfig:
    def test_loads_config_from_config_file(self):
        mock_data = {
            "eventhub": {
                "dedup_store": {
                    "type": "json",
                    "blob_storage_connection_string": "conn-str",
                    "container_name": "my-container",
                    "storage_path": "/tmp/dedup",
                    "ttl_seconds": 3600,
                }
            }
        }

        mock_file = MagicMock()
        mock_file.exists.return_value = True

        with patch("config.config.DEFAULT_CONFIG_FILE", mock_file):
            with patch("config.config.load_yaml", return_value=mock_data):
                with patch("config.config._expand_env_vars", side_effect=lambda x: x):
                    config = dedup_module._load_dedup_config()

        assert config["type"] == "json"
        assert config["blob_storage_connection_string"] == "conn-str"
        assert config["container_name"] == "my-container"
        assert config["storage_path"] == "/tmp/dedup"
        assert config["ttl_seconds"] == 3600

    def test_loads_config_from_env_vars_when_no_config_file(self):
        mock_file = MagicMock()
        mock_file.exists.return_value = False

        os.environ["EVENTHUB_DEDUP_STORE_TYPE"] = "json"
        os.environ["EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING"] = "env-conn"
        os.environ["EVENTHUB_DEDUP_CONTAINER_NAME"] = "env-container"
        os.environ["EVENTHUB_DEDUP_JSON_PATH"] = "/tmp/env-dedup"
        os.environ["EVENTHUB_DEDUP_TTL_SECONDS"] = "7200"

        with patch("config.config.DEFAULT_CONFIG_FILE", mock_file):
            config = dedup_module._load_dedup_config()

        assert config["type"] == "json"
        assert config["blob_storage_connection_string"] == "env-conn"
        assert config["container_name"] == "env-container"
        assert config["storage_path"] == "/tmp/env-dedup"
        assert config["ttl_seconds"] == 7200

    def test_defaults_when_nothing_configured(self):
        mock_file = MagicMock()
        mock_file.exists.return_value = False

        with patch("config.config.DEFAULT_CONFIG_FILE", mock_file):
            config = dedup_module._load_dedup_config()

        assert config["type"] == "blob"
        assert config["blob_storage_connection_string"] == ""
        assert config["container_name"] == "eventhub-dedup-cache"
        assert config["ttl_seconds"] == 86400

    def test_defaults_from_config_file_with_empty_dedup_section(self):
        mock_data = {"eventhub": {"dedup_store": {}}}
        mock_file = MagicMock()
        mock_file.exists.return_value = True

        with patch("config.config.DEFAULT_CONFIG_FILE", mock_file):
            with patch("config.config.load_yaml", return_value=mock_data):
                with patch("config.config._expand_env_vars", side_effect=lambda x: x):
                    config = dedup_module._load_dedup_config()

        assert config["type"] == "blob"
        assert config["container_name"] == "eventhub-dedup-cache"
        assert config["ttl_seconds"] == 86400


# =============================================================================
# get_dedup_store
# =============================================================================


class TestGetDedupStore:
    async def test_returns_none_when_blob_not_configured(self):
        with patch.object(dedup_module, "_load_dedup_config") as mock_load:
            mock_load.return_value = {
                "type": "blob",
                "blob_storage_connection_string": "",
                "container_name": "container",
                "storage_path": "/tmp",
                "ttl_seconds": 86400,
            }

            store = await dedup_module.get_dedup_store()

        assert store is None

    async def test_returns_json_store_when_type_json(self, tmp_path):
        from pipeline.common.eventhub.json_dedup_store import JsonDedupStore

        with patch.object(dedup_module, "_load_dedup_config") as mock_load:
            mock_load.return_value = {
                "type": "json",
                "storage_path": str(tmp_path),
                "blob_storage_connection_string": "",
                "container_name": "c",
                "ttl_seconds": 86400,
            }

            store = await dedup_module.get_dedup_store()

        assert isinstance(store, JsonDedupStore)

    async def test_returns_blob_store_when_configured(self):
        mock_blob_store = MagicMock()
        mock_blob_store.initialize = AsyncMock()

        with patch.object(dedup_module, "_load_dedup_config") as mock_load:
            mock_load.return_value = {
                "type": "blob",
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=t",
                "container_name": "dedup-cache",
                "storage_path": "/tmp",
                "ttl_seconds": 86400,
            }

            with patch(
                "pipeline.common.eventhub.blob_dedup_store.BlobDedupStore",
                return_value=mock_blob_store,
            ):
                store = await dedup_module.get_dedup_store()

        assert store is mock_blob_store
        mock_blob_store.initialize.assert_awaited_once()

    async def test_singleton_behavior(self, tmp_path):
        with patch.object(dedup_module, "_load_dedup_config") as mock_load:
            mock_load.return_value = {
                "type": "json",
                "storage_path": str(tmp_path),
                "blob_storage_connection_string": "",
                "container_name": "c",
                "ttl_seconds": 86400,
            }

            store1 = await dedup_module.get_dedup_store()
            store2 = await dedup_module.get_dedup_store()

        assert store1 is store2

    async def test_caches_none_result(self):
        with patch.object(dedup_module, "_load_dedup_config") as mock_load:
            mock_load.return_value = {
                "type": "blob",
                "blob_storage_connection_string": "",
                "container_name": "c",
                "storage_path": "/tmp",
                "ttl_seconds": 86400,
            }

            store1 = await dedup_module.get_dedup_store()
            store2 = await dedup_module.get_dedup_store()

        assert store1 is None
        assert store2 is None
        mock_load.assert_called_once()

    async def test_raises_on_unknown_type(self):
        with patch.object(dedup_module, "_load_dedup_config") as mock_load:
            mock_load.return_value = {
                "type": "redis",
                "blob_storage_connection_string": "",
                "container_name": "c",
                "storage_path": "/tmp",
                "ttl_seconds": 86400,
            }

            with pytest.raises(ValueError, match="Unknown dedup store type"):
                await dedup_module.get_dedup_store()

    async def test_raises_on_blob_initialization_failure(self):
        mock_blob_store = MagicMock()
        mock_blob_store.initialize = AsyncMock(side_effect=RuntimeError("blob fail"))

        with patch.object(dedup_module, "_load_dedup_config") as mock_load:
            mock_load.return_value = {
                "type": "blob",
                "blob_storage_connection_string": "conn",
                "container_name": "c",
                "storage_path": "/tmp",
                "ttl_seconds": 86400,
            }

            with patch(
                "pipeline.common.eventhub.blob_dedup_store.BlobDedupStore",
                return_value=mock_blob_store,
            ):
                with pytest.raises(RuntimeError, match="blob fail"):
                    await dedup_module.get_dedup_store()

    async def test_blob_store_uses_default_container_when_empty(self):
        mock_blob_store = MagicMock()
        mock_blob_store.initialize = AsyncMock()

        with patch.object(dedup_module, "_load_dedup_config") as mock_load:
            mock_load.return_value = {
                "type": "blob",
                "blob_storage_connection_string": "conn",
                "container_name": "",
                "storage_path": "/tmp",
                "ttl_seconds": 86400,
            }

            with patch(
                "pipeline.common.eventhub.blob_dedup_store.BlobDedupStore"
            ) as MockBlobStore:
                MockBlobStore.return_value = mock_blob_store

                await dedup_module.get_dedup_store()

                MockBlobStore.assert_called_once_with(
                    connection_string="conn",
                    container_name="eventhub-dedup-cache",
                )


# =============================================================================
# close_dedup_store
# =============================================================================


class TestCloseDedupStore:
    async def test_close_when_not_initialized(self):
        # Should not raise
        await dedup_module.close_dedup_store()

    async def test_close_calls_close_on_store(self, tmp_path):
        with patch.object(dedup_module, "_load_dedup_config") as mock_load:
            mock_load.return_value = {
                "type": "json",
                "storage_path": str(tmp_path),
                "blob_storage_connection_string": "",
                "container_name": "c",
                "ttl_seconds": 86400,
            }

            store = await dedup_module.get_dedup_store()
            assert store is not None

            # Mock close method
            store.close = AsyncMock()
            dedup_module._dedup_store = store

            await dedup_module.close_dedup_store()

            store.close.assert_awaited_once()
            assert dedup_module._dedup_store is None
            assert dedup_module._initialization_attempted is False

    async def test_close_safe_when_already_closed(self):
        await dedup_module.close_dedup_store()
        # Second call should not raise
        await dedup_module.close_dedup_store()

    async def test_close_handles_error(self, tmp_path):
        with patch.object(dedup_module, "_load_dedup_config") as mock_load:
            mock_load.return_value = {
                "type": "json",
                "storage_path": str(tmp_path),
                "blob_storage_connection_string": "",
                "container_name": "c",
                "ttl_seconds": 86400,
            }

            store = await dedup_module.get_dedup_store()
            store.close = AsyncMock(side_effect=RuntimeError("close fail"))
            dedup_module._dedup_store = store

            # Should not raise
            await dedup_module.close_dedup_store()

            assert dedup_module._dedup_store is None


# =============================================================================
# reset_dedup_store
# =============================================================================


class TestResetDedupStore:
    def test_reset_clears_singleton_state(self):
        dedup_module._dedup_store = MagicMock()
        dedup_module._initialization_attempted = True

        dedup_module.reset_dedup_store()

        assert dedup_module._dedup_store is None
        assert dedup_module._initialization_attempted is False

    def test_reset_does_not_close(self):
        mock_store = MagicMock()
        mock_store.close = AsyncMock()
        dedup_module._dedup_store = mock_store

        dedup_module.reset_dedup_store()

        mock_store.close.assert_not_called()
