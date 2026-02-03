"""
Tests for checkpoint store singleton factory.

These are unit tests that use mocks - no Azure Blob Storage required.
"""

import asyncio
import logging
import os
import sys
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch, call


@pytest.fixture(autouse=True)
def mock_azure_module():
    """Pre-populate sys.modules with mock azure SDK so blob store tests work
    without the azure-eventhub package installed."""
    mock_blobstore_module = MagicMock()
    modules_to_mock = {
        "azure": MagicMock(),
        "azure.eventhub": MagicMock(),
        "azure.eventhub.extensions": MagicMock(),
        "azure.eventhub.extensions.checkpointstoreblobaio": mock_blobstore_module,
    }

    originals = {k: sys.modules.get(k) for k in modules_to_mock}
    sys.modules.update(modules_to_mock)

    yield mock_blobstore_module

    # Restore originals
    for k, v in originals.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v


class TestCheckpointStoreConfiguration:
    """Test checkpoint store configuration loading."""

    @pytest.fixture(autouse=True)
    def reset_module_state(self):
        """Reset module-level state before each test."""
        from pipeline.common.eventhub.checkpoint_store import reset_checkpoint_store
        reset_checkpoint_store()
        yield
        reset_checkpoint_store()

    @pytest.fixture(autouse=True)
    def clear_env_vars(self):
        """Clear checkpoint-related environment variables before each test."""
        env_vars = [
            "EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING",
            "EVENTHUB_CHECKPOINT_CONTAINER_NAME",
        ]
        original_values = {}
        for var in env_vars:
            original_values[var] = os.environ.get(var)
            if var in os.environ:
                del os.environ[var]

        yield

        # Restore original values
        for var, value in original_values.items():
            if value is not None:
                os.environ[var] = value
            elif var in os.environ:
                del os.environ[var]

    def test_load_config_from_config_file(self):
        """Test loading configuration from config.yaml."""
        from pipeline.common.eventhub.checkpoint_store import _load_checkpoint_config

        mock_config_data = {
            "eventhub": {
                "checkpoint_store": {
                    "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test",
                    "container_name": "my-checkpoints"
                }
            }
        }

        mock_file = MagicMock()
        mock_file.exists.return_value = True

        with patch("config.config.DEFAULT_CONFIG_FILE", mock_file):
            with patch("config.config.load_yaml", return_value=mock_config_data):
                with patch("config.config._expand_env_vars", side_effect=lambda x: x):
                    config = _load_checkpoint_config()

        assert config["blob_storage_connection_string"] == "DefaultEndpointsProtocol=https;AccountName=test"
        assert config["container_name"] == "my-checkpoints"

    def test_load_config_from_env_vars(self):
        """Test loading configuration from environment variables when config file missing."""
        from pipeline.common.eventhub.checkpoint_store import _load_checkpoint_config

        os.environ["EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING"] = "DefaultEndpointsProtocol=https;AccountName=envtest"
        os.environ["EVENTHUB_CHECKPOINT_CONTAINER_NAME"] = "env-checkpoints"

        mock_file = MagicMock()
        mock_file.exists.return_value = False

        with patch("config.config.DEFAULT_CONFIG_FILE", mock_file):
            config = _load_checkpoint_config()

        assert config["blob_storage_connection_string"] == "DefaultEndpointsProtocol=https;AccountName=envtest"
        assert config["container_name"] == "env-checkpoints"

    def test_load_config_empty_when_not_configured(self):
        """Test configuration defaults to empty when not configured."""
        from pipeline.common.eventhub.checkpoint_store import _load_checkpoint_config

        mock_file = MagicMock()
        mock_file.exists.return_value = False

        with patch("config.config.DEFAULT_CONFIG_FILE", mock_file):
            config = _load_checkpoint_config()

        assert config["blob_storage_connection_string"] == ""
        assert config["container_name"] == "eventhub-checkpoints"

    def test_load_config_default_container_name(self):
        """Test default container name is used when not specified."""
        from pipeline.common.eventhub.checkpoint_store import _load_checkpoint_config

        mock_config_data = {
            "eventhub": {
                "checkpoint_store": {
                    "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test",
                }
            }
        }

        mock_file = MagicMock()
        mock_file.exists.return_value = True

        with patch("config.config.DEFAULT_CONFIG_FILE", mock_file):
            with patch("config.config.load_yaml", return_value=mock_config_data):
                with patch("config.config._expand_env_vars", side_effect=lambda x: x):
                    config = _load_checkpoint_config()

        assert config["container_name"] == "eventhub-checkpoints"


class TestCheckpointStoreFactory:
    """Test checkpoint store singleton factory functions."""

    @pytest.fixture(autouse=True)
    def reset_module_state(self):
        """Reset module-level state before each test."""
        from pipeline.common.eventhub.checkpoint_store import reset_checkpoint_store
        reset_checkpoint_store()
        yield
        reset_checkpoint_store()

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_returns_none_when_not_configured(self):
        """Test get_checkpoint_store returns None when connection string is empty."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        # Mock configuration with empty connection string
        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "",
                "container_name": "eventhub-checkpoints"
            }

            checkpoint_store = await get_checkpoint_store()

        assert checkpoint_store is None

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_returns_blob_checkpoint_store_when_configured(self):
        """Test get_checkpoint_store returns BlobCheckpointStore when properly configured."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        mock_blob_store = MagicMock()

        # Mock configuration with valid connection string
        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": "my-checkpoints"
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class:
                mock_blob_class.from_connection_string.return_value = mock_blob_store

                checkpoint_store = await get_checkpoint_store()

        assert checkpoint_store is mock_blob_store

        # Verify from_connection_string was called with correct parameters
        mock_blob_class.from_connection_string.assert_called_once_with(
            conn_str="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
            container_name="my-checkpoints"
        )

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_singleton_behavior(self):
        """Test get_checkpoint_store returns same instance on multiple calls."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        mock_blob_store = MagicMock()

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": "my-checkpoints"
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class:
                mock_blob_class.from_connection_string.return_value = mock_blob_store

                # First call
                store1 = await get_checkpoint_store()

                # Second call
                store2 = await get_checkpoint_store()

                # Third call
                store3 = await get_checkpoint_store()

        # All calls return the same instance
        assert store1 is store2
        assert store2 is store3

        # from_connection_string should only be called once
        mock_blob_class.from_connection_string.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_caches_none_result(self):
        """Test get_checkpoint_store caches None result and doesn't retry."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        call_count = 0

        def mock_load_config():
            nonlocal call_count
            call_count += 1
            return {
                "blob_storage_connection_string": "",
                "container_name": "eventhub-checkpoints"
            }

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config", side_effect=mock_load_config):
            # First call
            store1 = await get_checkpoint_store()

            # Second call
            store2 = await get_checkpoint_store()

            # Third call
            store3 = await get_checkpoint_store()

        # All calls return None
        assert store1 is None
        assert store2 is None
        assert store3 is None

        # Config should only be loaded once
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_thread_safe_initialization(self):
        """Test get_checkpoint_store is thread-safe with concurrent calls."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        mock_blob_store = MagicMock()
        create_count = 0

        def mock_from_connection_string(**kwargs):
            nonlocal create_count
            create_count += 1
            # Simulate some delay
            return mock_blob_store

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": "my-checkpoints"
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class:
                mock_blob_class.from_connection_string.side_effect = mock_from_connection_string

                # Make 10 concurrent calls
                stores = await asyncio.gather(*[get_checkpoint_store() for _ in range(10)])

        # All calls return the same instance
        assert all(store is mock_blob_store for store in stores)

        # Should only create once despite concurrent calls
        assert create_count == 1

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_raises_on_creation_error(self):
        """Test get_checkpoint_store raises exception when BlobCheckpointStore creation fails."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": "my-checkpoints"
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class:
                mock_blob_class.from_connection_string.side_effect = RuntimeError("Azure connection failed")

                with pytest.raises(RuntimeError, match="Azure connection failed"):
                    await get_checkpoint_store()

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_uses_default_container_when_empty(self):
        """Test get_checkpoint_store uses default container name when config value is empty."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        mock_blob_store = MagicMock()

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": ""  # Empty container name
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class:
                mock_blob_class.from_connection_string.return_value = mock_blob_store

                checkpoint_store = await get_checkpoint_store()

        # Should use default container name
        mock_blob_class.from_connection_string.assert_called_once_with(
            conn_str="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
            container_name="eventhub-checkpoints"
        )


class TestCheckpointStoreCleanup:
    """Test checkpoint store cleanup functions."""

    @pytest.fixture(autouse=True)
    def reset_module_state(self):
        """Reset module-level state before each test."""
        from pipeline.common.eventhub.checkpoint_store import reset_checkpoint_store
        reset_checkpoint_store()
        yield
        reset_checkpoint_store()

    @pytest.mark.asyncio
    async def test_close_checkpoint_store_closes_active_store(self):
        """Test close_checkpoint_store closes the active checkpoint store."""
        from pipeline.common.eventhub.checkpoint_store import (
            get_checkpoint_store,
            close_checkpoint_store
        )

        mock_blob_store = MagicMock()
        mock_blob_store.close = AsyncMock()

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": "my-checkpoints"
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class:
                mock_blob_class.from_connection_string.return_value = mock_blob_store

                # Create checkpoint store
                store = await get_checkpoint_store()
                assert store is mock_blob_store

                # Close it
                await close_checkpoint_store()

        # Verify close was called
        mock_blob_store.close.assert_called_once()

        # Verify state is reset (next call creates new instance)
        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load2:
            mock_load2.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": "my-checkpoints"
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class2:
                new_mock_store = MagicMock()
                mock_blob_class2.from_connection_string.return_value = new_mock_store

                store2 = await get_checkpoint_store()

                # Should create a new instance
                assert store2 is new_mock_store

    @pytest.mark.asyncio
    async def test_close_checkpoint_store_safe_when_not_initialized(self):
        """Test close_checkpoint_store is safe to call when store was never initialized."""
        from pipeline.common.eventhub.checkpoint_store import close_checkpoint_store

        # Should not raise
        await close_checkpoint_store()

    @pytest.mark.asyncio
    async def test_close_checkpoint_store_safe_when_already_closed(self):
        """Test close_checkpoint_store is safe to call multiple times."""
        from pipeline.common.eventhub.checkpoint_store import (
            get_checkpoint_store,
            close_checkpoint_store
        )

        mock_blob_store = MagicMock()
        mock_blob_store.close = AsyncMock()

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": "my-checkpoints"
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class:
                mock_blob_class.from_connection_string.return_value = mock_blob_store

                # Create checkpoint store
                await get_checkpoint_store()

                # Close it
                await close_checkpoint_store()

                # Close again
                await close_checkpoint_store()

                # Should still only call close once
                mock_blob_store.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_checkpoint_store_handles_close_error(self):
        """Test close_checkpoint_store handles errors during close gracefully."""
        from pipeline.common.eventhub.checkpoint_store import (
            get_checkpoint_store,
            close_checkpoint_store
        )

        mock_blob_store = MagicMock()
        mock_blob_store.close = AsyncMock(side_effect=RuntimeError("Close failed"))

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": "my-checkpoints"
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class:
                mock_blob_class.from_connection_string.return_value = mock_blob_store

                # Create checkpoint store
                await get_checkpoint_store()

                # Close should not raise - error is logged
                await close_checkpoint_store()

        # Verify close was attempted
        mock_blob_store.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_checkpoint_store_without_close_method(self):
        """Test close_checkpoint_store handles stores without close method."""
        from pipeline.common.eventhub.checkpoint_store import (
            get_checkpoint_store,
            close_checkpoint_store
        )

        # Mock store without close method
        mock_blob_store = MagicMock(spec=[])  # Empty spec means no close method

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": "my-checkpoints"
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class:
                mock_blob_class.from_connection_string.return_value = mock_blob_store

                # Create checkpoint store
                await get_checkpoint_store()

                # Close should not raise
                await close_checkpoint_store()

    def test_reset_checkpoint_store_clears_state(self):
        """Test reset_checkpoint_store clears singleton state for testing."""
        from pipeline.common.eventhub.checkpoint_store import (
            reset_checkpoint_store,
            _checkpoint_store,
            _initialization_attempted
        )
        import pipeline.common.eventhub.checkpoint_store as checkpoint_module

        # Set some state
        checkpoint_module._checkpoint_store = MagicMock()
        checkpoint_module._initialization_attempted = True

        # Reset
        reset_checkpoint_store()

        # Verify state is cleared
        assert checkpoint_module._checkpoint_store is None
        assert checkpoint_module._initialization_attempted is False

    def test_reset_checkpoint_store_does_not_close(self):
        """Test reset_checkpoint_store does not close the store (for testing only)."""
        from pipeline.common.eventhub.checkpoint_store import reset_checkpoint_store
        import pipeline.common.eventhub.checkpoint_store as checkpoint_module

        # Set up mock store with close method
        mock_blob_store = MagicMock()
        mock_blob_store.close = AsyncMock()
        checkpoint_module._checkpoint_store = mock_blob_store

        # Reset
        reset_checkpoint_store()

        # Close should not be called
        mock_blob_store.close.assert_not_called()


class TestCheckpointStoreLogging:
    """Test checkpoint store logging behavior."""

    @pytest.fixture(autouse=True)
    def reset_module_state(self):
        """Reset module-level state before each test."""
        from pipeline.common.eventhub.checkpoint_store import reset_checkpoint_store
        reset_checkpoint_store()
        yield
        reset_checkpoint_store()

    @pytest.mark.asyncio
    async def test_logs_info_when_not_configured(self, caplog):
        """Test appropriate INFO log when checkpoint store is not configured."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "",
                "container_name": "eventhub-checkpoints"
            }

            with caplog.at_level(logging.INFO):
                checkpoint_store = await get_checkpoint_store()

        assert checkpoint_store is None

        # Check for informative log message
        log_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        assert any("not configured" in msg.lower() for msg in log_messages)

    @pytest.mark.asyncio
    async def test_logs_info_when_initialized(self, caplog):
        """Test appropriate INFO log when checkpoint store is successfully initialized."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        mock_blob_store = MagicMock()

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": "my-checkpoints"
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class:
                mock_blob_class.from_connection_string.return_value = mock_blob_store

                with caplog.at_level(logging.INFO):
                    checkpoint_store = await get_checkpoint_store()

        assert checkpoint_store is mock_blob_store

        # Check for initialization log messages
        log_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        assert any("initializing" in msg.lower() for msg in log_messages)
        assert any("initialized successfully" in msg.lower() for msg in log_messages)

    @pytest.mark.asyncio
    async def test_logs_warning_when_container_name_empty(self, caplog):
        """Test WARNING log when container_name is empty."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        mock_blob_store = MagicMock()

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test123",
                "container_name": ""
            }

            with patch("azure.eventhub.extensions.checkpointstoreblobaio.BlobCheckpointStore") as mock_blob_class:
                mock_blob_class.from_connection_string.return_value = mock_blob_store

                with caplog.at_level(logging.WARNING):
                    await get_checkpoint_store()

        # Check for warning about empty container name
        log_messages = [record.message for record in caplog.records if record.levelname == "WARNING"]
        assert any("container_name is empty" in msg for msg in log_messages)


class TestCheckpointStoreFactoryJsonType:
    """Test checkpoint store factory with JSON backend."""

    @pytest.fixture(autouse=True)
    def reset_module_state(self):
        """Reset module-level state before each test."""
        from pipeline.common.eventhub.checkpoint_store import reset_checkpoint_store
        reset_checkpoint_store()
        yield
        reset_checkpoint_store()

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_returns_json_store_when_type_json(self, tmp_path):
        """Test factory returns JsonCheckpointStore when type=json."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store
        from pipeline.common.eventhub.json_checkpoint_store import JsonCheckpointStore

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "type": "json",
                "storage_path": str(tmp_path),
                "blob_storage_connection_string": "",
                "container_name": "eventhub-checkpoints",
            }

            store = await get_checkpoint_store()

        assert isinstance(store, JsonCheckpointStore)

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_defaults_to_blob_when_type_missing(self):
        """Test factory defaults to blob type when type is not specified."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "blob_storage_connection_string": "",
                "container_name": "eventhub-checkpoints",
            }

            store = await get_checkpoint_store()

        # Blob with empty connection string returns None (graceful degradation)
        assert store is None

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_raises_on_unknown_type(self):
        """Test factory raises ValueError for unknown store type."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "type": "redis",
                "blob_storage_connection_string": "",
                "container_name": "eventhub-checkpoints",
                "storage_path": "/tmp/checkpoints",
            }

            with pytest.raises(ValueError, match="Unknown checkpoint store type"):
                await get_checkpoint_store()

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_json_uses_storage_path_from_config(self, tmp_path):
        """Test JsonCheckpointStore receives the configured storage_path."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store
        from pipeline.common.eventhub.json_checkpoint_store import JsonCheckpointStore

        custom_path = tmp_path / "custom-checkpoints"

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "type": "json",
                "storage_path": str(custom_path),
                "blob_storage_connection_string": "",
                "container_name": "eventhub-checkpoints",
            }

            store = await get_checkpoint_store()

        assert isinstance(store, JsonCheckpointStore)
        assert store._base_path == custom_path

    @pytest.mark.asyncio
    async def test_get_checkpoint_store_json_singleton_behavior(self, tmp_path):
        """Test JSON store singleton: same instance returned on multiple calls."""
        from pipeline.common.eventhub.checkpoint_store import get_checkpoint_store

        with patch("pipeline.common.eventhub.checkpoint_store._load_checkpoint_config") as mock_load:
            mock_load.return_value = {
                "type": "json",
                "storage_path": str(tmp_path),
                "blob_storage_connection_string": "",
                "container_name": "eventhub-checkpoints",
            }

            store1 = await get_checkpoint_store()
            store2 = await get_checkpoint_store()

        assert store1 is store2

    @pytest.mark.asyncio
    async def test_load_config_includes_type_from_config_file(self):
        """Test _load_checkpoint_config reads the type field from config.yaml."""
        from pipeline.common.eventhub.checkpoint_store import _load_checkpoint_config

        mock_config_data = {
            "eventhub": {
                "checkpoint_store": {
                    "type": "json",
                    "storage_path": "/tmp/test-checkpoints",
                    "blob_storage_connection_string": "",
                    "container_name": "my-checkpoints",
                }
            }
        }

        mock_file = MagicMock()
        mock_file.exists.return_value = True

        with patch("config.config.DEFAULT_CONFIG_FILE", mock_file):
            with patch("config.config.load_yaml", return_value=mock_config_data):
                with patch("config.config._expand_env_vars", side_effect=lambda x: x):
                    config = _load_checkpoint_config()

        assert config["type"] == "json"
        assert config["storage_path"] == "/tmp/test-checkpoints"

    @pytest.mark.asyncio
    async def test_load_config_type_defaults_to_blob(self):
        """Test _load_checkpoint_config defaults type to blob."""
        from pipeline.common.eventhub.checkpoint_store import _load_checkpoint_config

        mock_config_data = {
            "eventhub": {
                "checkpoint_store": {
                    "blob_storage_connection_string": "",
                }
            }
        }

        mock_file = MagicMock()
        mock_file.exists.return_value = True

        with patch("config.config.DEFAULT_CONFIG_FILE", mock_file):
            with patch("config.config.load_yaml", return_value=mock_config_data):
                with patch("config.config._expand_env_vars", side_effect=lambda x: x):
                    config = _load_checkpoint_config()

        assert config["type"] == "blob"
