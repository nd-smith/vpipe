"""Tests for poller checkpoint store backends and factory.

Covers:
- JsonPollerCheckpointStore: load, save, atomic write, missing file, corrupt file
- BlobPollerCheckpointStore: load, save, missing blob, error handling
- Factory: get_poller_checkpoint_store with json/blob/fallback selection
- PollerCheckpointData: to_dict / from_dict round-trip
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.common.eventhouse.poller_checkpoint_store import (
    PollerCheckpointData,
    get_poller_checkpoint_store,
    reset_poller_checkpoint_store,
)


# =============================================================================
# PollerCheckpointData
# =============================================================================


class TestPollerCheckpointData:
    def test_to_dict(self):
        data = PollerCheckpointData(
            last_ingestion_time="2026-02-03T15:00:00+00:00",
            last_trace_id="trace-1",
            updated_at="2026-02-03T15:00:05+00:00",
        )
        d = data.to_dict()
        assert d["last_ingestion_time"] == "2026-02-03T15:00:00+00:00"
        assert d["last_trace_id"] == "trace-1"
        assert d["updated_at"] == "2026-02-03T15:00:05+00:00"

    def test_from_dict(self):
        d = {
            "last_ingestion_time": "2026-02-03T15:00:00+00:00",
            "last_trace_id": "trace-1",
            "updated_at": "2026-02-03T15:00:05+00:00",
        }
        data = PollerCheckpointData.from_dict(d)
        assert data.last_ingestion_time == "2026-02-03T15:00:00+00:00"
        assert data.last_trace_id == "trace-1"

    def test_from_dict_missing_updated_at(self):
        d = {
            "last_ingestion_time": "2026-02-03T15:00:00+00:00",
            "last_trace_id": "trace-1",
        }
        data = PollerCheckpointData.from_dict(d)
        assert data.updated_at == ""

    def test_round_trip(self):
        original = PollerCheckpointData(
            last_ingestion_time="2026-02-03T15:00:00+00:00",
            last_trace_id="trace-1",
            updated_at="2026-02-03T15:00:05+00:00",
        )
        restored = PollerCheckpointData.from_dict(original.to_dict())
        assert restored.last_ingestion_time == original.last_ingestion_time
        assert restored.last_trace_id == original.last_trace_id
        assert restored.updated_at == original.updated_at


# =============================================================================
# JsonPollerCheckpointStore
# =============================================================================


class TestJsonPollerCheckpointStore:
    @pytest.fixture
    def store(self, tmp_path):
        from pipeline.common.eventhouse.json_poller_checkpoint_store import (
            JsonPollerCheckpointStore,
        )

        return JsonPollerCheckpointStore(storage_path=tmp_path)

    @pytest.fixture
    def checkpoint(self):
        return PollerCheckpointData(
            last_ingestion_time="2026-02-03T15:00:00+00:00",
            last_trace_id="trace-abc",
            updated_at="2026-02-03T15:00:05+00:00",
        )

    async def test_load_returns_none_when_no_file(self, store):
        result = await store.load("nonexistent-domain")
        assert result is None

    async def test_save_and_load_round_trip(self, store, checkpoint):
        await store.save("verisk", checkpoint)
        loaded = await store.load("verisk")
        assert loaded is not None
        assert loaded.last_ingestion_time == checkpoint.last_ingestion_time
        assert loaded.last_trace_id == checkpoint.last_trace_id

    async def test_save_creates_file(self, store, checkpoint, tmp_path):
        await store.save("verisk", checkpoint)
        path = tmp_path / "poller_verisk.json"
        assert path.exists()
        with open(path) as f:
            data = json.load(f)
        assert data["last_ingestion_time"] == "2026-02-03T15:00:00+00:00"
        assert data["last_trace_id"] == "trace-abc"

    async def test_save_overwrites_existing(self, store, checkpoint):
        await store.save("verisk", checkpoint)

        updated = PollerCheckpointData(
            last_ingestion_time="2026-02-03T16:00:00+00:00",
            last_trace_id="trace-def",
            updated_at="2026-02-03T16:00:05+00:00",
        )
        await store.save("verisk", updated)

        loaded = await store.load("verisk")
        assert loaded is not None
        assert loaded.last_ingestion_time == "2026-02-03T16:00:00+00:00"
        assert loaded.last_trace_id == "trace-def"

    async def test_load_corrupt_json_returns_none(self, store, tmp_path):
        path = tmp_path / "poller_corrupt.json"
        path.write_text("not valid json{{{")
        result = await store.load("corrupt")
        assert result is None

    async def test_load_missing_keys_returns_none(self, store, tmp_path):
        path = tmp_path / "poller_badkeys.json"
        path.write_text('{"wrong_key": "value"}')
        result = await store.load("badkeys")
        assert result is None

    async def test_domains_are_isolated(self, store):
        cp1 = PollerCheckpointData("t1", "id1", "u1")
        cp2 = PollerCheckpointData("t2", "id2", "u2")
        await store.save("domain_a", cp1)
        await store.save("domain_b", cp2)

        loaded_a = await store.load("domain_a")
        loaded_b = await store.load("domain_b")
        assert loaded_a is not None
        assert loaded_b is not None
        assert loaded_a.last_trace_id == "id1"
        assert loaded_b.last_trace_id == "id2"

    async def test_close_is_noop(self, store):
        await store.close()  # Should not raise


# =============================================================================
# BlobPollerCheckpointStore
# =============================================================================


class TestBlobPollerCheckpointStore:
    @pytest.fixture
    def mock_container_client(self):
        # Container client uses sync get_blob_client, but async close
        client = MagicMock()
        client.close = AsyncMock()
        return client

    @pytest.fixture
    def store(self, mock_container_client):
        from pipeline.common.eventhouse.blob_poller_checkpoint_store import (
            BlobPollerCheckpointStore,
        )

        with patch(
            "pipeline.common.eventhouse.blob_poller_checkpoint_store.BlobPollerCheckpointStore.__init__",
            lambda self, **kwargs: None,
        ):
            s = BlobPollerCheckpointStore.__new__(BlobPollerCheckpointStore)
            s._container_client = mock_container_client
            s._container_name = "test-container"
            return s

    def _make_blob_client(self, download_result=None, download_error=None, upload_error=None):
        """Helper to create a mock blob client with async methods."""
        blob_client = MagicMock()
        if download_error:
            blob_client.download_blob = AsyncMock(side_effect=download_error)
        else:
            mock_download = MagicMock()
            mock_download.readall = AsyncMock(return_value=download_result)
            blob_client.download_blob = AsyncMock(return_value=mock_download)

        if upload_error:
            blob_client.upload_blob = AsyncMock(side_effect=upload_error)
        else:
            blob_client.upload_blob = AsyncMock()

        return blob_client

    async def test_load_returns_data(self, store, mock_container_client):
        blob_data = json.dumps({
            "last_ingestion_time": "2026-02-03T15:00:00+00:00",
            "last_trace_id": "trace-1",
            "updated_at": "2026-02-03T15:00:05+00:00",
        }).encode()

        mock_container_client.get_blob_client.return_value = self._make_blob_client(
            download_result=blob_data
        )

        result = await store.load("verisk")
        assert result is not None
        assert result.last_ingestion_time == "2026-02-03T15:00:00+00:00"
        assert result.last_trace_id == "trace-1"
        mock_container_client.get_blob_client.assert_called_once_with("poller_verisk.json")

    async def test_load_returns_none_on_not_found(self, store, mock_container_client):
        class ResourceNotFoundError(Exception):
            pass

        mock_container_client.get_blob_client.return_value = self._make_blob_client(
            download_error=ResourceNotFoundError()
        )

        result = await store.load("missing")
        assert result is None

    async def test_load_returns_none_on_error(self, store, mock_container_client):
        mock_container_client.get_blob_client.return_value = self._make_blob_client(
            download_error=ConnectionError("network")
        )

        result = await store.load("broken")
        assert result is None

    async def test_save_uploads_blob(self, store, mock_container_client):
        checkpoint = PollerCheckpointData(
            last_ingestion_time="2026-02-03T15:00:00+00:00",
            last_trace_id="trace-1",
            updated_at="2026-02-03T15:00:05+00:00",
        )

        blob_client = self._make_blob_client()
        mock_container_client.get_blob_client.return_value = blob_client

        await store.save("verisk", checkpoint)

        mock_container_client.get_blob_client.assert_called_once_with("poller_verisk.json")
        blob_client.upload_blob.assert_called_once()
        call_args = blob_client.upload_blob.call_args
        uploaded_content = json.loads(call_args[0][0])
        assert uploaded_content["last_ingestion_time"] == "2026-02-03T15:00:00+00:00"
        assert call_args[1]["overwrite"] is True

    async def test_save_raises_on_error(self, store, mock_container_client):
        checkpoint = PollerCheckpointData("t", "id", "u")
        mock_container_client.get_blob_client.return_value = self._make_blob_client(
            upload_error=ConnectionError("fail")
        )

        with pytest.raises(ConnectionError):
            await store.save("verisk", checkpoint)

    async def test_close_closes_container_client(self, store, mock_container_client):
        await store.close()
        mock_container_client.close.assert_called_once()


# =============================================================================
# Factory: get_poller_checkpoint_store
# =============================================================================


class TestFactory:
    def setup_method(self):
        reset_poller_checkpoint_store()

    def teardown_method(self):
        reset_poller_checkpoint_store()

    async def test_json_type_returns_json_store(self, tmp_path):
        from pipeline.common.eventhouse.json_poller_checkpoint_store import (
            JsonPollerCheckpointStore,
        )

        config = {
            "type": "json",
            "blob_storage_connection_string": "",
            "container_name": "poller-checkpoints",
            "storage_path": str(tmp_path),
        }
        with patch(
            "pipeline.common.eventhouse.poller_checkpoint_store._load_poller_checkpoint_config",
            return_value=config,
        ):
            store = await get_poller_checkpoint_store()
            assert isinstance(store, JsonPollerCheckpointStore)

    async def test_blob_type_without_connection_string_falls_back_to_json(self, tmp_path):
        from pipeline.common.eventhouse.json_poller_checkpoint_store import (
            JsonPollerCheckpointStore,
        )

        config = {
            "type": "blob",
            "blob_storage_connection_string": "",
            "container_name": "poller-checkpoints",
            "storage_path": str(tmp_path),
        }
        with patch(
            "pipeline.common.eventhouse.poller_checkpoint_store._load_poller_checkpoint_config",
            return_value=config,
        ):
            store = await get_poller_checkpoint_store()
            assert isinstance(store, JsonPollerCheckpointStore)

    async def test_unknown_type_raises_value_error(self):
        config = {
            "type": "s3",
            "blob_storage_connection_string": "",
            "container_name": "",
            "storage_path": "",
        }
        with patch(
            "pipeline.common.eventhouse.poller_checkpoint_store._load_poller_checkpoint_config",
            return_value=config,
        ):
            with pytest.raises(ValueError, match="Unknown poller checkpoint store type"):
                await get_poller_checkpoint_store()

    async def test_singleton_returns_same_instance(self, tmp_path):
        config = {
            "type": "json",
            "blob_storage_connection_string": "",
            "container_name": "",
            "storage_path": str(tmp_path),
        }
        with patch(
            "pipeline.common.eventhouse.poller_checkpoint_store._load_poller_checkpoint_config",
            return_value=config,
        ):
            store1 = await get_poller_checkpoint_store()
            store2 = await get_poller_checkpoint_store()
            assert store1 is store2

    async def test_reset_allows_new_instance(self, tmp_path):
        config = {
            "type": "json",
            "blob_storage_connection_string": "",
            "container_name": "",
            "storage_path": str(tmp_path),
        }
        with patch(
            "pipeline.common.eventhouse.poller_checkpoint_store._load_poller_checkpoint_config",
            return_value=config,
        ):
            store1 = await get_poller_checkpoint_store()
            reset_poller_checkpoint_store()
            store2 = await get_poller_checkpoint_store()
            assert store1 is not store2

    async def test_blob_type_with_connection_string_creates_blob_store(self):
        config = {
            "type": "blob",
            "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            "container_name": "poller-checkpoints",
            "storage_path": ".checkpoints",
        }

        mock_store = MagicMock()
        with patch(
            "pipeline.common.eventhouse.poller_checkpoint_store._load_poller_checkpoint_config",
            return_value=config,
        ), patch(
            "pipeline.common.eventhouse.blob_poller_checkpoint_store.BlobPollerCheckpointStore",
        ) as MockBlobStore:
            MockBlobStore.return_value = mock_store
            store = await get_poller_checkpoint_store()
            assert store is mock_store
            MockBlobStore.assert_called_once_with(
                connection_string=config["blob_storage_connection_string"],
                container_name="poller-checkpoints",
            )
