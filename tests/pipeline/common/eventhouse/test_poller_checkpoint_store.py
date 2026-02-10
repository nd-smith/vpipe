"""Tests for poller checkpoint store implementations and factory."""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.common.eventhouse.poller_checkpoint_store import (
    BlobPollerCheckpointStore,
    JsonPollerCheckpointStore,
    PollerCheckpoint,
    create_poller_checkpoint_store,
)

# =============================================================================
# PollerCheckpoint tests
# =============================================================================


class TestPollerCheckpoint:
    def test_to_datetime_iso_with_timezone(self):
        cp = PollerCheckpoint(
            last_ingestion_time="2026-02-02T23:00:00+00:00",
            last_trace_id="t1",
            updated_at="",
        )
        dt = cp.to_datetime()
        assert dt == datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)
        assert dt.tzinfo is not None

    def test_to_datetime_z_suffix(self):
        cp = PollerCheckpoint(
            last_ingestion_time="2026-02-02T23:00:00Z",
            last_trace_id="t1",
            updated_at="",
        )
        dt = cp.to_datetime()
        assert dt == datetime(2026, 2, 2, 23, 0, 0, tzinfo=UTC)

    def test_to_datetime_naive_gets_utc(self):
        cp = PollerCheckpoint(
            last_ingestion_time="2026-02-02T23:00:00",
            last_trace_id="t1",
            updated_at="",
        )
        dt = cp.to_datetime()
        assert dt.tzinfo == UTC

    def test_to_dict(self):
        cp = PollerCheckpoint(
            last_ingestion_time="2026-02-02T23:00:00+00:00",
            last_trace_id="abc",
            updated_at="2026-02-02T23:01:00+00:00",
        )
        d = cp.to_dict()
        assert d == {
            "last_ingestion_time": "2026-02-02T23:00:00+00:00",
            "last_trace_id": "abc",
            "updated_at": "2026-02-02T23:01:00+00:00",
        }

    def test_from_dict(self):
        data = {
            "last_ingestion_time": "2026-02-02T23:00:00+00:00",
            "last_trace_id": "xyz",
            "updated_at": "2026-02-02T23:01:00+00:00",
        }
        cp = PollerCheckpoint.from_dict(data)
        assert cp.last_ingestion_time == "2026-02-02T23:00:00+00:00"
        assert cp.last_trace_id == "xyz"
        assert cp.updated_at == "2026-02-02T23:01:00+00:00"

    def test_from_dict_missing_updated_at(self):
        data = {
            "last_ingestion_time": "2026-02-02T23:00:00+00:00",
            "last_trace_id": "xyz",
        }
        cp = PollerCheckpoint.from_dict(data)
        assert cp.updated_at == ""


# =============================================================================
# JsonPollerCheckpointStore tests
# =============================================================================


class TestJsonPollerCheckpointStoreLoad:
    async def test_load_returns_none_when_file_missing(self, tmp_path):
        store = JsonPollerCheckpointStore(storage_path=tmp_path, domain="test")
        result = await store.load()
        assert result is None

    async def test_load_returns_checkpoint_from_file(self, tmp_path):
        cp_file = tmp_path / "poller_test.json"
        cp_file.write_text(
            json.dumps(
                {
                    "last_ingestion_time": "2026-02-02T23:00:00+00:00",
                    "last_trace_id": "abc",
                    "updated_at": "2026-02-02T23:01:00+00:00",
                }
            )
        )

        store = JsonPollerCheckpointStore(storage_path=tmp_path, domain="test")
        result = await store.load()

        assert result is not None
        assert result.last_ingestion_time == "2026-02-02T23:00:00+00:00"
        assert result.last_trace_id == "abc"

    async def test_load_returns_none_on_corrupt_json(self, tmp_path):
        cp_file = tmp_path / "poller_test.json"
        cp_file.write_text("{bad json")

        store = JsonPollerCheckpointStore(storage_path=tmp_path, domain="test")
        result = await store.load()
        assert result is None

    async def test_load_returns_none_on_missing_keys(self, tmp_path):
        cp_file = tmp_path / "poller_test.json"
        cp_file.write_text(json.dumps({"unrelated": "data"}))

        store = JsonPollerCheckpointStore(storage_path=tmp_path, domain="test")
        result = await store.load()
        assert result is None


class TestJsonPollerCheckpointStoreSave:
    async def test_save_writes_checkpoint_file(self, tmp_path):
        store = JsonPollerCheckpointStore(storage_path=tmp_path, domain="test")
        cp = PollerCheckpoint(
            last_ingestion_time="2026-02-02T23:00:00+00:00",
            last_trace_id="abc",
            updated_at="",
        )
        result = await store.save(cp)
        assert result is True

        cp_file = tmp_path / "poller_test.json"
        assert cp_file.exists()

        data = json.loads(cp_file.read_text())
        assert data["last_ingestion_time"] == "2026-02-02T23:00:00+00:00"
        assert data["last_trace_id"] == "abc"
        assert data["updated_at"] != ""  # Should be set by save()

    async def test_save_creates_directories(self, tmp_path):
        deep_path = tmp_path / "deep" / "nested"
        store = JsonPollerCheckpointStore(storage_path=deep_path, domain="test")
        cp = PollerCheckpoint(
            last_ingestion_time="2026-01-01T00:00:00+00:00",
            last_trace_id="",
            updated_at="",
        )
        result = await store.save(cp)
        assert result is True
        assert (deep_path / "poller_test.json").exists()

    async def test_save_overwrites_existing(self, tmp_path):
        store = JsonPollerCheckpointStore(storage_path=tmp_path, domain="test")
        cp1 = PollerCheckpoint(
            last_ingestion_time="2026-01-01T00:00:00+00:00",
            last_trace_id="first",
            updated_at="",
        )
        await store.save(cp1)

        cp2 = PollerCheckpoint(
            last_ingestion_time="2026-02-02T00:00:00+00:00",
            last_trace_id="second",
            updated_at="",
        )
        await store.save(cp2)

        data = json.loads((tmp_path / "poller_test.json").read_text())
        assert data["last_trace_id"] == "second"

    async def test_save_returns_false_on_os_error(self, tmp_path):
        store = JsonPollerCheckpointStore(storage_path="/nonexistent/readonly/path", domain="test")
        cp = PollerCheckpoint(
            last_ingestion_time="2026-01-01T00:00:00+00:00",
            last_trace_id="",
            updated_at="",
        )
        result = await store.save(cp)
        assert result is False

    async def test_roundtrip_save_and_load(self, tmp_path):
        store = JsonPollerCheckpointStore(storage_path=tmp_path, domain="test")
        cp = PollerCheckpoint(
            last_ingestion_time="2026-06-15T12:30:45+00:00",
            last_trace_id="trace_xyz",
            updated_at="",
        )
        await store.save(cp)
        loaded = await store.load()

        assert loaded is not None
        assert loaded.last_ingestion_time == "2026-06-15T12:30:45+00:00"
        assert loaded.last_trace_id == "trace_xyz"


class TestJsonPollerCheckpointStoreClose:
    async def test_close_is_noop(self, tmp_path):
        store = JsonPollerCheckpointStore(storage_path=tmp_path, domain="test")
        await store.close()  # Should not raise


# =============================================================================
# BlobPollerCheckpointStore tests
# =============================================================================


class TestBlobPollerCheckpointStoreInit:
    @patch("pipeline.common.eventhouse.poller_checkpoint_store.BlobServiceClient", create=True)
    def test_init_sets_blob_name(self, _mock_blob):
        # BlobServiceClient is imported lazily, so we need to mock the import
        with patch.dict("sys.modules", {"azure.storage.blob.aio": MagicMock()}):
            store = BlobPollerCheckpointStore(
                connection_string="connstr",
                container_name="checkpoints",
                domain="verisk",
            )
        assert store._blob_name == "poller_verisk.json"
        assert store._container_name == "checkpoints"


class TestBlobPollerCheckpointStoreEnsureClient:
    async def test_ensure_client_creates_resources(self):
        mock_blob_svc = MagicMock()
        mock_container = MagicMock()
        mock_container.create_container = AsyncMock()
        mock_blob_client = MagicMock()
        mock_container.get_blob_client.return_value = mock_blob_client
        mock_blob_svc.get_container_client.return_value = mock_container

        mock_bsc_cls = MagicMock()
        mock_bsc_cls.from_connection_string.return_value = mock_blob_svc

        with patch.dict(
            "sys.modules",
            {
                "azure.storage.blob.aio": MagicMock(BlobServiceClient=mock_bsc_cls),
                "azure.storage.blob": MagicMock(),
            },
        ):
            store = BlobPollerCheckpointStore(
                connection_string="connstr",
                container_name="cp",
                domain="test",
            )
            store._blob_client = None
            await store._ensure_client()

        assert store._blob_client is mock_blob_client

    async def test_ensure_client_is_idempotent(self):
        with patch.dict(
            "sys.modules",
            {
                "azure.storage.blob.aio": MagicMock(),
                "azure.storage.blob": MagicMock(),
            },
        ):
            store = BlobPollerCheckpointStore(
                connection_string="connstr",
                container_name="cp",
                domain="test",
            )
        mock_blob_client = MagicMock()
        store._blob_client = mock_blob_client

        await store._ensure_client()
        # Should not have been replaced
        assert store._blob_client is mock_blob_client


class TestBlobPollerCheckpointStoreLoad:
    async def test_load_returns_checkpoint(self):
        with patch.dict(
            "sys.modules",
            {
                "azure.storage.blob.aio": MagicMock(),
                "azure.storage.blob": MagicMock(),
            },
        ):
            store = BlobPollerCheckpointStore(
                connection_string="connstr",
                container_name="cp",
                domain="test",
            )

        data = json.dumps(
            {
                "last_ingestion_time": "2026-02-02T23:00:00+00:00",
                "last_trace_id": "abc",
                "updated_at": "2026-02-02T23:01:00+00:00",
            }
        ).encode("utf-8")

        mock_download = AsyncMock()
        mock_download.readall = AsyncMock(return_value=data)
        mock_blob_client = AsyncMock()
        mock_blob_client.download_blob = AsyncMock(return_value=mock_download)
        store._blob_client = mock_blob_client

        result = await store.load()
        assert result is not None
        assert result.last_ingestion_time == "2026-02-02T23:00:00+00:00"

    async def test_load_returns_none_on_error(self):
        with patch.dict(
            "sys.modules",
            {
                "azure.storage.blob.aio": MagicMock(),
                "azure.storage.blob": MagicMock(),
            },
        ):
            store = BlobPollerCheckpointStore(
                connection_string="connstr",
                container_name="cp",
                domain="test",
            )

        mock_blob_client = AsyncMock()
        mock_blob_client.download_blob = AsyncMock(side_effect=Exception("not found"))
        store._blob_client = mock_blob_client

        result = await store.load()
        assert result is None


class TestBlobPollerCheckpointStoreSave:
    async def test_save_uploads_blob(self):
        with patch.dict(
            "sys.modules",
            {
                "azure.storage.blob.aio": MagicMock(),
                "azure.storage.blob": MagicMock(),
            },
        ):
            store = BlobPollerCheckpointStore(
                connection_string="connstr",
                container_name="cp",
                domain="test",
            )

        mock_blob_client = AsyncMock()
        mock_blob_client.upload_blob = AsyncMock()
        store._blob_client = mock_blob_client

        cp = PollerCheckpoint(
            last_ingestion_time="2026-02-02T23:00:00+00:00",
            last_trace_id="abc",
            updated_at="",
        )
        result = await store.save(cp)
        assert result is True
        mock_blob_client.upload_blob.assert_awaited_once()

    async def test_save_returns_false_on_error(self):
        with patch.dict(
            "sys.modules",
            {
                "azure.storage.blob.aio": MagicMock(),
                "azure.storage.blob": MagicMock(),
            },
        ):
            store = BlobPollerCheckpointStore(
                connection_string="connstr",
                container_name="cp",
                domain="test",
            )

        mock_blob_client = AsyncMock()
        mock_blob_client.upload_blob = AsyncMock(side_effect=Exception("upload failed"))
        store._blob_client = mock_blob_client

        cp = PollerCheckpoint(
            last_ingestion_time="2026-01-01T00:00:00+00:00",
            last_trace_id="",
            updated_at="",
        )
        result = await store.save(cp)
        assert result is False


class TestBlobPollerCheckpointStoreClose:
    async def test_close_closes_service_client(self):
        with patch.dict(
            "sys.modules",
            {
                "azure.storage.blob.aio": MagicMock(),
                "azure.storage.blob": MagicMock(),
            },
        ):
            store = BlobPollerCheckpointStore(
                connection_string="connstr",
                container_name="cp",
                domain="test",
            )
        mock_svc = AsyncMock()
        store._blob_service_client = mock_svc

        await store.close()
        mock_svc.close.assert_awaited_once()

    async def test_close_noop_when_no_client(self):
        with patch.dict(
            "sys.modules",
            {
                "azure.storage.blob.aio": MagicMock(),
                "azure.storage.blob": MagicMock(),
            },
        ):
            store = BlobPollerCheckpointStore(
                connection_string="connstr",
                container_name="cp",
                domain="test",
            )
        store._blob_service_client = None
        await store.close()  # Should not raise


# =============================================================================
# Factory function tests
# =============================================================================


class TestCreatePollerCheckpointStore:
    async def test_creates_json_store_by_default(self):
        store = await create_poller_checkpoint_store(
            domain="test",
            store_type="json",
            storage_path="/tmp/checkpoints",
        )
        assert isinstance(store, JsonPollerCheckpointStore)

    async def test_creates_blob_store(self):
        with patch.dict(
            "sys.modules",
            {
                "azure.storage.blob.aio": MagicMock(),
                "azure.storage.blob": MagicMock(),
            },
        ):
            store = await create_poller_checkpoint_store(
                domain="test",
                store_type="blob",
                connection_string="DefaultEndpointsProtocol=https;AccountName=test",
                container_name="my-checkpoints",
            )
        assert isinstance(store, BlobPollerCheckpointStore)

    async def test_blob_store_falls_back_to_json_without_connection_string(self):
        store = await create_poller_checkpoint_store(
            domain="test",
            store_type="blob",
            connection_string=None,
        )
        assert isinstance(store, JsonPollerCheckpointStore)

    async def test_unknown_store_type_raises(self):
        with pytest.raises(ValueError, match="Unknown checkpoint store type"):
            await create_poller_checkpoint_store(
                domain="test",
                store_type="redis",
            )

    async def test_loads_from_config_when_store_type_is_none(self):
        fake_config = {
            "type": "json",
            "blob_storage_connection_string": "",
            "container_name": "cp",
            "storage_path": "/tmp/cp",
        }
        with patch(
            "pipeline.common.eventhouse.poller_checkpoint_store._load_poller_checkpoint_config",
            return_value=fake_config,
        ):
            store = await create_poller_checkpoint_store(domain="test")
        assert isinstance(store, JsonPollerCheckpointStore)

    async def test_blob_from_config(self):
        fake_config = {
            "type": "blob",
            "blob_storage_connection_string": "DefaultEndpointsProtocol=https;AccountName=test",
            "container_name": "my-cp",
            "storage_path": "/tmp/cp",
        }
        with (
            patch(
                "pipeline.common.eventhouse.poller_checkpoint_store._load_poller_checkpoint_config",
                return_value=fake_config,
            ),
            patch.dict(
                "sys.modules",
                {
                    "azure.storage.blob.aio": MagicMock(),
                    "azure.storage.blob": MagicMock(),
                },
            ),
        ):
            store = await create_poller_checkpoint_store(domain="test")
        assert isinstance(store, BlobPollerCheckpointStore)

    async def test_explicit_args_override_config(self):
        """When connection_string is passed explicitly, config value is ignored."""
        fake_config = {
            "type": "blob",
            "blob_storage_connection_string": "",
            "container_name": "default-cp",
            "storage_path": "/tmp/cp",
        }
        with (
            patch(
                "pipeline.common.eventhouse.poller_checkpoint_store._load_poller_checkpoint_config",
                return_value=fake_config,
            ),
            patch.dict(
                "sys.modules",
                {
                    "azure.storage.blob.aio": MagicMock(),
                    "azure.storage.blob": MagicMock(),
                },
            ),
        ):
            store = await create_poller_checkpoint_store(
                domain="test",
                connection_string="explicit-conn-str",
                container_name="explicit-cp",
            )
        assert isinstance(store, BlobPollerCheckpointStore)
        assert store._connection_string == "explicit-conn-str"
        assert store._container_name == "explicit-cp"
