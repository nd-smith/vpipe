"""Tests for pipeline.common.eventhub.blob_dedup_store module.

Covers BlobDedupStore: initialize, check_duplicate, mark_processed,
cleanup_expired, and close.
"""

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# =============================================================================
# BlobDedupStore - Initialize
# =============================================================================


class TestBlobDedupStoreInitialize:
    @patch("pipeline.common.eventhub.blob_dedup_store.BlobServiceClient")
    async def test_initialize_creates_container(self, MockBlobService):
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        mock_client = MagicMock()
        mock_container = MagicMock()
        MockBlobService.from_connection_string.return_value = mock_client
        mock_client.get_container_client.return_value = mock_container

        store = BlobDedupStore(connection_string="conn", container_name="dedup-cache")
        await store.initialize()

        assert store._client is mock_client
        assert store._container is mock_container
        mock_client.get_container_client.assert_called_once_with("dedup-cache")

    @patch("pipeline.common.eventhub.blob_dedup_store.BlobServiceClient")
    async def test_initialize_handles_container_already_exists(self, MockBlobService):
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.create_container = AsyncMock(side_effect=Exception("ContainerAlreadyExists"))
        MockBlobService.from_connection_string.return_value = mock_client
        mock_client.get_container_client.return_value = mock_container

        store = BlobDedupStore(connection_string="conn", container_name="dedup-cache")

        # Should not raise
        await store.initialize()

    @patch("pipeline.common.eventhub.blob_dedup_store.BlobServiceClient")
    async def test_initialize_raises_on_other_container_error(self, MockBlobService):
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        mock_client = MagicMock()
        MockBlobService.from_connection_string.return_value = mock_client
        mock_client.get_container_client.side_effect = Exception("PermissionDenied")

        store = BlobDedupStore(connection_string="conn", container_name="dedup-cache")

        # Should raise so caller can fall back to memory-only
        with pytest.raises(Exception, match="PermissionDenied"):
            await store.initialize()


# =============================================================================
# BlobDedupStore - check_duplicate
# =============================================================================


def _make_container_mock():
    """Create a MagicMock container where get_blob_client is sync but blob operations are async."""
    container = MagicMock()
    return container


class TestBlobDedupStoreCheckDuplicate:
    def _make_store(self):
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        store = BlobDedupStore(connection_string="conn", container_name="dedup")
        store._container = _make_container_mock()
        return store

    async def test_returns_false_when_container_not_initialized(self):
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        store = BlobDedupStore(connection_string="conn", container_name="dedup")
        store._container = None

        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)

        assert is_dup is False
        assert metadata is None

    async def test_returns_true_for_valid_duplicate(self):
        store = self._make_store()

        stored_data = {"event_id": "evt1", "timestamp": time.time() - 10}
        mock_blob_client = MagicMock()
        mock_download = MagicMock()
        mock_download.readall = AsyncMock(return_value=json.dumps(stored_data).encode())
        mock_blob_client.download_blob = AsyncMock(return_value=mock_download)
        store._container.get_blob_client.return_value = mock_blob_client

        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)

        assert is_dup is True
        assert metadata["event_id"] == "evt1"

    async def test_returns_false_for_expired_entry(self):
        store = self._make_store()

        stored_data = {"event_id": "evt1", "timestamp": time.time() - 7200}
        mock_blob_client = MagicMock()
        mock_download = MagicMock()
        mock_download.readall = AsyncMock(return_value=json.dumps(stored_data).encode())
        mock_blob_client.download_blob = AsyncMock(return_value=mock_download)
        mock_blob_client.delete_blob = AsyncMock()
        store._container.get_blob_client.return_value = mock_blob_client

        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)

        assert is_dup is False
        assert metadata is None

    async def test_returns_false_when_blob_not_found(self):
        store = self._make_store()

        mock_blob_client = MagicMock()
        mock_blob_client.download_blob = AsyncMock(side_effect=Exception("BlobNotFound"))
        store._container.get_blob_client.return_value = mock_blob_client

        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)

        assert is_dup is False
        assert metadata is None

    async def test_returns_false_on_generic_error(self):
        store = self._make_store()

        mock_blob_client = MagicMock()
        mock_blob_client.download_blob = AsyncMock(side_effect=RuntimeError("network error"))
        store._container.get_blob_client.return_value = mock_blob_client

        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)

        assert is_dup is False
        assert metadata is None

    async def test_uses_correct_blob_path(self):
        store = self._make_store()

        mock_blob_client = MagicMock()
        mock_blob_client.download_blob = AsyncMock(side_effect=Exception("BlobNotFound"))
        store._container.get_blob_client.return_value = mock_blob_client

        await store.check_duplicate("my-worker", "trace-abc", 3600)

        store._container.get_blob_client.assert_called_once_with("my-worker/trace-abc.json")

    async def test_returns_false_when_timestamp_missing(self):
        store = self._make_store()

        stored_data = {"event_id": "evt1"}  # no timestamp
        mock_blob_client = MagicMock()
        mock_download = MagicMock()
        mock_download.readall = AsyncMock(return_value=json.dumps(stored_data).encode())
        mock_blob_client.download_blob = AsyncMock(return_value=mock_download)
        mock_blob_client.delete_blob = AsyncMock()
        store._container.get_blob_client.return_value = mock_blob_client

        # timestamp defaults to 0, so age = now - 0 which is > any reasonable TTL
        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)

        assert is_dup is False


# =============================================================================
# BlobDedupStore - mark_processed
# =============================================================================


class TestBlobDedupStoreMarkProcessed:
    def _make_store(self):
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        store = BlobDedupStore(connection_string="conn", container_name="dedup")
        store._container = _make_container_mock()
        return store

    async def test_returns_early_when_container_not_initialized(self):
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        store = BlobDedupStore(connection_string="conn", container_name="dedup")
        store._container = None

        # Should not raise
        await store.mark_processed("worker", "key1", {"event_id": "e1"})

    async def test_uploads_blob_with_metadata(self):
        store = self._make_store()

        mock_blob_client = MagicMock()
        mock_blob_client.upload_blob = AsyncMock()
        store._container.get_blob_client.return_value = mock_blob_client

        metadata = {"event_id": "evt1", "timestamp": 12345}
        await store.mark_processed("worker", "key1", metadata)

        mock_blob_client.upload_blob.assert_awaited_once()
        call_args = mock_blob_client.upload_blob.call_args
        content = json.loads(call_args[0][0])
        assert content["event_id"] == "evt1"
        assert content["timestamp"] == 12345
        assert call_args[1]["overwrite"] is True

    async def test_adds_timestamp_when_missing(self):
        store = self._make_store()

        mock_blob_client = MagicMock()
        mock_blob_client.upload_blob = AsyncMock()
        store._container.get_blob_client.return_value = mock_blob_client

        before = time.time()
        await store.mark_processed("worker", "key1", {"event_id": "e1"})
        after = time.time()

        call_args = mock_blob_client.upload_blob.call_args
        content = json.loads(call_args[0][0])
        assert before <= content["timestamp"] <= after

    async def test_uses_correct_blob_path(self):
        store = self._make_store()

        mock_blob_client = MagicMock()
        mock_blob_client.upload_blob = AsyncMock()
        store._container.get_blob_client.return_value = mock_blob_client

        await store.mark_processed("my-worker", "trace-xyz", {"event_id": "e1"})

        store._container.get_blob_client.assert_called_once_with("my-worker/trace-xyz.json")

    async def test_handles_upload_error(self):
        store = self._make_store()

        mock_blob_client = MagicMock()
        mock_blob_client.upload_blob = AsyncMock(side_effect=RuntimeError("upload fail"))
        store._container.get_blob_client.return_value = mock_blob_client

        # Should not raise (logs warning)
        await store.mark_processed("worker", "key1", {"event_id": "e1"})


# =============================================================================
# BlobDedupStore - cleanup_expired
# =============================================================================


class TestBlobDedupStoreCleanupExpired:
    def _make_store(self):
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        store = BlobDedupStore(connection_string="conn", container_name="dedup")
        store._container = _make_container_mock()
        return store

    async def test_returns_zero_when_container_not_initialized(self):
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        store = BlobDedupStore(connection_string="conn", container_name="dedup")
        store._container = None

        result = await store.cleanup_expired("worker", 3600)
        assert result == 0

    async def test_removes_expired_blobs(self):
        store = self._make_store()

        expired_data = json.dumps({"timestamp": time.time() - 7200}).encode()
        valid_data = json.dumps({"timestamp": time.time() - 10}).encode()

        expired_blob = MagicMock()
        expired_blob.name = "worker/expired-key.json"

        valid_blob = MagicMock()
        valid_blob.name = "worker/valid-key.json"

        # Mock list_blobs as async iterator
        async def mock_list_blobs(name_starts_with=None):
            yield expired_blob
            yield valid_blob

        store._container.list_blobs = mock_list_blobs

        # Set up blob clients
        expired_client = MagicMock()
        expired_download = MagicMock()
        expired_download.readall = AsyncMock(return_value=expired_data)
        expired_client.download_blob = AsyncMock(return_value=expired_download)
        expired_client.delete_blob = AsyncMock()

        valid_client = MagicMock()
        valid_download = MagicMock()
        valid_download.readall = AsyncMock(return_value=valid_data)
        valid_client.download_blob = AsyncMock(return_value=valid_download)
        valid_client.delete_blob = AsyncMock()

        def get_client(name):
            if "expired" in name:
                return expired_client
            return valid_client

        store._container.get_blob_client = get_client

        removed = await store.cleanup_expired("worker", 3600)

        assert removed == 1
        expired_client.delete_blob.assert_awaited_once()
        valid_client.delete_blob.assert_not_awaited()

    async def test_continues_on_individual_blob_error(self):
        store = self._make_store()

        blob1 = MagicMock()
        blob1.name = "worker/key1.json"
        blob2 = MagicMock()
        blob2.name = "worker/key2.json"

        async def mock_list_blobs(name_starts_with=None):
            yield blob1
            yield blob2

        store._container.list_blobs = mock_list_blobs

        # First blob errors, second blob is expired
        error_client = MagicMock()
        error_client.download_blob = AsyncMock(side_effect=RuntimeError("fail"))

        expired_data = json.dumps({"timestamp": time.time() - 7200}).encode()
        ok_client = MagicMock()
        ok_download = MagicMock()
        ok_download.readall = AsyncMock(return_value=expired_data)
        ok_client.download_blob = AsyncMock(return_value=ok_download)
        ok_client.delete_blob = AsyncMock()

        def get_client(name):
            if "key1" in name:
                return error_client
            return ok_client

        store._container.get_blob_client = get_client

        removed = await store.cleanup_expired("worker", 3600)

        assert removed == 1

    async def test_handles_list_blobs_error(self):
        store = self._make_store()

        async def mock_list_blobs(name_starts_with=None):
            raise RuntimeError("list fail")
            yield  # Make it an async generator

        store._container.list_blobs = mock_list_blobs

        # Should not raise (logs error)
        removed = await store.cleanup_expired("worker", 3600)
        assert removed == 0


# =============================================================================
# BlobDedupStore - close
# =============================================================================


class TestBlobDedupStoreProductionReadiness:
    """Production readiness: check_duplicate and mark_processed fire-and-forget safety."""

    async def test_dedup_check_returns_false_when_blob_unavailable(self):
        """Blob read fails → returns not-duplicate (safe: allows processing, doesn't block)."""
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        store = BlobDedupStore(connection_string="conn", container_name="dedup")
        store._container = _make_container_mock()

        mock_blob_client = MagicMock()
        mock_blob_client.download_blob = AsyncMock(
            side_effect=ConnectionError("blob storage unreachable")
        )
        store._container.get_blob_client.return_value = mock_blob_client

        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)

        # Must return False (not duplicate) — processing should continue
        assert is_dup is False
        assert metadata is None

    async def test_dedup_mark_processed_failure_is_fire_and_forget(self):
        """Blob write fails → logged, no exception raised, processing continues."""
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        store = BlobDedupStore(connection_string="conn", container_name="dedup")
        store._container = _make_container_mock()

        mock_blob_client = MagicMock()
        mock_blob_client.upload_blob = AsyncMock(
            side_effect=RuntimeError("storage write timeout")
        )
        store._container.get_blob_client.return_value = mock_blob_client

        # Should NOT raise — fire and forget
        await store.mark_processed("worker", "key1", {"event_id": "e1"})


class TestBlobDedupStoreClose:
    async def test_close_closes_client(self):
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        store = BlobDedupStore(connection_string="conn", container_name="dedup")
        mock_client = AsyncMock()
        store._client = mock_client
        store._container = MagicMock()

        await store.close()

        mock_client.close.assert_awaited_once()
        assert store._client is None
        assert store._container is None

    async def test_close_safe_when_not_initialized(self):
        from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

        store = BlobDedupStore(connection_string="conn", container_name="dedup")

        # Should not raise
        await store.close()
