"""Tests for pipeline.common.eventhub.json_dedup_store module.

Covers JsonDedupStore: check_duplicate, mark_processed, cleanup_expired, and close.
Uses tmp_path for isolated filesystem operations.
"""

import json
import time

from pipeline.common.eventhub.json_dedup_store import JsonDedupStore

# =============================================================================
# JsonDedupStore - Init
# =============================================================================


class TestJsonDedupStoreInit:
    def test_creates_storage_directory(self, tmp_path):
        storage = tmp_path / "dedup-cache"
        store = JsonDedupStore(storage_path=str(storage))

        assert storage.exists()
        assert store.storage_path == storage

    def test_handles_existing_directory(self, tmp_path):
        storage = tmp_path / "dedup-cache"
        storage.mkdir()

        JsonDedupStore(storage_path=str(storage))
        assert storage.exists()


# =============================================================================
# JsonDedupStore - check_duplicate
# =============================================================================


class TestJsonDedupStoreCheckDuplicate:
    def _make_store(self, tmp_path):
        return JsonDedupStore(storage_path=str(tmp_path / "dedup"))

    async def test_returns_false_when_key_not_found(self, tmp_path):
        store = self._make_store(tmp_path)

        is_dup, metadata = await store.check_duplicate("worker", "nonexistent", 3600)

        assert is_dup is False
        assert metadata is None

    async def test_returns_true_for_valid_duplicate(self, tmp_path):
        store = self._make_store(tmp_path)

        # Write a valid entry
        worker_dir = store.storage_path / "worker"
        worker_dir.mkdir(parents=True)
        entry = {"event_id": "evt1", "timestamp": time.time() - 10}
        (worker_dir / "key1.json").write_text(json.dumps(entry))

        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)

        assert is_dup is True
        assert metadata["event_id"] == "evt1"

    async def test_returns_false_for_expired_entry_and_deletes_file(self, tmp_path):
        store = self._make_store(tmp_path)

        worker_dir = store.storage_path / "worker"
        worker_dir.mkdir(parents=True)
        entry = {"event_id": "evt1", "timestamp": time.time() - 7200}
        file_path = worker_dir / "key1.json"
        file_path.write_text(json.dumps(entry))

        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)

        assert is_dup is False
        assert metadata is None
        assert not file_path.exists()

    async def test_returns_false_on_corrupt_json(self, tmp_path):
        store = self._make_store(tmp_path)

        worker_dir = store.storage_path / "worker"
        worker_dir.mkdir(parents=True)
        (worker_dir / "bad.json").write_text("not valid json{{{")

        is_dup, metadata = await store.check_duplicate("worker", "bad", 3600)

        assert is_dup is False
        assert metadata is None

    async def test_returns_false_when_timestamp_zero(self, tmp_path):
        store = self._make_store(tmp_path)

        worker_dir = store.storage_path / "worker"
        worker_dir.mkdir(parents=True)
        entry = {"event_id": "evt1"}  # no timestamp -> defaults to 0
        (worker_dir / "key1.json").write_text(json.dumps(entry))

        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)

        # timestamp=0 means age = now - 0 which is way more than 3600
        assert is_dup is False

    async def test_exact_ttl_boundary(self, tmp_path):
        store = self._make_store(tmp_path)

        worker_dir = store.storage_path / "worker"
        worker_dir.mkdir(parents=True)
        # Entry exactly at TTL boundary (just barely valid)
        entry = {"event_id": "evt1", "timestamp": time.time() - 3599}
        (worker_dir / "key1.json").write_text(json.dumps(entry))

        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)

        assert is_dup is True


# =============================================================================
# JsonDedupStore - mark_processed
# =============================================================================


class TestJsonDedupStoreMarkProcessed:
    def _make_store(self, tmp_path):
        return JsonDedupStore(storage_path=str(tmp_path / "dedup"))

    async def test_writes_json_file(self, tmp_path):
        store = self._make_store(tmp_path)

        await store.mark_processed("worker", "key1", {"event_id": "evt1", "timestamp": 12345})

        file_path = store.storage_path / "worker" / "key1.json"
        assert file_path.exists()

        content = json.loads(file_path.read_text())
        assert content["event_id"] == "evt1"
        assert content["timestamp"] == 12345

    async def test_creates_worker_directory(self, tmp_path):
        store = self._make_store(tmp_path)

        await store.mark_processed("new-worker", "key1", {"event_id": "e1"})

        worker_dir = store.storage_path / "new-worker"
        assert worker_dir.exists()

    async def test_adds_timestamp_when_missing(self, tmp_path):
        store = self._make_store(tmp_path)

        before = time.time()
        await store.mark_processed("worker", "key1", {"event_id": "e1"})
        after = time.time()

        file_path = store.storage_path / "worker" / "key1.json"
        content = json.loads(file_path.read_text())
        assert before <= content["timestamp"] <= after

    async def test_overwrites_existing_file(self, tmp_path):
        store = self._make_store(tmp_path)

        await store.mark_processed("worker", "key1", {"event_id": "old", "timestamp": 100})
        await store.mark_processed("worker", "key1", {"event_id": "new", "timestamp": 200})

        file_path = store.storage_path / "worker" / "key1.json"
        content = json.loads(file_path.read_text())
        assert content["event_id"] == "new"

    async def test_roundtrip_mark_then_check(self, tmp_path):
        store = self._make_store(tmp_path)

        await store.mark_processed("worker", "key1", {"event_id": "e1", "timestamp": time.time()})

        is_dup, metadata = await store.check_duplicate("worker", "key1", 3600)
        assert is_dup is True
        assert metadata["event_id"] == "e1"


# =============================================================================
# JsonDedupStore - cleanup_expired
# =============================================================================


class TestJsonDedupStoreCleanupExpired:
    def _make_store(self, tmp_path):
        return JsonDedupStore(storage_path=str(tmp_path / "dedup"))

    async def test_returns_zero_when_worker_dir_does_not_exist(self, tmp_path):
        store = self._make_store(tmp_path)

        removed = await store.cleanup_expired("nonexistent-worker", 3600)
        assert removed == 0

    async def test_removes_expired_entries(self, tmp_path):
        store = self._make_store(tmp_path)

        worker_dir = store.storage_path / "worker"
        worker_dir.mkdir(parents=True)

        # Expired entry
        expired = {"event_id": "old", "timestamp": time.time() - 7200}
        (worker_dir / "expired.json").write_text(json.dumps(expired))

        # Valid entry
        valid = {"event_id": "new", "timestamp": time.time() - 10}
        (worker_dir / "valid.json").write_text(json.dumps(valid))

        removed = await store.cleanup_expired("worker", 3600)

        assert removed == 1
        assert not (worker_dir / "expired.json").exists()
        assert (worker_dir / "valid.json").exists()

    async def test_removes_all_expired(self, tmp_path):
        store = self._make_store(tmp_path)

        worker_dir = store.storage_path / "worker"
        worker_dir.mkdir(parents=True)

        for i in range(5):
            entry = {"event_id": f"e{i}", "timestamp": time.time() - 7200}
            (worker_dir / f"key{i}.json").write_text(json.dumps(entry))

        removed = await store.cleanup_expired("worker", 3600)
        assert removed == 5

    async def test_keeps_all_valid(self, tmp_path):
        store = self._make_store(tmp_path)

        worker_dir = store.storage_path / "worker"
        worker_dir.mkdir(parents=True)

        for i in range(3):
            entry = {"event_id": f"e{i}", "timestamp": time.time() - 10}
            (worker_dir / f"key{i}.json").write_text(json.dumps(entry))

        removed = await store.cleanup_expired("worker", 3600)
        assert removed == 0

    async def test_handles_corrupt_file_during_cleanup(self, tmp_path):
        store = self._make_store(tmp_path)

        worker_dir = store.storage_path / "worker"
        worker_dir.mkdir(parents=True)

        # Corrupt file
        (worker_dir / "corrupt.json").write_text("not json")

        # Valid expired file
        expired = {"event_id": "e1", "timestamp": time.time() - 7200}
        (worker_dir / "expired.json").write_text(json.dumps(expired))

        # Should continue past corrupt file and still remove expired
        removed = await store.cleanup_expired("worker", 3600)
        assert removed == 1
        assert not (worker_dir / "expired.json").exists()

    async def test_ignores_non_json_files(self, tmp_path):
        store = self._make_store(tmp_path)

        worker_dir = store.storage_path / "worker"
        worker_dir.mkdir(parents=True)

        # Non-JSON file should be ignored by glob
        (worker_dir / "readme.txt").write_text("not a dedup entry")

        removed = await store.cleanup_expired("worker", 3600)
        assert removed == 0
        assert (worker_dir / "readme.txt").exists()


# =============================================================================
# JsonDedupStore - close
# =============================================================================


class TestJsonDedupStoreClose:
    async def test_close_is_noop(self, tmp_path):
        store = JsonDedupStore(storage_path=str(tmp_path / "dedup"))

        # Should not raise
        await store.close()
