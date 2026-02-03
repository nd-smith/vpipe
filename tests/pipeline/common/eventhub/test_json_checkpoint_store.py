"""Tests for JsonCheckpointStore.

Unit tests for the local filesystem JSON checkpoint store implementation.
Uses tmp_path fixture for filesystem isolation - no external dependencies.
"""

import asyncio
import json
import os
from unittest.mock import patch

import pytest

from pipeline.common.eventhub.json_checkpoint_store import JsonCheckpointStore

# Test constants
NS = "my-ns.servicebus.windows.net"
EH = "verisk-events"
CG = "$Default"


class TestJsonCheckpointStoreOwnership:
    """Test partition ownership operations."""

    @pytest.fixture
    def store(self, tmp_path):
        return JsonCheckpointStore(storage_path=tmp_path)

    @pytest.mark.asyncio
    async def test_list_ownership_empty_when_no_file(self, store):
        result = await store.list_ownership(NS, EH, CG)
        assert list(result) == []

    @pytest.mark.asyncio
    async def test_claim_ownership_new_partition(self, store):
        ownership = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "owner_id": "worker-1",
        }
        claimed = await store.claim_ownership([ownership])
        claimed = list(claimed)

        assert len(claimed) == 1
        assert claimed[0]["partition_id"] == "0"
        assert claimed[0]["owner_id"] == "worker-1"
        assert "etag" in claimed[0]
        assert "last_modified_time" in claimed[0]

    @pytest.mark.asyncio
    async def test_claim_ownership_with_matching_etag(self, store):
        """Reclaiming with the correct etag should succeed."""
        ownership = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "owner_id": "worker-1",
        }
        claimed = list(await store.claim_ownership([ownership]))
        etag = claimed[0]["etag"]

        # Reclaim with the etag from the first claim
        reclaim = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "owner_id": "worker-1",
            "etag": etag,
        }
        reclaimed = list(await store.claim_ownership([reclaim]))

        assert len(reclaimed) == 1
        assert reclaimed[0]["partition_id"] == "0"
        # Etag should be different after reclaim
        assert reclaimed[0]["etag"] != etag

    @pytest.mark.asyncio
    async def test_claim_ownership_with_stale_etag(self, store):
        """Claiming with a stale etag should be rejected."""
        ownership = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "owner_id": "worker-1",
        }
        await store.claim_ownership([ownership])

        # Try to claim with a wrong etag
        stale_claim = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "owner_id": "worker-2",
            "etag": "stale-etag-value",
        }
        result = list(await store.claim_ownership([stale_claim]))

        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_claim_ownership_multiple_partitions(self, store):
        ownerships = [
            {
                "fully_qualified_namespace": NS,
                "eventhub_name": EH,
                "consumer_group": CG,
                "partition_id": str(i),
                "owner_id": "worker-1",
            }
            for i in range(4)
        ]
        claimed = list(await store.claim_ownership(ownerships))

        assert len(claimed) == 4
        partition_ids = {c["partition_id"] for c in claimed}
        assert partition_ids == {"0", "1", "2", "3"}

    @pytest.mark.asyncio
    async def test_list_ownership_after_claims(self, store):
        """Round-trip: claim then list."""
        ownerships = [
            {
                "fully_qualified_namespace": NS,
                "eventhub_name": EH,
                "consumer_group": CG,
                "partition_id": str(i),
                "owner_id": "worker-1",
            }
            for i in range(3)
        ]
        await store.claim_ownership(ownerships)

        listed = list(await store.list_ownership(NS, EH, CG))
        assert len(listed) == 3

    @pytest.mark.asyncio
    async def test_claim_ownership_generates_new_etag(self, store):
        ownership = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "owner_id": "worker-1",
        }
        claimed1 = list(await store.claim_ownership([ownership]))
        etag1 = claimed1[0]["etag"]

        # Reclaim
        ownership["etag"] = etag1
        claimed2 = list(await store.claim_ownership([ownership]))
        etag2 = claimed2[0]["etag"]

        assert etag1 != etag2

    @pytest.mark.asyncio
    async def test_claim_ownership_updates_last_modified_time(self, store):
        ownership = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "owner_id": "worker-1",
        }
        claimed = list(await store.claim_ownership([ownership]))

        assert isinstance(claimed[0]["last_modified_time"], float)
        assert claimed[0]["last_modified_time"] > 0


class TestJsonCheckpointStoreCheckpoints:
    """Test checkpoint read/write operations."""

    @pytest.fixture
    def store(self, tmp_path):
        return JsonCheckpointStore(storage_path=tmp_path)

    @pytest.mark.asyncio
    async def test_list_checkpoints_empty_when_no_file(self, store):
        result = await store.list_checkpoints(NS, EH, CG)
        assert list(result) == []

    @pytest.mark.asyncio
    async def test_update_checkpoint_then_list(self, store):
        checkpoint = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "offset": 1024,
            "sequence_number": 42,
        }
        await store.update_checkpoint(checkpoint)

        listed = list(await store.list_checkpoints(NS, EH, CG))
        assert len(listed) == 1
        assert listed[0]["partition_id"] == "0"
        assert listed[0]["offset"] == 1024
        assert listed[0]["sequence_number"] == 42

    @pytest.mark.asyncio
    async def test_update_checkpoint_overwrites_previous(self, store):
        checkpoint = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "offset": 100,
            "sequence_number": 10,
        }
        await store.update_checkpoint(checkpoint)

        # Update with new offset
        checkpoint["offset"] = 200
        checkpoint["sequence_number"] = 20
        await store.update_checkpoint(checkpoint)

        listed = list(await store.list_checkpoints(NS, EH, CG))
        assert len(listed) == 1
        assert listed[0]["offset"] == 200
        assert listed[0]["sequence_number"] == 20

    @pytest.mark.asyncio
    async def test_update_multiple_partitions(self, store):
        for i in range(4):
            checkpoint = {
                "fully_qualified_namespace": NS,
                "eventhub_name": EH,
                "consumer_group": CG,
                "partition_id": str(i),
                "offset": i * 100,
                "sequence_number": i * 10,
            }
            await store.update_checkpoint(checkpoint)

        listed = list(await store.list_checkpoints(NS, EH, CG))
        assert len(listed) == 4

        by_partition = {c["partition_id"]: c for c in listed}
        assert by_partition["2"]["offset"] == 200
        assert by_partition["3"]["sequence_number"] == 30


class TestJsonCheckpointStoreIsolation:
    """Test isolation between consumer groups and event hubs."""

    @pytest.fixture
    def store(self, tmp_path):
        return JsonCheckpointStore(storage_path=tmp_path)

    @pytest.mark.asyncio
    async def test_separate_consumer_groups_are_isolated(self, store):
        cp1 = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": "group-a",
            "partition_id": "0",
            "offset": 100,
            "sequence_number": 10,
        }
        cp2 = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": "group-b",
            "partition_id": "0",
            "offset": 200,
            "sequence_number": 20,
        }
        await store.update_checkpoint(cp1)
        await store.update_checkpoint(cp2)

        listed_a = list(await store.list_checkpoints(NS, EH, "group-a"))
        listed_b = list(await store.list_checkpoints(NS, EH, "group-b"))

        assert len(listed_a) == 1
        assert listed_a[0]["offset"] == 100
        assert len(listed_b) == 1
        assert listed_b[0]["offset"] == 200

    @pytest.mark.asyncio
    async def test_separate_eventhubs_are_isolated(self, store):
        cp1 = {
            "fully_qualified_namespace": NS,
            "eventhub_name": "events-a",
            "consumer_group": CG,
            "partition_id": "0",
            "offset": 100,
            "sequence_number": 10,
        }
        cp2 = {
            "fully_qualified_namespace": NS,
            "eventhub_name": "events-b",
            "consumer_group": CG,
            "partition_id": "0",
            "offset": 200,
            "sequence_number": 20,
        }
        await store.update_checkpoint(cp1)
        await store.update_checkpoint(cp2)

        listed_a = list(await store.list_checkpoints(NS, "events-a", CG))
        listed_b = list(await store.list_checkpoints(NS, "events-b", CG))

        assert len(listed_a) == 1
        assert listed_a[0]["offset"] == 100
        assert len(listed_b) == 1
        assert listed_b[0]["offset"] == 200


class TestJsonCheckpointStoreResilience:
    """Test error handling and edge cases."""

    @pytest.fixture
    def store(self, tmp_path):
        return JsonCheckpointStore(storage_path=tmp_path)

    @pytest.mark.asyncio
    async def test_corrupted_json_returns_empty(self, store, tmp_path):
        """Write bad JSON to the file, verify the store recovers."""
        dir_path = (
            tmp_path
            / store._sanitize_name(NS)
            / store._sanitize_name(EH)
            / store._sanitize_name(CG)
        )
        dir_path.mkdir(parents=True)
        (dir_path / "checkpoints.json").write_text("{ not valid json !!!")

        result = list(await store.list_checkpoints(NS, EH, CG))
        assert result == []

    @pytest.mark.asyncio
    async def test_missing_partitions_key_returns_empty(self, store, tmp_path):
        """Write JSON without partitions key, verify recovery."""
        dir_path = (
            tmp_path
            / store._sanitize_name(NS)
            / store._sanitize_name(EH)
            / store._sanitize_name(CG)
        )
        dir_path.mkdir(parents=True)
        (dir_path / "checkpoints.json").write_text("{}")

        result = list(await store.list_checkpoints(NS, EH, CG))
        assert result == []

    @pytest.mark.asyncio
    async def test_concurrent_claims_within_process(self, store):
        """Multiple concurrent claims should not lose data."""
        async def claim_partition(partition_id: str):
            ownership = {
                "fully_qualified_namespace": NS,
                "eventhub_name": EH,
                "consumer_group": CG,
                "partition_id": partition_id,
                "owner_id": "worker-1",
            }
            return await store.claim_ownership([ownership])

        results = await asyncio.gather(
            *[claim_partition(str(i)) for i in range(8)]
        )

        # All claims should succeed (different partitions)
        total_claimed = sum(len(list(r)) for r in results)
        assert total_claimed == 8

        # Verify all are listed
        listed = list(await store.list_ownership(NS, EH, CG))
        assert len(listed) == 8

    @pytest.mark.asyncio
    async def test_write_retries_on_permission_error(self, store):
        """os.replace PermissionError should be retried (Windows file locking)."""
        original_replace = os.replace
        call_count = 0

        def flaky_replace(src, dst):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise PermissionError("[WinError 5] Access is denied")
            return original_replace(src, dst)

        checkpoint = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "offset": 100,
            "sequence_number": 10,
        }

        with patch("pipeline.common.eventhub.json_checkpoint_store.os.replace", side_effect=flaky_replace):
            with patch("pipeline.common.eventhub.json_checkpoint_store.time.sleep"):
                await store.update_checkpoint(checkpoint)

        # Verify checkpoint was written successfully after retries
        listed = list(await store.list_checkpoints(NS, EH, CG))
        assert len(listed) == 1
        assert listed[0]["offset"] == 100
        assert call_count == 3  # 2 failures + 1 success

    @pytest.mark.asyncio
    async def test_write_raises_after_max_retries(self, store):
        """PermissionError should propagate after exhausting retries."""
        checkpoint = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "offset": 100,
            "sequence_number": 10,
        }

        with patch(
            "pipeline.common.eventhub.json_checkpoint_store.os.replace",
            side_effect=PermissionError("[WinError 5] Access is denied"),
        ):
            with patch("pipeline.common.eventhub.json_checkpoint_store.time.sleep"):
                with pytest.raises(PermissionError):
                    await store.update_checkpoint(checkpoint)

    @pytest.mark.asyncio
    async def test_close_is_noop(self, store):
        """close() should not raise."""
        await store.close()
        # Store should still be usable after close
        result = list(await store.list_checkpoints(NS, EH, CG))
        assert result == []


class TestJsonCheckpointStoreFileStructure:
    """Test file and directory layout."""

    @pytest.fixture
    def store(self, tmp_path):
        return JsonCheckpointStore(storage_path=tmp_path)

    @pytest.mark.asyncio
    async def test_directory_structure_matches_spec(self, store, tmp_path):
        checkpoint = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "offset": 100,
            "sequence_number": 10,
        }
        await store.update_checkpoint(checkpoint)

        expected_dir = (
            tmp_path
            / "my-ns_servicebus_windows_net"
            / "verisk-events"
            / "$Default"
        )
        assert expected_dir.exists()
        assert (expected_dir / "checkpoints.json").exists()

    def test_sanitize_name_replaces_dots(self, store):
        assert store._sanitize_name("my-ns.servicebus.windows.net") == "my-ns_servicebus_windows_net"

    def test_sanitize_name_replaces_slashes(self, store):
        assert store._sanitize_name("path/to/thing") == "path_to_thing"

    def test_sanitize_name_replaces_colons(self, store):
        assert store._sanitize_name("host:port") == "host_port"

    @pytest.mark.asyncio
    async def test_json_is_human_readable(self, store, tmp_path):
        """JSON files should be indented for readability."""
        checkpoint = {
            "fully_qualified_namespace": NS,
            "eventhub_name": EH,
            "consumer_group": CG,
            "partition_id": "0",
            "offset": 100,
            "sequence_number": 10,
        }
        await store.update_checkpoint(checkpoint)

        file_path = (
            tmp_path
            / store._sanitize_name(NS)
            / store._sanitize_name(EH)
            / store._sanitize_name(CG)
            / "checkpoints.json"
        )
        content = file_path.read_text()
        data = json.loads(content)

        # Verify indented (multi-line)
        assert "\n" in content
        # Verify structure
        assert "partitions" in data
        assert "0" in data["partitions"]
