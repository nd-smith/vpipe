"""Local filesystem JSON checkpoint store for Azure Event Hub.

Implements the CheckpointStore protocol using local JSON files for
development and testing environments where Azure Blob Storage is
not yet configured.

Architecture:
- One JSON file per data type (ownership, checkpoints) per consumer group
- Atomic writes via write-to-temp + os.replace() for crash safety
- Optimistic concurrency via etag field (UUID4) on ownership records
- asyncio.Lock per file path for concurrent access within a single process

File structure:
    <storage_path>/
      <sanitized_namespace>/
        <eventhub_name>/
          <consumer_group>/
            ownership.json
            checkpoints.json

Limitations:
- Single-process concurrency only (no cross-process file locking)
- Not suitable for multi-instance horizontal scaling of the same worker
- Works correctly for single-instance-per-worker deployments with many
  different workers running concurrently (each writes to separate files)
- Designed as a development bridge until Azure Blob Storage is configured
"""

import asyncio
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)


class JsonCheckpointStore:
    """Checkpoint store backed by local JSON files.

    Implements the four async methods required by the Azure Event Hub SDK's
    CheckpointStore protocol: list_ownership, claim_ownership,
    update_checkpoint, and list_checkpoints.
    """

    def __init__(self, storage_path: str | Path) -> None:
        """Initialize the JSON checkpoint store.

        Args:
            storage_path: Base directory for storing checkpoint JSON files.
                Directories are created automatically on first write.
        """
        self._base_path = Path(storage_path)
        self._locks: dict[str, asyncio.Lock] = {}

        logger.info(
            "JsonCheckpointStore initialized",
            extra={"storage_path": str(self._base_path)},
        )

    # =========================================================================
    # Protocol methods
    # =========================================================================

    async def list_ownership(
        self,
        fully_qualified_namespace: str,
        eventhub_name: str,
        consumer_group: str,
        **kwargs: Any,
    ) -> Iterable[dict[str, Any]]:
        """List all partition ownership records.

        Returns:
            List of ownership dicts, one per claimed partition.
        """
        file_path = self._ownership_path(
            fully_qualified_namespace, eventhub_name, consumer_group
        )
        lock = self._get_lock(str(file_path))
        async with lock:
            data = self._read_json(file_path)
        return list(data["partitions"].values())

    async def claim_ownership(
        self,
        ownership_list: Iterable[dict[str, Any]],
        **kwargs: Any,
    ) -> Iterable[dict[str, Any]]:
        """Claim ownership of partitions using optimistic concurrency.

        For each ownership record in the input:
        - If no existing owner: claim succeeds unconditionally
        - If existing owner with matching etag: claim succeeds (reclaim)
        - If existing owner with different etag: claim is rejected

        Returns:
            List of ownership dicts that were successfully claimed.
        """
        ownership_items = list(ownership_list)
        if not ownership_items:
            return []

        # All items in a single call share the same namespace/eventhub/group
        first = ownership_items[0]
        ns = first["fully_qualified_namespace"]
        eh = first["eventhub_name"]
        cg = first["consumer_group"]

        file_path = self._ownership_path(ns, eh, cg)
        lock = self._get_lock(str(file_path))
        claimed: list[dict[str, Any]] = []

        async with lock:
            data = self._read_json(file_path)

            for ownership in ownership_items:
                partition_id = ownership["partition_id"]
                existing = data["partitions"].get(partition_id)

                # Claim succeeds if no existing owner or etag matches
                incoming_etag = ownership.get("etag")
                if existing is None or existing.get("etag") == incoming_etag:
                    claimed_record = {
                        "fully_qualified_namespace": ns,
                        "eventhub_name": eh,
                        "consumer_group": cg,
                        "partition_id": partition_id,
                        "owner_id": ownership["owner_id"],
                        "last_modified_time": time.time(),
                        "etag": str(uuid.uuid4()),
                    }
                    data["partitions"][partition_id] = claimed_record
                    claimed.append(claimed_record)
                else:
                    logger.debug(
                        f"Ownership claim rejected for partition {partition_id}: "
                        f"etag mismatch (stored={existing.get('etag')}, "
                        f"provided={incoming_etag})"
                    )

            self._write_json(file_path, data)

        return claimed

    async def update_checkpoint(
        self,
        checkpoint: dict[str, Any],
        **kwargs: Any,
    ) -> None:
        """Persist a checkpoint record for a partition.

        Args:
            checkpoint: Dict with keys: fully_qualified_namespace, eventhub_name,
                consumer_group, partition_id, offset, sequence_number.
        """
        ns = checkpoint["fully_qualified_namespace"]
        eh = checkpoint["eventhub_name"]
        cg = checkpoint["consumer_group"]
        partition_id = checkpoint["partition_id"]

        file_path = self._checkpoint_path(ns, eh, cg)
        lock = self._get_lock(str(file_path))

        async with lock:
            data = self._read_json(file_path)
            data["partitions"][partition_id] = {
                "fully_qualified_namespace": ns,
                "eventhub_name": eh,
                "consumer_group": cg,
                "partition_id": partition_id,
                "offset": checkpoint.get("offset"),
                "sequence_number": checkpoint.get("sequence_number"),
            }
            self._write_json(file_path, data)

    async def list_checkpoints(
        self,
        fully_qualified_namespace: str,
        eventhub_name: str,
        consumer_group: str,
        **kwargs: Any,
    ) -> Iterable[dict[str, Any]]:
        """List all checkpoint records.

        Returns:
            List of checkpoint dicts, one per partition.
        """
        file_path = self._checkpoint_path(
            fully_qualified_namespace, eventhub_name, consumer_group
        )
        lock = self._get_lock(str(file_path))
        async with lock:
            data = self._read_json(file_path)
        return list(data["partitions"].values())

    async def close(self) -> None:
        """No-op close for protocol compatibility."""
        logger.debug("JsonCheckpointStore closed (no-op)")

    # =========================================================================
    # Internal helpers
    # =========================================================================

    def _get_lock(self, file_path: str) -> asyncio.Lock:
        """Get or create an asyncio.Lock for the given file path."""
        if file_path not in self._locks:
            self._locks[file_path] = asyncio.Lock()
        return self._locks[file_path]

    @staticmethod
    def _sanitize_name(name: str) -> str:
        """Sanitize a name for filesystem path safety."""
        return name.replace(".", "_").replace("/", "_").replace(":", "_")

    def _get_dir(
        self, namespace: str, eventhub_name: str, consumer_group: str
    ) -> Path:
        """Get directory path for a namespace/eventhub/group triple."""
        return (
            self._base_path
            / self._sanitize_name(namespace)
            / self._sanitize_name(eventhub_name)
            / self._sanitize_name(consumer_group)
        )

    def _ownership_path(
        self, namespace: str, eventhub_name: str, consumer_group: str
    ) -> Path:
        return self._get_dir(namespace, eventhub_name, consumer_group) / "ownership.json"

    def _checkpoint_path(
        self, namespace: str, eventhub_name: str, consumer_group: str
    ) -> Path:
        return (
            self._get_dir(namespace, eventhub_name, consumer_group) / "checkpoints.json"
        )

    def _read_json(self, file_path: Path) -> dict[str, Any]:
        """Read a JSON file, returning empty structure if missing or corrupt."""
        if not file_path.exists():
            return {"partitions": {}}
        try:
            with open(file_path) as f:
                data = json.load(f)
            if not isinstance(data, dict) or "partitions" not in data:
                logger.warning(f"Malformed JSON in {file_path}, resetting")
                return {"partitions": {}}
            return data
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to read {file_path}: {e}, resetting")
            return {"partitions": {}}

    def _write_json(self, file_path: Path, data: dict[str, Any]) -> None:
        """Atomic write: write to temp file then os.replace().

        On Windows, os.replace() can fail with PermissionError when another
        process (antivirus, search indexer, etc.) briefly locks the target
        file. Retries with short delays handle this transient condition.
        """
        file_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = file_path.with_suffix(".json.tmp")
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

        max_retries = 5
        for attempt in range(max_retries):
            try:
                os.replace(str(tmp_path), str(file_path))
                return
            except PermissionError:
                if attempt < max_retries - 1:
                    delay = 0.05 * (2 ** attempt)  # 50ms, 100ms, 200ms, 400ms
                    logger.debug(
                        f"os.replace failed for {file_path.name} "
                        f"(attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {delay:.0f}ms"
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"os.replace failed for {file_path.name} "
                        f"after {max_retries} attempts, raising"
                    )
                    raise


__all__ = [
    "JsonCheckpointStore",
]
