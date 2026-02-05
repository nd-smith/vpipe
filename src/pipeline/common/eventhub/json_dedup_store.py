"""Local JSON file-based deduplication store for development.

Stores dedup keys as individual JSON files on the local filesystem.
Mirrors the blob storage structure for testing without Azure dependencies.

Storage structure:
    storage_path/worker-name/key.json -> {"event_id": "...", "timestamp": 1234567890}

Example:
    ./data/eventhub-dedup-cache/verisk-event-ingester/trace_abc123.json
"""

import json
import logging
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class JsonDedupStore:
    """Local filesystem JSON implementation of dedup store."""

    def __init__(self, storage_path: str):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized JSON dedup store at: {self.storage_path}")

    async def check_duplicate(
        self,
        worker_name: str,
        key: str,
        ttl_seconds: int,
    ) -> tuple[bool, dict[str, Any] | None]:
        """Check if key was processed recently in local storage.

        Args:
            worker_name: Worker identifier (e.g., "verisk-event-ingester")
            key: Dedup key (trace_id or event_id)
            ttl_seconds: Time-to-live in seconds

        Returns:
            (is_duplicate, metadata) where metadata is the stored data if found
        """
        file_path = self.storage_path / worker_name / f"{key}.json"

        if not file_path.exists():
            return False, None

        try:
            with open(file_path, "r") as f:
                metadata = json.load(f)

            # Check TTL
            stored_timestamp = metadata.get("timestamp", 0)
            now = time.time()
            age_seconds = now - stored_timestamp

            if age_seconds < ttl_seconds:
                # Still valid
                logger.debug(
                    f"Found duplicate in JSON store",
                    extra={
                        "worker": worker_name,
                        "key": key,
                        "age_seconds": age_seconds,
                    },
                )
                return True, metadata
            else:
                # Expired - delete it
                logger.debug(
                    f"Found expired entry in JSON store",
                    extra={
                        "worker": worker_name,
                        "key": key,
                        "age_seconds": age_seconds,
                    },
                )
                file_path.unlink()
                return False, None

        except Exception as e:
            logger.warning(
                f"Error checking JSON store for duplicate",
                extra={"worker": worker_name, "key": key, "error": str(e)},
                exc_info=False,
            )
            return False, None

    async def mark_processed(
        self,
        worker_name: str,
        key: str,
        metadata: dict[str, Any],
    ) -> None:
        """Mark key as processed by storing in local JSON file.

        Args:
            worker_name: Worker identifier
            key: Dedup key (trace_id or event_id)
            metadata: Data to store (must include "timestamp")
        """
        worker_dir = self.storage_path / worker_name
        worker_dir.mkdir(parents=True, exist_ok=True)

        # Ensure timestamp is present
        if "timestamp" not in metadata:
            metadata["timestamp"] = time.time()

        file_path = worker_dir / f"{key}.json"

        try:
            with open(file_path, "w") as f:
                json.dump(metadata, f)

            logger.debug(
                f"Marked key as processed in JSON store",
                extra={"worker": worker_name, "key": key},
            )

        except Exception as e:
            logger.warning(
                f"Error marking key as processed in JSON store",
                extra={"worker": worker_name, "key": key, "error": str(e)},
                exc_info=True,
            )

    async def cleanup_expired(
        self,
        worker_name: str,
        ttl_seconds: int,
    ) -> int:
        """Remove expired entries for a worker.

        Args:
            worker_name: Worker identifier
            ttl_seconds: Entries older than this are expired

        Returns:
            Number of entries removed
        """
        worker_dir = self.storage_path / worker_name

        if not worker_dir.exists():
            return 0

        now = time.time()
        removed_count = 0

        try:
            for file_path in worker_dir.glob("*.json"):
                try:
                    with open(file_path, "r") as f:
                        metadata = json.load(f)

                    stored_timestamp = metadata.get("timestamp", 0)
                    age_seconds = now - stored_timestamp

                    if age_seconds >= ttl_seconds:
                        # Expired - delete it
                        file_path.unlink()
                        removed_count += 1
                        logger.debug(
                            f"Removed expired file",
                            extra={
                                "worker": worker_name,
                                "file": file_path.name,
                                "age_seconds": age_seconds,
                            },
                        )

                except Exception as e:
                    logger.warning(
                        f"Error cleaning up file",
                        extra={
                            "worker": worker_name,
                            "file": file_path.name,
                            "error": str(e),
                        },
                        exc_info=False,
                    )
                    continue

            if removed_count > 0:
                logger.info(
                    f"Cleaned up expired dedup entries",
                    extra={"worker": worker_name, "removed_count": removed_count},
                )

        except Exception as e:
            logger.error(
                f"Error during cleanup_expired",
                extra={"worker": worker_name, "error": str(e)},
                exc_info=True,
            )

        return removed_count

    async def close(self) -> None:
        """No-op for JSON store (no connections to close)."""
        pass
