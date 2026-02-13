"""Azure Blob Storage backend for deduplication store.

Stores dedup keys as individual blobs in Azure Blob Storage.
Each blob contains JSON with event metadata and timestamp.

Storage structure:
    container/worker-name/key.json -> {"event_id": "...", "timestamp": 1234567890}

Example:
    eventhub-dedup-cache/verisk-event-ingester/trace_abc123.json
"""

import asyncio
import json
import logging
import time
from typing import Any

from azure.storage.blob.aio import BlobServiceClient, ContainerClient

from core.security.ssl_utils import get_ca_bundle_kwargs

logger = logging.getLogger(__name__)


class BlobDedupStore:
    """Azure Blob Storage implementation of dedup store."""

    def __init__(self, connection_string: str, container_name: str):
        self.connection_string = connection_string
        self.container_name = container_name
        self._client: BlobServiceClient | None = None
        self._container: ContainerClient | None = None

    async def initialize(self) -> None:
        """Initialize blob client."""
        self._client = BlobServiceClient.from_connection_string(
            self.connection_string,
            **get_ca_bundle_kwargs(),
        )
        self._container = self._client.get_container_client(self.container_name)

        logger.info(
            "BlobDedupStore client initialized",
            extra={"container_name": self.container_name},
        )

    async def check_duplicate(
        self,
        worker_name: str,
        key: str,
        ttl_seconds: int,
    ) -> tuple[bool, dict[str, Any] | None]:
        """Check if key was processed recently in blob storage.

        Args:
            worker_name: Worker identifier (e.g., "verisk-event-ingester")
            key: Dedup key (trace_id or event_id)
            ttl_seconds: Time-to-live in seconds

        Returns:
            (is_duplicate, metadata) where metadata is the stored data if found
        """
        if not self._container:
            return False, None

        blob_name = f"{worker_name}/{key}.json"
        blob_client = self._container.get_blob_client(blob_name)

        try:
            # Download blob content
            download = await blob_client.download_blob()
            content = await download.readall()
            metadata = json.loads(content)

            # Check TTL
            stored_timestamp = metadata.get("timestamp", 0)
            now = time.time()
            age_seconds = now - stored_timestamp

            if age_seconds < ttl_seconds:
                # Still valid
                logger.debug(
                    "Found duplicate in blob storage",
                    extra={
                        "worker": worker_name,
                        "key": key,
                        "age_seconds": age_seconds,
                    },
                )
                return True, metadata
            else:
                # Expired — cleanup_expired() handles bulk deletion
                logger.debug(
                    "Found expired entry in blob storage",
                    extra={
                        "worker": worker_name,
                        "key": key,
                        "age_seconds": age_seconds,
                    },
                )
                return False, None

        except Exception as e:
            # Blob doesn't exist or other error
            if "BlobNotFound" in str(e):
                logger.debug(f"Key not found in blob storage: {worker_name}/{key}")
            else:
                logger.warning(
                    "Error checking blob storage for duplicate",
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
        """Mark key as processed by storing in blob storage.

        Args:
            worker_name: Worker identifier
            key: Dedup key (trace_id or event_id)
            metadata: Data to store (must include "timestamp")
        """
        if not self._container:
            return

        # Ensure timestamp is present
        if "timestamp" not in metadata:
            metadata["timestamp"] = time.time()

        blob_name = f"{worker_name}/{key}.json"
        blob_client = self._container.get_blob_client(blob_name)

        try:
            content = json.dumps(metadata)
            await blob_client.upload_blob(
                content,
                overwrite=True,
                content_type="application/json",
            )
            logger.debug(
                "Marked key as processed in blob storage",
                extra={"worker": worker_name, "key": key},
            )

        except Exception as e:
            logger.warning(
                "Error marking key as processed in blob storage",
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
        if not self._container:
            return 0

        now = time.time()
        removed_count = 0
        prefix = f"{worker_name}/"

        try:
            # List all blobs for this worker
            async for blob in self._container.list_blobs(name_starts_with=prefix):
                try:
                    blob_client = self._container.get_blob_client(blob.name)

                    # Download and check timestamp
                    download = await blob_client.download_blob()
                    content = await download.readall()
                    metadata = json.loads(content)

                    stored_timestamp = metadata.get("timestamp", 0)
                    age_seconds = now - stored_timestamp

                    if age_seconds >= ttl_seconds:
                        # Expired - delete it
                        await blob_client.delete_blob()
                        removed_count += 1
                        logger.debug(
                            "Removed expired blob",
                            extra={
                                "worker": worker_name,
                                "blob": blob.name,
                                "age_seconds": age_seconds,
                            },
                        )

                except Exception as e:
                    logger.warning(
                        "Error cleaning up blob",
                        extra={"worker": worker_name, "blob": blob.name, "error": str(e)},
                        exc_info=False,
                    )
                    continue

            if removed_count > 0:
                logger.info(
                    "Cleaned up expired dedup entries",
                    extra={"worker": worker_name, "removed_count": removed_count},
                )

        except Exception as e:
            logger.error(
                "Error during cleanup_expired",
                extra={"worker": worker_name, "error": str(e)},
                exc_info=True,
            )

        return removed_count

    async def close(self) -> None:
        """Close blob client connection."""
        if self._client:
            await self._client.close()
            self._client = None
            self._container = None
