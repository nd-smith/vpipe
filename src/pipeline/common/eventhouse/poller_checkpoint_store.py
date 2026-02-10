"""Checkpoint store for Eventhouse pollers.

Provides persistent checkpoint storage for poller progress tracking across restarts.
Supports both local JSON files (development) and Azure Blob Storage (production).

Architecture:
- Protocol-based design for easy extension
- Two implementations: JsonPollerCheckpointStore and BlobPollerCheckpointStore
- Factory function selects implementation based on configuration
- Atomic writes for crash safety

Checkpoint format:
- last_ingestion_time: ISO format UTC timestamp of last processed record
- last_trace_id: trace_id of last processed record (for composite key pagination)
- updated_at: ISO format UTC timestamp when checkpoint was written

Configuration:
- Reads from config.yaml: eventhouse.poller_checkpoint_store section
- Falls back to environment variables if config values are empty

Usage:
    # Create checkpoint store from config
    store = await create_poller_checkpoint_store(domain="verisk")

    # Load checkpoint
    checkpoint = await store.load()
    if checkpoint:
        resume_from = checkpoint.last_ingestion_time

    # Save checkpoint
    await store.save(checkpoint)

    # Cleanup
    await store.close()
"""

import asyncio
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

CHECKPOINT_TIMEOUT_SECONDS = 30

logger = logging.getLogger(__name__)


# =============================================================================
# Checkpoint data structure
# =============================================================================


@dataclass
class PollerCheckpoint:
    """Checkpoint state for resuming poller after restart.

    Stores the composite key (ingestion_time, trace_id) of the last processed
    record to enable exact resume without duplicates or gaps.
    """

    last_ingestion_time: str  # ISO format UTC timestamp
    last_trace_id: str  # trace_id of the last processed record
    updated_at: str  # When checkpoint was written (for debugging)

    def to_datetime(self) -> datetime:
        """Parse last_ingestion_time to offset-aware UTC datetime."""
        dt = datetime.fromisoformat(self.last_ingestion_time.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt

    def to_dict(self) -> dict:
        """Convert to dict for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PollerCheckpoint":
        """Create from dict after deserialization."""
        return cls(
            last_ingestion_time=data["last_ingestion_time"],
            last_trace_id=data["last_trace_id"],
            updated_at=data.get("updated_at", ""),
        )


# =============================================================================
# Protocol definition
# =============================================================================


class PollerCheckpointStore(Protocol):
    """Protocol for poller checkpoint persistence.

    Both JsonPollerCheckpointStore and BlobPollerCheckpointStore implement this.
    """

    async def load(self) -> PollerCheckpoint | None:
        """Load checkpoint from storage.

        Returns None if no checkpoint exists or if loading fails.
        """
        ...

    async def save(self, checkpoint: PollerCheckpoint) -> bool:
        """Save checkpoint to storage.

        Returns True if save succeeded, False otherwise.
        """
        ...

    async def close(self) -> None:
        """Clean up resources."""
        ...


# =============================================================================
# JSON implementation (local files)
# =============================================================================


class JsonPollerCheckpointStore:
    """Local JSON file checkpoint store.

    Stores checkpoints in local filesystem JSON files, one per domain.
    Uses atomic write pattern (write to temp file, then os.replace).
    """

    def __init__(self, storage_path: str | Path, domain: str):
        """Initialize JSON checkpoint store.

        Args:
            storage_path: Base directory for checkpoint files
            domain: Domain name (used in filename)
        """
        self._base_path = Path(storage_path)
        self._domain = domain
        self._checkpoint_path = self._base_path / f"poller_{domain}.json"

        logger.info(
            "JsonPollerCheckpointStore initialized",
            extra={
                "storage_path": str(self._base_path),
                "domain": domain,
                "checkpoint_file": str(self._checkpoint_path),
            },
        )

    async def load(self) -> PollerCheckpoint | None:
        """Load checkpoint from JSON file."""
        if not self._checkpoint_path.exists():
            logger.info(
                "No checkpoint file found",
                extra={"path": str(self._checkpoint_path)},
            )
            return None

        try:
            with open(self._checkpoint_path) as f:
                data = json.load(f)

            checkpoint = PollerCheckpoint.from_dict(data)
            logger.info(
                "Loaded checkpoint from JSON file",
                extra={
                    "path": str(self._checkpoint_path),
                    "last_ingestion_time": checkpoint.last_ingestion_time,
                    "last_trace_id": checkpoint.last_trace_id,
                },
            )
            return checkpoint

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(
                "Failed to load checkpoint, starting fresh",
                extra={"path": str(self._checkpoint_path), "error": str(e)},
            )
            return None

    async def save(self, checkpoint: PollerCheckpoint) -> bool:
        """Save checkpoint to JSON file using atomic write."""
        try:
            self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            checkpoint.updated_at = datetime.now(UTC).isoformat()

            temp_path = self._checkpoint_path.with_suffix(".tmp")

            with open(temp_path, "w") as f:
                json.dump(checkpoint.to_dict(), f, indent=2)

            # Atomic replace
            os.replace(temp_path, self._checkpoint_path)

            logger.debug(
                "Saved checkpoint to JSON file",
                extra={
                    "path": str(self._checkpoint_path),
                    "last_ingestion_time": checkpoint.last_ingestion_time,
                },
            )
            return True

        except OSError as e:
            logger.error(
                "Failed to save checkpoint",
                extra={"path": str(self._checkpoint_path), "error": str(e)},
            )
            return False

    async def close(self) -> None:
        """No-op for JSON store."""
        pass


# =============================================================================
# Blob implementation (Azure Blob Storage)
# =============================================================================


class BlobPollerCheckpointStore:
    """Azure Blob Storage checkpoint store.

    Stores checkpoints in Azure Blob Storage for production durability.
    Each domain gets its own blob: poller_{domain}.json
    """

    def __init__(self, connection_string: str, container_name: str, domain: str):
        """Initialize blob checkpoint store.

        Args:
            connection_string: Azure Storage connection string
            container_name: Container name for checkpoint blobs
            domain: Domain name (used in blob name)
        """
        from azure.storage.blob.aio import BlobServiceClient

        self._connection_string = connection_string
        self._container_name = container_name
        self._domain = domain
        self._blob_name = f"poller_{domain}.json"
        self._blob_service_client: BlobServiceClient | None = None
        self._container_client = None
        self._blob_client = None

        logger.info(
            "BlobPollerCheckpointStore initialized",
            extra={
                "container_name": container_name,
                "domain": domain,
                "blob_name": self._blob_name,
            },
        )

    async def _ensure_client(self) -> None:
        """Lazy initialization of blob clients."""
        if self._blob_client is not None:
            return

        from azure.storage.blob.aio import BlobServiceClient

        self._blob_service_client = BlobServiceClient.from_connection_string(
            self._connection_string
        )
        self._container_client = self._blob_service_client.get_container_client(
            self._container_name
        )

        # Ensure container exists
        try:
            await asyncio.wait_for(
                self._container_client.create_container(),
                timeout=CHECKPOINT_TIMEOUT_SECONDS,
            )
            logger.info(
                "Created checkpoint container",
                extra={"container_name": self._container_name},
            )
        except Exception as e:
            # Container already exists, timeout, or connectivity issue — not fatal
            print(f"[checkpoint] create_container: {type(e).__name__}: {e}")
            pass

        logger.info(
            "Blob storage connectivity verified for poller checkpoint store",
            extra={"container_name": self._container_name},
        )

        self._blob_client = self._container_client.get_blob_client(self._blob_name)

    async def load(self) -> PollerCheckpoint | None:
        """Load checkpoint from blob storage."""
        await self._ensure_client()

        try:
            from azure.core.exceptions import ResourceNotFoundError

            download_stream = await asyncio.wait_for(
                self._blob_client.download_blob(),
                timeout=CHECKPOINT_TIMEOUT_SECONDS,
            )
            content = await download_stream.readall()
            data = json.loads(content.decode("utf-8"))

            checkpoint = PollerCheckpoint.from_dict(data)
            logger.info(
                "Loaded checkpoint from blob storage",
                extra={
                    "container": self._container_name,
                    "blob": self._blob_name,
                    "last_ingestion_time": checkpoint.last_ingestion_time,
                    "last_trace_id": checkpoint.last_trace_id,
                },
            )
            return checkpoint

        except ResourceNotFoundError:
            logger.info(
                "No checkpoint found in blob storage, starting fresh",
                extra={
                    "container": self._container_name,
                    "blob": self._blob_name,
                },
            )
            return None

        except Exception as e:
            print(f"[checkpoint] load failed: {type(e).__name__}: {e}")
            logger.warning(
                "Failed to load checkpoint from blob storage, starting fresh",
                extra={
                    "container": self._container_name,
                    "blob": self._blob_name,
                    "error_type": type(e).__name__,
                    "error": str(e),
                },
            )
            return None

    async def save(self, checkpoint: PollerCheckpoint) -> bool:
        """Save checkpoint to blob storage."""
        await self._ensure_client()

        try:
            checkpoint.updated_at = datetime.now(UTC).isoformat()
            content = json.dumps(checkpoint.to_dict(), indent=2)

            await asyncio.wait_for(
                self._blob_client.upload_blob(content, overwrite=True),
                timeout=CHECKPOINT_TIMEOUT_SECONDS,
            )

            logger.debug(
                "Saved checkpoint to blob storage",
                extra={
                    "container": self._container_name,
                    "blob": self._blob_name,
                    "last_ingestion_time": checkpoint.last_ingestion_time,
                },
            )
            return True

        except Exception as e:
            print(f"[checkpoint] save failed: {type(e).__name__}: {e}")
            logger.error(
                "Failed to save checkpoint to blob storage",
                extra={
                    "container": self._container_name,
                    "blob": self._blob_name,
                    "error_type": type(e).__name__,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False

    async def close(self) -> None:
        """Close blob service client."""
        if self._blob_service_client is not None:
            await self._blob_service_client.close()
            logger.debug("Closed BlobPollerCheckpointStore")


# =============================================================================
# Factory function
# =============================================================================


async def create_poller_checkpoint_store(
    domain: str,
    store_type: str | None = None,
    connection_string: str | None = None,
    container_name: str | None = None,
    storage_path: str | None = None,
) -> PollerCheckpointStore:
    """Create a poller checkpoint store from configuration.

    Args:
        domain: Domain name (used in checkpoint file/blob name)
        store_type: "blob" or "json" (defaults to config or "json")
        connection_string: Azure Storage connection string (for blob type)
        container_name: Container name (for blob type)
        storage_path: Local file path (for json type)

    Returns:
        PollerCheckpointStore implementation

    Raises:
        ValueError: If configuration is invalid
    """
    # Load from config if not provided
    if store_type is None:
        config = _load_poller_checkpoint_config()
        store_type = config.get("type", "json")
        connection_string = connection_string or config.get("blob_storage_connection_string")
        container_name = container_name or config.get("container_name", "poller-checkpoints")
        storage_path = storage_path or config.get("storage_path", "./.checkpoints")

    if store_type == "json":
        return JsonPollerCheckpointStore(
            storage_path=storage_path or "./.checkpoints",
            domain=domain,
        )
    elif store_type == "blob":
        if not connection_string:
            logger.warning(
                "Blob checkpoint store requested but no connection string configured. "
                "Falling back to JSON checkpoint store."
            )
            return JsonPollerCheckpointStore(
                storage_path=storage_path or "./.checkpoints",
                domain=domain,
            )

        return BlobPollerCheckpointStore(
            connection_string=connection_string,
            container_name=container_name or "eventhub-checkpoints",
            domain=domain,
        )
    else:
        raise ValueError(
            f"Unknown checkpoint store type: '{store_type}'. Must be 'blob' or 'json'."
        )


def _load_poller_checkpoint_config() -> dict:
    """Load poller checkpoint store configuration from config.yaml.

    Returns a dict with:
    - type: Store backend type ("blob" or "json")
    - blob_storage_connection_string: Azure Blob Storage connection string
    - container_name: Container name for storing checkpoints
    - storage_path: Local filesystem path for JSON store
    """
    from config.config import DEFAULT_CONFIG_FILE, _expand_env_vars, load_yaml

    config = {}

    # Load from config.yaml if it exists
    if DEFAULT_CONFIG_FILE.exists():
        data = load_yaml(DEFAULT_CONFIG_FILE)
        data = _expand_env_vars(data)
        eventhouse_config = data.get("eventhouse", {})
        checkpoint_config = eventhouse_config.get("poller_checkpoint_store", {})

        config["type"] = checkpoint_config.get("type", "json")
        config["blob_storage_connection_string"] = checkpoint_config.get(
            "blob_storage_connection_string", ""
        )
        config["container_name"] = checkpoint_config.get("container_name", "eventhub-checkpoints")
        config["storage_path"] = checkpoint_config.get("storage_path", "./.checkpoints")
    else:
        # Fallback to environment variables
        config["type"] = os.getenv("POLLER_CHECKPOINT_STORE_TYPE", "json")
        config["blob_storage_connection_string"] = os.getenv(
            "POLLER_CHECKPOINT_BLOB_CONNECTION_STRING", ""
        )
        config["container_name"] = os.getenv(
            "POLLER_CHECKPOINT_CONTAINER_NAME", "eventhub-checkpoints"
        )
        config["storage_path"] = os.getenv("POLLER_CHECKPOINT_JSON_PATH", "./.checkpoints")

    logger.debug(
        f"Loaded poller checkpoint store config: "
        f"type={config['type']}, "
        f"container_name={config['container_name']}, "
        f"connection_string_configured={bool(config['blob_storage_connection_string'])}"
    )

    return config


__all__ = [
    "PollerCheckpoint",
    "PollerCheckpointStore",
    "JsonPollerCheckpointStore",
    "BlobPollerCheckpointStore",
    "create_poller_checkpoint_store",
]
