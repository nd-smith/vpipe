"""Event deduplication store with persistent blob storage backend.

Provides persistent deduplication across worker restarts by storing
processed event/trace IDs in Azure Blob Storage. Complements in-memory
caching for fast lookups while ensuring durability.

Architecture:
- Hybrid approach: fast in-memory cache + persistent blob storage
- Workers check memory first, then blob storage on miss
- Writes go to both memory and blob for durability
- TTL-based expiration via blob metadata timestamps

Supported backends:
- "blob": Azure Blob Storage (production)
- "json": Local filesystem JSON files (development)

Storage layout:
    Container: eventhub-dedup-cache
    ├── verisk-event-ingester/{trace_id}.json
    └── claimx-event-ingester/{event_id}.json

Each blob contains: {"event_id": "...", "timestamp": 1234567890}

Configuration:
- Reads from config.yaml: eventhub.dedup_store section
- Reuses same blob storage connection as checkpoint store
- Different container name for logical separation

Usage:
    store = await get_dedup_store()
    if store:
        is_dup = await store.check_duplicate("worker-name", "trace_id_123")
        if not is_dup:
            await store.mark_processed("worker-name", "trace_id_123", {"event_id": "..."})
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)


# =============================================================================
# Protocol definition
# =============================================================================


class DedupStoreProtocol(Protocol):
    """Protocol for deduplication storage backends.

    Both blob and JSON implementations must provide these methods.
    """

    async def check_duplicate(
        self,
        worker_name: str,
        key: str,
        ttl_seconds: int,
    ) -> tuple[bool, dict[str, Any] | None]:
        """Check if key was processed recently.

        Args:
            worker_name: Worker identifier (e.g., "verisk-event-ingester")
            key: Dedup key (trace_id or event_id)
            ttl_seconds: Time-to-live in seconds

        Returns:
            (is_duplicate, metadata) where metadata is the stored data if found
        """
        ...

    async def mark_processed(
        self,
        worker_name: str,
        key: str,
        metadata: dict[str, Any],
    ) -> None:
        """Mark key as processed with metadata.

        Args:
            worker_name: Worker identifier
            key: Dedup key (trace_id or event_id)
            metadata: Data to store (e.g., {"event_id": "...", "timestamp": 123})
        """
        ...

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
        ...


# =============================================================================
# Singleton state
# =============================================================================

_dedup_store: DedupStoreProtocol | None = None
_dedup_store_lock = asyncio.Lock()
_initialization_attempted = False


# =============================================================================
# Configuration loading
# =============================================================================


def _load_dedup_config() -> dict:
    """Load dedup store configuration from config.yaml.

    Returns a dict with:
    - type: Store backend type ("blob" or "json")
    - blob_storage_connection_string: Azure Blob Storage connection string
    - container_name: Container name for storing dedup cache
    - storage_path: Local filesystem path for JSON store
    - ttl_seconds: Default TTL for dedup entries

    Priority for each value:
    1. Environment variables
    2. config.yaml: eventhub.dedup_store section
    3. Defaults
    """
    from config.config import DEFAULT_CONFIG_FILE, _expand_env_vars, load_yaml

    config = {}

    # Load from config.yaml if it exists
    if DEFAULT_CONFIG_FILE.exists():
        data = load_yaml(DEFAULT_CONFIG_FILE)
        data = _expand_env_vars(data)
        eventhub_config = data.get("eventhub", {})
        dedup_config = eventhub_config.get("dedup_store", {})

        config["type"] = dedup_config.get("type", "blob")
        config["blob_storage_connection_string"] = dedup_config.get(
            "blob_storage_connection_string", ""
        )
        config["container_name"] = dedup_config.get(
            "container_name", "eventhub-dedup-cache"
        )
        config["storage_path"] = dedup_config.get(
            "storage_path", "./data/eventhub-dedup-cache"
        )
        config["ttl_seconds"] = dedup_config.get("ttl_seconds", 86400)
    else:
        # Fallback to environment variables
        config["type"] = os.getenv("EVENTHUB_DEDUP_STORE_TYPE", "blob")
        config["blob_storage_connection_string"] = os.getenv(
            "EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING", ""
        )
        config["container_name"] = os.getenv(
            "EVENTHUB_DEDUP_CONTAINER_NAME", "eventhub-dedup-cache"
        )
        config["storage_path"] = os.getenv(
            "EVENTHUB_DEDUP_JSON_PATH", "./data/eventhub-dedup-cache"
        )
        config["ttl_seconds"] = int(os.getenv("EVENTHUB_DEDUP_TTL_SECONDS", "86400"))

    logger.debug(
        f"Loaded dedup store config: "
        f"type={config['type']}, "
        f"container_name={config['container_name']}, "
        f"ttl_seconds={config['ttl_seconds']}, "
        f"connection_string_configured={bool(config['blob_storage_connection_string'])}"
    )

    return config


# =============================================================================
# Factory functions
# =============================================================================


async def get_dedup_store() -> DedupStoreProtocol | None:
    """Get or create the singleton dedup store instance.

    Returns None if type is "blob" and connection string is not configured,
    allowing graceful degradation to memory-only deduplication.

    Returns:
        DedupStoreProtocol instance, or None if not configured

    Raises:
        ValueError: If dedup store type is unknown
        Exception: If dedup store creation fails
    """
    global _dedup_store, _initialization_attempted

    # Fast path: already initialized
    if _dedup_store is not None:
        return _dedup_store

    # Fast path: already attempted and determined not configured
    if _initialization_attempted and _dedup_store is None:
        return None

    # Slow path: need to initialize
    async with _dedup_store_lock:
        # Double-check after acquiring lock
        if _dedup_store is not None:
            return _dedup_store

        if _initialization_attempted:
            return None

        _initialization_attempted = True

        # Load configuration
        config = _load_dedup_config()
        store_type = config.get("type", "blob")

        if store_type == "json":
            _dedup_store = _create_json_store(config)
            return _dedup_store
        elif store_type == "blob":
            _dedup_store = await _create_blob_store(config)
            return _dedup_store
        else:
            raise ValueError(
                f"Unknown dedup store type: '{store_type}'. "
                f"Must be 'blob' or 'json'."
            )


def _create_json_store(config: dict) -> DedupStoreProtocol:
    """Create a JSON file-based dedup store (for development)."""
    from pipeline.common.eventhub.json_dedup_store import JsonDedupStore

    storage_path = config.get("storage_path", "./data/eventhub-dedup-cache")
    return JsonDedupStore(storage_path=storage_path)


async def _create_blob_store(config: dict) -> DedupStoreProtocol | None:
    """Create a blob-based dedup store, or None if not configured."""
    from pipeline.common.eventhub.blob_dedup_store import BlobDedupStore

    connection_string = config["blob_storage_connection_string"]
    container_name = config["container_name"]

    # Graceful degradation if not configured
    if not connection_string:
        logger.info(
            "Dedup store not configured - blob_storage_connection_string is empty. "
            "Workers will use memory-only deduplication. "
            "To enable persistent dedup, set EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING "
            "or configure eventhub.dedup_store.blob_storage_connection_string in config.yaml."
        )
        return None

    if not container_name:
        logger.warning(
            "Dedup store container_name is empty. "
            "Using default container name: 'eventhub-dedup-cache'"
        )
        container_name = "eventhub-dedup-cache"

    try:
        logger.info(
            "Initializing BlobDedupStore",
            extra={"container_name": container_name},
        )

        store = BlobDedupStore(
            connection_string=connection_string,
            container_name=container_name,
        )

        await store.initialize()

        logger.info(
            "BlobDedupStore initialized successfully",
            extra={"container_name": container_name},
        )

        return store

    except Exception as e:
        logger.error(
            "Failed to initialize BlobDedupStore",
            extra={
                "container_name": container_name,
                "connection_string_configured": bool(connection_string),
                "error": str(e),
            },
            exc_info=True,
        )
        raise


async def close_dedup_store() -> None:
    """Close and clean up the dedup store instance.

    Safe to call multiple times.
    """
    global _dedup_store, _initialization_attempted

    if _dedup_store is None:
        logger.debug("Dedup store already closed or never initialized")
        return

    async with _dedup_store_lock:
        if _dedup_store is None:
            return

        try:
            logger.info(
                "Closing dedup store",
                extra={"store_type": type(_dedup_store).__name__},
            )

            if hasattr(_dedup_store, "close"):
                await _dedup_store.close()

            logger.info("Dedup store closed successfully")

        except Exception as e:
            logger.error(
                "Error closing dedup store",
                extra={"error": str(e)},
                exc_info=True,
            )

        finally:
            _dedup_store = None
            _initialization_attempted = False


def reset_dedup_store() -> None:
    """Reset the dedup store singleton (for testing only).

    WARNING: Does NOT close the store connection.
    """
    global _dedup_store, _initialization_attempted

    logger.debug("Resetting dedup store singleton state (testing only)")
    _dedup_store = None
    _initialization_attempted = False


__all__ = [
    "DedupStoreProtocol",
    "get_dedup_store",
    "close_dedup_store",
    "reset_dedup_store",
]
