"""Azure Event Hub checkpoint store factory.

Provides a singleton checkpoint store instance for durable offset persistence.
Checkpoint stores enable Event Hub consumers to persist their progress,
allowing for graceful restarts and partition rebalancing across
multiple consumer instances.

Architecture:
- Singleton pattern ensures ONE checkpoint store instance per process
- Lazy initialization - store is created only when first requested
- Graceful degradation - returns None if storage is not configured
- Thread-safe initialization with double-checked locking pattern

Supported backends:
- "blob": Azure Blob Storage via BlobCheckpointStore (production)
- "json": Local filesystem JSON files via JsonCheckpointStore (development)

Configuration:
- Reads from config.yaml: eventhub.checkpoint_store section
- Backend selected by 'type' field (default: "blob")
- Falls back to environment variables if config.yaml values are empty

Usage:
    # In consumer initialization:
    checkpoint_store = await get_checkpoint_store()
    if checkpoint_store:
        consumer = EventHubConsumerClient.from_connection_string(
            ...,
            checkpoint_store=checkpoint_store,
        )

    # At application shutdown:
    await close_checkpoint_store()
"""

import asyncio
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# Singleton state
# =============================================================================

_checkpoint_store: Any = None
_checkpoint_store_lock = asyncio.Lock()
_initialization_attempted = False


# =============================================================================
# Configuration loading
# =============================================================================


def _load_checkpoint_config() -> dict:
    """Load checkpoint store configuration from config.yaml.

    Returns a dict with:
    - type: Store backend type ("blob" or "json")
    - blob_storage_connection_string: Azure Blob Storage connection string
    - container_name: Container name for storing checkpoints
    - storage_path: Local filesystem path for JSON store

    Priority for each value:
    1. Environment variables (EVENTHUB_CHECKPOINT_STORE_TYPE, etc.)
    2. config.yaml: eventhub.checkpoint_store section
    3. Defaults (type="blob", graceful degradation)
    """
    from config.config import DEFAULT_CONFIG_FILE, _expand_env_vars, load_yaml

    config = {}

    # Load from config.yaml if it exists
    if DEFAULT_CONFIG_FILE.exists():
        data = load_yaml(DEFAULT_CONFIG_FILE)
        data = _expand_env_vars(data)
        eventhub_config = data.get("eventhub", {})
        checkpoint_config = eventhub_config.get("checkpoint_store", {})

        config["type"] = checkpoint_config.get("type", "blob")
        config["blob_storage_connection_string"] = checkpoint_config.get(
            "blob_storage_connection_string", ""
        )
        config["container_name"] = checkpoint_config.get(
            "container_name", "eventhub-checkpoints"
        )
        config["storage_path"] = checkpoint_config.get(
            "storage_path", "./data/eventhub-checkpoints"
        )
    else:
        # Fallback defaults if config file doesn't exist
        config["type"] = os.getenv("EVENTHUB_CHECKPOINT_STORE_TYPE", "blob")
        config["blob_storage_connection_string"] = os.getenv(
            "EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING", ""
        )
        config["container_name"] = os.getenv(
            "EVENTHUB_CHECKPOINT_CONTAINER_NAME", "eventhub-checkpoints"
        )
        config["storage_path"] = os.getenv(
            "EVENTHUB_CHECKPOINT_JSON_PATH", "./data/eventhub-checkpoints"
        )

    logger.debug(
        f"Loaded checkpoint store config: "
        f"type={config['type']}, "
        f"container_name={config['container_name']}, "
        f"connection_string_configured={bool(config['blob_storage_connection_string'])}"
    )

    return config


# =============================================================================
# Factory functions
# =============================================================================


async def get_checkpoint_store() -> Any:
    """Get or create the singleton checkpoint store instance.

    The backend is selected by the 'type' field in config:
    - "json": Local filesystem JSON files (no external deps)
    - "blob": Azure Blob Storage via BlobCheckpointStore

    Returns None if type is "blob" and connection string is not configured,
    allowing graceful degradation to in-memory checkpointing.

    This function is thread-safe and uses lazy initialization with
    double-checked locking to ensure only one store is created.

    Returns:
        Checkpoint store instance, or None if not configured

    Raises:
        ValueError: If checkpoint store type is unknown
        Exception: If checkpoint store creation fails
    """
    global _checkpoint_store, _initialization_attempted

    # Fast path: already initialized
    if _checkpoint_store is not None:
        return _checkpoint_store

    async with _checkpoint_store_lock:
        # Double-check after acquiring lock
        if _checkpoint_store is not None:
            return _checkpoint_store

        if _initialization_attempted:
            return None

        _initialization_attempted = True

        # Load configuration
        config = _load_checkpoint_config()
        store_type = config.get("type", "blob")

        if store_type == "json":
            _checkpoint_store = _create_json_store(config)
            return _checkpoint_store
        elif store_type == "blob":
            _checkpoint_store = await _create_blob_store(config)
            return _checkpoint_store
        else:
            raise ValueError(
                f"Unknown checkpoint store type: '{store_type}'. "
                f"Must be 'blob' or 'json'."
            )


def _create_json_store(config: dict) -> Any:
    """Create a JsonCheckpointStore from config."""
    from pipeline.common.eventhub.json_checkpoint_store import JsonCheckpointStore

    storage_path = config.get("storage_path", "./data/eventhub-checkpoints")
    return JsonCheckpointStore(storage_path=storage_path)


async def _create_blob_store(config: dict) -> Any:
    """Create a BlobCheckpointStore from config, or None if not configured."""
    from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

    connection_string = config["blob_storage_connection_string"]
    container_name = config["container_name"]

    # Graceful degradation if not configured
    if not connection_string:
        logger.info(
            "Checkpoint store not configured - blob_storage_connection_string is empty. "
            "Event Hub consumers will use default checkpointing behavior. "
            "To enable persistent checkpoints, set EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING "
            "or configure eventhub.checkpoint_store.blob_storage_connection_string in config.yaml."
        )
        return None

    if not container_name:
        logger.warning(
            "Checkpoint store container_name is empty. "
            "Using default container name: 'eventhub-checkpoints'"
        )
        container_name = "eventhub-checkpoints"

    try:
        logger.info(
            "Initializing BlobCheckpointStore",
            extra={"container_name": container_name},
        )

        store = BlobCheckpointStore.from_connection_string(
            conn_str=connection_string,
            container_name=container_name,
        )

        # Smoke test: verify connection string and permissions work
        # 15s timeout to fail fast when blob storage is unreachable
        # instead of hanging for minutes on the default TCP timeout
        from azure.storage.blob.aio import ContainerClient

        container_client = ContainerClient.from_connection_string(
            conn_str=connection_string,
            container_name=container_name,
        )
        try:
            await asyncio.wait_for(
                container_client.get_container_properties(),
                timeout=15,
            )
            logger.info(
                "Blob storage connectivity verified for checkpoint store",
                extra={"container_name": container_name},
            )
        finally:
            await container_client.close()

        return store

    except Exception as e:
        logger.error(
            "Failed to initialize BlobCheckpointStore",
            extra={
                "container_name": container_name,
                "connection_string_configured": bool(connection_string),
                "error": str(e),
            },
            exc_info=True,
        )
        raise


async def close_checkpoint_store() -> None:
    """Close and clean up the checkpoint store instance.

    This should be called during application shutdown to ensure proper
    cleanup of Azure SDK resources.

    Safe to call multiple times - subsequent calls are no-ops.
    Safe to call even if checkpoint store was never initialized.
    """
    global _checkpoint_store, _initialization_attempted

    if _checkpoint_store is None:
        logger.debug("Checkpoint store already closed or never initialized")
        return

    async with _checkpoint_store_lock:
        if _checkpoint_store is None:
            return

        try:
            logger.info(
                "Closing checkpoint store",
                extra={"store_type": type(_checkpoint_store).__name__},
            )

            if hasattr(_checkpoint_store, "close"):
                await _checkpoint_store.close()

            logger.info("Checkpoint store closed successfully")

        except Exception as e:
            logger.error(
                "Error closing checkpoint store",
                extra={"error": str(e)},
                exc_info=True,
            )

        finally:
            _checkpoint_store = None
            _initialization_attempted = False


def reset_checkpoint_store() -> None:
    """Reset the checkpoint store singleton (for testing only).

    This is a synchronous function that resets the module-level state
    without closing the checkpoint store. Use this in test teardown
    to ensure a clean state between tests.

    WARNING: This does NOT close the checkpoint store connection.
    For production shutdown, use close_checkpoint_store() instead.
    """
    global _checkpoint_store, _initialization_attempted

    logger.debug("Resetting checkpoint store singleton state (testing only)")
    _checkpoint_store = None
    _initialization_attempted = False


__all__ = [
    "get_checkpoint_store",
    "close_checkpoint_store",
    "reset_checkpoint_store",
]
