"""Azure Event Hub checkpoint store factory.

Provides a singleton BlobCheckpointStore instance for durable offset persistence.
Checkpoint stores enable Event Hub consumers to persist their progress in Azure
Blob Storage, allowing for graceful restarts and partition rebalancing across
multiple consumer instances.

Architecture:
- Singleton pattern ensures ONE checkpoint store instance per process
- Lazy initialization - store is created only when first requested
- Graceful degradation - returns None if blob storage is not configured
- Thread-safe initialization with double-checked locking pattern

Configuration:
- Reads from config.yaml: eventhub.checkpoint_store section
- Requires: blob_storage_connection_string and container_name
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

from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

from core.logging import get_logger, log_exception, log_with_context

logger = get_logger(__name__)


# =============================================================================
# Singleton state
# =============================================================================

_checkpoint_store: BlobCheckpointStore | None = None
_checkpoint_store_lock = asyncio.Lock()
_initialization_attempted = False


# =============================================================================
# Configuration loading
# =============================================================================


def _load_checkpoint_config() -> dict:
    """Load checkpoint store configuration from config.yaml.

    Returns a dict with:
    - blob_storage_connection_string: Azure Blob Storage connection string
    - container_name: Container name for storing checkpoints

    Priority for each value:
    1. Environment variables (EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING, etc.)
    2. config.yaml: eventhub.checkpoint_store section
    3. Empty string (graceful degradation)
    """
    from config.config import DEFAULT_CONFIG_FILE, _expand_env_vars, load_yaml

    config = {}

    # Load from config.yaml if it exists
    if DEFAULT_CONFIG_FILE.exists():
        data = load_yaml(DEFAULT_CONFIG_FILE)
        data = _expand_env_vars(data)
        eventhub_config = data.get("eventhub", {})
        checkpoint_config = eventhub_config.get("checkpoint_store", {})

        config["blob_storage_connection_string"] = checkpoint_config.get(
            "blob_storage_connection_string", ""
        )
        config["container_name"] = checkpoint_config.get(
            "container_name", "eventhub-checkpoints"
        )
    else:
        # Fallback defaults if config file doesn't exist
        config["blob_storage_connection_string"] = os.getenv(
            "EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING", ""
        )
        config["container_name"] = os.getenv(
            "EVENTHUB_CHECKPOINT_CONTAINER_NAME", "eventhub-checkpoints"
        )

    logger.debug(
        f"Loaded checkpoint store config: "
        f"container_name={config['container_name']}, "
        f"connection_string_configured={bool(config['blob_storage_connection_string'])}"
    )

    return config


# =============================================================================
# Factory functions
# =============================================================================


async def get_checkpoint_store() -> BlobCheckpointStore | None:
    """Get or create the singleton BlobCheckpointStore instance.

    Returns None if blob_storage_connection_string is not configured,
    allowing graceful degradation to manual checkpointing.

    This function is thread-safe and uses lazy initialization with
    double-checked locking to ensure only one store is created.

    Returns:
        BlobCheckpointStore instance, or None if not configured

    Raises:
        Exception: If checkpoint store creation fails with a configured
                   connection string (indicates misconfiguration or Azure issues)
    """
    global _checkpoint_store, _initialization_attempted

    # Fast path: already initialized
    if _checkpoint_store is not None:
        return _checkpoint_store

    # Fast path: already attempted and determined not configured
    if _initialization_attempted and _checkpoint_store is None:
        return None

    # Slow path: need to initialize
    async with _checkpoint_store_lock:
        # Double-check after acquiring lock
        if _checkpoint_store is not None:
            return _checkpoint_store

        if _initialization_attempted:
            return None

        _initialization_attempted = True

        # Load configuration
        config = _load_checkpoint_config()
        connection_string = config["blob_storage_connection_string"]
        container_name = config["container_name"]

        # Graceful degradation if not configured
        if not connection_string:
            log_with_context(
                logger,
                logging.INFO,
                "Checkpoint store not configured - blob_storage_connection_string is empty. "
                "Event Hub consumers will use default checkpointing behavior. "
                "To enable persistent checkpoints, set EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING "
                "or configure eventhub.checkpoint_store.blob_storage_connection_string in config.yaml.",
            )
            return None

        if not container_name:
            log_with_context(
                logger,
                logging.WARNING,
                "Checkpoint store container_name is empty. "
                "Using default container name: 'eventhub-checkpoints'",
            )
            container_name = "eventhub-checkpoints"

        # Create the checkpoint store
        try:
            log_with_context(
                logger,
                logging.INFO,
                "Initializing BlobCheckpointStore",
                container_name=container_name,
            )

            _checkpoint_store = BlobCheckpointStore.from_connection_string(
                conn_str=connection_string,
                container_name=container_name,
            )

            log_with_context(
                logger,
                logging.INFO,
                "BlobCheckpointStore initialized successfully",
                container_name=container_name,
            )

            return _checkpoint_store

        except Exception as e:
            log_exception(
                logger,
                e,
                "Failed to initialize BlobCheckpointStore",
                container_name=container_name,
                connection_string_configured=bool(connection_string),
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
            log_with_context(
                logger,
                logging.INFO,
                "Closing BlobCheckpointStore",
            )

            # BlobCheckpointStore uses async context manager protocol
            # Call close() if available (SDK may not expose explicit close)
            if hasattr(_checkpoint_store, "close"):
                await _checkpoint_store.close()

            log_with_context(
                logger,
                logging.INFO,
                "BlobCheckpointStore closed successfully",
            )

        except Exception as e:
            log_exception(
                logger,
                e,
                "Error closing BlobCheckpointStore",
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
