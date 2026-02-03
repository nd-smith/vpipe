"""Poller checkpoint store factory.

Provides a singleton poller checkpoint store for durable state persistence.
The poller checkpoint store allows KQLEventPoller to persist its progress
(last_ingestion_time, last_trace_id) so that it can resume after pod restarts
without reprocessing data.

Supported backends:
- "blob": Azure Blob Storage (production / K8s)
- "json": Local filesystem JSON files (development / local)

Configuration via environment variables:
- POLLER_CHECKPOINT_STORE_TYPE: "blob" or "json" (default: "blob")
- POLLER_CHECKPOINT_BLOB_CONNECTION_STRING: Azure Blob Storage connection string
- POLLER_CHECKPOINT_CONTAINER_NAME: Container name (default: "poller-checkpoints")
- POLLER_CHECKPOINT_JSON_PATH: Local path for JSON backend (default: ".checkpoints")
"""

import asyncio
import json
import logging
import os
from typing import Any, Protocol

from core.logging import get_logger, log_exception, log_with_context

logger = get_logger(__name__)


class PollerCheckpointData:
    """Simple container for poller checkpoint state."""

    __slots__ = ("last_ingestion_time", "last_trace_id", "updated_at")

    def __init__(
        self,
        last_ingestion_time: str,
        last_trace_id: str,
        updated_at: str = "",
    ) -> None:
        self.last_ingestion_time = last_ingestion_time
        self.last_trace_id = last_trace_id
        self.updated_at = updated_at

    def to_dict(self) -> dict[str, str]:
        return {
            "last_ingestion_time": self.last_ingestion_time,
            "last_trace_id": self.last_trace_id,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PollerCheckpointData":
        return cls(
            last_ingestion_time=data["last_ingestion_time"],
            last_trace_id=data["last_trace_id"],
            updated_at=data.get("updated_at", ""),
        )


class PollerCheckpointStoreProtocol(Protocol):
    """Protocol for poller checkpoint persistence backends."""

    async def load(self, domain: str) -> PollerCheckpointData | None:
        """Load checkpoint for the given domain.

        Returns None if no checkpoint exists.
        """
        ...

    async def save(self, domain: str, checkpoint: PollerCheckpointData) -> None:
        """Persist checkpoint for the given domain."""
        ...

    async def close(self) -> None:
        """Release any resources held by the store."""
        ...


# =============================================================================
# Singleton state
# =============================================================================

_poller_checkpoint_store: PollerCheckpointStoreProtocol | None = None
_poller_checkpoint_store_lock = asyncio.Lock()
_initialization_attempted = False


# =============================================================================
# Configuration loading
# =============================================================================


def _load_poller_checkpoint_config() -> dict[str, str]:
    """Load poller checkpoint store configuration.

    Priority:
    1. config.yaml poller_checkpoint_store section (with env var expansion)
    2. Direct environment variables
    3. Defaults
    """
    from config.config import DEFAULT_CONFIG_FILE, _expand_env_vars, load_yaml

    config: dict[str, str] = {}

    if DEFAULT_CONFIG_FILE.exists():
        data = load_yaml(DEFAULT_CONFIG_FILE)
        data = _expand_env_vars(data)
        checkpoint_config = data.get("poller_checkpoint_store", {})

        config["type"] = checkpoint_config.get("type", "blob")
        config["blob_storage_connection_string"] = checkpoint_config.get(
            "blob_storage_connection_string", ""
        )
        config["container_name"] = checkpoint_config.get(
            "container_name", "poller-checkpoints"
        )
        config["storage_path"] = checkpoint_config.get(
            "storage_path", ".checkpoints"
        )
    else:
        config["type"] = os.getenv("POLLER_CHECKPOINT_STORE_TYPE", "blob")
        config["blob_storage_connection_string"] = os.getenv(
            "POLLER_CHECKPOINT_BLOB_CONNECTION_STRING", ""
        )
        config["container_name"] = os.getenv(
            "POLLER_CHECKPOINT_CONTAINER_NAME", "poller-checkpoints"
        )
        config["storage_path"] = os.getenv(
            "POLLER_CHECKPOINT_JSON_PATH", ".checkpoints"
        )

    logger.debug(
        f"Loaded poller checkpoint store config: "
        f"type={config['type']}, "
        f"container_name={config['container_name']}, "
        f"connection_string_configured={bool(config['blob_storage_connection_string'])}"
    )

    return config


# =============================================================================
# Factory functions
# =============================================================================


async def get_poller_checkpoint_store() -> PollerCheckpointStoreProtocol:
    """Get or create the singleton poller checkpoint store.

    Returns:
        PollerCheckpointStoreProtocol instance.

    Raises:
        ValueError: If store type is unknown.
    """
    global _poller_checkpoint_store, _initialization_attempted

    if _poller_checkpoint_store is not None:
        return _poller_checkpoint_store

    async with _poller_checkpoint_store_lock:
        if _poller_checkpoint_store is not None:
            return _poller_checkpoint_store

        _initialization_attempted = True
        config = _load_poller_checkpoint_config()
        store_type = config.get("type", "blob")

        if store_type == "json":
            _poller_checkpoint_store = _create_json_store(config)
        elif store_type == "blob":
            _poller_checkpoint_store = _create_blob_store(config)
        else:
            raise ValueError(
                f"Unknown poller checkpoint store type: '{store_type}'. "
                f"Must be 'blob' or 'json'."
            )

        return _poller_checkpoint_store


def _create_json_store(
    config: dict[str, str],
) -> "PollerCheckpointStoreProtocol":
    """Create a JsonPollerCheckpointStore from config."""
    from pipeline.common.eventhouse.json_poller_checkpoint_store import (
        JsonPollerCheckpointStore,
    )

    storage_path = config.get("storage_path", ".checkpoints")
    return JsonPollerCheckpointStore(storage_path=storage_path)


def _create_blob_store(
    config: dict[str, str],
) -> "PollerCheckpointStoreProtocol":
    """Create a BlobPollerCheckpointStore from config.

    Falls back to JSON store if connection string is not configured.
    """
    connection_string = config["blob_storage_connection_string"]
    container_name = config["container_name"]

    if not connection_string:
        log_with_context(
            logger,
            logging.WARNING,
            "Poller checkpoint blob_storage_connection_string is empty. "
            "Falling back to JSON file checkpoint store. "
            "To enable blob storage, set POLLER_CHECKPOINT_BLOB_CONNECTION_STRING.",
        )
        return _create_json_store(config)

    if not container_name:
        container_name = "poller-checkpoints"

    try:
        from pipeline.common.eventhouse.blob_poller_checkpoint_store import (
            BlobPollerCheckpointStore,
        )

        log_with_context(
            logger,
            logging.INFO,
            "Initializing BlobPollerCheckpointStore",
            container_name=container_name,
        )

        store = BlobPollerCheckpointStore(
            connection_string=connection_string,
            container_name=container_name,
        )

        log_with_context(
            logger,
            logging.INFO,
            "BlobPollerCheckpointStore initialized successfully",
            container_name=container_name,
        )

        return store

    except Exception as e:
        log_exception(
            logger,
            e,
            "Failed to initialize BlobPollerCheckpointStore",
            container_name=container_name,
        )
        raise


async def close_poller_checkpoint_store() -> None:
    """Close and clean up the poller checkpoint store.

    Safe to call multiple times or if never initialized.
    """
    global _poller_checkpoint_store, _initialization_attempted

    if _poller_checkpoint_store is None:
        return

    async with _poller_checkpoint_store_lock:
        if _poller_checkpoint_store is None:
            return

        try:
            log_with_context(
                logger,
                logging.INFO,
                "Closing poller checkpoint store",
                store_type=type(_poller_checkpoint_store).__name__,
            )
            await _poller_checkpoint_store.close()
        except Exception as e:
            log_exception(logger, e, "Error closing poller checkpoint store")
        finally:
            _poller_checkpoint_store = None
            _initialization_attempted = False


def reset_poller_checkpoint_store() -> None:
    """Reset singleton state (testing only)."""
    global _poller_checkpoint_store, _initialization_attempted
    _poller_checkpoint_store = None
    _initialization_attempted = False


__all__ = [
    "PollerCheckpointData",
    "PollerCheckpointStoreProtocol",
    "get_poller_checkpoint_store",
    "close_poller_checkpoint_store",
    "reset_poller_checkpoint_store",
]
