"""Local filesystem JSON checkpoint store for the KQL Event Poller.

Implements PollerCheckpointStoreProtocol using local JSON files.
Suitable for local development or single-instance deployments
with persistent storage.

File structure:
    <storage_path>/
      poller_<domain>.json

Each file contains:
    {
      "last_ingestion_time": "2026-02-03T15:31:00+00:00",
      "last_trace_id": "abc123",
      "updated_at": "2026-02-03T15:31:05+00:00"
    }
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from core.logging import get_logger, log_with_context

logger = get_logger(__name__)


class JsonPollerCheckpointStore:
    """Poller checkpoint store backed by local JSON files.

    Uses atomic write (temp file + os.replace) for crash safety.
    Retries os.replace on Windows PermissionError from transient file locks.
    """

    def __init__(self, storage_path: str | Path) -> None:
        self._base_path = Path(storage_path)
        log_with_context(
            logger,
            logging.INFO,
            "JsonPollerCheckpointStore initialized",
            storage_path=str(self._base_path),
        )

    def _checkpoint_path(self, domain: str) -> Path:
        return self._base_path / f"poller_{domain}.json"

    async def load(
        self, domain: str
    ) -> "PollerCheckpointData | None":
        from pipeline.common.eventhouse.poller_checkpoint_store import (
            PollerCheckpointData,
        )

        path = self._checkpoint_path(domain)
        if not path.exists():
            logger.info(
                "No poller checkpoint file found",
                extra={"path": str(path), "domain": domain},
            )
            return None

        try:
            with open(path) as f:
                data: dict[str, Any] = json.load(f)

            return PollerCheckpointData.from_dict(data)

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(
                "Failed to load poller checkpoint, starting fresh",
                extra={"path": str(path), "domain": domain, "error": str(e)},
            )
            return None

    async def save(
        self, domain: str, checkpoint: "PollerCheckpointData"
    ) -> None:
        path = self._checkpoint_path(domain)
        path.parent.mkdir(parents=True, exist_ok=True)

        tmp_path = path.with_suffix(".json.tmp")
        with open(tmp_path, "w") as f:
            json.dump(checkpoint.to_dict(), f, indent=2)

        max_retries = 5
        for attempt in range(max_retries):
            try:
                os.replace(str(tmp_path), str(path))
                return
            except PermissionError:
                if attempt < max_retries - 1:
                    delay = 0.05 * (2**attempt)
                    logger.debug(
                        f"os.replace failed for poller checkpoint {domain} "
                        f"(attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {delay * 1000:.0f}ms"
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"os.replace failed for poller checkpoint {domain} "
                        f"after {max_retries} attempts, raising"
                    )
                    raise

    async def close(self) -> None:
        logger.debug("JsonPollerCheckpointStore closed (no-op)")


__all__ = [
    "JsonPollerCheckpointStore",
]
