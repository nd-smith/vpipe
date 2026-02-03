"""Azure Blob Storage checkpoint store for the KQL Event Poller.

Implements PollerCheckpointStoreProtocol using Azure Blob Storage.
Each domain's checkpoint is stored as a single blob:

    <container_name>/poller_<domain>.json

Suitable for production K8s deployments where local filesystem
is ephemeral and checkpoints must survive pod restarts.
"""

import json
import logging
from typing import Any

from core.logging import get_logger, log_exception, log_with_context

logger = get_logger(__name__)


class BlobPollerCheckpointStore:
    """Poller checkpoint store backed by Azure Blob Storage.

    Uses the azure-storage-blob async SDK to persist checkpoint state
    as individual JSON blobs per domain.
    """

    def __init__(self, connection_string: str, container_name: str) -> None:
        from azure.storage.blob.aio import ContainerClient

        self._container_client = ContainerClient.from_connection_string(
            conn_str=connection_string,
            container_name=container_name,
        )
        self._container_name = container_name

        log_with_context(
            logger,
            logging.INFO,
            "BlobPollerCheckpointStore initialized",
            container_name=container_name,
        )

    def _blob_name(self, domain: str) -> str:
        return f"poller_{domain}.json"

    async def load(
        self, domain: str
    ) -> "PollerCheckpointData | None":
        from pipeline.common.eventhouse.poller_checkpoint_store import (
            PollerCheckpointData,
        )

        blob_name = self._blob_name(domain)

        try:
            blob_client = self._container_client.get_blob_client(blob_name)
            download = await blob_client.download_blob()
            content = await download.readall()
            data: dict[str, Any] = json.loads(content)

            log_with_context(
                logger,
                logging.DEBUG,
                "Loaded poller checkpoint from blob",
                domain=domain,
                blob_name=blob_name,
            )

            return PollerCheckpointData.from_dict(data)

        except Exception as e:
            # ResourceNotFoundError or any other error — treat as no checkpoint
            error_type = type(e).__name__
            if "ResourceNotFound" in error_type or "BlobNotFound" in error_type:
                logger.info(
                    "No poller checkpoint blob found",
                    extra={"domain": domain, "blob_name": blob_name},
                )
            else:
                logger.warning(
                    "Failed to load poller checkpoint from blob, starting fresh",
                    extra={
                        "domain": domain,
                        "blob_name": blob_name,
                        "error": str(e),
                        "error_type": error_type,
                    },
                )
            return None

    async def save(
        self, domain: str, checkpoint: "PollerCheckpointData"
    ) -> None:
        blob_name = self._blob_name(domain)
        content = json.dumps(checkpoint.to_dict(), indent=2)

        try:
            blob_client = self._container_client.get_blob_client(blob_name)
            await blob_client.upload_blob(content, overwrite=True)

            log_with_context(
                logger,
                logging.DEBUG,
                "Saved poller checkpoint to blob",
                domain=domain,
                blob_name=blob_name,
            )

        except Exception as e:
            log_exception(
                logger,
                e,
                "Failed to save poller checkpoint to blob",
                domain=domain,
                blob_name=blob_name,
            )
            raise

    async def close(self) -> None:
        try:
            await self._container_client.close()
            log_with_context(
                logger,
                logging.INFO,
                "BlobPollerCheckpointStore closed",
                container_name=self._container_name,
            )
        except Exception as e:
            log_exception(
                logger,
                e,
                "Error closing BlobPollerCheckpointStore",
            )


__all__ = [
    "BlobPollerCheckpointStore",
]
