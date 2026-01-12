"""
Async OneLake client for Kafka pipeline workers.

Provides async-friendly interface to Azure Data Lake Storage Gen2 (OneLake)
for uploading downloaded attachments. Wraps legacy OneLakeClient with
asyncio.to_thread for non-blocking operations.
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional

from kafka_pipeline.common.storage.onelake import OneLakeClient as SyncOneLakeClient

logger = logging.getLogger(__name__)


class OneLakeClient:
    """
    Async wrapper for OneLake file operations.

    Provides async interface to Azure Data Lake Storage Gen2 for
    uploading attachments downloaded by worker. Uses asyncio.to_thread
    to make blocking Azure SDK calls non-blocking.

    Usage:
        client = OneLakeClient("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files")
        async with client:
            await client.upload_file("folder/file.pdf", Path("/tmp/file.pdf"))
            exists = await client.exists("folder/file.pdf")
    """

    def __init__(
        self,
        base_path: str,
        max_pool_size: Optional[int] = None,
        connection_timeout: int = 300,
    ):
        """
        Initialize OneLake client.

        Args:
            base_path: abfss:// path to files directory
                      e.g. "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files"
            max_pool_size: HTTP connection pool size (defaults to config value)
            connection_timeout: Connection timeout in seconds (default: 300s/5min)
        """
        self.base_path = base_path
        self._sync_client: Optional[SyncOneLakeClient] = None
        self._max_pool_size = max_pool_size
        self._connection_timeout = connection_timeout

        logger.debug(
            "Initialized async OneLake client",
            extra={"base_path": base_path},
        )

    async def __aenter__(self):
        """Async context manager entry - create and initialize client."""
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - close client."""
        await self.close()
        return False

    async def _ensure_client(self) -> None:
        """Ensure sync client is initialized (runs in thread pool)."""
        if self._sync_client is None:
            # Create client in thread pool since it may do blocking I/O
            def _create():
                client = SyncOneLakeClient(
                    base_path=self.base_path,
                    max_pool_size=self._max_pool_size,
                    connection_timeout=self._connection_timeout,
                )
                # Call __enter__ to initialize connection
                client.__enter__()
                return client

            self._sync_client = await asyncio.to_thread(_create)

            logger.debug("Sync OneLake client initialized")

    async def close(self) -> None:
        """Close the client and release resources."""
        if self._sync_client is not None:
            # Run close in thread pool since it may do blocking I/O
            await asyncio.to_thread(self._sync_client.close)
            self._sync_client = None
            logger.debug("OneLake client closed")

    async def upload_file(
        self,
        relative_path: str,
        local_path: Path,
        overwrite: bool = True,
    ) -> str:
        """
        Upload file from local path to OneLake (async, non-blocking).

        Args:
            relative_path: Path relative to base_path (e.g. "claims/C-123/file.pdf")
            local_path: Local file path to upload
            overwrite: Whether to overwrite existing file (default: True)

        Returns:
            Full abfss:// path to uploaded file

        Raises:
            FileNotFoundError: If local_path doesn't exist
            Exception: On upload failures (auth, network, etc.)
        """
        await self._ensure_client()

        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        # Run blocking upload in thread pool
        result_path = await asyncio.to_thread(
            self._sync_client.upload_file,  # type: ignore
            relative_path,
            str(local_path),
            overwrite,
        )

        logger.info(
            "Uploaded file to OneLake",
            extra={
                "relative_path": relative_path,
                "local_path": str(local_path),
                "result_path": result_path,
            },
        )

        return result_path

    async def upload_bytes(
        self,
        relative_path: str,
        data: bytes,
        overwrite: bool = True,
    ) -> str:
        """
        Upload bytes to OneLake (async, non-blocking).

        Args:
            relative_path: Path relative to base_path
            data: File content as bytes
            overwrite: Whether to overwrite existing file (default: True)

        Returns:
            Full abfss:// path to uploaded file

        Raises:
            Exception: On upload failures (auth, network, etc.)
        """
        await self._ensure_client()

        # Run blocking upload in thread pool
        result_path = await asyncio.to_thread(
            self._sync_client.upload_bytes,  # type: ignore
            relative_path,
            data,
            overwrite,
        )

        logger.info(
            "Uploaded bytes to OneLake",
            extra={
                "relative_path": relative_path,
                "bytes": len(data),
                "result_path": result_path,
            },
        )

        return result_path

    async def exists(self, relative_path: str) -> bool:
        """
        Check if file exists in OneLake (async, non-blocking).

        Args:
            relative_path: Path relative to base_path

        Returns:
            True if file exists, False otherwise
        """
        await self._ensure_client()

        # Run blocking check in thread pool
        result = await asyncio.to_thread(
            self._sync_client.exists,  # type: ignore
            relative_path,
        )

        return result

    async def delete(self, relative_path: str) -> bool:
        """
        Delete a file from OneLake (async, non-blocking).

        Args:
            relative_path: Path relative to base_path

        Returns:
            True if deleted, False if didn't exist
        """
        await self._ensure_client()

        # Run blocking delete in thread pool
        result = await asyncio.to_thread(
            self._sync_client.delete,  # type: ignore
            relative_path,
        )

        return result


__all__ = ["OneLakeClient"]
