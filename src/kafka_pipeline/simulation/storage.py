"""
Local filesystem storage adapter mimicking OneLake blob storage behavior.

This adapter provides the same interface as OneLakeClient but writes to the local
filesystem instead of Azure OneLake. Used in simulation mode to avoid cloud dependencies
and enable testing without Azure credentials.

Directory Structure:
    /tmp/pcesdopodappv1_simulation/
        claimx/
            media/
                proj_123/
                    media_456_photo.jpg
        xact/
            attachments/
                assignment_789/
                    doc.pdf

The adapter implements atomic writes, path validation, and mimics OneLake's
API behavior for compatibility with upload workers.
"""

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from core.logging.setup import get_logger

logger = get_logger(__name__)


class LocalStorageAdapter:
    """
    Local filesystem adapter mimicking OneLake blob storage.

    Implements same interface as OneLakeClient for upload workers.
    Writes to local directory with same structure as OneLake.

    The adapter provides:
    - Same async API as OneLakeClient (async_upload_file, async_exists, etc.)
    - Atomic writes using temp file + rename pattern
    - Path validation to prevent directory traversal attacks
    - Optional metadata tracking (content type, size, timestamps)
    - Thread-safe operations

    Usage:
        adapter = LocalStorageAdapter(base_path=Path("/tmp/pcesdopodappv1_simulation"))
        async with adapter:
            await adapter.async_upload_file("media/test.jpg", "/tmp/test.jpg")
            exists = await adapter.async_exists("media/test.jpg")
            await adapter.async_delete("media/test.jpg")

    Security:
        - Validates all paths to prevent directory traversal (../)
        - Rejects absolute paths in blob_path parameter
        - All operations confined to base_path directory
    """

    def __init__(self, base_path: Path, track_metadata: bool = True):
        """
        Initialize local storage adapter.

        Args:
            base_path: Root directory for all storage operations.
                      Will be created if it doesn't exist.
            track_metadata: If True, saves metadata (content_type, size, timestamps)
                          in sidecar .meta.json files. Useful for debugging.
        """
        self.base_path = Path(base_path).resolve()
        self.track_metadata = track_metadata

        # Lock for concurrent write safety (if needed)
        self._write_lock = asyncio.Lock()

        logger.info(
            "Initialized LocalStorageAdapter",
            extra={
                "base_path": str(self.base_path),
                "track_metadata": track_metadata,
            },
        )

    async def __aenter__(self):
        """Async context manager entry - ensure base directory exists."""
        await asyncio.to_thread(self.base_path.mkdir, parents=True, exist_ok=True)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - no cleanup needed for local storage."""
        return False

    def close(self) -> None:
        """Close the adapter (no-op for local storage, provided for API compatibility)."""
        pass

    def _validate_blob_path(self, blob_path: str) -> None:
        """
        Validate blob path is safe and doesn't escape base directory.

        Args:
            blob_path: Relative path to validate

        Raises:
            ValueError: If path is invalid or potentially dangerous
        """
        if os.path.isabs(blob_path):
            raise ValueError(
                f"Blob path must be relative, got absolute path: {blob_path}"
            )

        # Check for directory traversal attempts
        if ".." in blob_path:
            raise ValueError(f"Blob path cannot contain '..': {blob_path}")

        # Normalize path and verify it doesn't escape base
        normalized = (self.base_path / blob_path).resolve()
        try:
            normalized.relative_to(self.base_path)
        except ValueError:
            raise ValueError(f"Blob path escapes base directory: {blob_path}")

    def get_full_path(self, blob_path: str) -> Path:
        """
        Convert blob path to full filesystem path.

        Args:
            blob_path: Relative path within storage

        Returns:
            Full absolute path

        Raises:
            ValueError: If blob_path is invalid
        """
        self._validate_blob_path(blob_path)
        return self.base_path / blob_path

    async def async_upload_file(
        self,
        relative_path: str,
        local_path,  # str or Path
        overwrite: bool = True,
    ) -> str:
        """
        Upload file from local path to storage (async, non-blocking).

        Mimics OneLakeClient.async_upload_file() API.
        Uses atomic write pattern: write to .tmp file, then rename.

        Args:
            relative_path: Path relative to base_path (e.g. "media/test.jpg")
            local_path: Local file path to upload (str or pathlib.Path)
            overwrite: Whether to overwrite existing file (default: True)

        Returns:
            Full path to uploaded file (for OneLake compatibility,
            returns "file://{base_path}/{relative_path}")

        Raises:
            FileNotFoundError: If local_path doesn't exist
            FileExistsError: If file exists and overwrite=False
            ValueError: If relative_path is invalid
        """
        # Convert to Path if needed
        source_path = Path(local_path)

        # Validate source exists
        if not await asyncio.to_thread(source_path.exists):
            raise FileNotFoundError(f"Source file not found: {local_path}")

        # Get destination path and validate
        dest_path = self.get_full_path(relative_path)

        # Check if exists and overwrite is False
        if not overwrite and await asyncio.to_thread(dest_path.exists):
            raise FileExistsError(f"File already exists: {relative_path}")

        # Get file size for metadata
        file_size = await asyncio.to_thread(source_path.stat)
        file_size_bytes = file_size.st_size

        # Create parent directories
        await asyncio.to_thread(dest_path.parent.mkdir, parents=True, exist_ok=True)

        # Atomic write: write to temp file, then rename
        temp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")

        try:
            # Copy file to temp location
            await asyncio.to_thread(self._copy_file, source_path, temp_path)

            # Atomic rename
            await asyncio.to_thread(temp_path.rename, dest_path)

            logger.debug(
                "Uploaded file to local storage",
                extra={
                    "relative_path": relative_path,
                    "source_path": str(source_path),
                    "dest_path": str(dest_path),
                    "bytes": file_size_bytes,
                },
            )

            # Save metadata if enabled
            if self.track_metadata:
                await self._save_metadata(relative_path, None, file_size_bytes)

            # Return full path (mimic OneLake's return format)
            # OneLake returns abfss:// path, we return file:// path
            return f"file://{dest_path}"

        except Exception as e:
            # Clean up temp file on error
            if await asyncio.to_thread(temp_path.exists):
                await asyncio.to_thread(temp_path.unlink)
            raise

    def _copy_file(self, src: Path, dst: Path) -> None:
        """Copy file (sync helper for async_to_thread)."""
        import shutil

        shutil.copy2(src, dst)

    async def async_upload_bytes(
        self,
        relative_path: str,
        data: bytes,
        overwrite: bool = True,
        content_type: Optional[str] = None,
    ) -> str:
        """
        Upload bytes to storage (async, non-blocking).

        Mimics OneLakeClient.async_upload_bytes() API.
        Uses atomic write pattern: write to .tmp file, then rename.

        Args:
            relative_path: Path relative to base_path
            data: File content as bytes
            overwrite: Whether to overwrite existing file (default: True)
            content_type: MIME type (optional, saved in metadata)

        Returns:
            Full path to uploaded file

        Raises:
            FileExistsError: If file exists and overwrite=False
            ValueError: If relative_path is invalid
        """
        # Get destination path and validate
        dest_path = self.get_full_path(relative_path)

        # Check if exists and overwrite is False
        if not overwrite and await asyncio.to_thread(dest_path.exists):
            raise FileExistsError(f"File already exists: {relative_path}")

        # Create parent directories
        await asyncio.to_thread(dest_path.parent.mkdir, parents=True, exist_ok=True)

        # Atomic write: write to temp file, then rename
        temp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")

        try:
            # Write bytes to temp file
            await asyncio.to_thread(temp_path.write_bytes, data)

            # Atomic rename
            await asyncio.to_thread(temp_path.rename, dest_path)

            logger.debug(
                "Uploaded bytes to local storage",
                extra={
                    "relative_path": relative_path,
                    "dest_path": str(dest_path),
                    "bytes": len(data),
                    "content_type": content_type,
                },
            )

            # Save metadata if enabled
            if self.track_metadata:
                await self._save_metadata(relative_path, content_type, len(data))

            return f"file://{dest_path}"

        except Exception as e:
            # Clean up temp file on error
            if await asyncio.to_thread(temp_path.exists):
                await asyncio.to_thread(temp_path.unlink)
            raise

    async def async_download_bytes(self, relative_path: str) -> bytes:
        """
        Download file as bytes (async, non-blocking).

        Mimics OneLakeClient download behavior.

        Args:
            relative_path: Path relative to base_path

        Returns:
            File content as bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If relative_path is invalid
        """
        full_path = self.get_full_path(relative_path)

        if not await asyncio.to_thread(full_path.exists):
            raise FileNotFoundError(f"File not found: {relative_path}")

        data = await asyncio.to_thread(full_path.read_bytes)

        logger.debug(
            "Downloaded bytes from local storage",
            extra={
                "relative_path": relative_path,
                "bytes": len(data),
            },
        )

        return data

    async def async_exists(self, relative_path: str) -> bool:
        """
        Check if file exists (async, non-blocking).

        Args:
            relative_path: Path relative to base_path

        Returns:
            True if file exists, False otherwise
        """
        try:
            full_path = self.get_full_path(relative_path)
            exists = await asyncio.to_thread(full_path.exists)
            return exists
        except ValueError:
            # Invalid path
            return False

    async def async_delete(self, relative_path: str) -> bool:
        """
        Delete a file (async, non-blocking).

        Args:
            relative_path: Path relative to base_path

        Returns:
            True if deleted, False if didn't exist

        Raises:
            ValueError: If relative_path is invalid
        """
        full_path = self.get_full_path(relative_path)

        if not await asyncio.to_thread(full_path.exists):
            return False

        await asyncio.to_thread(full_path.unlink)

        # Also delete metadata file if it exists
        if self.track_metadata:
            metadata_path = full_path.with_suffix(full_path.suffix + ".meta.json")
            if await asyncio.to_thread(metadata_path.exists):
                await asyncio.to_thread(metadata_path.unlink)

        # Try to remove parent directory if empty
        parent = full_path.parent
        try:
            await asyncio.to_thread(parent.rmdir)
        except OSError:
            # Directory not empty or other error, ignore
            pass

        logger.debug(
            "Deleted file from local storage",
            extra={"relative_path": relative_path},
        )

        return True

    async def _save_metadata(
        self,
        relative_path: str,
        content_type: Optional[str],
        size: int,
    ) -> None:
        """
        Save blob metadata to sidecar JSON file.

        Args:
            relative_path: Path to blob
            content_type: MIME type (optional)
            size: File size in bytes
        """
        metadata = {
            "content_type": content_type,
            "size": size,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
            "relative_path": relative_path,
        }

        full_path = self.get_full_path(relative_path)
        metadata_path = full_path.with_suffix(full_path.suffix + ".meta.json")

        metadata_json = json.dumps(metadata, indent=2)
        await asyncio.to_thread(metadata_path.write_text, metadata_json)

    async def _load_metadata(self, relative_path: str) -> Optional[dict]:
        """
        Load blob metadata from sidecar JSON file.

        Args:
            relative_path: Path to blob

        Returns:
            Metadata dict or None if not found
        """
        full_path = self.get_full_path(relative_path)
        metadata_path = full_path.with_suffix(full_path.suffix + ".meta.json")

        if not await asyncio.to_thread(metadata_path.exists):
            return None

        metadata_json = await asyncio.to_thread(metadata_path.read_text)
        return json.loads(metadata_json)

    # Utility methods for debugging and inspection

    async def list_blobs(self, prefix: str = "") -> List[str]:
        """
        List all blobs with given prefix.

        Args:
            prefix: Prefix to filter by (e.g., "media/")

        Returns:
            List of relative paths (sorted)
        """
        search_path = self.base_path / prefix if prefix else self.base_path

        blobs = []

        def _scan_directory(path: Path) -> None:
            """Recursively scan directory for files."""
            if not path.exists():
                return

            for item in path.rglob("*"):
                if (
                    item.is_file()
                    and not item.name.endswith(".meta.json")
                    and not item.name.endswith(".tmp")
                ):
                    try:
                        relative = item.relative_to(self.base_path)
                        blobs.append(str(relative))
                    except ValueError:
                        # File outside base_path, skip
                        pass

        await asyncio.to_thread(_scan_directory, search_path)

        return sorted(blobs)

    async def get_blob_size(self, relative_path: str) -> int:
        """
        Get size of blob in bytes.

        Args:
            relative_path: Path to blob

        Returns:
            Size in bytes, or 0 if doesn't exist
        """
        try:
            full_path = self.get_full_path(relative_path)
            if not await asyncio.to_thread(full_path.exists):
                return 0

            stat = await asyncio.to_thread(full_path.stat)
            return stat.st_size
        except (ValueError, OSError):
            return 0

    async def get_storage_stats(self) -> dict:
        """
        Get storage statistics.

        Returns:
            Dict with total_files, total_bytes, base_path
        """
        blobs = await self.list_blobs()
        total_bytes = 0

        for blob in blobs:
            total_bytes += await self.get_blob_size(blob)

        return {
            "total_files": len(blobs),
            "total_bytes": total_bytes,
            "base_path": str(self.base_path),
        }


__all__ = ["LocalStorageAdapter"]
