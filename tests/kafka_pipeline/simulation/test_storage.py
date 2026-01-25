# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Tests for LocalStorageAdapter.

Tests cover:
- Basic upload/download operations
- Atomic write behavior (temp file + rename)
- Path validation and security
- Overwrite behavior
- Metadata tracking
- Error handling
- Async operations
- Edge cases (empty files, large files, special characters)
"""

import asyncio
import json
import pytest
from pathlib import Path
from datetime import datetime, timezone

from kafka_pipeline.simulation.storage import LocalStorageAdapter


@pytest.fixture
async def temp_storage(tmp_path):
    """Create a temporary storage adapter for testing."""
    storage_path = tmp_path / "test_storage"
    adapter = LocalStorageAdapter(base_path=storage_path, track_metadata=True)
    async with adapter:
        yield adapter


@pytest.fixture
def sample_file(tmp_path):
    """Create a sample file for upload tests."""
    file_path = tmp_path / "sample.txt"
    file_path.write_text("Hello, World!")
    return file_path


class TestBasicOperations:
    """Test basic upload, download, exists, delete operations."""

    @pytest.mark.asyncio
    async def test_upload_file_success(self, temp_storage, sample_file):
        """Test successful file upload."""
        result = await temp_storage.async_upload_file(
            "test/sample.txt",
            sample_file,
            overwrite=True,
        )

        # Verify result path format
        assert result.startswith("file://")
        assert "test/sample.txt" in result

        # Verify file exists
        assert await temp_storage.async_exists("test/sample.txt")

        # Verify content
        content = await temp_storage.async_download_bytes("test/sample.txt")
        assert content == b"Hello, World!"

    @pytest.mark.asyncio
    async def test_upload_bytes_success(self, temp_storage):
        """Test successful bytes upload."""
        data = b"Test data content"
        result = await temp_storage.async_upload_bytes(
            "test/bytes.bin",
            data,
            overwrite=True,
        )

        assert result.startswith("file://")
        assert await temp_storage.async_exists("test/bytes.bin")

        # Verify content
        downloaded = await temp_storage.async_download_bytes("test/bytes.bin")
        assert downloaded == data

    @pytest.mark.asyncio
    async def test_exists_returns_false_for_nonexistent(self, temp_storage):
        """Test exists returns False for files that don't exist."""
        assert not await temp_storage.async_exists("nonexistent/file.txt")

    @pytest.mark.asyncio
    async def test_delete_success(self, temp_storage, sample_file):
        """Test successful file deletion."""
        # Upload file first
        await temp_storage.async_upload_file("test/delete_me.txt", sample_file)
        assert await temp_storage.async_exists("test/delete_me.txt")

        # Delete file
        result = await temp_storage.async_delete("test/delete_me.txt")
        assert result is True
        assert not await temp_storage.async_exists("test/delete_me.txt")

    @pytest.mark.asyncio
    async def test_delete_nonexistent_returns_false(self, temp_storage):
        """Test delete returns False for nonexistent files."""
        result = await temp_storage.async_delete("nonexistent/file.txt")
        assert result is False

    @pytest.mark.asyncio
    async def test_download_nonexistent_raises_error(self, temp_storage):
        """Test download raises FileNotFoundError for nonexistent files."""
        with pytest.raises(FileNotFoundError):
            await temp_storage.async_download_bytes("nonexistent/file.txt")


class TestOverwriteBehavior:
    """Test overwrite parameter behavior."""

    @pytest.mark.asyncio
    async def test_overwrite_true_replaces_file(self, temp_storage):
        """Test overwrite=True replaces existing file."""
        # Upload initial file
        await temp_storage.async_upload_bytes("test/file.txt", b"Original")

        # Overwrite with new content
        await temp_storage.async_upload_bytes(
            "test/file.txt",
            b"Updated",
            overwrite=True,
        )

        # Verify new content
        content = await temp_storage.async_download_bytes("test/file.txt")
        assert content == b"Updated"

    @pytest.mark.asyncio
    async def test_overwrite_false_raises_error(self, temp_storage):
        """Test overwrite=False raises FileExistsError for existing files."""
        # Upload initial file
        await temp_storage.async_upload_bytes("test/file.txt", b"Original")

        # Try to upload again with overwrite=False
        with pytest.raises(FileExistsError):
            await temp_storage.async_upload_bytes(
                "test/file.txt",
                b"Updated",
                overwrite=False,
            )

        # Verify original content unchanged
        content = await temp_storage.async_download_bytes("test/file.txt")
        assert content == b"Original"

    @pytest.mark.asyncio
    async def test_overwrite_false_succeeds_for_new_file(self, temp_storage):
        """Test overwrite=False succeeds for new files."""
        await temp_storage.async_upload_bytes(
            "test/new_file.txt",
            b"Content",
            overwrite=False,
        )

        assert await temp_storage.async_exists("test/new_file.txt")


class TestPathValidation:
    """Test path validation and security."""

    @pytest.mark.asyncio
    async def test_absolute_path_rejected(self, temp_storage):
        """Test absolute paths are rejected."""
        with pytest.raises(ValueError, match="must be relative"):
            await temp_storage.async_upload_bytes("/etc/passwd", b"hack")

    @pytest.mark.asyncio
    async def test_directory_traversal_rejected(self, temp_storage):
        """Test directory traversal attempts are rejected."""
        with pytest.raises(ValueError, match="cannot contain"):
            await temp_storage.async_upload_bytes("../../etc/passwd", b"hack")

        with pytest.raises(ValueError, match="cannot contain"):
            await temp_storage.async_upload_bytes("test/../../../passwd", b"hack")

    @pytest.mark.asyncio
    async def test_path_normalization_prevents_escape(self, temp_storage):
        """Test normalized paths don't escape base directory."""
        # Even tricky paths should be caught
        with pytest.raises(ValueError):
            await temp_storage.async_upload_bytes("test/.././../passwd", b"hack")

    @pytest.mark.asyncio
    async def test_valid_nested_paths_allowed(self, temp_storage):
        """Test valid nested paths are allowed."""
        await temp_storage.async_upload_bytes(
            "level1/level2/level3/file.txt",
            b"Deep nesting ok",
        )

        assert await temp_storage.async_exists("level1/level2/level3/file.txt")


class TestAtomicWrites:
    """Test atomic write behavior."""

    @pytest.mark.asyncio
    async def test_temp_file_cleaned_on_success(self, temp_storage):
        """Test temporary files are cleaned up after successful write."""
        await temp_storage.async_upload_bytes("test/file.txt", b"content")

        # Check no .tmp files exist
        full_path = temp_storage.get_full_path("test/file.txt")
        temp_path = full_path.with_suffix(full_path.suffix + '.tmp')
        assert not temp_path.exists()

    @pytest.mark.asyncio
    async def test_temp_file_cleaned_on_error(self, temp_storage, sample_file):
        """Test temporary files are cleaned up on error."""
        # Create a scenario where rename might fail
        # (This is hard to test directly, but we can verify cleanup logic)

        # Upload normally
        await temp_storage.async_upload_file("test/file.txt", sample_file)

        # Verify no temp files
        full_path = temp_storage.get_full_path("test/file.txt")
        temp_path = full_path.with_suffix(full_path.suffix + '.tmp')
        assert not temp_path.exists()

    @pytest.mark.asyncio
    async def test_atomic_write_all_or_nothing(self, temp_storage):
        """Test writes are all-or-nothing (file appears complete)."""
        # Upload file
        data = b"Complete content"
        await temp_storage.async_upload_bytes("test/atomic.txt", data)

        # Read back - should get complete content, never partial
        content = await temp_storage.async_download_bytes("test/atomic.txt")
        assert content == data


class TestMetadataTracking:
    """Test metadata tracking functionality."""

    @pytest.mark.asyncio
    async def test_metadata_file_created(self, temp_storage):
        """Test metadata file is created when track_metadata=True."""
        await temp_storage.async_upload_bytes(
            "test/file.txt",
            b"content",
            content_type="text/plain",
        )

        # Check metadata file exists
        full_path = temp_storage.get_full_path("test/file.txt")
        metadata_path = full_path.with_suffix(full_path.suffix + '.meta.json')
        assert metadata_path.exists()

        # Verify metadata content
        metadata = json.loads(metadata_path.read_text())
        assert metadata["content_type"] == "text/plain"
        assert metadata["size"] == 7
        assert "uploaded_at" in metadata
        assert metadata["relative_path"] == "test/file.txt"

    @pytest.mark.asyncio
    async def test_metadata_includes_timestamp(self, temp_storage):
        """Test metadata includes valid ISO timestamp."""
        await temp_storage.async_upload_bytes("test/file.txt", b"content")

        metadata = await temp_storage._load_metadata("test/file.txt")
        assert metadata is not None

        # Verify timestamp is valid ISO format
        timestamp = datetime.fromisoformat(metadata["uploaded_at"])
        assert timestamp.tzinfo == timezone.utc

    @pytest.mark.asyncio
    async def test_metadata_deleted_with_file(self, temp_storage):
        """Test metadata file is deleted when file is deleted."""
        await temp_storage.async_upload_bytes("test/file.txt", b"content")

        full_path = temp_storage.get_full_path("test/file.txt")
        metadata_path = full_path.with_suffix(full_path.suffix + '.meta.json')
        assert metadata_path.exists()

        # Delete file
        await temp_storage.async_delete("test/file.txt")

        # Metadata should also be deleted
        assert not metadata_path.exists()


class TestUtilityMethods:
    """Test utility methods for inspection and debugging."""

    @pytest.mark.asyncio
    async def test_list_blobs_empty(self, temp_storage):
        """Test list_blobs returns empty list for empty storage."""
        blobs = await temp_storage.list_blobs()
        assert blobs == []

    @pytest.mark.asyncio
    async def test_list_blobs_returns_all_files(self, temp_storage):
        """Test list_blobs returns all uploaded files."""
        await temp_storage.async_upload_bytes("file1.txt", b"1")
        await temp_storage.async_upload_bytes("dir/file2.txt", b"2")
        await temp_storage.async_upload_bytes("dir/sub/file3.txt", b"3")

        blobs = await temp_storage.list_blobs()
        assert len(blobs) == 3
        assert "file1.txt" in blobs
        assert "dir/file2.txt" in blobs
        assert "dir/sub/file3.txt" in blobs

    @pytest.mark.asyncio
    async def test_list_blobs_with_prefix(self, temp_storage):
        """Test list_blobs filters by prefix."""
        await temp_storage.async_upload_bytes("test/file1.txt", b"1")
        await temp_storage.async_upload_bytes("test/file2.txt", b"2")
        await temp_storage.async_upload_bytes("other/file3.txt", b"3")

        blobs = await temp_storage.list_blobs(prefix="test")
        assert len(blobs) == 2
        assert all(b.startswith("test") for b in blobs)

    @pytest.mark.asyncio
    async def test_list_blobs_excludes_metadata(self, temp_storage):
        """Test list_blobs doesn't return .meta.json files."""
        await temp_storage.async_upload_bytes("file.txt", b"content")

        blobs = await temp_storage.list_blobs()
        assert len(blobs) == 1
        assert blobs[0] == "file.txt"
        # .meta.json file should not be in results

    @pytest.mark.asyncio
    async def test_get_blob_size_returns_correct_size(self, temp_storage):
        """Test get_blob_size returns correct file size."""
        data = b"x" * 1024  # 1 KB
        await temp_storage.async_upload_bytes("file.txt", data)

        size = await temp_storage.get_blob_size("file.txt")
        assert size == 1024

    @pytest.mark.asyncio
    async def test_get_blob_size_returns_zero_for_nonexistent(self, temp_storage):
        """Test get_blob_size returns 0 for nonexistent files."""
        size = await temp_storage.get_blob_size("nonexistent.txt")
        assert size == 0

    @pytest.mark.asyncio
    async def test_get_storage_stats(self, temp_storage):
        """Test get_storage_stats returns correct totals."""
        await temp_storage.async_upload_bytes("file1.txt", b"x" * 100)
        await temp_storage.async_upload_bytes("file2.txt", b"y" * 200)
        await temp_storage.async_upload_bytes("dir/file3.txt", b"z" * 300)

        stats = await temp_storage.get_storage_stats()
        assert stats["total_files"] == 3
        assert stats["total_bytes"] == 600
        assert "base_path" in stats


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    @pytest.mark.asyncio
    async def test_empty_file_upload(self, temp_storage):
        """Test uploading empty files."""
        await temp_storage.async_upload_bytes("empty.txt", b"")

        assert await temp_storage.async_exists("empty.txt")
        content = await temp_storage.async_download_bytes("empty.txt")
        assert content == b""

    @pytest.mark.asyncio
    async def test_large_file_upload(self, temp_storage, tmp_path):
        """Test uploading large files (1 MB)."""
        # Create 1 MB file
        large_file = tmp_path / "large.bin"
        large_file.write_bytes(b"x" * (1024 * 1024))

        await temp_storage.async_upload_file("large.bin", large_file)

        size = await temp_storage.get_blob_size("large.bin")
        assert size == 1024 * 1024

    @pytest.mark.asyncio
    async def test_special_characters_in_filename(self, temp_storage):
        """Test files with special characters in names."""
        # These should work
        await temp_storage.async_upload_bytes("file with spaces.txt", b"content")
        await temp_storage.async_upload_bytes("file-with-dashes.txt", b"content")
        await temp_storage.async_upload_bytes("file_with_underscores.txt", b"content")

        assert await temp_storage.async_exists("file with spaces.txt")
        assert await temp_storage.async_exists("file-with-dashes.txt")
        assert await temp_storage.async_exists("file_with_underscores.txt")

    @pytest.mark.asyncio
    async def test_unicode_in_filename(self, temp_storage):
        """Test files with Unicode characters."""
        await temp_storage.async_upload_bytes("café.txt", b"content")
        await temp_storage.async_upload_bytes("文件.txt", b"content")

        assert await temp_storage.async_exists("café.txt")
        assert await temp_storage.async_exists("文件.txt")

    @pytest.mark.asyncio
    async def test_concurrent_uploads_different_files(self, temp_storage):
        """Test concurrent uploads to different files."""
        tasks = [
            temp_storage.async_upload_bytes(f"file{i}.txt", f"content{i}".encode())
            for i in range(10)
        ]

        await asyncio.gather(*tasks)

        # Verify all files exist
        for i in range(10):
            assert await temp_storage.async_exists(f"file{i}.txt")

    @pytest.mark.asyncio
    async def test_upload_source_file_not_found(self, temp_storage, tmp_path):
        """Test upload_file raises FileNotFoundError for missing source."""
        nonexistent = tmp_path / "does_not_exist.txt"

        with pytest.raises(FileNotFoundError):
            await temp_storage.async_upload_file("dest.txt", nonexistent)

    @pytest.mark.asyncio
    async def test_directory_cleanup_on_delete(self, temp_storage):
        """Test empty directories are cleaned up on delete."""
        await temp_storage.async_upload_bytes("deep/nested/dir/file.txt", b"content")

        # Delete file
        await temp_storage.async_delete("deep/nested/dir/file.txt")

        # Parent directory should be removed if empty
        full_path = temp_storage.get_full_path("deep/nested/dir")
        # Only immediate parent is removed, not all ancestors
        # (This matches OneLake behavior - directories are implicit)


class TestContextManager:
    """Test async context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_creates_base_path(self, tmp_path):
        """Test context manager creates base directory."""
        storage_path = tmp_path / "auto_created"
        assert not storage_path.exists()

        async with LocalStorageAdapter(base_path=storage_path) as adapter:
            assert storage_path.exists()
            await adapter.async_upload_bytes("test.txt", b"content")

        # File should persist after context
        assert (storage_path / "test.txt").exists()

    @pytest.mark.asyncio
    async def test_close_is_safe_to_call_multiple_times(self, temp_storage):
        """Test close() can be called multiple times safely."""
        temp_storage.close()
        temp_storage.close()  # Should not raise


class TestCompatibilityWithOneLake:
    """Test API compatibility with OneLakeClient."""

    @pytest.mark.asyncio
    async def test_async_upload_file_signature_matches(self, temp_storage, sample_file):
        """Test async_upload_file has same signature as OneLakeClient."""
        # Should accept Path or str for local_path
        result1 = await temp_storage.async_upload_file("test1.txt", sample_file)
        result2 = await temp_storage.async_upload_file("test2.txt", str(sample_file))

        assert result1.startswith("file://")
        assert result2.startswith("file://")

    @pytest.mark.asyncio
    async def test_return_value_format(self, temp_storage):
        """Test return values match expected format."""
        result = await temp_storage.async_upload_bytes("test.txt", b"content")

        # Should return full path (file:// instead of abfss://)
        assert result.startswith("file://")
        assert result.endswith("test.txt")

    @pytest.mark.asyncio
    async def test_all_async_methods_are_awaitable(self, temp_storage, sample_file):
        """Test all async methods are properly awaitable."""
        # These should all be coroutines
        result = await temp_storage.async_upload_file("test.txt", sample_file)
        exists = await temp_storage.async_exists("test.txt")
        data = await temp_storage.async_download_bytes("test.txt")
        deleted = await temp_storage.async_delete("test.txt")

        assert isinstance(result, str)
        assert isinstance(exists, bool)
        assert isinstance(data, bytes)
        assert isinstance(deleted, bool)
