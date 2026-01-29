"""
Integration tests for LocalStorageAdapter with realistic upload scenarios.

These tests simulate realistic upload worker workflows to verify
the adapter works correctly in real-world scenarios.
"""

import asyncio
import pytest
from pathlib import Path

from kafka_pipeline.simulation.storage import LocalStorageAdapter


@pytest.fixture
async def storage(tmp_path):
    """Create storage adapter for integration tests with unique path per test."""
    # Create unique subdirectory for each test to prevent interference
    import uuid
    unique_path = tmp_path / f"storage_{uuid.uuid4().hex[:8]}"
    adapter = LocalStorageAdapter(base_path=unique_path, track_metadata=True)
    async with adapter:
        yield adapter


@pytest.fixture
def sample_files(tmp_path):
    """Create sample files simulating download worker output."""
    files_dir = tmp_path / "cache"
    files_dir.mkdir()

    # Simulate various file types
    files = {
        "photo.jpg": b"\xFF\xD8\xFF\xE0" + b"x" * 1000,  # JPEG marker + data
        "document.pdf": b"%PDF-1.4" + b"x" * 2000,  # PDF marker + data
        "video.mp4": b"\x00\x00\x00\x20ftypmp42" + b"x" * 5000,  # MP4 marker + data
        "archive.zip": b"PK\x03\x04" + b"x" * 3000,  # ZIP marker + data
    }

    created_files = {}
    for name, content in files.items():
        file_path = files_dir / name
        file_path.write_bytes(content)
        created_files[name] = file_path

    return created_files


class TestRealisticUploadScenarios:
    """Test realistic upload worker scenarios."""

    @pytest.mark.asyncio
    async def test_claimx_media_upload_workflow(self, storage, sample_files):
        """
        Test ClaimX media upload workflow.

        Simulates:
        1. Download worker caches files locally
        2. Upload worker uploads to storage
        3. Upload worker cleans up cache
        """
        # Simulate download worker output
        project_id = "P12345"
        media_id = "M67890"

        # Upload photo
        photo_path = f"claimx/media/{project_id}/{media_id}_photo.jpg"
        result = await storage.async_upload_file(
            photo_path,
            sample_files["photo.jpg"],
            overwrite=True,
        )

        assert result.startswith("file://")
        assert photo_path in result

        # Verify upload
        assert await storage.async_exists(photo_path)

        # Verify content
        downloaded = await storage.async_download_bytes(photo_path)
        original = sample_files["photo.jpg"].read_bytes()
        assert downloaded == original

        # Simulate cache cleanup
        # (In real worker, this would delete the local cache file)
        # Here we just verify we can delete from storage
        await storage.async_delete(photo_path)
        assert not await storage.async_exists(photo_path)

    @pytest.mark.asyncio
    async def test_xact_attachment_upload_workflow(self, storage, sample_files):
        """
        Test XACT attachment upload workflow.

        Simulates:
        1. Multiple attachments for same assignment
        2. Different file types
        3. Proper path organization
        """
        assignment_id = "A98765"

        # Upload multiple attachments
        attachments = [
            (f"xact/attachments/{assignment_id}/document_1.pdf", "document.pdf"),
            (f"xact/attachments/{assignment_id}/archive_1.zip", "archive.zip"),
            (f"xact/attachments/{assignment_id}/video_1.mp4", "video.mp4"),
        ]

        for dest_path, source_name in attachments:
            await storage.async_upload_file(
                dest_path,
                sample_files[source_name],
                overwrite=True,
            )

        # Verify all uploaded
        for dest_path, _ in attachments:
            assert await storage.async_exists(dest_path)

        # List all attachments for this assignment
        blobs = await storage.list_blobs(prefix=f"xact/attachments/{assignment_id}")
        assert len(blobs) == 3

        # Verify files are in correct directory
        for blob in blobs:
            assert blob.startswith(f"xact/attachments/{assignment_id}/")

    @pytest.mark.asyncio
    async def test_concurrent_uploads_different_projects(self, storage, sample_files):
        """
        Test concurrent uploads for different projects.

        Simulates:
        - Multiple upload workers running in parallel
        - Each uploading to different projects
        - No interference between uploads
        """
        async def upload_project(project_id: int):
            """Upload media for a single project."""
            photo_path = f"claimx/media/P{project_id}/M{project_id}_photo.jpg"
            await storage.async_upload_file(
                photo_path,
                sample_files["photo.jpg"],
                overwrite=True,
            )
            return photo_path

        # Upload 10 projects concurrently
        tasks = [upload_project(i) for i in range(10)]
        uploaded_paths = await asyncio.gather(*tasks)

        # Verify all uploaded successfully
        for path in uploaded_paths:
            assert await storage.async_exists(path)

        # Verify each project has its own file
        for i in range(10):
            blobs = await storage.list_blobs(prefix=f"claimx/media/P{i}")
            assert len(blobs) == 1

    @pytest.mark.asyncio
    async def test_upload_retry_with_overwrite(self, storage, sample_files):
        """
        Test upload retry scenario with overwrite.

        Simulates:
        - Initial upload succeeds
        - Worker crashes or retries
        - Retry succeeds due to overwrite=True
        """
        path = "claimx/media/P123/M456_photo.jpg"

        # Initial upload
        await storage.async_upload_file(path, sample_files["photo.jpg"], overwrite=True)

        # Simulate retry (should succeed with overwrite=True)
        await storage.async_upload_file(path, sample_files["photo.jpg"], overwrite=True)

        # Verify file still exists
        assert await storage.async_exists(path)

    @pytest.mark.asyncio
    async def test_failed_upload_cleanup(self, storage, tmp_path):
        """
        Test cleanup of failed uploads.

        Simulates:
        - Upload starts
        - Source file doesn't exist (error)
        - No partial files left behind
        """
        path = "claimx/media/P123/M456_photo.jpg"
        nonexistent = tmp_path / "nonexistent.jpg"

        # Try to upload nonexistent file
        with pytest.raises(FileNotFoundError):
            await storage.async_upload_file(path, nonexistent)

        # Verify no partial upload
        assert not await storage.async_exists(path)

        # Verify no temp files
        full_path = storage.get_full_path(path)
        if full_path.parent.exists():
            temp_files = list(full_path.parent.glob("*.tmp"))
            assert len(temp_files) == 0

    @pytest.mark.asyncio
    async def test_storage_organization_matches_onelake(self, storage, sample_files):
        """
        Test storage organization matches OneLake structure.

        Verifies:
        - Files organized by domain (claimx, xact)
        - Subdirectories created correctly
        - Path structure matches production
        """
        # Upload files for different domains
        uploads = [
            ("claimx/media/P1/M1_photo.jpg", "photo.jpg"),
            ("claimx/media/P2/M2_photo.jpg", "photo.jpg"),
            ("xact/attachments/A1/doc1.pdf", "document.pdf"),
            ("xact/attachments/A2/doc2.pdf", "document.pdf"),
        ]

        for dest, source in uploads:
            await storage.async_upload_file(dest, sample_files[source])

        # Verify domain separation
        claimx_files = await storage.list_blobs(prefix="claimx/")
        xact_files = await storage.list_blobs(prefix="xact/")

        assert len(claimx_files) == 2
        assert len(xact_files) == 2

        # Verify all claimx files start with claimx/
        assert all(f.startswith("claimx/") for f in claimx_files)

        # Verify all xact files start with xact/
        assert all(f.startswith("xact/") for f in xact_files)

    @pytest.mark.asyncio
    async def test_metadata_tracking_for_debugging(self, storage, sample_files):
        """
        Test metadata tracking helps with debugging.

        Verifies:
        - Metadata contains useful debugging info
        - Can track upload timing
        - Can verify file sizes
        """
        path = "claimx/media/P123/M456_photo.jpg"

        # Upload with metadata
        await storage.async_upload_file(path, sample_files["photo.jpg"])

        # Load metadata
        metadata = await storage._load_metadata(path)
        assert metadata is not None

        # Verify debugging information
        assert metadata["relative_path"] == path
        assert metadata["size"] > 0
        assert "uploaded_at" in metadata

        # Verify timestamp is recent (within last minute)
        from datetime import datetime, timezone, timedelta

        uploaded_at = datetime.fromisoformat(metadata["uploaded_at"])
        now = datetime.now(timezone.utc)
        assert (now - uploaded_at) < timedelta(minutes=1)

    @pytest.mark.asyncio
    async def test_batch_upload_and_cleanup(self, storage, sample_files):
        """
        Test batch upload and cleanup workflow.

        Simulates:
        - Upload worker processes batch of messages
        - Uploads all files successfully
        - Cleans up all files at end
        """
        # Simulate batch of 5 uploads
        batch = [
            f"claimx/media/P123/M{i}_photo.jpg" for i in range(5)
        ]

        # Upload batch
        for path in batch:
            await storage.async_upload_file(
                path,
                sample_files["photo.jpg"],
                overwrite=True,
            )

        # Verify all uploaded
        for path in batch:
            assert await storage.async_exists(path)

        # Get stats before cleanup
        stats_before = await storage.get_storage_stats()
        assert stats_before["total_files"] == 5

        # Cleanup batch
        for path in batch:
            await storage.async_delete(path)

        # Verify all cleaned up
        for path in batch:
            assert not await storage.async_exists(path)

        # Get stats after cleanup
        stats_after = await storage.get_storage_stats()
        assert stats_after["total_files"] == 0


class TestEdgeCasesInProduction:
    """Test edge cases that might occur in production."""

    @pytest.mark.asyncio
    async def test_very_long_file_path(self, storage, sample_files):
        """Test handling of very long file paths."""
        # Create a very long but valid path
        long_path = "claimx/media/" + "/".join([f"dir{i}" for i in range(20)]) + "/file.jpg"

        await storage.async_upload_file(long_path, sample_files["photo.jpg"])
        assert await storage.async_exists(long_path)

    @pytest.mark.asyncio
    async def test_special_characters_in_path(self, storage, sample_files):
        """Test handling of special characters in paths."""
        # These should work
        special_paths = [
            "claimx/media/P-123/M_456_photo.jpg",
            "claimx/media/P.123/M.456.photo.jpg",
            "claimx/media/P (123)/M [456] photo.jpg",
        ]

        for path in special_paths:
            await storage.async_upload_file(path, sample_files["photo.jpg"])
            assert await storage.async_exists(path)

    @pytest.mark.asyncio
    async def test_empty_file_upload(self, storage, tmp_path):
        """Test uploading empty files (0 bytes)."""
        empty_file = tmp_path / "empty.txt"
        empty_file.write_bytes(b"")

        path = "claimx/media/P123/empty.txt"
        await storage.async_upload_file(path, empty_file)

        # Verify uploaded
        assert await storage.async_exists(path)

        # Verify size is 0
        size = await storage.get_blob_size(path)
        assert size == 0

        # Verify can download
        data = await storage.async_download_bytes(path)
        assert data == b""

    @pytest.mark.asyncio
    async def test_large_batch_statistics(self, storage, sample_files):
        """Test storage statistics with large number of files."""
        # Upload 100 files
        for i in range(100):
            path = f"claimx/media/P{i}/M{i}_photo.jpg"
            await storage.async_upload_file(path, sample_files["photo.jpg"])

        # Get statistics
        stats = await storage.get_storage_stats()
        assert stats["total_files"] == 100

        # Calculate expected total bytes
        file_size = len(sample_files["photo.jpg"].read_bytes())
        assert stats["total_bytes"] == file_size * 100
