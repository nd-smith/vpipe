#!/usr/bin/env python3
# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Example demonstrating LocalStorageAdapter usage.

This script shows how to use the LocalStorageAdapter as a drop-in
replacement for OneLakeClient in simulation mode.

Usage:
    python examples/local_storage_example.py
"""

import asyncio
import tempfile
from pathlib import Path

from kafka_pipeline.simulation.storage import LocalStorageAdapter


async def main():
    """Demonstrate LocalStorageAdapter functionality."""

    print("=" * 70)
    print("LocalStorageAdapter Example")
    print("=" * 70)
    print()

    # Create temporary directory for demonstration
    with tempfile.TemporaryDirectory() as temp_dir:
        storage_path = Path(temp_dir) / "simulation_storage"

        print(f"Storage base path: {storage_path}")
        print()

        # Create adapter with metadata tracking
        adapter = LocalStorageAdapter(
            base_path=storage_path,
            track_metadata=True,
        )

        async with adapter:
            print("1. Upload some test files")
            print("-" * 70)

            # Upload bytes
            await adapter.async_upload_bytes(
                "media/project_123/photo.jpg",
                b"fake image data (JPEG)",
                content_type="image/jpeg",
            )
            print("✅ Uploaded: media/project_123/photo.jpg")

            await adapter.async_upload_bytes(
                "media/project_123/document.pdf",
                b"fake PDF data",
                content_type="application/pdf",
            )
            print("✅ Uploaded: media/project_123/document.pdf")

            await adapter.async_upload_bytes(
                "attachments/assignment_456/report.docx",
                b"fake Word document data",
                content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
            print("✅ Uploaded: attachments/assignment_456/report.docx")

            print()

            # List all blobs
            print("2. List all uploaded files")
            print("-" * 70)
            blobs = await adapter.list_blobs()
            for blob in blobs:
                size = await adapter.get_blob_size(blob)
                print(f"  - {blob} ({size} bytes)")
            print()

            # List with prefix
            print("3. List files with prefix 'media/'")
            print("-" * 70)
            media_blobs = await adapter.list_blobs(prefix="media/")
            for blob in media_blobs:
                print(f"  - {blob}")
            print()

            # Check existence
            print("4. Check file existence")
            print("-" * 70)
            exists = await adapter.async_exists("media/project_123/photo.jpg")
            print(f"  media/project_123/photo.jpg exists: {exists}")

            exists = await adapter.async_exists("nonexistent/file.txt")
            print(f"  nonexistent/file.txt exists: {exists}")
            print()

            # Download file
            print("5. Download file")
            print("-" * 70)
            data = await adapter.async_download_bytes("media/project_123/photo.jpg")
            print(f"  Downloaded {len(data)} bytes")
            print(f"  Content: {data[:30]}...")
            print()

            # Get storage statistics
            print("6. Storage statistics")
            print("-" * 70)
            stats = await adapter.get_storage_stats()
            print(f"  Total files: {stats['total_files']}")
            print(f"  Total bytes: {stats['total_bytes']}")
            print(f"  Base path: {stats['base_path']}")
            print()

            # Show metadata file
            print("7. Metadata tracking")
            print("-" * 70)
            metadata = await adapter._load_metadata("media/project_123/photo.jpg")
            if metadata:
                print(f"  Content type: {metadata['content_type']}")
                print(f"  Size: {metadata['size']} bytes")
                print(f"  Uploaded at: {metadata['uploaded_at']}")
                print(f"  Relative path: {metadata['relative_path']}")
            print()

            # Overwrite file
            print("8. Overwrite existing file")
            print("-" * 70)
            await adapter.async_upload_bytes(
                "media/project_123/photo.jpg",
                b"updated image data (JPEG v2)",
                overwrite=True,
            )
            print("✅ Overwritten: media/project_123/photo.jpg")

            # Verify new content
            data = await adapter.async_download_bytes("media/project_123/photo.jpg")
            print(f"  New content: {data}")
            print()

            # Try to upload without overwrite (should fail)
            print("9. Try to upload without overwrite (should fail)")
            print("-" * 70)
            try:
                await adapter.async_upload_bytes(
                    "media/project_123/photo.jpg",
                    b"another update",
                    overwrite=False,
                )
                print("❌ Should have raised FileExistsError!")
            except FileExistsError as e:
                print(f"✅ Caught expected error: {e}")
            print()

            # Delete file
            print("10. Delete file")
            print("-" * 70)
            deleted = await adapter.async_delete("media/project_123/photo.jpg")
            print(f"  Deleted: {deleted}")

            exists = await adapter.async_exists("media/project_123/photo.jpg")
            print(f"  Still exists: {exists}")
            print()

            # Final statistics
            print("11. Final statistics")
            print("-" * 70)
            stats = await adapter.get_storage_stats()
            print(f"  Total files: {stats['total_files']}")
            print(f"  Total bytes: {stats['total_bytes']}")
            print()

            # Show directory structure
            print("12. Directory structure")
            print("-" * 70)
            print(f"  Base: {storage_path}")

            def print_tree(path: Path, indent: int = 2):
                """Print directory tree."""
                if not path.exists():
                    return

                for item in sorted(path.iterdir()):
                    rel = item.relative_to(storage_path)
                    prefix = " " * indent + "├── "

                    if item.is_dir():
                        print(f"{prefix}{item.name}/")
                        print_tree(item, indent + 2)
                    else:
                        size = item.stat().st_size
                        suffix = " (metadata)" if item.suffix == ".json" else ""
                        print(f"{prefix}{item.name} ({size} bytes){suffix}")

            print_tree(storage_path)
            print()

    print("=" * 70)
    print("Example complete!")
    print("=" * 70)
    print()
    print("Note: All files were in a temporary directory and have been cleaned up.")


if __name__ == "__main__":
    asyncio.run(main())
