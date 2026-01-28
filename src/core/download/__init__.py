"""
Async download module with clean interface.

Provides:
    - AttachmentDownloader: High-level interface (DownloadTask -> DownloadOutcome)
    - HTTP download with aiohttp (in-memory and streaming)
    - URL validation with SSRF prevention
    - File type validation (extension and MIME type)
    - Error classification for retry decisions

Components:
    - downloader: Unified AttachmentDownloader class
    - models: DownloadTask and DownloadOutcome data models
    - http_client: Low-level HTTP download (in-memory)
    - streaming: Streaming download for large files (>50MB)

Example usage:
    from core.download import AttachmentDownloader, DownloadTask

    downloader = AttachmentDownloader()
    task = DownloadTask(
        url="https://example.com/file.pdf",
        destination=Path("file.pdf")
    )
    outcome = await downloader.download(task)

    if outcome.success:
        print(f"Downloaded {outcome.bytes_downloaded} bytes")
    else:
        print(f"Failed: {outcome.error_message}")
"""

from core.download.downloader import AttachmentDownloader
from core.download.http_client import DownloadError, DownloadResponse, create_session, download_url
from core.download.models import DownloadOutcome, DownloadTask
from core.download.streaming import (
    CHUNK_SIZE,
    STREAM_THRESHOLD,
    StreamDownloadError,
    StreamDownloadResponse,
    download_to_file,
    should_stream,
    stream_download_url,
)

__all__ = [
    # High-level interface
    "AttachmentDownloader",
    "DownloadTask",
    "DownloadOutcome",
    # HTTP client
    "download_url",
    "create_session",
    "DownloadResponse",
    "DownloadError",
    # Streaming
    "stream_download_url",
    "download_to_file",
    "should_stream",
    "StreamDownloadResponse",
    "StreamDownloadError",
    "CHUNK_SIZE",
    "STREAM_THRESHOLD",
]
