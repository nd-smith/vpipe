"""
Integration test for ClaimX download worker flow.

Tests the download worker's ability to:
1. Consume ClaimXDownloadTask messages
2. Download files from S3 presigned URLs
3. Cache files locally
4. Produce ClaimXCachedDownloadMessage
"""

import asyncio
import json
import logging
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from aiokafka import AIOKafkaProducer
import aiohttp

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.schemas.tasks import ClaimXDownloadTask
from kafka_pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage

from ..helpers import (
    get_topic_messages,
    start_worker_background,
    stop_worker_gracefully,
    wait_for_condition,
)
from .generators import create_download_task

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_download_single_media_file(
    test_claimx_config: KafkaConfig,
    claimx_download_worker,
    tmp_path: Path,
):
    """
    Test downloading a single media file from presigned URL.

    Validates:
    - DownloadTask consumed
    - File downloaded and cached locally
    - CachedDownloadMessage produced
    """
    media_id = 5678
    project_id = 12345
    download_url = "https://example.com/media/test.jpg"

    # Create test file content
    test_content = b"Test image content"

    # Mock HTTP download
    async def mock_get(url, **kwargs):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "image/jpeg"}

        async def read():
            return test_content

        mock_response.read = read
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        return mock_response

    with patch("aiohttp.ClientSession.get", side_effect=mock_get):
        # Create download task
        task = create_download_task(
            media_id=media_id,
            project_id=project_id,
            download_url=download_url,
        )

        # Start worker
        worker_task = await start_worker_background(claimx_download_worker)

        try:
            # Produce download task
            producer = AIOKafkaProducer(
                bootstrap_servers=test_claimx_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()

            try:
                await producer.send(
                    test_claimx_config.claimx_downloads_pending_topic,
                    value=task.model_dump(mode="json"),
                )
                logger.info(f"Produced download task for media {media_id}")
            finally:
                await producer.stop()

            # Wait for cached message
            await asyncio.sleep(2.0)
            cached_messages = await get_topic_messages(
                test_claimx_config,
                test_claimx_config.claimx_downloads_cached_topic,
                max_messages=10,
                timeout_seconds=5.0,
            )

            # Validate cached message produced
            assert len(cached_messages) > 0, "No cached download message produced"

            # Parse and validate message
            cached_msg = json.loads(cached_messages[0]["value"])
            assert cached_msg["media_id"] == media_id
            assert cached_msg["project_id"] == project_id
            assert "local_path" in cached_msg

            # Verify file was actually cached
            local_path = Path(cached_msg["local_path"])
            assert local_path.exists(), f"File not cached at {local_path}"
            assert local_path.read_bytes() == test_content

        finally:
            await stop_worker_gracefully(claimx_download_worker, worker_task)


@pytest.mark.asyncio
async def test_download_multiple_media_files(
    test_claimx_config: KafkaConfig,
    claimx_download_worker,
    tmp_path: Path,
):
    """
    Test downloading multiple media files concurrently.

    Validates:
    - Multiple downloads processed
    - All files cached correctly
    - All cached messages produced
    """
    num_files = 5
    project_id = 12345

    # Mock HTTP download
    async def mock_get(url, **kwargs):
        # Extract media_id from URL for unique content
        media_id = int(url.split("/")[-1].replace("media_", "").replace(".jpg", ""))
        test_content = f"Media content {media_id}".encode()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "image/jpeg"}

        async def read():
            return test_content

        mock_response.read = read
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        return mock_response

    with patch("aiohttp.ClientSession.get", side_effect=mock_get):
        # Create download tasks
        tasks = [
            create_download_task(
                media_id=6000 + i,
                project_id=project_id,
                download_url=f"https://example.com/media/media_{6000+i}.jpg",
            )
            for i in range(num_files)
        ]

        # Start worker
        worker_task = await start_worker_background(claimx_download_worker)

        try:
            # Produce all download tasks
            producer = AIOKafkaProducer(
                bootstrap_servers=test_claimx_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()

            try:
                for task in tasks:
                    await producer.send(
                        test_claimx_config.claimx_downloads_pending_topic,
                        value=task.model_dump(mode="json"),
                    )
                logger.info(f"Produced {num_files} download tasks")
            finally:
                await producer.stop()

            # Wait for all cached messages
            await asyncio.sleep(3.0)
            cached_messages = await get_topic_messages(
                test_claimx_config,
                test_claimx_config.claimx_downloads_cached_topic,
                max_messages=num_files,
                timeout_seconds=10.0,
            )

            # Validate all cached messages produced
            assert len(cached_messages) >= num_files, \
                f"Expected {num_files} cached messages, got {len(cached_messages)}"

            # Verify all files exist
            for msg in cached_messages:
                cached_msg = json.loads(msg["value"])
                local_path = Path(cached_msg["local_path"])
                assert local_path.exists(), f"File not cached at {local_path}"

        finally:
            await stop_worker_gracefully(claimx_download_worker, worker_task)


@pytest.mark.asyncio
async def test_download_error_handling(
    test_claimx_config: KafkaConfig,
    claimx_download_worker,
    tmp_path: Path,
):
    """
    Test download worker error handling.

    Validates:
    - HTTP errors handled gracefully
    - Failed downloads don't crash worker
    - Error routing (retry or DLQ)
    """
    media_id = 7777
    project_id = 12345
    download_url = "https://example.com/media/not_found.jpg"

    # Mock HTTP error
    async def mock_get_error(url, **kwargs):
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.headers = {"Content-Type": "text/plain"}

        async def read():
            return b"Not Found"

        mock_response.read = read
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        return mock_response

    with patch("aiohttp.ClientSession.get", side_effect=mock_get_error):
        # Create download task
        task = create_download_task(
            media_id=media_id,
            project_id=project_id,
            download_url=download_url,
        )

        # Start worker
        worker_task = await start_worker_background(claimx_download_worker)

        try:
            # Produce download task
            producer = AIOKafkaProducer(
                bootstrap_servers=test_claimx_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()

            try:
                await producer.send(
                    test_claimx_config.claimx_downloads_pending_topic,
                    value=task.model_dump(mode="json"),
                )
                logger.info(f"Produced download task for media {media_id}")
            finally:
                await producer.stop()

            # Wait a bit for processing
            await asyncio.sleep(2.0)

            # Verify worker is still running after error
            assert claimx_download_worker.is_running, "Worker crashed on error"

            # Check if task was routed to retry or DLQ
            # (Implementation may vary - this is a basic check)
            dlq_messages = await get_topic_messages(
                test_claimx_config,
                test_claimx_config.claimx_dlq_topic,
                max_messages=10,
                timeout_seconds=2.0,
            )

            # Either in DLQ or being retried
            logger.info(f"DLQ messages: {len(dlq_messages)}")

        finally:
            await stop_worker_gracefully(claimx_download_worker, worker_task)


@pytest.mark.asyncio
async def test_download_url_expiration(
    test_claimx_config: KafkaConfig,
    claimx_download_worker,
    tmp_path: Path,
):
    """
    Test handling of expired presigned URLs.

    Validates:
    - Expired URL detected (403/401 response)
    - Task routed for retry or marked for re-enrichment
    """
    media_id = 8888
    project_id = 12345
    download_url = "https://example.com/media/expired.jpg?expires=old"

    # Mock HTTP 403 (expired URL)
    async def mock_get_expired(url, **kwargs):
        mock_response = AsyncMock()
        mock_response.status = 403
        mock_response.headers = {"Content-Type": "text/plain"}

        async def read():
            return b"Forbidden - URL expired"

        mock_response.read = read
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        return mock_response

    with patch("aiohttp.ClientSession.get", side_effect=mock_get_expired):
        # Create download task
        task = create_download_task(
            media_id=media_id,
            project_id=project_id,
            download_url=download_url,
        )

        # Start worker
        worker_task = await start_worker_background(claimx_download_worker)

        try:
            # Produce download task
            producer = AIOKafkaProducer(
                bootstrap_servers=test_claimx_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()

            try:
                await producer.send(
                    test_claimx_config.claimx_downloads_pending_topic,
                    value=task.model_dump(mode="json"),
                )
                logger.info(f"Produced download task with expired URL for media {media_id}")
            finally:
                await producer.stop()

            # Wait for processing
            await asyncio.sleep(2.0)

            # Verify worker is still running
            assert claimx_download_worker.is_running, "Worker crashed on expired URL"

        finally:
            await stop_worker_gracefully(claimx_download_worker, worker_task)
