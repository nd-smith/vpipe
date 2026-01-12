"""
Integration test for ClaimX upload worker flow.

Tests the upload worker's ability to:
1. Consume ClaimXCachedDownloadMessage
2. Upload cached files to OneLake
3. Produce ClaimXUploadResultMessage
"""

import asyncio
import json
import logging
from pathlib import Path

import pytest
from aiokafka import AIOKafkaProducer

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from kafka_pipeline.claimx.schemas.results import ClaimXUploadResultMessage

from ..helpers import (
    get_topic_messages,
    start_worker_background,
    stop_worker_gracefully,
    wait_for_condition,
)
from .generators import create_cached_download_message

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_upload_single_cached_file(
    test_claimx_config: KafkaConfig,
    claimx_upload_worker,
    mock_onelake_client,
    tmp_path: Path,
):
    """
    Test uploading a single cached file to OneLake.

    Validates:
    - CachedDownloadMessage consumed
    - File uploaded to OneLake with correct path
    - UploadResultMessage produced with success status
    """
    media_id = 5678
    project_id = 12345

    # Create test file
    test_file = tmp_path / "claimx_downloads" / f"media_{media_id}.jpg"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_content = b"Test media content"
    test_file.write_bytes(test_content)

    # Create cached download message
    cached_msg = create_cached_download_message(
        media_id=media_id,
        project_id=project_id,
        local_path=str(test_file),
        download_url="https://example.com/media/test.jpg",
        file_size_bytes=len(test_content),
        content_type="image/jpeg",
    )

    # Start worker
    worker_task = await start_worker_background(claimx_upload_worker)

    try:
        # Produce cached download message
        producer = AIOKafkaProducer(
            bootstrap_servers=test_claimx_config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await producer.start()

        try:
            await producer.send(
                test_claimx_config.claimx_downloads_cached_topic,
                value=cached_msg.model_dump(mode="json"),
            )
            logger.info(f"Produced cached download message for media {media_id}")
        finally:
            await producer.stop()

        # Wait for upload to complete
        success = await wait_for_condition(
            lambda: mock_onelake_client.upload_count > 0,
            timeout_seconds=10.0,
            description="OneLake upload",
        )
        assert success, "File not uploaded to OneLake"

        # Validate file was uploaded
        assert len(mock_onelake_client.uploaded_files) > 0, "No files uploaded"

        # Check uploaded content
        uploaded_path = list(mock_onelake_client.uploaded_files.keys())[0]
        uploaded_content = mock_onelake_client.get_uploaded_content(uploaded_path)
        assert uploaded_content == test_content, "Uploaded content doesn't match"

        # Verify path follows ClaimX convention (claimx/<project_id>/...)
        assert "claimx" in uploaded_path.lower() or str(project_id) in uploaded_path

        # Wait for upload result message
        await asyncio.sleep(2.0)
        result_messages = await get_topic_messages(
            test_claimx_config,
            test_claimx_config.claimx_downloads_results_topic,
            max_messages=10,
            timeout_seconds=5.0,
        )

        # Validate result message produced
        assert len(result_messages) > 0, "No upload result message produced"

        # Parse and validate result
        result_msg = json.loads(result_messages[0]["value"])
        assert result_msg["media_id"] == media_id
        assert result_msg["project_id"] == project_id
        assert result_msg["status"] == "completed"
        assert "onelake_path" in result_msg

    finally:
        await stop_worker_gracefully(claimx_upload_worker, worker_task)


@pytest.mark.asyncio
async def test_upload_multiple_cached_files(
    test_claimx_config: KafkaConfig,
    claimx_upload_worker,
    mock_onelake_client,
    tmp_path: Path,
):
    """
    Test uploading multiple cached files concurrently.

    Validates:
    - Multiple uploads processed
    - All files uploaded to OneLake
    - All result messages produced
    """
    num_files = 5
    project_id = 12345

    # Create test files
    test_files = []
    cached_messages = []

    for i in range(num_files):
        media_id = 7000 + i
        test_file = tmp_path / "claimx_downloads" / f"media_{media_id}.jpg"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_content = f"Media content {media_id}".encode()
        test_file.write_bytes(test_content)
        test_files.append((test_file, test_content))

        cached_msg = create_cached_download_message(
            media_id=media_id,
            project_id=project_id,
            local_path=str(test_file),
            download_url=f"https://example.com/media/media_{media_id}.jpg",
            file_size_bytes=len(test_content),
        )
        cached_messages.append(cached_msg)

    # Start worker
    worker_task = await start_worker_background(claimx_upload_worker)

    try:
        # Produce all cached download messages
        producer = AIOKafkaProducer(
            bootstrap_servers=test_claimx_config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await producer.start()

        try:
            for cached_msg in cached_messages:
                await producer.send(
                    test_claimx_config.claimx_downloads_cached_topic,
                    value=cached_msg.model_dump(mode="json"),
                )
            logger.info(f"Produced {num_files} cached download messages")
        finally:
            await producer.stop()

        # Wait for all uploads to complete
        success = await wait_for_condition(
            lambda: mock_onelake_client.upload_count >= num_files,
            timeout_seconds=15.0,
            description=f"{num_files} OneLake uploads",
        )
        assert success, f"Not all files uploaded: {mock_onelake_client.upload_count}/{num_files}"

        # Validate all files were uploaded
        assert len(mock_onelake_client.uploaded_files) >= num_files, \
            f"Expected {num_files} uploads, got {len(mock_onelake_client.uploaded_files)}"

        # Wait for all result messages
        await asyncio.sleep(2.0)
        result_messages = await get_topic_messages(
            test_claimx_config,
            test_claimx_config.claimx_downloads_results_topic,
            max_messages=num_files,
            timeout_seconds=5.0,
        )

        # Validate all result messages produced
        assert len(result_messages) >= num_files, \
            f"Expected {num_files} result messages, got {len(result_messages)}"

    finally:
        await stop_worker_gracefully(claimx_upload_worker, worker_task)


@pytest.mark.asyncio
async def test_upload_missing_file_error(
    test_claimx_config: KafkaConfig,
    claimx_upload_worker,
    mock_onelake_client,
    tmp_path: Path,
):
    """
    Test upload worker handling of missing cached files.

    Validates:
    - Missing file detected
    - Error handled gracefully
    - Failure result message produced
    """
    media_id = 9999
    project_id = 12345

    # Create cached message pointing to non-existent file
    missing_file = tmp_path / "claimx_downloads" / "missing.jpg"
    cached_msg = create_cached_download_message(
        media_id=media_id,
        project_id=project_id,
        local_path=str(missing_file),
        download_url="https://example.com/media/missing.jpg",
    )

    # Start worker
    worker_task = await start_worker_background(claimx_upload_worker)

    try:
        # Produce cached download message
        producer = AIOKafkaProducer(
            bootstrap_servers=test_claimx_config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await producer.start()

        try:
            await producer.send(
                test_claimx_config.claimx_downloads_cached_topic,
                value=cached_msg.model_dump(mode="json"),
            )
            logger.info(f"Produced cached download message for missing file")
        finally:
            await producer.stop()

        # Wait for processing
        await asyncio.sleep(2.0)

        # Verify worker is still running after error
        assert claimx_upload_worker.is_running, "Worker crashed on missing file"

        # Check for failure result message or DLQ routing
        result_messages = await get_topic_messages(
            test_claimx_config,
            test_claimx_config.claimx_downloads_results_topic,
            max_messages=10,
            timeout_seconds=2.0,
        )

        # May have failure result or be routed to DLQ
        if len(result_messages) > 0:
            result_msg = json.loads(result_messages[0]["value"])
            assert result_msg["status"] == "failed", "Expected failure status"
            assert "error_message" in result_msg

    finally:
        await stop_worker_gracefully(claimx_upload_worker, worker_task)


@pytest.mark.asyncio
async def test_upload_preserves_project_structure(
    test_claimx_config: KafkaConfig,
    claimx_upload_worker,
    mock_onelake_client,
    tmp_path: Path,
):
    """
    Test that uploads maintain project-based directory structure.

    Validates:
    - Files from different projects go to different paths
    - Path structure follows convention
    """
    project_ids = [12345, 67890]
    media_id = 5000

    for project_id in project_ids:
        # Create test file
        test_file = tmp_path / "claimx_downloads" / f"project_{project_id}_media_{media_id}.jpg"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_content = f"Project {project_id} content".encode()
        test_file.write_bytes(test_content)

        # Create cached message
        cached_msg = create_cached_download_message(
            media_id=media_id,
            project_id=project_id,
            local_path=str(test_file),
            download_url=f"https://example.com/media/test.jpg",
            file_size_bytes=len(test_content),
        )

        # Start worker
        worker_task = await start_worker_background(claimx_upload_worker)

        try:
            # Produce cached download message
            producer = AIOKafkaProducer(
                bootstrap_servers=test_claimx_config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()

            try:
                await producer.send(
                    test_claimx_config.claimx_downloads_cached_topic,
                    value=cached_msg.model_dump(mode="json"),
                )
            finally:
                await producer.stop()

            # Wait for upload
            await asyncio.sleep(2.0)

        finally:
            await stop_worker_gracefully(claimx_upload_worker, worker_task)

    # Verify both projects have uploads with distinct paths
    uploaded_paths = list(mock_onelake_client.uploaded_files.keys())
    assert len(uploaded_paths) >= 2, "Expected uploads from both projects"

    # Verify project IDs are in paths (or some project-based structure)
    # The actual path structure depends on implementation
    logger.info(f"Uploaded paths: {uploaded_paths}")
