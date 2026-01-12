"""
Integration test for ClaimX enrichment worker flow.

Tests the enrichment worker's ability to:
1. Consume ClaimXEnrichmentTask messages
2. Call ClaimX API via handlers
3. Write entity data to Delta tables
4. Produce download tasks for media files
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from aiokafka import AIOKafkaProducer

from kafka_pipeline.config import KafkaConfig
from kafka_pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask

from ..helpers import (
    get_topic_messages,
    start_worker_background,
    stop_worker_gracefully,
    wait_for_condition,
)
from .generators import (
    create_enrichment_task,
    create_mock_project_response,
    create_mock_contact_response,
    create_mock_media_response,
)

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_enrichment_project_created_event(
    test_claimx_config: KafkaConfig,
    claimx_enrichment_worker,
    mock_claimx_api_client,
    mock_claimx_entity_writer,
):
    """
    Test enrichment flow for PROJECT_CREATED event.

    Validates:
    - EnrichmentTask → API call → EntityRows
    - Project and contact data written to Delta
    - Handler routing works correctly
    """
    project_id = 12345
    event_id = 1001

    # Set up mock API responses
    mock_claimx_api_client.set_project(
        project_id,
        create_mock_project_response(project_id, mfn="MFN-TEST-001")
    )

    # Create enrichment task
    task = create_enrichment_task(
        event_id=event_id,
        event_type="PROJECT_CREATED",
        project_id=project_id,
        user_id=999,
    )

    # Start worker
    worker_task = await start_worker_background(claimx_enrichment_worker)

    try:
        # Produce enrichment task
        producer = AIOKafkaProducer(
            bootstrap_servers=test_claimx_config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await producer.start()

        try:
            await producer.send(
                test_claimx_config.claimx_enrichment_pending_topic,
                value=task.model_dump(mode="json"),
            )
            logger.info(f"Produced enrichment task for event {event_id}")
        finally:
            await producer.stop()

        # Wait for entity write
        success = await wait_for_condition(
            lambda: mock_claimx_entity_writer.write_count > 0,
            timeout_seconds=10.0,
            description="entity write",
        )
        assert success, "Entity data not written"

        # Validate API was called
        assert mock_claimx_api_client.request_count > 0, "API not called"

        # Validate project data was written
        projects = mock_claimx_entity_writer.get_projects_by_id(project_id)
        assert len(projects) > 0, "No project data written"
        assert projects[0]["project_id"] == project_id
        assert projects[0]["mfn"] == "MFN-TEST-001"

    finally:
        await stop_worker_gracefully(claimx_enrichment_worker, worker_task)


@pytest.mark.asyncio
async def test_enrichment_project_file_added_event(
    test_claimx_config: KafkaConfig,
    claimx_enrichment_worker,
    mock_claimx_api_client,
    mock_claimx_entity_writer,
):
    """
    Test enrichment flow for PROJECT_FILE_ADDED event.

    Validates:
    - Media handler fetches media data
    - Media data written to Delta
    - Download tasks produced for media with URLs
    """
    project_id = 12345
    media_id = 5678
    event_id = 1002
    download_url = "https://example.com/media/test.jpg"

    # Set up mock API response with media
    mock_claimx_api_client.set_media(
        project_id,
        [create_mock_media_response(media_id, project_id, download_url)]
    )

    # Create enrichment task
    task = create_enrichment_task(
        event_id=event_id,
        event_type="PROJECT_FILE_ADDED",
        project_id=project_id,
        media_id=media_id,
    )

    # Start worker
    worker_task = await start_worker_background(claimx_enrichment_worker)

    try:
        # Produce enrichment task
        producer = AIOKafkaProducer(
            bootstrap_servers=test_claimx_config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await producer.start()

        try:
            await producer.send(
                test_claimx_config.claimx_enrichment_pending_topic,
                value=task.model_dump(mode="json"),
            )
            logger.info(f"Produced enrichment task for event {event_id}")
        finally:
            await producer.stop()

        # Wait for entity write
        success = await wait_for_condition(
            lambda: mock_claimx_entity_writer.write_count > 0,
            timeout_seconds=10.0,
            description="entity write",
        )
        assert success, "Entity data not written"

        # Wait for download task to be produced
        await asyncio.sleep(2.0)
        download_messages = await get_topic_messages(
            test_claimx_config,
            test_claimx_config.claimx_downloads_pending_topic,
            max_messages=10,
            timeout_seconds=5.0,
        )

        # Validate download task was produced
        assert len(download_messages) > 0, "No download task produced"

        # Validate media data was written
        media_items = mock_claimx_entity_writer.get_media_by_project(project_id)
        assert len(media_items) > 0, "No media data written"
        assert media_items[0]["media_id"] == media_id

    finally:
        await stop_worker_gracefully(claimx_enrichment_worker, worker_task)


@pytest.mark.asyncio
async def test_enrichment_custom_task_assigned_event(
    test_claimx_config: KafkaConfig,
    claimx_enrichment_worker,
    mock_claimx_api_client,
    mock_claimx_entity_writer,
):
    """
    Test enrichment flow for CUSTOM_TASK_ASSIGNED event.

    Validates:
    - Task handler fetches task data
    - Task data written to Delta
    - Template and link data also written
    """
    project_id = 12345
    task_id = 9999
    event_id = 1003

    # Set up mock API response
    mock_claimx_api_client.set_task(
        task_id,
        create_mock_task_response(task_id, project_id)
    )

    # Create enrichment task
    task = create_enrichment_task(
        event_id=event_id,
        event_type="CUSTOM_TASK_ASSIGNED",
        project_id=project_id,
        task_id=task_id,
        custom_task_template_id=456,
    )

    # Start worker
    worker_task = await start_worker_background(claimx_enrichment_worker)

    try:
        # Produce enrichment task
        producer = AIOKafkaProducer(
            bootstrap_servers=test_claimx_config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await producer.start()

        try:
            await producer.send(
                test_claimx_config.claimx_enrichment_pending_topic,
                value=task.model_dump(mode="json"),
            )
            logger.info(f"Produced enrichment task for event {event_id}")
        finally:
            await producer.stop()

        # Wait for entity write
        success = await wait_for_condition(
            lambda: mock_claimx_entity_writer.write_count > 0,
            timeout_seconds=10.0,
            description="entity write",
        )
        assert success, "Entity data not written"

        # Validate task data was written
        assert len(mock_claimx_entity_writer.tasks) > 0, "No task data written"

    finally:
        await stop_worker_gracefully(claimx_enrichment_worker, worker_task)


@pytest.mark.asyncio
async def test_enrichment_batch_processing(
    test_claimx_config: KafkaConfig,
    claimx_enrichment_worker,
    mock_claimx_api_client,
    mock_claimx_entity_writer,
):
    """
    Test enrichment worker's batch processing capability.

    Validates:
    - Multiple tasks processed in batch
    - All entities written correctly
    - API calls made efficiently
    """
    project_id = 12345
    num_events = 5

    # Set up mock API responses for all events
    for i in range(num_events):
        mock_claimx_api_client.set_project(
            project_id,
            create_mock_project_response(project_id, mfn=f"MFN-{i}")
        )

    # Create multiple enrichment tasks
    tasks = [
        create_enrichment_task(
            event_id=2000 + i,
            event_type="PROJECT_CREATED",
            project_id=project_id,
        )
        for i in range(num_events)
    ]

    # Start worker
    worker_task = await start_worker_background(claimx_enrichment_worker)

    try:
        # Produce all tasks
        producer = AIOKafkaProducer(
            bootstrap_servers=test_claimx_config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await producer.start()

        try:
            for task in tasks:
                await producer.send(
                    test_claimx_config.claimx_enrichment_pending_topic,
                    value=task.model_dump(mode="json"),
                )
            logger.info(f"Produced {num_events} enrichment tasks")
        finally:
            await producer.stop()

        # Wait for all writes to complete
        success = await wait_for_condition(
            lambda: mock_claimx_entity_writer.write_count >= num_events,
            timeout_seconds=15.0,
            description=f"{num_events} entity writes",
        )
        assert success, f"Not all entity writes completed: {mock_claimx_entity_writer.write_count}/{num_events}"

        # Validate all entities were written
        projects = mock_claimx_entity_writer.get_projects_by_id(project_id)
        assert len(projects) >= num_events, f"Expected {num_events} projects, got {len(projects)}"

    finally:
        await stop_worker_gracefully(claimx_enrichment_worker, worker_task)
