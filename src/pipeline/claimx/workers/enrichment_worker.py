"""ClaimX enrichment worker.

Enriches events with ClaimX API data and produces download tasks.
Routes events by type to specialized handlers.
"""

import asyncio
import contextlib
import json
import logging
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from config.config import KafkaConfig
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import format_cycle_output, log_worker_error
from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiClient, ClaimXApiError
from pipeline.claimx.handlers import HandlerRegistry, get_handler_registry
from pipeline.claimx.handlers.project_cache import ProjectCache
from pipeline.claimx.retry import EnrichmentRetryHandler
from pipeline.claimx.schemas.entities import EntityRowsMessage
from pipeline.claimx.schemas.events import ClaimXEventMessage
from pipeline.claimx.schemas.tasks import (
    ClaimXDownloadTask,
    ClaimXEnrichmentTask,
)
from pipeline.claimx.workers.download_factory import DownloadTaskFactory
from pipeline.claimx.workers.worker_defaults import WorkerDefaults
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    record_delta_write,
    record_processing_error,
)
from pipeline.common.storage.delta import DeltaTableReader
from pipeline.common.transport import create_consumer, create_producer
from pipeline.common.types import PipelineMessage

logger = logging.getLogger(__name__)


class ClaimXEnrichmentWorker:
    """
    Worker to consume ClaimX enrichment tasks and enrich them with API data.

    Architecture:
    - Event routing via handler registry
    - Single-task processing (no batching - delta writer handles batching)
    - Entity data writes to 7 Delta tables
    - Download task generation for media files

    Transport Layer:
    - Message consumption is handled by the transport layer (pipeline.common.transport)
    - The transport layer calls _handle_enrichment_task() for EACH message individually
    - Offsets are committed AFTER the handler completes successfully
    - All work must be awaited synchronously to prevent data loss (Issue #38)
    - No background task tracking - the handler must complete all work before returning
    - Transport type (EventHub/Kafka) is selected via PIPELINE_TRANSPORT env var
    """

    WORKER_NAME = "enrichment_worker"

    def __init__(
        self,
        config: KafkaConfig,
        entity_writer: Any = None,
        domain: str = "claimx",
        enable_delta_writes: bool = True,
        enrichment_topic: str = "",
        download_topic: str = "",
        producer_config: KafkaConfig | None = None,
        projects_table_path: str = "",
        instance_id: str | None = None,
        api_client: Any | None = None,
    ):
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.instance_id = instance_id
        self.enrichment_topic = enrichment_topic or config.get_topic(
            domain, "enrichment_pending"
        )
        self.download_topic = download_topic or config.get_topic(
            domain, "downloads_pending"
        )
        self.entity_rows_topic = config.get_topic(domain, "enriched")
        self.enable_delta_writes = enable_delta_writes

        # Only consume from pending topic
        # Unified retry scheduler handles routing retry messages back to pending
        self.topics = [self.enrichment_topic]

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        self.consumer_group = config.get_consumer_group(domain, "enrichment_worker")
        self.processing_config = config.get_worker_config(
            domain, "enrichment_worker", "processing"
        )

        self._retry_delays = config.get_retry_delays(domain)
        self._max_retries = config.get_max_retries(domain)

        self.producer = None
        self.download_producer = None
        self.consumer = None
        self.api_client: Any | None = None
        self._injected_api_client = api_client
        self.retry_handler: EnrichmentRetryHandler | None = None

        self.handler_registry: HandlerRegistry = get_handler_registry()

        # Project cache prevents redundant API calls for in-flight verification
        cache_config = self.processing_config.get("project_cache", {})
        self._preload_cache_from_delta = cache_config.get("preload_from_delta", False)
        self.project_cache = ProjectCache()

        # port=0 for dynamic port assignment (avoids conflicts with multiple workers)
        health_port = self.processing_config.get("health_port", 0)
        health_enabled = self.processing_config.get("health_enabled", True)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-enricher",
            enabled=health_enabled,
        )

        self._running = False

        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0

        self._stats_logger: PeriodicStatsLogger | None = None

        self._projects_table_path = projects_table_path

        logger.info(
            "Initialized ClaimXEnrichmentWorker",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "consumer_group": config.get_consumer_group(
                    domain, "enrichment_worker"
                ),
                "enrichment_topic": self.enrichment_topic,
                "download_topic": self.download_topic,
                "delta_writes_enabled": self.enable_delta_writes,
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
                "project_cache_preload": self._preload_cache_from_delta,
                "api_client_injected": api_client is not None,
            },
        )

    async def _preload_project_cache(self) -> None:
        """Preload project cache with existing project IDs from Delta table to reduce API calls."""
        if not self._projects_table_path:
            logger.warning(
                "Cannot preload project cache - projects_table_path not configured"
            )
            return

        try:
            logger.info(
                "Preloading project cache from Delta",
                extra={"table_path": self._projects_table_path},
            )

            reader = DeltaTableReader(self._projects_table_path)
            if not reader.exists():
                logger.info("Projects table does not exist yet, skipping preload")
                return

            df = reader.read(columns=["project_id"])
            if df.is_empty():
                logger.info("Projects table is empty, skipping preload")
                return

            project_ids = df["project_id"].drop_nulls().unique().to_list()
            loaded_count = self.project_cache.load_from_ids(project_ids)

            logger.info(
                "Project cache preloaded from Delta",
                extra={
                    "table_path": self._projects_table_path,
                    "unique_project_ids": len(project_ids),
                    "loaded_to_cache": loaded_count,
                    "cache_size": self.project_cache.size(),
                },
            )

        except Exception as e:
            logger.warning(
                "Failed to preload project cache from Delta, continuing without preload",
                extra={
                    "table_path": self._projects_table_path,
                    "error": str(e)[:200],
                },
                exc_info=True,
            )

    async def start(self) -> None:
        if self._running:
            logger.warning("Worker already running")
            return

        logger.info("Starting ClaimXEnrichmentWorker")
        self._running = True

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "enrichment-worker")

        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=WorkerDefaults.CYCLE_LOG_INTERVAL_SECONDS,
            get_stats=self._get_cycle_stats,
            stage="enrichment",
            worker_id=self.worker_id,
        )
        self._stats_logger.start()

        await self.health_server.start()

        # Use injected API client if provided, otherwise create production client
        if self._injected_api_client is not None:
            self.api_client = self._injected_api_client
            logger.info(
                "Using injected API client",
                extra={
                    "api_client_type": type(self.api_client).__name__,
                    "worker_id": self.worker_id,
                },
            )
        else:
            self.api_client = ClaimXApiClient(
                base_url=self.consumer_config.claimx_api_url,
                token=self.consumer_config.claimx_api_token,
                timeout_seconds=self.consumer_config.claimx_api_timeout_seconds,
                max_concurrent=self.consumer_config.claimx_api_concurrency,
            )

        await self.api_client._ensure_session()

        api_reachable = not self.api_client.is_circuit_open
        self.health_server.set_ready(
            kafka_connected=False,
            api_reachable=api_reachable,
        )

        self.producer = create_producer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="enrichment_worker",
            topic_key="enriched",
        )
        await self.producer.start()

        self.download_producer = create_producer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="enrichment_worker",
            topic_key="downloads_pending",
        )
        await self.download_producer.start()

        self.retry_handler = EnrichmentRetryHandler(
            config=self.consumer_config,
        )
        await self.retry_handler.start()
        logger.info(
            "Retry handler initialized",
            extra={
                "retry_topics": [t for t in self.topics if "retry" in t],
                "dlq_topic": self.retry_handler.dlq_topic,
            },
        )

        if self._preload_cache_from_delta:
            await self._preload_project_cache()

        try:
            self.consumer = await create_consumer(
                config=self.consumer_config,
                domain=self.domain,
                worker_name="enrichment_worker",
                topics=[self.enrichment_topic],
                message_handler=self._handle_enrichment_task,
                topic_key="enrichment_pending",
                instance_id=self.instance_id,
            )

            self.health_server.set_ready(
                kafka_connected=True,
                api_reachable=api_reachable,
                circuit_open=self.api_client.is_circuit_open,
            )

            await self.consumer.start()

        except asyncio.CancelledError:
            logger.info("Worker cancelled during startup/run")
            raise
        except Exception:
            self._running = False
            raise

    async def stop(self) -> None:
        logger.info("Stopping ClaimXEnrichmentWorker")
        self._running = False

        if self._stats_logger:
            await self._stats_logger.stop()

        if self.consumer:
            await self.consumer.stop()

        if self.retry_handler:
            await self.retry_handler.stop()

        if self.producer:
            await self.producer.stop()

        if self.download_producer:
            await self.download_producer.stop()

        if self.api_client:
            await self.api_client.close()

        await self.health_server.stop()

        logger.info("ClaimXEnrichmentWorker stopped successfully")

    async def request_shutdown(self) -> None:
        logger.info("Graceful shutdown requested")
        self._running = False

    async def _handle_enrichment_task(self, record: PipelineMessage) -> None:
        """Process enrichment task. No batching at enricher level - delta writer handles batching."""
        try:
            message_data = json.loads(record.value.decode("utf-8"))
            task = ClaimXEnrichmentTask.model_validate(message_data)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(
                "Failed to parse ClaimXEnrichmentTask",
                extra={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        self._records_processed += 1

        logger.debug(
            "Processing enrichment task",
            extra={
                "event_id": task.event_id,
                "event_type": task.event_type,
                "project_id": task.project_id,
                "retry_count": task.retry_count,
            },
        )

        await self._process_single_task(task)

    async def _execute_handler(
        self,
        handler,
        event: ClaimXEventMessage,
        task: ClaimXEnrichmentTask,
    ):
        """
        Execute handler to enrich event with API data.

        Args:
            handler: Handler instance for this event type
            event: Event message to process
            task: Original enrichment task

        Returns:
            HandlerResult with entity rows and metadata
        """
        handler_result = await handler.process([event])
        return handler_result

    async def _dispatch_entity_rows(
        self,
        task: ClaimXEnrichmentTask,
        entity_rows: EntityRowsMessage,
    ) -> None:
        """
        Dispatch entity rows to Kafka for Delta Lake writing.

        Awaits entity row production to ensure writes complete before offset commit.
        Critical for preventing data loss (Issue #38).

        Args:
            task: Original enrichment task
            entity_rows: Entity rows to produce
        """
        if not self.enable_delta_writes or entity_rows.is_empty():
            return

        await self._produce_entity_rows(entity_rows, [task])

    async def _dispatch_download_tasks(
        self,
        entity_rows: EntityRowsMessage,
    ) -> None:
        """
        Create and dispatch download tasks for media files.

        Args:
            entity_rows: Entity rows containing media metadata
        """
        if not entity_rows.media:
            return

        download_tasks = DownloadTaskFactory.create_download_tasks_from_media(
            entity_rows.media
        )
        if download_tasks:
            await self._produce_download_tasks(download_tasks)

    async def _process_single_task(self, task: ClaimXEnrichmentTask) -> None:
        """
        Process single enrichment task through the pipeline.

        Flow:
        1. Ensure project exists in cache
        2. Create event message from task
        3. Execute handler to fetch API data
        4. Dispatch entity rows to Kafka
        5. Dispatch download tasks for media files
        """
        start_time = datetime.now(UTC)

        # Step 1: Ensure project exists in cache
        if task.project_id:
            await self._ensure_projects_exist([task.project_id])

        # Step 2: Create event message from task
        event = ClaimXEventMessage(
            event_id=task.event_id,
            event_type=task.event_type,
            project_id=task.project_id,
            media_id=task.media_id,
            task_assignment_id=task.task_assignment_id,
            video_collaboration_id=task.video_collaboration_id,
            master_file_name=task.master_file_name,
            ingested_at=task.created_at,
        )

        # Step 3: Get handler for event type
        handler_class = self.handler_registry.get_handler_class(event.event_type)
        if not handler_class:
            logger.warning(
                "No handler found for event",
                extra={
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                },
            )
            self._records_skipped += 1
            return

        handler = handler_class(self.api_client, project_cache=self.project_cache)

        try:
            # Step 4: Execute handler to enrich event with API data
            handler_result = await self._execute_handler(handler, event, task)

            # Check if handler succeeded - HandlerResult has succeeded/failed counts, not a success boolean
            if handler_result.failed > 0:
                # Handler returned failure - route to retry/DLQ
                # Extract error info from handler result
                error_msg = (
                    handler_result.errors[0]
                    if handler_result.errors
                    else "Handler returned failure"
                )
                error = Exception(error_msg)

                # Default to TRANSIENT unless it's a permanent failure
                if handler_result.failed_permanent > 0:
                    error_category = ErrorCategory.PERMANENT
                else:
                    error_category = ErrorCategory.TRANSIENT

                log_worker_error(
                    logger,
                    "Handler returned failure result",
                    event_id=task.event_id,
                    error_category=error_category.value,
                    handler=handler_class.__name__,
                    error_detail=error_msg[:200],
                    succeeded=handler_result.succeeded,
                    failed=handler_result.failed,
                    failed_permanent=handler_result.failed_permanent,
                )

                await self._handle_enrichment_failure(task, error, error_category)
                return

            entity_rows = handler_result.rows
            self._records_succeeded += 1

            # Step 5: Dispatch entity rows to Kafka
            await self._dispatch_entity_rows(task, entity_rows)

            # Step 6: Dispatch download tasks for media files
            await self._dispatch_download_tasks(entity_rows)

            # Log completion
            elapsed_ms = (datetime.now(UTC) - start_time).total_seconds() * 1000
            logger.debug(
                "Enrichment task complete",
                extra={
                    "event_id": task.event_id,
                    "event_type": task.event_type,
                    "handler": handler_class.__name__,
                    "entity_rows": entity_rows.row_count(),
                    "api_calls": handler_result.api_calls,
                    "duration_ms": round(elapsed_ms, 2),
                },
            )

        except ClaimXApiError as e:
            log_worker_error(
                logger,
                "Handler failed with API error",
                event_id=task.event_id,
                error_category=e.category.value,
                exc=e,
                handler=handler_class.__name__,
            )
            record_processing_error(
                topic=self.enrichment_topic,
                consumer_group=self.consumer_group,
                error_category=e.category.value,
            )
            await self._handle_enrichment_failure(task, e, e.category)

        except Exception as e:
            error_category = ErrorCategory.UNKNOWN
            log_worker_error(
                logger,
                "Handler failed with unexpected error",
                event_id=task.event_id,
                error_category=error_category.value,
                exc=e,
                handler=handler_class.__name__,
                error_type=type(e).__name__,
            )
            record_processing_error(
                topic=self.enrichment_topic,
                consumer_group=self.consumer_group,
                error_category=error_category.value,
            )
            await self._handle_enrichment_failure(task, e, error_category)

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        msg = format_cycle_output(
            cycle_count=cycle_count,
            succeeded=self._records_succeeded,
            failed=self._records_failed,
            skipped=self._records_skipped,
            deduplicated=0,
        )
        extra = {
            "records_processed": self._records_processed,
            "records_succeeded": self._records_succeeded,
            "records_failed": self._records_failed,
            "records_skipped": self._records_skipped,
            "project_cache_size": self.project_cache.size(),
        }
        return msg, extra

    async def _ensure_projects_exist(
        self,
        project_ids: list[str],
    ) -> None:
        """Pre-flight check disabled - project existence handled by downstream delta writer."""
        return

    async def _produce_entity_rows(
        self,
        entity_rows: EntityRowsMessage,
        tasks: list[ClaimXEnrichmentTask],
    ) -> None:
        """Write entity rows to Kafka. On failure, routes all tasks to retry/DLQ."""
        batch_id = uuid.uuid4().hex[:8]
        event_ids = [task.event_id for task in tasks[:5]]

        try:
            event_id = tasks[0].event_id if tasks else batch_id
            await self.producer.send(
                value=entity_rows,
                key=event_id,
            )

            logger.info(
                "Produced entity rows batch",
                extra={
                    "batch_id": batch_id,
                    "event_ids": event_ids,
                    "row_count": entity_rows.row_count(),
                },
            )

        except Exception as e:
            logger.error(
                "Error writing entities to Delta - routing batch to retry",
                extra={
                    "batch_id": batch_id,
                    "event_ids": event_ids,
                    "row_count": entity_rows.row_count(),
                    "task_count": len(tasks),
                    "error_category": ErrorCategory.TRANSIENT.value,
                    "error": str(e)[:200],
                },
                exc_info=True,
            )

            record_delta_write(
                table="claimx_entities_produce",
                event_count=entity_rows.row_count(),
                success=False,
            )

            error_category = ErrorCategory.TRANSIENT
            for task in tasks:
                await self._handle_enrichment_failure(task, e, error_category)

    async def _produce_download_tasks(
        self,
        download_tasks: list[ClaimXDownloadTask],
    ) -> None:
        logger.info(
            "Producing download tasks",
            extra={"task_count": len(download_tasks)},
        )

        for task in download_tasks:
            try:
                metadata = await self.download_producer.send(
                    value=task,
                    key=task.source_event_id,
                    headers={"event_id": task.source_event_id},
                )

                logger.debug(
                    "Produced download task",
                    extra={
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                    },
                )

            except Exception as e:
                logger.error(
                    "Failed to produce download task",
                    extra={
                        "media_id": task.media_id,
                        "project_id": task.project_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    async def _handle_enrichment_failure(
        self,
        task: ClaimXEnrichmentTask,
        error: Exception,
        error_category: "ErrorCategory",
    ) -> None:
        """
        Route failed task to retry topic or DLQ based on error category and retry count.
        TRANSIENT/AUTH/CIRCUIT_OPEN/UNKNOWN -> retry with backoff, PERMANENT -> DLQ immediately.
        """
        if self.retry_handler is None:
            raise RuntimeError("RetryHandler not initialized - call start() first")

        log_worker_error(
            logger,
            "Enrichment task failed",
            event_id=task.event_id,
            error_category=error_category.value,
            exc=error,
            event_type=task.event_type,
            project_id=task.project_id,
            retry_count=task.retry_count,
        )

        self._records_failed += 1

        await self.retry_handler.handle_failure(
            task=task,
            error=error,
            error_category=error_category,
        )


__all__ = ["ClaimXEnrichmentWorker"]
