"""
ClaimX Enrichment Worker - Enriches events with API data and produces download tasks.

This worker is the core of the ClaimX enrichment pipeline:
1. Consumes ClaimXEnrichmentTask from enrichment pending topic
2. Routes events to appropriate handlers based on event_type
3. Handlers call ClaimX API to fetch entity data
4. Writes entity rows to Delta Lake tables
5. Produces ClaimXDownloadTask for media files with download URLs

Consumer group: {prefix}-claimx-enrichment-worker
Input topic: claimx.enrichment.pending
Output topic: claimx.entities.rows (entity data), claimx.downloads.pending (downloads)
Delta tables: claimx_projects, claimx_contacts, claimx_attachment_metadata, claimx_tasks,
              claimx_task_templates, claimx_external_links, claimx_video_collab
"""

import asyncio
import contextlib
import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from config.config import KafkaConfig
from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging.setup import get_logger
from core.logging.utilities import format_cycle_output, log_worker_error
from core.types import ErrorCategory
from kafka_pipeline.claimx.api_client import ClaimXApiClient, ClaimXApiError
from kafka_pipeline.claimx.handlers import HandlerRegistry, get_handler_registry
from kafka_pipeline.claimx.handlers.project_cache import ProjectCache
from kafka_pipeline.claimx.retry import EnrichmentRetryHandler
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import (
    ClaimXDownloadTask,
    ClaimXEnrichmentTask,
)
from kafka_pipeline.claimx.workers.download_factory import DownloadTaskFactory
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.common.metrics import (
    record_delta_write,
    record_processing_error,
    update_assigned_partitions,
    update_connection_status,
)
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.storage.delta import DeltaTableReader
from kafka_pipeline.common.types import PipelineMessage

logger = get_logger(__name__)


class ClaimXEnrichmentWorker:
    """
    Worker to consume ClaimX enrichment tasks and enrich them with API data.

    Architecture:
    - Event routing via handler registry
    - Single-task processing (no batching - delta writer handles batching)
    - Entity data writes to 7 Delta tables
    - Download task generation for media files
    - Background task tracking for graceful shutdown
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

        # Create worker_id with instance suffix (coolname) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        self.consumer_group = config.get_consumer_group(domain, "enrichment_worker")
        self.processing_config = config.get_worker_config(
            domain, "enrichment_worker", "processing"
        )
        self.max_poll_records = self.processing_config.get("max_poll_records", 100)

        self._retry_delays = config.get_retry_delays(domain)
        self._max_retries = config.get_max_retries(domain)

        # Only consume from pending topic
        # Unified retry scheduler handles routing retry messages back to pending
        self.topics = [self.enrichment_topic]

        self.producer: BaseKafkaProducer | None = None
        self.consumer: AIOKafkaConsumer | None = None
        self.api_client: Any | None = None
        self._injected_api_client = api_client
        self.retry_handler: EnrichmentRetryHandler | None = None

        self.handler_registry: HandlerRegistry = get_handler_registry()

        # Project cache prevents redundant API calls for in-flight verification
        cache_config = self.processing_config.get("project_cache", {})
        cache_ttl = cache_config.get("ttl_seconds", 1800)
        self._preload_cache_from_delta = cache_config.get("preload_from_delta", False)
        self.project_cache = ProjectCache(ttl_seconds=cache_ttl)

        # port=0 for dynamic port assignment (avoids conflicts with multiple workers)
        health_port = self.processing_config.get("health_port", 0)
        health_enabled = self.processing_config.get("health_enabled", True)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-enricher",
            enabled=health_enabled,
        )

        self._pending_tasks: set[asyncio.Task] = set()
        self._task_counter = 0
        self._consume_task: asyncio.Task | None = None
        self._running = False

        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: asyncio.Task | None = None

        # Cycle-specific metrics (reset each cycle)
        self._last_cycle_processed = 0
        self._last_cycle_failed = 0

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
                "topics": self.topics,
                "enrichment_topic": self.enrichment_topic,
                "download_topic": self.download_topic,
                "delta_writes_enabled": self.enable_delta_writes,
                "max_poll_records": self.max_poll_records,
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
                "project_cache_ttl": cache_ttl,
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

        from kafka_pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "enrichment-worker")

        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        await self.health_server.start()

        # Use injected API client if provided (simulation mode), otherwise create production client
        if self._injected_api_client is not None:
            self.api_client = self._injected_api_client
            logger.info(
                "Using injected API client (simulation mode)",
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

        self.producer = BaseKafkaProducer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="enrichment_worker",
        )
        await self.producer.start()

        self.retry_handler = EnrichmentRetryHandler(
            config=self.consumer_config,
            producer=self.producer,
        )
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
            self.consumer = self._create_consumer()
            await self.consumer.start()
            update_connection_status(
                True,
                self.consumer_config.get_consumer_group(
                    self.domain, "enrichment_worker"
                ),
            )

            self.health_server.set_ready(
                kafka_connected=True,
                api_reachable=api_reachable,
                circuit_open=self.api_client.is_circuit_open,
            )

            self._consume_task = asyncio.create_task(self._consume_loop())
            await self._consume_task

        except asyncio.CancelledError:
            logger.info("Worker cancelled during startup/run")
            raise
        except Exception:
            self._running = False
            raise

    async def stop(self) -> None:
        logger.info("Stopping ClaimXEnrichmentWorker")
        self._running = False

        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cycle_task

        await self._wait_for_pending_tasks(timeout_seconds=30)

        if self.consumer:
            await self.consumer.stop()
            update_connection_status(
                False,
                self.consumer_config.get_consumer_group(
                    self.domain, "enrichment_worker"
                ),
            )

        if self.producer:
            await self.producer.stop()

        if self.api_client:
            await self.api_client.close()

        await self.health_server.stop()

        logger.info("ClaimXEnrichmentWorker stopped successfully")

    async def request_shutdown(self) -> None:
        logger.info("Graceful shutdown requested")
        self._running = False

    def _create_consumer(self) -> AIOKafkaConsumer:
        group_id = self.consumer_config.get_consumer_group(
            self.domain, "enrichment_worker"
        )

        consumer_config_dict = self.consumer_config.get_worker_config(
            self.domain, "enrichment_worker", "consumer"
        )

        common_args = {
            "bootstrap_servers": self.consumer_config.bootstrap_servers,
            "group_id": group_id,
            "client_id": (
                f"{self.domain}-{self.WORKER_NAME.replace('_', '-')}-{self.instance_id}"
                if self.instance_id
                else f"{self.domain}-{self.WORKER_NAME.replace('_', '-')}"
            ),
            "enable_auto_commit": False,
            "auto_offset_reset": consumer_config_dict.get(
                "auto_offset_reset", "earliest"
            ),
            "metadata_max_age_ms": 30000,
            "session_timeout_ms": consumer_config_dict.get("session_timeout_ms", 45000),
            "max_poll_interval_ms": consumer_config_dict.get(
                "max_poll_interval_ms", 600000
            ),
            "request_timeout_ms": self.consumer_config.request_timeout_ms,
            "connections_max_idle_ms": self.consumer_config.connections_max_idle_ms,
        }

        if "heartbeat_interval_ms" in consumer_config_dict:
            common_args["heartbeat_interval_ms"] = consumer_config_dict[
                "heartbeat_interval_ms"
            ]

        if self.consumer_config.security_protocol != "PLAINTEXT":
            common_args["security_protocol"] = self.consumer_config.security_protocol
            common_args["sasl_mechanism"] = self.consumer_config.sasl_mechanism

            if self.consumer_config.sasl_mechanism == "OAUTHBEARER":
                common_args["sasl_oauth_token_provider"] = create_kafka_oauth_callback()

        return AIOKafkaConsumer(*self.topics, **common_args)

    async def _consume_loop(self) -> None:
        """
        Main consumption loop.

        CRITICAL (Issue #38): Commits offsets only after verifying all background tasks
        (entity row production) complete successfully. This prevents data loss from
        race conditions where offsets are committed before writes finish.
        """
        logger.info("Started consumption loop")

        try:
            while self._running:
                msg_dict = await self.consumer.getmany(
                    timeout_ms=1000, max_records=self.max_poll_records
                )

                if not msg_dict:
                    continue

                for _partition, messages in msg_dict.items():
                    for msg in messages:
                        await self._handle_enrichment_task(msg)

                # CRITICAL (Issue #38): Wait for all background tasks to complete before committing offsets.
                # This ensures entity rows are produced to Kafka before we advance the consumer offset.
                if self._pending_tasks:
                    await self._wait_for_pending_tasks(timeout_seconds=30)

                await self.consumer.commit()
                update_assigned_partitions(
                    self.consumer_group, len(self.consumer.assignment())
                )

        except asyncio.CancelledError:
            logger.debug("Consumption loop cancelled")
            raise
        except Exception as e:
            logger.error(
                "Error in consumption loop", extra={"error": str(e)}, exc_info=True
            )
            raise
        finally:
            logger.info("Consumption loop ended")

    def _create_tracked_task(
        self,
        coro,
        task_name: str,
        context: dict[str, Any] | None = None,
    ) -> asyncio.Task:
        self._task_counter += 1
        full_name = f"{task_name}-{self._task_counter}"
        context = context or {}

        task = asyncio.create_task(coro, name=full_name)
        self._pending_tasks.add(task)

        logger.debug(
            "Background task created",
            extra={
                "task_name": full_name,
                "pending_tasks": len(self._pending_tasks),
                **context,
            },
        )

        def _on_task_done(t: asyncio.Task) -> None:
            self._pending_tasks.discard(t)

            if t.cancelled():
                logger.debug(
                    "Background task cancelled",
                    extra={
                        "task_name": t.get_name(),
                        "pending_tasks": len(self._pending_tasks),
                    },
                )
            elif t.exception() is not None:
                exc = t.exception()
                logger.error(
                    "Background task failed",
                    extra={
                        "task_name": t.get_name(),
                        "error": str(exc)[:200],
                        "pending_tasks": len(self._pending_tasks),
                        **context,
                    },
                )
            else:
                logger.debug(
                    "Background task completed",
                    extra={
                        "task_name": t.get_name(),
                        "pending_tasks": len(self._pending_tasks),
                    },
                )

        task.add_done_callback(_on_task_done)
        return task

    async def _wait_for_pending_tasks(self, timeout_seconds: float = 30) -> None:
        if not self._pending_tasks:
            return

        pending_count = len(self._pending_tasks)
        task_names = [t.get_name() for t in self._pending_tasks]

        logger.info(
            "Waiting for pending background tasks to complete",
            extra={
                "pending_count": pending_count,
                "task_names": task_names,
                "timeout_seconds": timeout_seconds,
            },
        )

        tasks_to_wait = list(self._pending_tasks)

        try:
            done, pending = await asyncio.wait(
                tasks_to_wait,
                timeout=timeout_seconds,
                return_when=asyncio.ALL_COMPLETED,
            )

            if pending:
                pending_names = [t.get_name() for t in pending]
                logger.warning(
                    "Cancelling background tasks that did not complete in time",
                    extra={
                        "pending_count": len(pending),
                        "pending_task_names": pending_names,
                        "timeout_seconds": timeout_seconds,
                    },
                )

                for task in pending:
                    task.cancel()

                await asyncio.gather(*pending, return_exceptions=True)

            completed_count = len(done)
            failed_count = sum(1 for t in done if t.exception() is not None)

            logger.info(
                "Background task cleanup complete",
                extra={
                    "completed": completed_count,
                    "failed": failed_count,
                    "cancelled": len(pending),
                },
            )

        except Exception as e:
            logger.error(
                "Error waiting for pending tasks",
                extra={"error": str(e)[:200]},
                exc_info=True,
            )

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
        from kafka_pipeline.common.telemetry import get_tracer

        tracer = get_tracer(__name__)
        with tracer.start_active_span("claimx.api.enrich") as scope:
            span = scope.span if hasattr(scope, "span") else scope
            span.set_tag("span.kind", "client")
            span.set_tag("event.id", task.event_id)
            span.set_tag("event.type", task.event_type)
            span.set_tag("project.id", task.project_id)
            handler_result = await handler.process([event])

        return handler_result

    def _dispatch_entity_rows(
        self,
        task: ClaimXEnrichmentTask,
        entity_rows: EntityRowsMessage,
    ) -> None:
        """
        Dispatch entity rows to Kafka for Delta Lake writing.

        Creates background task to produce rows without blocking.

        Args:
            task: Original enrichment task
            entity_rows: Entity rows to produce
        """
        if not self.enable_delta_writes or entity_rows.is_empty():
            return

        self._create_tracked_task(
            self._produce_entity_rows(entity_rows, [task]),
            task_name="produce_entity_rows",
            context={
                "event_id": task.event_id,
                "row_count": entity_rows.row_count(),
            },
        )

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
            self._dispatch_entity_rows(task, entity_rows)

            # Step 6: Dispatch download tasks for media files
            await self._dispatch_download_tasks(entity_rows)

            # Log completion
            elapsed_ms = (
                datetime.now(UTC) - start_time
            ).total_seconds() * 1000
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

    async def _periodic_cycle_output(self) -> None:
        logger.info(
            format_cycle_output(
                cycle_count=0,
                succeeded=0,
                failed=0,
                skipped=0,
                deduplicated=0,
            ),
            extra={
                "worker_id": self.worker_id,
                "stage": "enrichment",
                "cycle": 0,
                "cycle_id": "cycle-0",
            },
        )
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        try:
            while True:
                await asyncio.sleep(1)

                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= 30:
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()

                    # Calculate cycle-specific deltas
                    (
                        self._records_processed - self._last_cycle_processed
                    )
                    self._records_failed - self._last_cycle_failed

                    logger.info(
                        format_cycle_output(
                            cycle_count=self._cycle_count,
                            succeeded=self._records_succeeded,
                            failed=self._records_failed,
                            skipped=self._records_skipped,
                            deduplicated=0,
                        ),
                        extra={
                            "worker_id": self.worker_id,
                            "stage": "enrichment",
                            "cycle": self._cycle_count,
                            "cycle_id": f"cycle-{self._cycle_count}",
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_failed": self._records_failed,
                            "records_skipped": self._records_skipped,
                            "project_cache_size": self.project_cache.size(),
                            "cycle_interval_seconds": 30,
                        },
                    )

                    # Update last cycle counters
                    self._last_cycle_processed = self._records_processed
                    self._last_cycle_failed = self._records_failed

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise

    async def _ensure_projects_exist(
        self,
        project_ids: list[str],
    ) -> None:
        """
        Pre-flight check: Ensure projects exist to prevent referential integrity issues
        when writing child entities. Failures are non-fatal.
        """
        if not project_ids or not self.enable_delta_writes:
            return

        unique_project_ids = list(set(project_ids))

        logger.debug(
            "Pre-flight: Checking project existence",
            extra={
                "project_count": len(unique_project_ids),
                "project_ids": unique_project_ids[:10],
            },
        )

        try:

            # Pre-flight check disabled in decoupled writer mode - project existence handled by downstream delta writer
            return

        except Exception as e:
            logger.error(
                "Pre-flight check failed",
                extra={
                    "error": str(e)[:200],
                    "project_count": len(project_ids),
                },
                exc_info=True,
            )

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
                topic=self.entity_rows_topic,
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
                metadata = await self.producer.send(
                    topic=self.download_topic,
                    key=task.source_event_id,
                    value=task,
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
