"""
XACT Enrichment Worker - Executes plugins and produces download tasks.

This worker provides a dedicated enrichment stage for the XACT pipeline:
1. Consumes XACTEnrichmentTask from enrichment pending topic
2. Executes plugins for custom logic (filtering, routing, notifications)
3. Produces DownloadTaskMessage for each attachment (if not filtered)

Unlike ClaimX enrichment (which calls APIs), XACT enrichment is primarily
for plugin execution and event routing before download task creation.

Consumer group: {prefix}-xact-enrichment-worker
Input topic: xact.enrichment.pending
Output topic: xact.downloads.pending
"""

import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from config.config import MessageConfig
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import format_cycle_output, log_worker_error
from core.paths.resolver import generate_blob_path
from core.security.exceptions import URLValidationError
from core.security.url_validation import sanitize_url, validate_download_url
from core.types import ErrorCategory
from pipeline.common.health import HealthCheckServer
from pipeline.common.telemetry import initialize_worker_telemetry
from pipeline.common.transport import create_consumer, create_producer
from pipeline.common.types import PipelineMessage
from pipeline.plugins.shared.base import (
    Domain,
    PipelineStage,
    PluginContext,
)
from pipeline.plugins.shared.loader import load_plugins_from_directory
from pipeline.plugins.shared.registry import (
    ActionExecutor,
    PluginOrchestrator,
    PluginRegistry,
)
from pipeline.verisk.retry import RetryHandler
from pipeline.verisk.schemas.tasks import (
    DownloadTaskMessage,
    XACTEnrichmentTask,
)
from pipeline.verisk.workers.worker_defaults import WorkerDefaults

logger = logging.getLogger(__name__)


class XACTEnrichmentWorker:
    """
    Worker to consume XACT enrichment tasks, execute plugins, and produce download tasks.

    Architecture:
    - Single-task processing (no batching needed - simple flow)
    - Plugin execution at ENRICHMENT_COMPLETE stage
    - Download task generation for each attachment

    Transport Layer:
    - Message consumption is handled by the transport layer (pipeline.common.transport)
    - The transport layer calls _handle_enrichment_task() for EACH message individually
    - Offsets are committed AFTER the handler completes successfully
    - All work must be awaited synchronously to prevent data loss (Issue #38)
    - No background task tracking - the handler must complete all work before returning
    - Transport type (EventHub/Kafka) is selected via PIPELINE_TRANSPORT env var
    """

    WORKER_NAME = "enrichment_worker"

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = WorkerDefaults.CYCLE_LOG_INTERVAL_SECONDS

    def __init__(
        self,
        config: MessageConfig,
        domain: str = "verisk",
        enrichment_topic: str = "",
        download_topic: str = "",
        producer_config: MessageConfig | None = None,
        instance_id: str | None = None,
    ):
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.instance_id = instance_id
        self.enrichment_topic = enrichment_topic or config.get_topic(domain, "enrichment_pending")
        self.download_topic = download_topic or config.get_topic(domain, "downloads_pending")

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = (
                f"{self.WORKER_NAME}-{instance_id}"  # e.g., "enrichment_worker-happy-tiger"
            )
        else:
            self.worker_id = self.WORKER_NAME

        self.consumer_group = config.get_consumer_group(domain, "enrichment_worker")
        self.processing_config = config.get_worker_config(domain, "enrichment_worker", "processing")

        self._retry_delays = config.get_retry_delays(domain)
        self._max_retries = config.get_max_retries(domain)

        self.producer = None
        self.consumer = None
        self.retry_handler: RetryHandler | None = None

        self.plugin_registry = PluginRegistry()
        self.plugin_orchestrator: PluginOrchestrator | None = None
        self.action_executor: ActionExecutor | None = None

        # Health check server
        health_port = self.processing_config.get("health_port", 8081)
        health_enabled = self.processing_config.get("health_enabled", True)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name=self.WORKER_NAME,
            enabled=health_enabled,
        )

        self._running = False

        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._stats_logger: PeriodicStatsLogger | None = None

        # Cycle-specific metrics (reset each cycle)
        self._last_cycle_processed = 0
        self._last_cycle_failed = 0

        # UUID namespace for deterministic media_id generation
        self.MEDIA_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "http://xactPipeline/media_id")

        logger.info(
            "Initialized XACTEnrichmentWorker",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": "enrichment_worker",
                "instance_id": instance_id,
                "consumer_group": config.get_consumer_group(domain, "enrichment_worker"),
                "enrichment_topic": self.enrichment_topic,
                "download_topic": self.download_topic,
                "retry_delays": self._retry_delays,
                "max_retries": self._max_retries,
            },
        )

    async def start(self) -> None:
        if self._running:
            logger.warning("Worker already running")
            return

        logger.info("Starting XACTEnrichmentWorker")
        self._running = True

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        initialize_worker_telemetry(self.domain, "enrichment-worker")

        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=self.CYCLE_LOG_INTERVAL_SECONDS,
            get_stats=self._get_cycle_stats,
            stage="enrichment",
            worker_id=self.worker_id,
        )
        self._stats_logger.start()

        self.producer = create_producer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="enrichment_worker",
            topic_key="downloads_pending",
        )
        await self.producer.start()

        # Sync topic with producer's actual entity name (Event Hub entity may
        # differ from the Kafka topic name resolved by get_topic()).
        if hasattr(self.producer, "eventhub_name"):
            self.download_topic = self.producer.eventhub_name

        plugins_dir = self.processing_config.get("plugins_dir", "config/plugins")
        try:
            import os

            if os.path.exists(plugins_dir):
                loaded_plugins = load_plugins_from_directory(plugins_dir, self.plugin_registry)
                logger.info(
                    "Loaded plugins from directory",
                    extra={
                        "plugins_dir": plugins_dir,
                        "plugins_loaded": len(loaded_plugins),
                        "plugin_names": [p.name for p in loaded_plugins],
                    },
                )
            else:
                logger.info(
                    "Plugin directory not found, continuing without plugins",
                    extra={"plugins_dir": plugins_dir},
                )
        except Exception as e:
            logger.warning(
                "Failed to load plugins, continuing without plugins",
                extra={
                    "plugins_dir": plugins_dir,
                    "error": str(e),
                },
                exc_info=True,
            )

        self.action_executor = ActionExecutor(
            producer=self.producer,
            http_client=None,
        )
        self.plugin_orchestrator = PluginOrchestrator(
            registry=self.plugin_registry,
            action_executor=self.action_executor,
        )
        logger.info(
            "Plugin orchestrator initialized",
            extra={
                "registered_plugins": len(self.plugin_registry.list_plugins()),
            },
        )

        for plugin in self.plugin_registry.list_plugins():
            logger.debug(
                "Plugin loaded",
                extra={"plugin_name": plugin.name},
            )

        self.retry_handler = RetryHandler(
            config=self.consumer_config,
        )
        await self.retry_handler.start()
        logger.info(
            "Retry handler initialized",
            extra={
                "dlq_topic": self.retry_handler.dlq_topic,
            },
        )

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

            self.health_server.set_ready(transport_connected=True)

            await self.consumer.start()

        except asyncio.CancelledError:
            logger.info("Worker cancelled during startup/run")
            raise
        except Exception:
            self._running = False
            raise

    async def stop(self) -> None:
        logger.info("Stopping XACTEnrichmentWorker")
        self._running = False

        if self._stats_logger:
            await self._stats_logger.stop()

        if self.consumer:
            await self.consumer.stop()

        if self.retry_handler:
            await self.retry_handler.stop()

        if self.producer:
            await self.producer.stop()

        await self.health_server.stop()

        logger.info("XACTEnrichmentWorker stopped successfully")

    async def request_shutdown(self) -> None:
        logger.info("Graceful shutdown requested")
        self._running = False

    async def _handle_enrichment_task(self, message: PipelineMessage) -> None:
        """Process enrichment task - execute plugins and create download tasks."""
        try:
            message_data = json.loads(message.value.decode("utf-8"))
            task = XACTEnrichmentTask.model_validate(message_data)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(
                "Failed to parse XACTEnrichmentTask",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
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
                "trace_id": task.trace_id,
                "event_type": task.event_type,
                "status_subtype": task.status_subtype,
                "retry_count": task.retry_count,
            },
        )

        await self._process_single_task(task)

    async def _process_single_task(self, task: XACTEnrichmentTask) -> None:
        start_time = datetime.now(UTC)

        try:
            # Execute plugins at ENRICHMENT_COMPLETE stage
            if self.plugin_orchestrator:
                plugin_context = PluginContext(
                    domain=Domain.VERISK,
                    stage=PipelineStage.ENRICHMENT_COMPLETE,
                    message=task,
                    event_id=task.event_id,
                    event_type=task.event_type,
                    project_id=None,  # XACT doesn't have project_id
                    data={
                        "trace_id": task.trace_id,
                        "status_subtype": task.status_subtype,
                        "assignment_id": task.assignment_id,
                        "estimate_version": task.estimate_version,
                        "attachment_count": len(task.attachments),
                    },
                    headers={},
                )

                try:
                    orchestrator_result = await self.plugin_orchestrator.execute(plugin_context)

                    if orchestrator_result.terminated:
                        logger.info(
                            "Pipeline terminated by plugin",
                            extra={
                                "event_id": task.event_id,
                                "trace_id": task.trace_id,
                                "status_subtype": task.status_subtype,
                                "reason": orchestrator_result.termination_reason,
                                "plugin_results": orchestrator_result.success_count,
                            },
                        )
                        self._records_skipped += 1
                        return

                    if orchestrator_result.actions_executed > 0:
                        logger.debug(
                            "Plugin actions executed",
                            extra={
                                "event_id": task.event_id,
                                "actions": orchestrator_result.actions_executed,
                                "plugins": orchestrator_result.success_count,
                            },
                        )

                except Exception as e:
                    logger.error(
                        "Plugin execution failed",
                        extra={
                            "event_id": task.event_id,
                            "error": str(e),
                        },
                        exc_info=True,
                    )
                    # Continue processing - don't fail the task due to plugin error

            # Create download tasks for each attachment
            download_tasks = await self._create_download_tasks_from_attachments(task)
            if download_tasks:
                await self._produce_download_tasks(download_tasks)

            self._records_succeeded += 1

            elapsed_ms = (datetime.now(UTC) - start_time).total_seconds() * 1000
            logger.debug(
                "Enrichment task complete",
                extra={
                    "event_id": task.event_id,
                    "trace_id": task.trace_id,
                    "status_subtype": task.status_subtype,
                    "download_tasks": len(download_tasks),
                    "duration_ms": round(elapsed_ms, 2),
                },
            )

        except Exception as e:
            error_category = ErrorCategory.UNKNOWN
            log_worker_error(
                logger,
                "Enrichment task failed with unexpected error",
                event_id=task.event_id,
                error_category=error_category.value,
                exc=e,
                status_subtype=task.status_subtype,
                error_type=type(e).__name__,
            )
            await self._handle_enrichment_failure(task, e, error_category)

    async def _create_download_tasks_from_attachments(
        self,
        task: XACTEnrichmentTask,
    ) -> list[DownloadTaskMessage]:
        """Create download tasks from enrichment task attachments."""
        download_tasks = []

        for attachment_url in task.attachments:
            try:
                # Validate attachment URL
                try:
                    validate_download_url(
                        attachment_url,
                        allow_localhost=False,
                    )
                except URLValidationError as e:
                    logger.warning(
                        "Invalid attachment URL, skipping",
                        extra={
                            "event_id": task.event_id,
                            "trace_id": task.trace_id,
                            "url": sanitize_url(attachment_url),
                            "validation_error": str(e),
                        },
                    )
                    continue

                # Generate blob storage path
                blob_path, file_type = generate_blob_path(
                    status_subtype=task.status_subtype,
                    trace_id=task.trace_id,
                    assignment_id=task.assignment_id,
                    download_url=attachment_url,
                    estimate_version=task.estimate_version,
                )

                # Generate deterministic media_id
                media_id = str(
                    uuid.uuid5(self.MEDIA_ID_NAMESPACE, f"{task.trace_id}:{attachment_url}")
                )

                # Create download task
                download_task = DownloadTaskMessage(
                    media_id=media_id,
                    trace_id=task.trace_id,
                    attachment_url=attachment_url,
                    blob_path=blob_path,
                    status_subtype=task.status_subtype,
                    file_type=file_type,
                    assignment_id=task.assignment_id,
                    estimate_version=task.estimate_version,
                    retry_count=0,
                    event_type=self.domain,
                    event_subtype=task.status_subtype,
                    original_timestamp=task.original_timestamp,
                )
                download_tasks.append(download_task)

            except Exception as e:
                logger.error(
                    "Failed to create download task for attachment",
                    extra={
                        "event_id": task.event_id,
                        "trace_id": task.trace_id,
                        "url": sanitize_url(attachment_url),
                        "error": str(e),
                    },
                    exc_info=True,
                )
                # Continue with other attachments

        logger.debug(
            "Created download tasks from attachments",
            extra={
                "event_id": task.event_id,
                "attachments": len(task.attachments),
                "download_tasks": len(download_tasks),
            },
        )

        return download_tasks

    async def _produce_download_tasks(
        self,
        download_tasks: list[DownloadTaskMessage],
    ) -> None:
        """Produce download tasks to downloads.pending topic."""
        logger.info(
            "Producing download tasks",
            extra={"task_count": len(download_tasks)},
        )

        for task in download_tasks:
            try:
                metadata = await self.producer.send(
                    value=task,
                    key=task.trace_id,
                    headers={"trace_id": task.trace_id, "media_id": task.media_id},
                )

                logger.debug(
                    "Produced download task",
                    extra={
                        "media_id": task.media_id,
                        "trace_id": task.trace_id,
                        "blob_path": task.blob_path,
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                    },
                )

            except Exception as e:
                logger.error(
                    "Failed to produce download task",
                    extra={
                        "media_id": task.media_id,
                        "trace_id": task.trace_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                # Re-raise to trigger retry of the entire enrichment task
                raise

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        """Get cycle statistics for periodic logging."""
        msg = format_cycle_output(
            cycle_count=cycle_count,
            succeeded=self._records_succeeded,
            failed=self._records_failed,
            skipped=self._records_skipped,
        )
        extra = {
            "records_processed": self._records_processed,
            "records_succeeded": self._records_succeeded,
            "records_failed": self._records_failed,
            "records_skipped": self._records_skipped,
        }
        return msg, extra

    async def _handle_enrichment_failure(
        self,
        task: XACTEnrichmentTask,
        error: Exception,
        error_category: ErrorCategory,
    ) -> None:
        """
        Route failed task to retry topic or DLQ based on error category and retry count.
        """
        if self.retry_handler is None:
            raise RuntimeError("RetryHandler not initialized - call start() first")

        log_worker_error(
            logger,
            "Enrichment task failed",
            event_id=task.event_id,
            error_category=error_category.value,
            exc=error,
            status_subtype=task.status_subtype,
            retry_count=task.retry_count,
        )

        self._records_failed += 1

        # Convert XACTEnrichmentTask to dict for retry handler
        task_dict = task.model_dump()

        await self.retry_handler.handle_failure(
            task=task_dict,
            error=error,
            error_category=error_category,
        )


__all__ = ["XACTEnrichmentWorker"]
