"""
ClaimX Event Ingester Worker - Consumes events and produces enrichment tasks.

This worker is the entry point to the ClaimX pipeline:
1. Consumes ClaimXEventMessage from claimx events topic
2. Produces ClaimXEnrichmentTask to enrichment pending topic
3. Writes events to Delta Lake (claimx_events table)

Different from xact pipeline:
- Produces enrichment tasks (not download tasks)
- No URL validation at this stage (URLs come from API enrichment)
- All events trigger enrichment (not just ones with attachments)

Consumer group: {prefix}-claimx-event-ingester
Input topic: claimx.events.raw
Output topic: claimx.enrichment.pending
Delta table: claimx_events
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from core.logging.context import set_log_context
from core.logging.setup import get_logger
from config.config import KafkaConfig
from kafka_pipeline.common.metrics import (
    event_ingestion_duration_seconds,
    record_event_ingested,
    record_event_task_produced,
)
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.claimx.monitoring import HealthCheckServer
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask

logger = get_logger(__name__)


class ClaimXEventIngesterWorker:
    """
    Worker to consume ClaimX events and produce enrichment tasks.

    Processes ClaimXEventMessage records from the claimx events topic
    and produces ClaimXEnrichmentTask records to the enrichment pending
    topic for processing by enrichment workers.

    Unlike xact pipeline (which directly downloads attachments), claimx
    requires API enrichment first to get entity data and download URLs.

    Also writes events to Delta Lake claimx_events table for analytics.

    Features:
    - Deterministic SHA256 event_id generation from stable Eventhouse fields
    - All events trigger enrichment (not just file events)
    - Graceful shutdown with background task tracking

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> worker = ClaimXEventIngesterWorker(config)
        >>> await worker.start()
        >>> # Worker runs until stopped
        >>> await worker.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        domain: str = "claimx",
        enrichment_topic: str = "",
        producer_config: Optional[KafkaConfig] = None,
    ):
        """
        Initialize ClaimX event ingester worker.

        Args:
            config: Kafka configuration for consumer (topic names, connection settings)
            domain: Domain identifier (default: "claimx")
            enable_delta_writes: Whether to enable Delta Lake writes (default: True)
            events_table_path: Full abfss:// path to claimx_events Delta table
            enrichment_topic: Topic name for enrichment tasks (e.g., "claimx.enrichment.pending")
            producer_config: Optional separate Kafka config for producer. If not provided,
                uses the consumer config. This is needed when reading from Event Hub
                but writing to local Kafka.
        """
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.enrichment_topic = enrichment_topic or config.get_topic(domain, "enrichment_pending")
        self.producer: Optional[BaseKafkaProducer] = None
        self.consumer: Optional[BaseKafkaConsumer] = None

        # Background task tracking for graceful shutdown
        self._pending_tasks: Set[asyncio.Task] = set()
        self._task_counter = 0  # For unique task naming

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: Optional[asyncio.Task] = None

        # Health check server - use worker-specific port from config
        # Use port=0 by default for dynamic port assignment (avoids conflicts with multiple workers)
        # Set health_enabled=False to disable health checks entirely
        processing_config = config.get_worker_config(domain, "event_ingester", "processing")
        health_port = processing_config.get("health_port", 0)
        health_enabled = processing_config.get("health_enabled", True)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-event-ingester",
            enabled=health_enabled,
        )

        logger.info(
            "Initialized ClaimXEventIngesterWorker",
            extra={
                "domain": domain,
                "worker_name": "event_ingester",
                "consumer_group": config.get_consumer_group(domain, "event_ingester"),
                "events_topic": config.get_topic(domain, "events"),
                "enrichment_topic": self.enrichment_topic,
                "separate_producer_config": producer_config is not None,
            },
        )

    @property
    def config(self) -> KafkaConfig:
        """Backward-compatible property returning consumer_config."""
        return self.consumer_config

    async def start(self) -> None:
        """
        Start the ClaimX event ingester worker.

        Initializes producer and consumer, then begins consuming events
        from the claimx events topic. This method runs until stop() is called.

        Raises:
            Exception: If producer or consumer fails to start
        """
        logger.info("Starting ClaimXEventIngesterWorker")

        # Start cycle output background task
        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        # Start health check server first
        await self.health_server.start()

        # Start producer first (uses producer_config for local Kafka)
        self.producer = BaseKafkaProducer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="event_ingester",
        )
        await self.producer.start()

        # Create and start consumer with message handler (uses consumer_config)
        self.consumer = BaseKafkaConsumer(
            config=self.consumer_config,
            domain=self.domain,
            worker_name="event_ingester",
            topics=[self.consumer_config.get_topic(self.domain, "events")],
            message_handler=self._handle_event_message,
        )

        # Update health check readiness (event ingester doesn't use API)
        self.health_server.set_ready(kafka_connected=True, api_reachable=True)

        # Start consumer (this blocks until stopped)
        await self.consumer.start()

    async def stop(self) -> None:
        """
        Stop the ClaimX event ingester worker.

        Waits for pending background tasks (with timeout), then gracefully
        shuts down consumer and producer, committing any pending offsets
        and flushing pending messages.
        """
        logger.info("Stopping ClaimXEventIngesterWorker")

        # Cancel cycle output task
        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            try:
                await self._cycle_task
            except asyncio.CancelledError:
                pass

        # Wait for pending background tasks with timeout
        await self._wait_for_pending_tasks(timeout_seconds=30)

        # Stop consumer first (stops receiving new messages)
        if self.consumer:
            await self.consumer.stop()

        # Then stop producer (flushes pending messages)
        if self.producer:
            await self.producer.stop()

        # Stop health check server
        await self.health_server.stop()

        logger.info("ClaimXEventIngesterWorker stopped successfully")

    def _create_tracked_task(
        self,
        coro,
        task_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> asyncio.Task:
        """
        Create a background task with tracking and lifecycle logging.

        The task is added to _pending_tasks and automatically removed on completion.
        Task lifecycle events (creation, completion, error) are logged.

        Args:
            coro: Coroutine to run as a task
            task_name: Descriptive name for the task (e.g., "delta_write")
            context: Optional dict of context info for logging

        Returns:
            The created asyncio.Task
        """
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
            """Callback when task completes (success, error, or cancelled)."""
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
        """
        Wait for pending background tasks to complete with timeout.

        Logs task information and handles cancellation of tasks that
        don't complete within the timeout.

        Args:
            timeout_seconds: Maximum time to wait for tasks (default: 30s)
        """
        if not self._pending_tasks:
            logger.debug("No pending background tasks to wait for")
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

        # Copy the set since it may be modified by callbacks during gather
        tasks_to_wait = list(self._pending_tasks)

        try:
            # Wait with timeout
            done, pending = await asyncio.wait(
                tasks_to_wait,
                timeout=timeout_seconds,
                return_when=asyncio.ALL_COMPLETED,
            )

            if pending:
                # Log and cancel tasks that didn't complete
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
                    logger.warning(
                        "Cancelled pending task",
                        extra={"task_name": task.get_name()},
                    )

                # Wait briefly for cancellations to propagate
                await asyncio.gather(*pending, return_exceptions=True)

            # Log summary of completed tasks
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

    async def _handle_event_message(self, record: ConsumerRecord) -> None:
        """
        Process a single ClaimX event message from Kafka.

        Parses the ClaimXEventMessage, generates deterministic UUID5 event_id,
        creates an enrichment task, and produces it to the enrichment pending
        topic. Also writes event to Delta Lake for analytics.

        Args:
            record: ConsumerRecord containing ClaimXEventMessage JSON

        Raises:
            Exception: If message processing fails (will be handled by consumer error routing)
        """
        # Start timing for metrics
        start_time = time.perf_counter()

        # Decode and parse ClaimXEventMessage
        try:
            message_data = json.loads(record.value.decode("utf-8"))
            event = ClaimXEventMessage.from_eventhouse_row(message_data)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(
                "Failed to parse ClaimXEventMessage",
                extra={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            # Record parse error in metrics
            record_event_ingested(domain=self.domain, status="parse_error")
            raise

        # event_id is now generated deterministically in the schema from stable Eventhouse fields
        # Do NOT regenerate it here to ensure consistency across duplicate polls
        # The schema uses: project_id | event_type | ingestion_time (from Eventhouse) | optional IDs
        event_id = event.event_id

        # Set logging context for this request (enables trace correlation)
        set_log_context(trace_id=event.event_id)

        # Track records processed
        self._records_processed += 1

        logger.info(
            "Processing ClaimX event",
            extra={
                "event_id": event.event_id,
                "event_type": event.event_type,
                "project_id": event.project_id,
                "media_id": event.media_id,
                "task_assignment_id": event.task_assignment_id,
            },
        )

        # Create enrichment task for this event
        # All events need enrichment (to fetch entity data from API)
        await self._create_enrichment_task(event)

        # Record successful ingestion and duration
        duration = time.perf_counter() - start_time
        event_ingestion_duration_seconds.labels(domain=self.domain).observe(duration)
        record_event_ingested(domain=self.domain, status="success")

    async def _create_enrichment_task(self, event: ClaimXEventMessage) -> None:
        """
        Create and produce an enrichment task for a ClaimX event.

        Enrichment tasks trigger the enrichment worker to call the ClaimX API
        to fetch entity data (projects, contacts, media, tasks, etc.) and
        generate download tasks for any attachments.

        Args:
            event: Source ClaimXEventMessage to create enrichment task from
        """
        # Create enrichment task
        enrichment_task = ClaimXEnrichmentTask(
            event_id=event.event_id,
            event_type=event.event_type,
            project_id=event.project_id,
            retry_count=0,
            created_at=datetime.now(timezone.utc),
            media_id=event.media_id,
            task_assignment_id=event.task_assignment_id,
            video_collaboration_id=event.video_collaboration_id,
            master_file_name=event.master_file_name,
        )

        # Produce enrichment task to pending topic
        try:
            metadata = await self.producer.send(
                topic=self.enrichment_topic,
                key=event.event_id,
                value=enrichment_task,
                headers={"event_id": event.event_id},
            )

            logger.info(
                "Created ClaimX enrichment task",
                extra={
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "project_id": event.project_id,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                },
            )
            self._records_succeeded += 1
            # Record task produced metric
            record_event_task_produced(domain=self.domain, task_type="enrichment_task")
        except Exception as e:
            logger.error(
                "Failed to produce ClaimX enrichment task",
                extra={
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise


    async def _periodic_cycle_output(self) -> None:
        """
        Background task for periodic cycle logging.
        """
        logger.info(
            "Cycle 0: processed=0, succeeded=0 (pending=0) [cycle output every %ds]",
            30,
        )
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        try:
            while True:  # Runs until cancelled
                await asyncio.sleep(1)

                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= 30:  # 30 matches standard interval
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()

                    logger.info(
                        f"Cycle {self._cycle_count}: processed={self._records_processed}, "
                        f"succeeded={self._records_succeeded}",
                        extra={
                            "cycle": self._cycle_count,
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "cycle_interval_seconds": 30,
                        },
                    )

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise


__all__ = ["ClaimXEventIngesterWorker"]
