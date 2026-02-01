"""
Event Ingester Worker - Consumes events and produces enrichment tasks.

Entry point to the enrichment and download pipeline:
1. Consumes EventMessage from events.raw topic
2. Creates XACTEnrichmentTask for each event
3. Produces to enrichment.pending topic for plugin execution

Note: The enrichment worker will then create download tasks and execute plugins.
Delta Lake writes are handled separately by DeltaEventsWorker (also consuming
from events.raw with its own consumer group).

Consumer group: {prefix}-event-ingester
Input topic: events.raw
Output topic: enrichment.pending
"""

import asyncio
import contextlib
import json
import time
import uuid
from datetime import UTC, datetime

from pydantic import ValidationError

from config.config import KafkaConfig
from core.logging.context import set_log_context
from core.logging.setup import get_logger
from core.logging.utilities import format_cycle_output, log_worker_error
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.common.metrics import (
    message_processing_duration_seconds,
    record_processing_error,
)
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.types import PipelineMessage
from kafka_pipeline.verisk.schemas.events import EventMessage
from kafka_pipeline.verisk.schemas.tasks import XACTEnrichmentTask

logger = get_logger(__name__)


class EventIngesterWorker:
    """Worker to consume events and produce download tasks."""

    WORKER_NAME = "event_ingester"

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = 30

    # Namespace for generating deterministic media_ids (UUID5)
    # Using a fixed namespace ensures the same trace_id + url always yields the same media_id
    MEDIA_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "http://nsmkdvPipe/media_id")

    # Namespace for generating deterministic event_ids (UUID5)
    # Using a fixed namespace ensures the same trace_id always yields the same event_id
    # This provides replayability and consistent tracking across the pipeline
    EVENT_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "http://nsmkdvPipe/event_id")

    def __init__(
        self,
        config: KafkaConfig,
        domain: str = "verisk",
        producer_config: KafkaConfig | None = None,
        instance_id: str | None = None,
    ):
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.instance_id = instance_id

        # Create worker_id with instance suffix (coolname) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        self.producer: BaseKafkaProducer | None = None
        self.consumer: BaseKafkaConsumer | None = None

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_skipped = 0
        self._records_deduplicated = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: asyncio.Task | None = None
        self._running = False

        # In-memory dedup cache: trace_id -> event_id
        # Prevents duplicate event processing when Eventhouse sends duplicates
        self._dedup_cache: dict[str, str] = {}
        self._dedup_cache_ttl_seconds = 86400  # 24 hours
        self._dedup_cache_max_size = 100_000  # ~2MB memory for 100k entries
        self._dedup_cache_timestamps: dict[str, float] = {}  # trace_id -> timestamp

        # Health check server - use worker-specific port from config
        processing_config = config.get_worker_config(
            domain, "event_ingester", "processing"
        )
        health_port = processing_config.get("health_port", 8092)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="xact-event-ingester",
        )

        logger.info(
            "Initialized EventIngesterWorker",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": "event_ingester",
                "instance_id": instance_id,
                "events_topic": config.get_topic(domain, "events"),
                "enrichment_topic": self.producer_config.get_topic(
                    domain, "enrichment_pending"
                ),
                "pipeline_domain": self.domain,
                "separate_producer_config": producer_config is not None,
            },
        )

    @property
    def config(self) -> KafkaConfig:
        """Backward-compatible property returning consumer_config."""
        return self.consumer_config

    async def start(self) -> None:
        logger.info("Starting EventIngesterWorker")
        self._running = True

        from kafka_pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "event-ingester")

        # Start health check server first
        await self.health_server.start()

        # Start producer first (uses producer_config for local Kafka)
        self.producer = BaseKafkaProducer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="event_ingester",
        )
        await self.producer.start()

        # Start cycle output background task
        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        # Create and start consumer with message handler (uses consumer_config)
        self.consumer = BaseKafkaConsumer(
            config=self.consumer_config,
            domain=self.domain,
            worker_name="event_ingester",
            topics=[self.consumer_config.get_topic(self.domain, "events")],
            message_handler=self._handle_event_message,
        )

        # Update health check readiness
        self.health_server.set_ready(kafka_connected=True)

        try:
            # Start consumer (this blocks until stopped)
            await self.consumer.start()
        finally:
            self._running = False

    async def stop(self) -> None:
        logger.info("Stopping EventIngesterWorker")
        self._running = False

        # Cancel cycle output task
        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cycle_task

        # Stop consumer first (stops receiving new messages)
        if self.consumer:
            await self.consumer.stop()

        # Then stop producer (flushes pending messages)
        if self.producer:
            await self.producer.stop()

        # Stop health check server
        await self.health_server.stop()

        logger.info("EventIngesterWorker stopped successfully")

    async def _handle_event_message(self, record: PipelineMessage) -> None:
        # Start timing for metrics
        start_time = time.perf_counter()

        # Track events received for cycle output
        self._records_processed += 1

        # Decode and parse EventMessage
        from kafka_pipeline.common.telemetry import get_tracer

        tracer = get_tracer(__name__)
        try:
            with tracer.start_active_span("event.parse") as scope:
                span = scope.span if hasattr(scope, "span") else scope
                span.set_tag("span.kind", "internal")
                message_data = json.loads(record.value.decode("utf-8"))
                event = EventMessage.from_eventhouse_row(message_data)
                span.set_tag("event.type", event.type)
                span.set_tag("event.status_subtype", event.status_subtype)
                span.set_tag(
                    "event.attachment_count",
                    len(event.attachments) if event.attachments else 0,
                )
        except (json.JSONDecodeError, ValidationError) as e:
            logger.error(
                "Failed to parse EventMessage",
                extra={
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        # Generate deterministic event_id from trace_id (UUID5)
        # This provides stable IDs across retries/replays for consistent tracking
        event_id = str(uuid.uuid5(self.EVENT_ID_NAMESPACE, event.trace_id))
        event.event_id = event_id

        # Set logging context for this request
        set_log_context(trace_id=event.trace_id)

        # Check for duplicates (same trace_id processed recently)
        is_duplicate, cached_event_id = self._is_duplicate(event.trace_id)
        if is_duplicate:
            self._records_deduplicated += 1
            logger.debug(
                "Skipping duplicate event (already processed recently)",
                extra={
                    "trace_id": event.trace_id,
                    "event_id": event_id,
                    "cached_event_id": cached_event_id,
                },
            )
            return

        logger.info(
            "Ingested event",
            extra={
                "trace_id": event.trace_id,
                "event_id": event_id,
                "type": event.type,
                "status_subtype": event.status_subtype,
                "attachment_count": len(event.attachments) if event.attachments else 0,
            },
        )

        # Extract assignment_id from data (required for enrichment)
        assignment_id = event.assignment_id
        if not assignment_id:
            self._records_skipped += 1
            logger.warning(
                "Event missing assignmentId in data, skipping enrichment",
                extra={
                    "trace_id": event.trace_id,
                    "event_id": event.event_id,
                    "type": event.type,
                },
            )
            return

        # Create enrichment task for this event
        # Note: Even events without attachments are sent to enrichment for plugin execution
        await self._create_enrichment_task(event, event_id, assignment_id)

        # Mark event as processed in dedup cache
        self._mark_processed(event.trace_id, event_id)

        # Periodic cleanup of expired cache entries
        self._cleanup_dedup_cache()

        # Record successful ingestion and duration
        duration = time.perf_counter() - start_time
        message_processing_duration_seconds.labels(
            topic=self.consumer_config.get_topic(self.domain, "events"),
            consumer_group=f"{self.domain}-event-ingester",
        ).observe(duration)

    async def _create_enrichment_task(
        self,
        event: EventMessage,
        event_id: str,
        assignment_id: str,
    ) -> None:
        """Create and produce enrichment task for this event."""
        from kafka_pipeline.common.telemetry import get_tracer

        tracer = get_tracer(__name__)
        with tracer.start_active_span("event.create_enrichment") as scope:
            span = scope.span if hasattr(scope, "span") else scope
            span.set_tag("span.kind", "internal")
            span.set_tag("trace_id", event.trace_id)
            span.set_tag("event_id", event_id)
            span.set_tag(
                "attachment_count", len(event.attachments) if event.attachments else 0
            )

            # Parse original timestamp from event
            original_timestamp = datetime.fromisoformat(
                event.utc_datetime.replace("Z", "+00:00")
            )

            # Create enrichment task with all event data
            enrichment_task = XACTEnrichmentTask(
                event_id=event_id,
                trace_id=event.trace_id,
                event_type=self.domain,
                status_subtype=event.status_subtype,
                assignment_id=assignment_id,
                estimate_version=event.estimate_version,
                attachments=event.attachments or [],
                retry_count=0,
                created_at=datetime.now(UTC),
                original_timestamp=original_timestamp,
            )

            # Produce enrichment task to pending topic
            # CRITICAL: Must await send confirmation before allowing offset commit
            try:
                metadata = await self.producer.send(
                    topic=self.producer_config.get_topic(
                        self.domain, "enrichment_pending"
                    ),
                    key=event.trace_id,
                    value=enrichment_task,
                    headers={"trace_id": event.trace_id, "event_id": event_id},
                )

                # Track successful task creation for cycle output
                self._records_succeeded += 1

                span.set_tag("task.created", True)
                span.set_tag("task.partition", metadata.partition)
                span.set_tag("task.offset", metadata.offset)

                logger.info(
                    "Created enrichment task",
                    extra={
                        "trace_id": event.trace_id,
                        "event_id": event_id,
                        "status_subtype": event.status_subtype,
                        "assignment_id": assignment_id,
                        "attachment_count": (
                            len(event.attachments) if event.attachments else 0
                        ),
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                    },
                )
            except Exception as e:
                # Record send failure metric
                record_processing_error(
                    topic=self.producer_config.get_topic(
                        self.domain, "enrichment_pending"
                    ),
                    consumer_group=f"{self.domain}-event-ingester",
                    error_type="SEND_FAILED",
                )

                span.set_tag("task.created", False)
                span.set_tag("error", str(e))

                log_worker_error(
                    logger,
                    "Failed to produce enrichment task - will retry on next poll",
                    event_id=event_id,
                    error_category="TRANSIENT",
                    exc=e,
                    trace_id=event.trace_id,
                    error_type=type(e).__name__,
                )
                # Re-raise to prevent offset commit - message will be retried
                # This ensures at-least-once semantics: if send fails, we retry
                raise

    async def _periodic_cycle_output(self) -> None:
        logger.info(
            f"{format_cycle_output(0, 0, 0, 0, 0)} [cycle output every {self.CYCLE_LOG_INTERVAL_SECONDS}s]",
            extra={
                "worker_id": self.WORKER_NAME,
                "stage": "ingestion",
                "cycle": 0,
            },
        )
        self._last_cycle_log = time.monotonic()

        try:
            while self._running:
                await asyncio.sleep(1)

                # Log cycle output at regular intervals
                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= self.CYCLE_LOG_INTERVAL_SECONDS:
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()

                    logger.info(
                        format_cycle_output(
                            cycle_count=self._cycle_count,
                            succeeded=self._records_succeeded,
                            failed=0,
                            skipped=self._records_skipped,
                            deduplicated=self._records_deduplicated,
                        ),
                        extra={
                            "worker_id": self.WORKER_NAME,
                            "stage": "ingestion",
                            "cycle": self._cycle_count,
                            "cycle_id": f"cycle-{self._cycle_count}",
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_skipped": self._records_skipped,
                            "records_deduplicated": self._records_deduplicated,
                            "cycle_interval_seconds": self.CYCLE_LOG_INTERVAL_SECONDS,
                        },
                    )

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise

    def _is_duplicate(self, trace_id: str) -> tuple[bool, str | None]:
        now = time.time()

        # Check if in cache and not expired
        if trace_id in self._dedup_cache:
            cached_time = self._dedup_cache_timestamps.get(trace_id, 0)
            if now - cached_time < self._dedup_cache_ttl_seconds:
                return True, self._dedup_cache[trace_id]
            # Expired - remove from both caches
            del self._dedup_cache[trace_id]
            self._dedup_cache_timestamps.pop(trace_id, None)

        return False, None

    def _mark_processed(self, trace_id: str, event_id: str) -> None:
        """Add trace_id -> event_id mapping to dedup cache (LRU eviction)."""
        now = time.time()

        # If cache is full, evict oldest entries (simple LRU)
        if len(self._dedup_cache) >= self._dedup_cache_max_size:
            # Sort by timestamp and remove oldest 10%
            sorted_items = sorted(
                self._dedup_cache_timestamps.items(), key=lambda x: x[1]
            )
            evict_count = self._dedup_cache_max_size // 10
            for trace_id_to_evict, _ in sorted_items[:evict_count]:
                self._dedup_cache.pop(trace_id_to_evict, None)
                self._dedup_cache_timestamps.pop(trace_id_to_evict, None)

            logger.debug(
                "Evicted old entries from event dedup cache",
                extra={
                    "evicted_count": evict_count,
                    "cache_size": len(self._dedup_cache),
                },
            )

        # Add to caches
        self._dedup_cache[trace_id] = event_id
        self._dedup_cache_timestamps[trace_id] = now

    def _cleanup_dedup_cache(self) -> None:
        now = time.time()
        expired_keys = [
            trace_id
            for trace_id, cached_time in self._dedup_cache_timestamps.items()
            if now - cached_time >= self._dedup_cache_ttl_seconds
        ]

        for trace_id in expired_keys:
            self._dedup_cache.pop(trace_id, None)
            self._dedup_cache_timestamps.pop(trace_id, None)

        if expired_keys:
            logger.debug(
                "Cleaned up expired event dedup cache entries",
                extra={
                    "expired_count": len(expired_keys),
                    "cache_size": len(self._dedup_cache),
                },
            )


__all__ = ["EventIngesterWorker"]
