"""
Event Ingester Worker - Consumes events and produces download tasks.

This worker is the entry point to the download pipeline:
1. Consumes EventMessage from events.raw topic
2. Validates attachment URLs against domain allowlist
3. Generates blob storage paths for each attachment
4. Produces DownloadTaskMessage to downloads.pending topic

Note: Delta Lake writes are handled separately by DeltaEventsWorker,
which consumes from the same topic with a different consumer group.

Schema compatibility:
- EventMessage matches verisk_pipeline EventRecord
- DownloadTaskMessage matches verisk_pipeline Task

Consumer group: {prefix}-event-ingester
Input topic: events.raw
Output topic: downloads.pending
"""

import asyncio
import json
import time
import uuid
from datetime import datetime
from typing import Optional

from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from core.logging.context import set_log_context
from core.logging.setup import get_logger
from core.paths.resolver import generate_blob_path
from core.security.url_validation import validate_download_url, sanitize_url
from config.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.xact.schemas.events import EventMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.common.metrics import (
    event_ingestion_duration_seconds,
    record_event_ingested,
    record_event_task_produced,
    record_processing_error,
)

logger = get_logger(__name__)


class EventIngesterWorker:
    """
    Worker to consume events and produce download tasks.

    Processes EventMessage records from the events.raw topic, validates
    attachment URLs, generates storage paths, and produces DownloadTaskMessage
    records to the downloads.pending topic for processing by download workers.

    Note: Delta Lake writes are handled by a separate DeltaEventsWorker that
    consumes from the same topic with a different consumer group.

    Features:
    - URL validation with domain allowlist
    - Automatic blob path generation
    - Trace ID preservation for downstream tracking
    - Graceful handling of events without attachments
    - Sanitized logging of validation failures

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> worker = EventIngesterWorker(config)
        >>> await worker.start()
        >>> # Worker runs until stopped
        >>> await worker.stop()
    """

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
        domain: str = "xact",
        producer_config: Optional[KafkaConfig] = None,
    ):
        """
        Initialize event ingester worker.

        Args:
            config: Kafka configuration for consumer (topic names, connection settings)
            domain: Domain identifier for OneLake routing (e.g., "xact", "claimx")
            producer_config: Optional separate Kafka config for producer. If not provided,
                uses the consumer config. This is needed when reading from Event Hub
                but writing to local Kafka.
        """
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.producer: Optional[BaseKafkaProducer] = None
        self.consumer: Optional[BaseKafkaConsumer] = None

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_skipped = 0
        self._records_deduplicated = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: Optional[asyncio.Task] = None
        self._running = False

        # In-memory dedup cache: trace_id -> event_id
        # Prevents duplicate event processing when Eventhouse sends duplicates
        self._dedup_cache: dict[str, str] = {}
        self._dedup_cache_ttl_seconds = 86400  # 24 hours
        self._dedup_cache_max_size = 100_000  # ~2MB memory for 100k entries
        self._dedup_cache_timestamps: dict[str, float] = {}  # trace_id -> timestamp

        # Health check server - use worker-specific port from config
        processing_config = config.get_worker_config(domain, "event_ingester", "processing")
        health_port = processing_config.get("health_port", 8092)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="xact-event-ingester",
        )

        logger.info(
            "Initialized EventIngesterWorker",
            extra={
                "domain": domain,
                "worker_name": "event_ingester",
                "events_topic": config.get_topic(domain, "events"),
                "ingested_topic": self.producer_config.get_topic(domain, "events_ingested"),
                "pending_topic": self.producer_config.get_topic(domain, "downloads_pending"),
                "pipeline_domain": self.domain,
                "separate_producer_config": producer_config is not None,
            },
        )

    @property
    def config(self) -> KafkaConfig:
        """Backward-compatible property returning consumer_config."""
        return self.consumer_config

    async def start(self) -> None:
        """
        Start the event ingester worker.

        Initializes producer and consumer, then begins consuming events
        from the events.raw topic. This method runs until stop() is called.

        Raises:
            Exception: If producer or consumer fails to start
        """
        logger.info("Starting EventIngesterWorker")
        self._running = True

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
        """
        Stop the event ingester worker.

        Gracefully shuts down consumer and producer, committing any pending
        offsets and flushing pending messages.
        """
        logger.info("Stopping EventIngesterWorker")
        self._running = False

        # Cancel cycle output task
        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            try:
                await self._cycle_task
            except asyncio.CancelledError:
                pass

        # Stop consumer first (stops receiving new messages)
        if self.consumer:
            await self.consumer.stop()

        # Then stop producer (flushes pending messages)
        if self.producer:
            await self.producer.stop()

        # Stop health check server
        await self.health_server.stop()

        logger.info("EventIngesterWorker stopped successfully")

    async def _handle_event_message(self, record: ConsumerRecord) -> None:
        """
        Process a single event message from Kafka.

        Parses the EventMessage (matching verisk_pipeline EventRecord schema),
        validates attachments, generates download tasks, and produces them to
        the pending topic.

        Args:
            record: ConsumerRecord containing EventMessage JSON

        Raises:
            Exception: If message processing fails (will be handled by consumer error routing)
        """
        # Start timing for metrics
        start_time = time.perf_counter()

        # Track events received for cycle output
        self._records_processed += 1

        # Decode and parse EventMessage
        try:
            message_data = json.loads(record.value.decode("utf-8"))
            event = EventMessage.from_eventhouse_row(message_data)
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
            # Record parse error in metrics
            record_event_ingested(domain=self.domain, status="parse_error")
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

        # Produce to ingested topic (ALL events)
        # CRITICAL: Must await send confirmation before allowing offset commit
        try:
            metadata = await self.producer.send(
                topic=self.producer_config.get_topic(self.domain, "events_ingested"),
                key=event.trace_id,
                value=event,
                headers={"trace_id": event.trace_id, "event_id": event_id},
            )
            logger.debug(
                "Event produced to ingested topic",
                extra={
                    "trace_id": event.trace_id,
                    "event_id": event_id,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                },
            )
        except Exception as e:
            # Record send failure metric
            record_processing_error(
                topic=self.producer_config.get_topic(self.domain, "events_ingested"),
                consumer_group=f"{self.domain}-event-ingester",
                error_type="SEND_FAILED"
            )

            logger.error(
                "Failed to produce to ingested topic - will retry on next poll",
                extra={
                    "trace_id": event.trace_id,
                    "event_id": event_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )
            # Re-raise to prevent offset commit - message will be retried
            # This ensures at-least-once semantics: if send fails, we retry
            raise

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

        # Skip events without attachments (many events are just status updates)
        if not event.attachments:
            self._records_skipped += 1
            logger.debug(
                "Event has no attachments, skipping download task creation",
                extra={"trace_id": event.trace_id, "event_id": event.event_id},
            )
            return

        # Extract assignment_id from data (required for path generation)
        assignment_id = event.assignment_id
        if not assignment_id:
            self._records_skipped += 1
            logger.warning(
                "Event missing assignmentId in data, cannot generate paths",
                extra={
                    "trace_id": event.trace_id,
                    "event_id": event.event_id,
                    "type": event.type,
                },
            )
            return

        # Process each attachment
        for attachment_url in event.attachments:
            await self._process_attachment(
                event=event,
                attachment_url=attachment_url,
                assignment_id=assignment_id,
            )

        # Mark event as processed in dedup cache
        self._mark_processed(event.trace_id, event_id)

        # Periodic cleanup of expired cache entries
        self._cleanup_dedup_cache()

        # Record successful ingestion and duration
        duration = time.perf_counter() - start_time
        event_ingestion_duration_seconds.labels(domain=self.domain).observe(duration)
        record_event_ingested(domain=self.domain, status="success")

    async def _process_attachment(
        self,
        event: EventMessage,
        attachment_url: str,
        assignment_id: str,
    ) -> None:
        """
        Process a single attachment from an event.

        Validates the URL, generates a blob path, creates a download task
        matching verisk_pipeline Task schema, and produces it to pending topic.

        Args:
            event: Source EventMessage (matches verisk_pipeline EventRecord)
            attachment_url: URL of the attachment to download
            assignment_id: Assignment ID for path generation
        """
        # Validate attachment URL
        is_valid, error_message = validate_download_url(attachment_url)
        if not is_valid:
            logger.warning(
                "Invalid attachment URL, skipping",
                extra={
                    "trace_id": event.trace_id,
                    "event_id": event.event_id,
                    "type": event.type,
                    "url": sanitize_url(attachment_url),
                    "validation_error": error_message,
                },
            )
            return

        # Generate blob storage path (using status_subtype from event type)
        try:
            blob_path, file_type = generate_blob_path(
                status_subtype=event.status_subtype,
                trace_id=event.trace_id,
                assignment_id=assignment_id,
                download_url=attachment_url,
                estimate_version=event.estimate_version,
            )
        except Exception as e:
            logger.error(
                "Failed to generate blob path",
                extra={
                    "trace_id": event.trace_id,
                    "event_id": event.event_id,
                    "status_subtype": event.status_subtype,
                    "assignment_id": assignment_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise

        # Parse original timestamp from event
        original_timestamp = datetime.fromisoformat(
            event.utc_datetime.replace("Z", "+00:00")
        )

        # Generate deterministic media_id from trace_id and attachment_url
        # This provides a unique ID for each attachment that is stable across retries/replays
        media_id = str(uuid.uuid5(self.MEDIA_ID_NAMESPACE, f"{event.trace_id}:{attachment_url}"))

        # Create download task message matching verisk_pipeline Task schema
        download_task = DownloadTaskMessage(
            media_id=media_id,
            trace_id=event.trace_id,
            attachment_url=attachment_url,
            blob_path=blob_path,
            status_subtype=event.status_subtype,
            file_type=file_type,
            assignment_id=assignment_id,
            estimate_version=event.estimate_version,
            retry_count=0,
            event_type=self.domain,  # Use configured domain for OneLake routing
            event_subtype=event.status_subtype,
            original_timestamp=original_timestamp,
        )

        # Produce download task to pending topic
        # CRITICAL: Must await send confirmation before allowing offset commit
        try:
            metadata = await self.producer.send(
                topic=self.producer_config.get_topic(self.domain, "downloads_pending"),
                key=event.trace_id,
                value=download_task,
                headers={"trace_id": event.trace_id},
            )

            # Track successful task creation for cycle output
            self._records_succeeded += 1

            # Record task produced metric
            record_event_task_produced(domain=self.domain, task_type="download_task")

            logger.info(
                "Created download task",
                extra={
                    "trace_id": event.trace_id,
                    "event_id": event.event_id,
                    "media_id": media_id,
                    "blob_path": blob_path,
                    "status_subtype": event.status_subtype,
                    "file_type": file_type,
                    "assignment_id": assignment_id,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                },
            )
        except Exception as e:
            # Record send failure metric
            record_processing_error(
                topic=self.producer_config.get_topic(self.domain, "downloads_pending"),
                consumer_group=f"{self.domain}-event-ingester",
                error_type="SEND_FAILED"
            )

            logger.error(
                "Failed to produce download task - will retry on next poll",
                extra={
                    "trace_id": event.trace_id,
                    "event_id": event.event_id,
                    "media_id": media_id,
                    "blob_path": blob_path,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )
            # Re-raise to prevent offset commit - message will be retried
            # This ensures at-least-once semantics: if send fails, we retry
            raise

    async def _periodic_cycle_output(self) -> None:
        """
        Background task for periodic cycle logging.

        Logs processing statistics at regular intervals for operational visibility.
        """
        logger.info(
            "Cycle 0: events=0 (tasks=0, skipped=0, deduped=0) [cycle output every %ds]",
            self.CYCLE_LOG_INTERVAL_SECONDS,
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
                        f"Cycle {self._cycle_count}: processed={self._records_processed} "
                        f"(succeeded={self._records_succeeded}, skipped={self._records_skipped}, "
                        f"deduped={self._records_deduplicated})",
                        extra={
                            "cycle": self._cycle_count,
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

    def _is_duplicate(self, trace_id: str) -> tuple[bool, Optional[str]]:
        """
        Check if trace_id is in dedup cache (already processed recently).

        Args:
            trace_id: Trace ID to check

        Returns:
            Tuple of (is_duplicate, cached_event_id or None)
        """
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
        """
        Add trace_id -> event_id mapping to dedup cache.

        Implements simple LRU eviction if cache is full.

        Args:
            trace_id: Trace ID to mark as processed
            event_id: Event ID generated for this trace
        """
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
        """Remove expired entries from dedup cache (TTL-based cleanup)."""
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
