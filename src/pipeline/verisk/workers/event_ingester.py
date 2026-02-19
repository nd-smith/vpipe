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
import logging
import time
import uuid
from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from config.config import MessageConfig
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import format_cycle_output
from pipeline.common.eventhub.dedup_store import (
    DedupStoreProtocol,
    close_dedup_store,
    get_dedup_store,
    is_dedup_enabled,
)
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    message_processing_duration_seconds,
    record_processing_error,
)
from pipeline.common.consumer_config import ConsumerConfig
from pipeline.common.transport import (
    create_batch_consumer,
    create_producer,
    get_source_connection_string,
)
from pipeline.common.types import BatchResult, PipelineMessage
from pipeline.verisk.schemas.events import EventMessage
from pipeline.verisk.schemas.tasks import XACTEnrichmentTask
from pipeline.common.worker_defaults import CYCLE_LOG_INTERVAL_SECONDS

logger = logging.getLogger(__name__)


class EventIngesterWorker:
    """Worker to consume events and produce download tasks."""

    WORKER_NAME = "event_ingester"

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = CYCLE_LOG_INTERVAL_SECONDS

    # Backfill prefetch mode — dynamically increase batch size for stale data
    BACKFILL_BATCH_SIZE = 2000
    REALTIME_BATCH_SIZE = 100
    BACKFILL_THRESHOLD_SECONDS = 3600  # 1 hour

    # Namespace for generating deterministic media_ids (UUID5)
    # Using a fixed namespace ensures the same trace_id + url always yields the same media_id
    MEDIA_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "http://nsmkdvPipe/media_id")

    # Namespace for generating deterministic event_ids (UUID5)
    # Using a fixed namespace ensures the same trace_id always yields the same event_id
    # This provides replayability and consistent tracking across the pipeline
    EVENT_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "http://nsmkdvPipe/event_id")

    def __init__(
        self,
        config: MessageConfig,
        domain: str = "verisk",
        producer_config: MessageConfig | None = None,
        instance_id: str | None = None,
    ):
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.instance_id = instance_id

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        self.enrichment_topic = self.producer_config.get_topic(domain, "enrichment_pending")
        self.producer = None
        self.consumer = None

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_skipped = 0
        self._records_deduplicated = 0
        self._dedup_memory_hits = 0
        self._dedup_blob_hits = 0
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        self._stats_logger: PeriodicStatsLogger | None = None
        self._running = False

        # Dedup bypass flag (checked at start from config)
        self._dedup_enabled = True

        # Hybrid dedup: in-memory cache (fast path) + blob storage (persistent)
        # OrderedDict for O(1) LRU eviction: trace_id -> (event_id, timestamp)
        self._dedup_cache: OrderedDict[str, tuple[str, float]] = OrderedDict()
        self._dedup_cache_ttl_seconds = 86400  # 24 hours
        self._dedup_cache_max_size = 100_000  # ~2MB memory for 100k entries

        # Persistent blob storage (survives worker restarts)
        self._dedup_store: DedupStoreProtocol | None = None
        self._dedup_worker_name = "verisk-event-ingester"
        self._dedup_cleanup_task: asyncio.Task | None = None
        self._blob_write_tasks: set[asyncio.Task] = set()
        self._blob_semaphore = asyncio.Semaphore(50)

        # Health check server
        health_port = 8092
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
                "enrichment_topic": self.producer_config.get_topic(domain, "enrichment_pending"),
                "pipeline_domain": self.domain,
                "separate_producer_config": producer_config is not None,
            },
        )

    async def start(self) -> None:
        logger.info("Starting EventIngesterWorker")
        self._running = True

        # Clean up resources from a previous failed start attempt (retry safety)
        await self._cleanup_stale_resource("stats logger", self._stats_logger, "_stats_logger")
        await self._cleanup_stale_resource("consumer", self.consumer, "consumer")
        await self._cleanup_stale_resource("producer", self.producer, "producer")

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "event-ingester")

        # Initialize dedup subsystem
        self._dedup_enabled = is_dedup_enabled()
        if not self._dedup_enabled:
            logger.info("Dedup disabled via config")
        else:
            self._dedup_store = await get_dedup_store()
            if self._dedup_store:
                logger.info("Persistent dedup store enabled")
            else:
                logger.info("Persistent dedup store not configured - using memory-only deduplication")

        # Start producer first (uses transport factory for Event Hub support)
        self.producer = create_producer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="event_ingester",
            topic_key="enrichment_pending",
        )
        await self.producer.start()

        # Sync topic with producer's actual entity name (Event Hub entity may
        # differ from the Kafka topic name resolved by get_topic()).
        if hasattr(self.producer, "eventhub_name"):
            self.enrichment_topic = self.producer.eventhub_name

        # Start periodic stats logger
        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=self.CYCLE_LOG_INTERVAL_SECONDS,
            get_stats=self._get_cycle_stats,
            stage="ingestion",
            worker_id=self.WORKER_NAME,
        )
        self._stats_logger.start()

        # Start periodic dedup cache cleanup (every 60s)
        if self._dedup_enabled:
            self._dedup_cleanup_task = asyncio.create_task(self._periodic_dedup_cleanup())

        # Create and start batch consumer (uses transport factory)
        self.consumer = await create_batch_consumer(
            config=self.consumer_config,
            domain=self.domain,
            worker_name="event_ingester",
            topics=[self.consumer_config.get_topic(self.domain, "events")],
            batch_handler=self._handle_event_batch,
            topic_key="events",
            connection_string=get_source_connection_string(),
            consumer_config=ConsumerConfig(
                batch_size=self.REALTIME_BATCH_SIZE,
                max_batch_size=self.BACKFILL_BATCH_SIZE,
                batch_timeout_ms=500,
                prefetch=3000,
            ),
        )

        # Update health check readiness
        self.health_server.set_ready(transport_connected=True)

        try:
            # Start consumer (this blocks until stopped)
            await self.consumer.start()
        except asyncio.CancelledError:
            logger.info("EventIngesterWorker cancelled, shutting down...")
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        logger.info("Stopping EventIngesterWorker")
        self._running = False

        await self._close_resource("stats logger", self._stop_stats_logger)
        await self._close_resource("dedup cleanup task", self._cancel_dedup_cleanup)
        await self._close_resource("consumer", self._stop_consumer, clear="consumer")
        await self._close_resource("producer", self._stop_producer, clear="producer")
        await self._close_resource("dedup store", close_dedup_store)
        await self._close_resource("health server", self.health_server.stop)

        logger.info("EventIngesterWorker stopped successfully")

    async def _close_resource(self, name: str, method, *, clear: str | None = None) -> None:
        try:
            await method()
        except asyncio.CancelledError:
            logger.warning(f"Cancelled while stopping {name}")
        except Exception as e:
            logger.error(f"Error stopping {name}", extra={"error": str(e)})
        finally:
            if clear:
                setattr(self, clear, None)

    async def _cleanup_stale_resource(self, name: str, resource, attr: str) -> None:
        if resource is None:
            return
        try:
            await resource.stop()
        except Exception as e:
            logger.warning(f"Error cleaning up stale {name}", extra={"error": str(e)})
        finally:
            setattr(self, attr, None)

    async def _stop_stats_logger(self) -> None:
        if self._stats_logger:
            await self._stats_logger.stop()

    async def _cancel_dedup_cleanup(self) -> None:
        if self._dedup_cleanup_task and not self._dedup_cleanup_task.done():
            self._dedup_cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._dedup_cleanup_task

    async def _stop_consumer(self) -> None:
        if self.consumer:
            await self.consumer.stop()

    async def _stop_producer(self) -> None:
        if self.producer:
            await self.producer.stop()

    async def _handle_event_batch(self, records: list[PipelineMessage]) -> BatchResult:
        """Process a batch of event messages.

        Parses, deduplicates, and batch-produces enrichment tasks.
        Returns BatchResult with commit flag and any permanent parse failures for DLQ routing.
        """
        start_time = time.perf_counter()

        # Parse, dedup (memory + blob), and collect non-duplicate events
        parsed_events, dedup_counts, permanent_failures = await self._parse_and_dedup_events(records)

        # Build enrichment tasks from non-duplicate events
        enrichment_tasks, processed_trace_ids = self._build_enrichment_tasks(parsed_events)

        # Log dedup diagnostics with sample trace_ids for KQL cross-referencing
        self._log_dedup_diagnostics(dedup_counts)

        # Batch-produce all enrichment tasks
        if enrichment_tasks:
            try:
                await self.producer.send_batch(messages=enrichment_tasks)
                self._records_succeeded += len(enrichment_tasks)
            except Exception as e:
                # Roll back dedup marks — messages will be redelivered
                for trace_id, _ in processed_trace_ids:
                    self._dedup_cache.pop(trace_id, None)

                record_processing_error(
                    topic=self.producer_config.get_topic(self.domain, "enrichment_pending"),
                    consumer_group=f"{self.domain}-event-ingester",
                    error_type="SEND_BATCH_FAILED",
                )
                logger.error(
                    "Failed to send enrichment batch — will retry",
                    extra={
                        "batch_size": len(enrichment_tasks),
                        "error": str(e),
                    },
                    exc_info=True,
                )
                return BatchResult(commit=False, permanent_failures=permanent_failures)

        # Adjust batch size based on event age (backfill vs realtime)
        if enrichment_tasks:
            _, last_task = enrichment_tasks[-1]
            ts = last_task.original_timestamp
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            event_age = (datetime.now(UTC) - ts).total_seconds()
            self._adjust_batch_size(event_age)

        # Periodic cleanup of completed fire-and-forget blob write tasks
        if len(self._blob_write_tasks) > 200:
            self._blob_write_tasks = {t for t in self._blob_write_tasks if not t.done()}

        # Record batch processing duration
        duration = time.perf_counter() - start_time
        message_processing_duration_seconds.labels(
            topic=self.consumer_config.get_topic(self.domain, "events"),
            consumer_group=f"{self.domain}-event-ingester",
        ).observe(duration)

        return BatchResult(commit=True, permanent_failures=permanent_failures)

    def _update_cycle_offsets(self, ts) -> None:
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

    def _is_memory_duplicate(self, trace_id: str, now: float) -> bool:
        """Check memory cache for duplicate. Returns True if duplicate found."""
        if trace_id not in self._dedup_cache:
            return False
        cached_event_id, cached_time = self._dedup_cache[trace_id]
        if now - cached_time < self._dedup_cache_ttl_seconds:
            self._dedup_cache.move_to_end(trace_id)
            self._dedup_memory_hits += 1
            self._records_deduplicated += 1
            return True
        del self._dedup_cache[trace_id]
        return False

    def _check_dedup(
        self, trace_id: str, seen_in_batch: set[str], now: float,
        dedup_counts: dict[str, list[str]],
    ) -> bool:
        """Check intra-batch and memory cache for duplicate. Returns True if duplicate."""
        if trace_id in seen_in_batch:
            dedup_counts["intra_batch"].append(trace_id)
            self._records_deduplicated += 1
            return True
        seen_in_batch.add(trace_id)
        if self._is_memory_duplicate(trace_id, now):
            dedup_counts["memory"].append(trace_id)
            return True
        return False

    async def _parse_and_dedup_events(
        self, records: list[PipelineMessage],
    ) -> tuple[list[tuple[EventMessage, str]], dict[str, list[str]], list[tuple[PipelineMessage, Exception]]]:
        """Parse records, deduplicate via memory cache and blob storage.

        Returns non-duplicate (event, event_id) pairs, dedup diagnostic counts,
        and permanent failures (unparseable messages) for DLQ routing.
        """
        parsed_events: list[tuple[EventMessage, str]] = []
        permanent_failures: list[tuple[PipelineMessage, Exception]] = []
        seen_in_batch: set[str] = set()
        dedup_counts: dict[str, list[str]] = {
            "intra_batch": [], "memory": [], "blob": [],
        }
        now = time.time()

        for record in records:
            self._records_processed += 1
            self._update_cycle_offsets(record.timestamp)

            try:
                message_data = json.loads(record.value.decode("utf-8"))
                event = EventMessage.from_raw_event(message_data)
            except (json.JSONDecodeError, ValidationError) as e:
                logger.error(
                    "Failed to parse EventMessage, routing to DLQ",
                    extra={
                        "trace_id": record.key.decode("utf-8") if record.key else None,
                        "topic": record.topic,
                        "partition": record.partition,
                        "offset": record.offset,
                        "error": str(e),
                    },
                )
                permanent_failures.append((record, e))
                continue

            event_id = str(uuid.uuid5(self.EVENT_ID_NAMESPACE, event.trace_id))
            event.event_id = event_id

            if self._dedup_enabled and self._check_dedup(
                event.trace_id, seen_in_batch, now, dedup_counts,
            ):
                continue

            parsed_events.append((event, event_id))

        if self._dedup_store and parsed_events:
            blob_duplicates = await self._check_blob_duplicates(parsed_events)
            dedup_counts["blob"] = list(blob_duplicates)
            parsed_events = [
                (ev, eid) for ev, eid in parsed_events
                if ev.trace_id not in blob_duplicates
            ]

        return parsed_events, dedup_counts, permanent_failures

    async def _check_blob_duplicates(
        self, events: list[tuple[EventMessage, str]],
    ) -> set[str]:
        """Check blob storage for duplicates, return set of duplicate trace IDs."""
        async def _check_one(trace_id: str) -> tuple[str, bool]:
            async with self._blob_semaphore:
                try:
                    is_dup, metadata = await self._dedup_store.check_duplicate(
                        self._dedup_worker_name, trace_id, self._dedup_cache_ttl_seconds,
                    )
                    if is_dup and metadata:
                        cached_event_id = metadata.get("event_id")
                        timestamp = metadata.get("timestamp", time.time())
                        self._dedup_cache[trace_id] = (cached_event_id, timestamp)
                        self._dedup_blob_hits += 1
                        return trace_id, True
                except Exception as e:
                    logger.warning(
                        "Error checking blob storage for duplicate (falling back to memory-only)",
                        extra={"trace_id": trace_id, "error": str(e)},
                    )
                return trace_id, False

        results = await asyncio.gather(
            *(_check_one(ev.trace_id) for ev, _ in events)
        )
        duplicates = {tid for tid, is_dup in results if is_dup}

        for _ in duplicates:
            self._records_deduplicated += 1

        return duplicates

    def _build_enrichment_tasks(
        self, parsed_events: list[tuple[EventMessage, str]],
    ) -> tuple[list[tuple[str, XACTEnrichmentTask]], list[tuple[str, str]]]:
        """Create enrichment tasks from validated, non-duplicate events."""
        enrichment_tasks: list[tuple[str, XACTEnrichmentTask]] = []
        processed_trace_ids: list[tuple[str, str]] = []

        for event, event_id in parsed_events:
            assignment_id = event.assignment_id
            if not assignment_id:
                self._records_skipped += 1
                logger.warning(
                    "Event missing assignmentId in data, skipping enrichment",
                    extra={
                        "trace_id": event.trace_id,
                        "type": event.type,
                    },
                )
                continue

            original_timestamp = datetime.fromisoformat(event.utc_datetime.replace("Z", "+00:00"))

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

            enrichment_tasks.append((event.trace_id, enrichment_task))
            processed_trace_ids.append((event.trace_id, event_id))

            logger.info(
                "Event ingested",
                extra={
                    "trace_id": event.trace_id,
                    "assignment_id": assignment_id,
                    "event_type": event.type,
                    "status_subtype": event.status_subtype,
                },
            )
            if self._dedup_enabled:
                self._mark_processed(event.trace_id, event_id)

        return enrichment_tasks, processed_trace_ids

    def _log_dedup_diagnostics(self, dedup_counts: dict[str, list[str]]) -> None:
        """Log dedup diagnostics with sample trace_ids for KQL cross-referencing."""
        intra_batch = dedup_counts["intra_batch"]
        memory = dedup_counts["memory"]
        blob = dedup_counts["blob"]
        total_deduped = len(intra_batch) + len(memory) + len(blob)

        if not total_deduped:
            return

        sample_ids = (memory + blob + intra_batch)[:10]
        logger.info(
            "Batch dedup: %d duplicates (%d memory, %d blob, %d intra-batch)",
            total_deduped, len(memory), len(blob), len(intra_batch),
            extra={
                "duplicate_count": total_deduped,
                "dedup_memory_count": len(memory),
                "dedup_blob_count": len(blob),
                "dedup_intra_batch_count": len(intra_batch),
                "sample_trace_ids": sample_ids,
            },
        )

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        """Get cycle statistics for periodic logging."""
        msg = format_cycle_output(
            cycle_count=cycle_count,
            succeeded=self._records_succeeded,
            failed=0,
            skipped=self._records_skipped,
            deduplicated=self._records_deduplicated,
        )
        extra = {
            "records_processed": self._records_processed,
            "records_succeeded": self._records_succeeded,
            "records_skipped": self._records_skipped,
            "records_deduplicated": self._records_deduplicated,
            "dedup_memory_hits": self._dedup_memory_hits,
            "dedup_blob_hits": self._dedup_blob_hits,
            "batch_mode": "backfill" if self.consumer and self.consumer.batch_size == self.BACKFILL_BATCH_SIZE else "realtime",
            "current_batch_size": self.consumer.batch_size if self.consumer else self.REALTIME_BATCH_SIZE,
            "cycle_offset_start_ts": self._cycle_offset_start_ts,
            "cycle_offset_end_ts": self._cycle_offset_end_ts,
        }
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        return msg, extra

    def _mark_processed(self, trace_id: str, event_id: str) -> None:
        """Add trace_id -> event_id mapping to memory cache, fire-and-forget to blob."""
        now = time.time()

        # If memory cache is full, evict oldest entries (LRU via OrderedDict)
        if len(self._dedup_cache) >= self._dedup_cache_max_size:
            evict_count = self._dedup_cache_max_size // 10
            for _ in range(evict_count):
                self._dedup_cache.popitem(last=False)

            logger.debug(
                "Evicted old entries from memory dedup cache",
                extra={
                    "evicted_count": evict_count,
                    "cache_size": len(self._dedup_cache),
                },
            )

        # Add to memory cache (at end for LRU ordering)
        self._dedup_cache[trace_id] = (event_id, now)
        self._dedup_cache.move_to_end(trace_id)

        # Persist to blob storage — truly fire-and-forget (don't block on HTTP)
        if self._dedup_store:
            task = asyncio.create_task(self._persist_dedup_to_blob(trace_id, event_id, now))
            self._blob_write_tasks.add(task)
            task.add_done_callback(self._blob_write_tasks.discard)

    async def _persist_dedup_to_blob(self, trace_id: str, event_id: str, timestamp: float) -> None:
        """Fire-and-forget blob persistence for dedup markers."""
        async with self._blob_semaphore:
            try:
                await self._dedup_store.mark_processed(
                    self._dedup_worker_name,
                    trace_id,
                    {"event_id": event_id, "timestamp": timestamp},
                )
            except Exception as e:
                logger.warning(
                    "Error persisting to blob storage (memory cache still updated)",
                    extra={"trace_id": trace_id, "error": str(e)},
                )

    def _cleanup_dedup_cache(self) -> None:
        now = time.time()
        expired_keys = [
            trace_id
            for trace_id, (_, cached_time) in self._dedup_cache.items()
            if now - cached_time >= self._dedup_cache_ttl_seconds
        ]

        for trace_id in expired_keys:
            self._dedup_cache.pop(trace_id, None)

        if expired_keys:
            logger.debug(
                "Cleaned up expired event dedup cache entries",
                extra={
                    "expired_count": len(expired_keys),
                    "cache_size": len(self._dedup_cache),
                },
            )

    async def _periodic_dedup_cleanup(self) -> None:
        """Run dedup cache cleanup every 60 seconds."""
        try:
            while True:
                await asyncio.sleep(60)
                self._cleanup_dedup_cache()
        except asyncio.CancelledError:
            pass

    def _adjust_batch_size(self, event_age_seconds: float) -> None:
        """Switch between backfill and realtime batch sizes."""
        if event_age_seconds > self.BACKFILL_THRESHOLD_SECONDS:
            new_size = self.BACKFILL_BATCH_SIZE
        else:
            new_size = self.REALTIME_BATCH_SIZE

        if self.consumer and self.consumer.batch_size != new_size:
            old_size = self.consumer.batch_size
            self.consumer.batch_size = new_size
            logger.info(
                "Batch size adjusted",
                extra={
                    "old_batch_size": old_size,
                    "new_batch_size": new_size,
                    "event_age_seconds": round(event_age_seconds),
                    "mode": "backfill" if new_size == self.BACKFILL_BATCH_SIZE else "realtime",
                },
            )


__all__ = ["EventIngesterWorker"]
