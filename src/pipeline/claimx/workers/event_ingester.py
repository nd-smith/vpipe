"""ClaimX event ingester worker.

Consumes raw events and produces enrichment tasks.
All events trigger API enrichment for entity data.
"""

import asyncio
import contextlib
import json
import logging
import time
from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from config.config import MessageConfig
from core.logging.periodic_logger import PeriodicStatsLogger
from pipeline.claimx.schemas.events import ClaimXEventMessage
from pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask
from pipeline.common.eventhub.dedup_store import (
    DedupStoreProtocol,
    close_dedup_store,
    get_dedup_store,
    is_dedup_enabled,
)
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    message_processing_duration_seconds,
)
from pipeline.common.transport import (
    create_batch_consumer,
    create_producer,
    get_source_connection_string,
)
from pipeline.common.types import BatchResult, PipelineMessage

logger = logging.getLogger(__name__)


class ClaimXEventIngesterWorker:
    """
    Unlike xact pipeline (which directly downloads attachments), claimx
    requires API enrichment first to get entity data and download URLs.
    All events trigger enrichment (not just file events).
    Deterministic SHA256 trace_id generation from stable Eventhouse fields.
    """

    WORKER_NAME = "event_ingester"

    # Backfill prefetch mode — dynamically increase batch size for stale data
    BACKFILL_BATCH_SIZE = 2000
    REALTIME_BATCH_SIZE = 100
    BACKFILL_THRESHOLD_SECONDS = 3600  # 1 hour

    def __init__(
        self,
        config: MessageConfig,
        domain: str = "claimx",
        enrichment_topic: str = "",
        producer_config: MessageConfig | None = None,
        instance_id: str | None = None,
    ):
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.instance_id = instance_id
        self.enrichment_topic = enrichment_topic or config.get_topic(domain, "enrichment_pending")
        self.producer = None
        self.consumer = None

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        # Background task tracking for graceful shutdown
        self._pending_tasks: set[asyncio.Task] = set()
        self._task_counter = 0
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_deduplicated = 0
        self._dedup_memory_hits = 0
        self._dedup_blob_hits = 0
        self._stats_logger: PeriodicStatsLogger | None = None
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        self._running = False

        # Dedup bypass flag (checked at start from config)
        self._dedup_enabled = True

        # Hybrid dedup: in-memory cache (fast path) + blob storage (persistent)
        # OrderedDict for O(1) LRU eviction: trace_id -> timestamp
        self._dedup_cache: OrderedDict[str, float] = OrderedDict()
        self._dedup_cache_ttl_seconds = 86400  # 24 hours (matches Verisk)
        self._dedup_cache_max_size = 100_000  # ~2MB memory for 100k entries

        # Persistent blob storage (survives worker restarts)
        self._dedup_store: DedupStoreProtocol | None = None
        self._dedup_worker_name = "claimx-event-ingester"
        self._dedup_cleanup_task: asyncio.Task | None = None
        self._blob_write_tasks: set[asyncio.Task] = set()
        self._blob_semaphore = asyncio.Semaphore(50)
        health_port = 0
        health_enabled = True
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-event-ingester",
            enabled=health_enabled,
        )

        logger.info(
            "Initialized ClaimXEventIngesterWorker",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "consumer_group": config.get_consumer_group(domain, "event_ingester"),
                "events_topic": config.get_topic(domain, "events"),
                "enrichment_topic": self.enrichment_topic,
                "separate_producer_config": producer_config is not None,
            },
        )

    @property
    def config(self) -> MessageConfig:
        return self.consumer_config

    async def _close_resource(self, name: str, method: str = "stop", *, clear: bool = False) -> None:
        """Close a resource by attribute name, logging errors. Optionally set to None."""
        resource = getattr(self, name, None)
        if resource is None:
            return
        try:
            await getattr(resource, method)()
        except asyncio.CancelledError:
            logger.warning(f"Cancelled while stopping {name}")
        except Exception as e:
            logger.error(f"Error stopping {name}", extra={"error": str(e)})
        finally:
            if clear:
                setattr(self, name, None)

    async def start(self) -> None:
        logger.info("Starting ClaimXEventIngesterWorker")
        self._running = True

        # Clean up resources from a previous failed start attempt (retry safety)
        await self._close_resource("_stats_logger", clear=True)
        await self._close_resource("consumer", clear=True)
        await self._close_resource("producer", clear=True)

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "event-ingester")

        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=30,
            get_stats=self._get_cycle_stats,
            stage="ingestion",
            worker_id=self.worker_id,
        )
        self._stats_logger.start()

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

        self.producer = create_producer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="event_ingester",
            topic_key="enrichment_pending",
        )
        await self.producer.start()

        if hasattr(self.producer, "eventhub_name"):
            self.enrichment_topic = self.producer.eventhub_name

        # Start periodic dedup cache cleanup (every 60s)
        if self._dedup_enabled:
            self._dedup_cleanup_task = asyncio.create_task(self._periodic_dedup_cleanup())

        self.consumer = await create_batch_consumer(
            config=self.consumer_config,
            domain=self.domain,
            worker_name="event_ingester",
            topics=[self.consumer_config.get_topic(self.domain, "events")],
            batch_handler=self._handle_event_batch,
            batch_size=self.REALTIME_BATCH_SIZE,
            max_batch_size=self.BACKFILL_BATCH_SIZE,
            batch_timeout_ms=500,
            topic_key="events",
            connection_string=get_source_connection_string(),
            prefetch=3000,
        )

        self.health_server.set_ready(transport_connected=True, api_reachable=True)

        try:
            await self.consumer.start()
        except asyncio.CancelledError:
            logger.info("ClaimXEventIngesterWorker cancelled, shutting down...")
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        logger.info("Stopping ClaimXEventIngesterWorker")
        self._running = False

        await self._close_resource("_stats_logger", clear=True)

        # Cancel dedup cleanup task
        if self._dedup_cleanup_task and not self._dedup_cleanup_task.done():
            self._dedup_cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._dedup_cleanup_task

        await self._wait_for_pending_tasks(timeout_seconds=30)

        await self._close_resource("consumer", clear=True)
        await self._close_resource("producer", clear=True)

        # Close persistent dedup store
        try:
            await close_dedup_store()
        except Exception as e:
            logger.error("Error closing dedup store", extra={"error": str(e)})

        await self._close_resource("health_server")

        logger.info("ClaimXEventIngesterWorker stopped successfully")

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
                    logger.warning(
                        "Cancelled pending task",
                        extra={"task_name": task.get_name()},
                    )

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

    async def _handle_event_batch(self, records: list[PipelineMessage]) -> BatchResult:
        """Process a batch of event messages.

        Parses, deduplicates, and batch-produces enrichment tasks.
        Returns BatchResult with commit flag and any permanent parse failures for DLQ routing.
        """
        start_time = time.perf_counter()

        # Parse, dedup (memory + blob), and collect non-duplicate events
        parsed_events, latest_timestamp, permanent_failures = await self._parse_and_dedup_events(records)

        # Build enrichment tasks from non-duplicate events
        enrichment_tasks, processed_trace_ids = self._build_enrichment_tasks(parsed_events)

        # Batch-produce all enrichment tasks
        if enrichment_tasks:
            try:
                await self.producer.send_batch(messages=enrichment_tasks)
                self._records_succeeded += len(enrichment_tasks)
            except Exception as e:
                # Roll back dedup marks — messages will be redelivered
                for trace_id in processed_trace_ids:
                    self._dedup_cache.pop(trace_id, None)

                logger.error(
                    "Failed to send ClaimX enrichment batch — will retry",
                    extra={
                        "batch_size": len(enrichment_tasks),
                        "error": str(e),
                    },
                    exc_info=True,
                )
                return BatchResult(commit=False, permanent_failures=permanent_failures)

        # Adjust batch size based on event age (backfill vs realtime)
        if latest_timestamp:
            if latest_timestamp.tzinfo is None:
                latest_timestamp = latest_timestamp.replace(tzinfo=UTC)
            event_age = (datetime.now(UTC) - latest_timestamp).total_seconds()
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

    def _update_cycle_offsets(self, ts: int | None) -> None:
        """Update cycle offset tracking with a message timestamp."""
        if ts is None:
            return
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

    def _is_memory_duplicate(self, trace_id: str, now: float) -> bool:
        """Check if trace_id is a duplicate in the memory cache. Returns True if duplicate."""
        if trace_id in self._dedup_cache:
            cached_time = self._dedup_cache[trace_id]
            if now - cached_time < self._dedup_cache_ttl_seconds:
                self._dedup_cache.move_to_end(trace_id)
                self._dedup_memory_hits += 1
                self._records_deduplicated += 1
                return True
            del self._dedup_cache[trace_id]
        return False

    def _is_batch_or_memory_duplicate(
        self, trace_id: str, seen_in_batch: set[str], now: float,
    ) -> bool:
        """Check intra-batch and memory cache for duplicate. Returns True if duplicate."""
        if trace_id in seen_in_batch:
            self._records_deduplicated += 1
            return True
        seen_in_batch.add(trace_id)
        return self._is_memory_duplicate(trace_id, now)

    async def _parse_and_dedup_events(
        self, records: list[PipelineMessage],
    ) -> tuple[list[ClaimXEventMessage], datetime | None, list[tuple[PipelineMessage, Exception]]]:
        """Parse records, deduplicate via memory cache and blob storage.

        Returns non-duplicate parsed events, the latest ingested_at timestamp,
        and permanent failures (unparseable messages) for DLQ routing.
        """
        parsed_events: list[ClaimXEventMessage] = []
        permanent_failures: list[tuple[PipelineMessage, Exception]] = []
        seen_in_batch: set[str] = set()
        latest_timestamp = None
        now = time.time()

        for record in records:
            self._update_cycle_offsets(record.timestamp)

            try:
                message_data = json.loads(record.value.decode("utf-8"))
                event = ClaimXEventMessage.from_raw_event(message_data)
            except (json.JSONDecodeError, ValidationError) as e:
                logger.error(
                    "Failed to parse ClaimXEventMessage, routing to DLQ",
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

            latest_timestamp = event.ingested_at

            if self._dedup_enabled and self._is_batch_or_memory_duplicate(
                event.trace_id, seen_in_batch, now,
            ):
                continue

            parsed_events.append(event)

        if self._dedup_store and parsed_events:
            blob_duplicates = await self._check_blob_duplicates(parsed_events)
            parsed_events = [ev for ev in parsed_events if ev.trace_id not in blob_duplicates]

        return parsed_events, latest_timestamp, permanent_failures

    async def _check_blob_duplicates(
        self, events: list[ClaimXEventMessage],
    ) -> set[str]:
        """Check blob storage for duplicates, return set of duplicate trace IDs."""
        async def _check_one(trace_id: str) -> tuple[str, bool]:
            async with self._blob_semaphore:
                try:
                    is_dup, metadata = await self._dedup_store.check_duplicate(
                        self._dedup_worker_name, trace_id, self._dedup_cache_ttl_seconds,
                    )
                    if is_dup and metadata:
                        timestamp = metadata.get("timestamp", time.time())
                        self._dedup_cache[trace_id] = timestamp
                        self._dedup_blob_hits += 1
                        return trace_id, True
                except Exception as e:
                    logger.warning(
                        "Error checking blob storage for duplicate (falling back to memory-only)",
                        extra={"trace_id": trace_id, "error": str(e)},
                    )
                return trace_id, False

        results = await asyncio.gather(
            *(_check_one(ev.trace_id) for ev in events)
        )
        duplicates = {eid for eid, is_dup in results if is_dup}

        for _eid in duplicates:
            self._records_deduplicated += 1

        return duplicates

    def _build_enrichment_tasks(
        self, events: list[ClaimXEventMessage],
    ) -> tuple[list[tuple[str, ClaimXEnrichmentTask]], list[str]]:
        """Create enrichment tasks from validated, non-duplicate events."""
        enrichment_tasks: list[tuple[str, ClaimXEnrichmentTask]] = []
        processed_trace_ids: list[str] = []

        for event in events:
            self._records_processed += 1

            logger.info(
                "Event ingested",
                extra={
                    "trace_id": event.trace_id,
                    "project_id": event.project_id,
                    "event_type": event.event_type,
                    "media_id": event.media_id,
                },
            )

            enrichment_task = ClaimXEnrichmentTask(
                trace_id=event.trace_id,
                event_type=event.event_type,
                project_id=event.project_id,
                retry_count=0,
                created_at=datetime.now(UTC),
                media_id=event.media_id,
                task_assignment_id=event.task_assignment_id,
                video_collaboration_id=event.video_collaboration_id,
                master_file_name=event.master_file_name,
            )

            enrichment_tasks.append((event.trace_id, enrichment_task))
            processed_trace_ids.append(event.trace_id)
            if self._dedup_enabled:
                self._mark_processed(event.trace_id)

        return enrichment_tasks, processed_trace_ids

    def _mark_processed(self, trace_id: str) -> None:
        """Add trace_id to memory cache, fire-and-forget to blob storage."""
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
        self._dedup_cache[trace_id] = now
        self._dedup_cache.move_to_end(trace_id)

        # Persist to blob storage — truly fire-and-forget (don't block on HTTP)
        if self._dedup_store:
            task = asyncio.create_task(self._persist_dedup_to_blob(trace_id, now))
            self._blob_write_tasks.add(task)
            task.add_done_callback(self._blob_write_tasks.discard)

    async def _persist_dedup_to_blob(self, trace_id: str, timestamp: float) -> None:
        """Fire-and-forget blob persistence for dedup markers."""
        async with self._blob_semaphore:
            try:
                await self._dedup_store.mark_processed(
                    self._dedup_worker_name,
                    trace_id,
                    {"timestamp": timestamp},
                )
            except Exception as e:
                logger.warning(
                    "Error persisting to blob storage (memory cache still updated)",
                    extra={"trace_id": trace_id, "error": str(e)},
                )

    def _cleanup_dedup_cache(self) -> None:
        """Remove expired entries from memory cache (periodic maintenance)."""
        now = time.time()
        expired_keys = [
            trace_id
            for trace_id, cached_time in self._dedup_cache.items()
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

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict]:
        extra = {
            "records_processed": self._records_processed,
            "records_succeeded": self._records_succeeded,
            "records_failed": 0,
            "records_skipped": 0,
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
        return "", extra


__all__ = ["ClaimXEventIngesterWorker"]
