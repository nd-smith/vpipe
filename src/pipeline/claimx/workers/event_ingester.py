"""ClaimX event ingester worker.

Consumes raw events and produces enrichment tasks.
All events trigger API enrichment for entity data.
"""

import asyncio
import contextlib
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from config.config import MessageConfig
from core.logging.context import set_log_context
from core.logging.utilities import format_cycle_output
from pipeline.claimx.schemas.events import ClaimXEventMessage
from pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask
from pipeline.common.eventhub.dedup_store import (
    DedupStoreProtocol,
    close_dedup_store,
    get_dedup_store,
)
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    message_processing_duration_seconds,
)
from pipeline.common.transport import create_consumer, create_producer
from pipeline.common.types import PipelineMessage

logger = logging.getLogger(__name__)


class ClaimXEventIngesterWorker:
    """
    Unlike xact pipeline (which directly downloads attachments), claimx
    requires API enrichment first to get entity data and download URLs.
    All events trigger enrichment (not just file events).
    Deterministic SHA256 event_id generation from stable Eventhouse fields.
    """

    WORKER_NAME = "event_ingester"

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
        self.enrichment_topic = enrichment_topic or config.get_topic(
            domain, "enrichment_pending"
        )
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
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: asyncio.Task | None = None

        # Cycle-specific metrics (reset each cycle)
        self._last_cycle_processed = 0
        self._last_cycle_deduped = 0

        # Hybrid dedup: in-memory cache (fast path) + blob storage (persistent)
        # In-memory cache: event_id -> timestamp
        self._dedup_cache: dict[str, float] = {}
        self._dedup_cache_ttl_seconds = 86400  # 24 hours (matches Verisk)
        self._dedup_cache_max_size = 100_000  # ~2MB memory for 100k entries

        # Persistent blob storage (survives worker restarts)
        self._dedup_store: DedupStoreProtocol | None = None
        self._dedup_worker_name = "claimx-event-ingester"
        processing_config = config.get_worker_config(
            domain, "event_ingester", "processing"
        )
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

    async def start(self) -> None:
        logger.info("Starting ClaimXEventIngesterWorker")

        # Clean up resources from a previous failed start attempt (retry safety)
        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cycle_task
        if self.consumer:
            await self.consumer.stop()
            self.consumer = None
        if self.producer:
            await self.producer.stop()
            self.producer = None

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "event-ingester")

        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        # Initialize persistent dedup store (blob storage)
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

        self.consumer = await create_consumer(
            config=self.consumer_config,
            domain=self.domain,
            worker_name="event_ingester",
            topics=[self.consumer_config.get_topic(self.domain, "events")],
            message_handler=self._handle_event_message,
            topic_key="events",
        )

        self.health_server.set_ready(kafka_connected=True, api_reachable=True)
        await self.consumer.start()

    async def stop(self) -> None:
        logger.info("Stopping ClaimXEventIngesterWorker")
        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cycle_task

        await self._wait_for_pending_tasks(timeout_seconds=30)
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

        # Close persistent dedup store
        await close_dedup_store()

        await self.health_server.stop()

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

    async def _handle_event_message(self, record: PipelineMessage) -> None:
        start_time = time.perf_counter()
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
            raise

        # event_id generated deterministically in schema from stable Eventhouse fields
        # Do NOT regenerate to ensure consistency across duplicate polls
        event_id = event.event_id
        set_log_context(trace_id=event.event_id)

        # Check for duplicates (hybrid: memory + blob storage)
        if await self._is_duplicate(event_id):
            self._records_deduplicated += 1
            logger.debug(
                "Skipping duplicate ClaimX event",
                extra={
                    "event_id": event_id,
                    "event_type": event.event_type,
                    "project_id": event.project_id,
                },
            )
            return

        self._records_processed += 1
        logger.debug(
            "Processing ClaimX event",
            extra={
                "event_id": event.event_id,
                "event_type": event.event_type,
                "project_id": event.project_id,
                "media_id": event.media_id,
                "task_assignment_id": event.task_assignment_id,
            },
        )
        await self._create_enrichment_task(event)

        # Mark event as processed in both memory and blob storage
        await self._mark_processed(event_id)

        # Periodic cleanup of expired cache entries
        self._cleanup_dedup_cache()

        duration = time.perf_counter() - start_time
        message_processing_duration_seconds.labels(
            topic=self.consumer_config.get_topic(self.domain, "events"),
            consumer_group=f"{self.domain}-event-ingester",
        ).observe(duration)

    async def _create_enrichment_task(self, event: ClaimXEventMessage) -> None:
        enrichment_task = ClaimXEnrichmentTask(
            event_id=event.event_id,
            event_type=event.event_type,
            project_id=event.project_id,
            retry_count=0,
            created_at=datetime.now(UTC),
            media_id=event.media_id,
            task_assignment_id=event.task_assignment_id,
            video_collaboration_id=event.video_collaboration_id,
            master_file_name=event.master_file_name,
        )
        try:
            metadata = await self.producer.send(
                value=enrichment_task,
                key=event.event_id,
                headers={"event_id": event.event_id},
            )

            logger.debug(
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

    async def _is_duplicate(self, event_id: str) -> bool:
        """Check if event_id was processed recently (hybrid: memory + blob storage).

        Fast path: Check in-memory cache first
        Slow path: On miss, check blob storage and update memory cache
        """
        now = time.time()

        # Fast path: Check in-memory cache
        if event_id in self._dedup_cache:
            cached_time = self._dedup_cache.get(event_id, 0)
            if now - cached_time < self._dedup_cache_ttl_seconds:
                return True
            # Expired - remove from memory cache
            del self._dedup_cache[event_id]

        # Slow path: Check blob storage (persistent across restarts)
        if self._dedup_store:
            try:
                is_dup, metadata = await self._dedup_store.check_duplicate(
                    self._dedup_worker_name,
                    event_id,
                    self._dedup_cache_ttl_seconds,
                )
                if is_dup and metadata:
                    # Found in blob - update memory cache for faster subsequent lookups
                    timestamp = metadata.get("timestamp", now)
                    self._dedup_cache[event_id] = timestamp
                    logger.debug(
                        "Found duplicate in blob storage (restored to memory cache)",
                        extra={"event_id": event_id},
                    )
                    return True
            except Exception as e:
                logger.warning(
                    "Error checking blob storage for duplicate (falling back to memory-only)",
                    extra={"event_id": event_id, "error": str(e)},
                    exc_info=False,
                )

        return False

    async def _mark_processed(self, event_id: str) -> None:
        """Add event_id to both memory and blob storage with TTL+LRU eviction."""
        now = time.time()

        # If memory cache is full, evict oldest entries (LRU)
        if len(self._dedup_cache) >= self._dedup_cache_max_size:
            # Sort by timestamp and remove oldest 10%
            sorted_items = sorted(
                self._dedup_cache.items(), key=lambda x: x[1]
            )
            evict_count = self._dedup_cache_max_size // 10
            for event_id_to_evict, _ in sorted_items[:evict_count]:
                self._dedup_cache.pop(event_id_to_evict, None)

            logger.debug(
                "Evicted old entries from memory dedup cache",
                extra={
                    "evicted_count": evict_count,
                    "cache_size": len(self._dedup_cache),
                },
            )

        # Add to memory cache
        self._dedup_cache[event_id] = now

        # Persist to blob storage (fire-and-forget - don't block on this)
        if self._dedup_store:
            try:
                await self._dedup_store.mark_processed(
                    self._dedup_worker_name,
                    event_id,
                    {"timestamp": now},
                )
            except Exception as e:
                logger.warning(
                    "Error persisting to blob storage (memory cache still updated)",
                    extra={"event_id": event_id, "error": str(e)},
                    exc_info=False,
                )

    def _cleanup_dedup_cache(self) -> None:
        """Remove expired entries from memory cache (periodic maintenance)."""
        now = time.time()
        expired_keys = [
            event_id
            for event_id, cached_time in self._dedup_cache.items()
            if now - cached_time >= self._dedup_cache_ttl_seconds
        ]

        for event_id in expired_keys:
            self._dedup_cache.pop(event_id, None)

        if expired_keys:
            logger.debug(
                "Cleaned up expired event dedup cache entries",
                extra={
                    "expired_count": len(expired_keys),
                    "cache_size": len(self._dedup_cache),
                },
            )

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
                "stage": "ingestion",
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

                    logger.info(
                        format_cycle_output(
                            cycle_count=self._cycle_count,
                            succeeded=self._records_succeeded,
                            failed=0,
                            skipped=0,
                            deduplicated=self._records_deduplicated,
                        ),
                        extra={
                            "worker_id": self.worker_id,
                            "stage": "ingestion",
                            "cycle": self._cycle_count,
                            "cycle_id": f"cycle-{self._cycle_count}",
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_deduplicated": self._records_deduplicated,
                            "recent_events_size": len(self._recent_events),
                            "cycle_interval_seconds": 30,
                        },
                    )

                    # Update last cycle counters
                    self._last_cycle_processed = self._records_processed
                    self._last_cycle_deduped = self._records_deduplicated

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise


__all__ = ["ClaimXEventIngesterWorker"]
