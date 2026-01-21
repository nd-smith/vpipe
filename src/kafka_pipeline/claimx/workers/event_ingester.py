"""
ClaimX Event Ingester Worker - Consumes events and produces enrichment tasks.

Different from xact pipeline:
- Produces enrichment tasks (not download tasks)
- No URL validation at this stage (URLs come from API enrichment)
- All events trigger enrichment (not just ones with attachments)
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
from core.logging.utilities import format_cycle_output, log_worker_error
from config.config import KafkaConfig
from kafka_pipeline.common.metrics import (
    event_ingestion_duration_seconds,
)
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.claimx.monitoring import HealthCheckServer
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXEnrichmentTask

logger = get_logger(__name__)


class ClaimXEventIngesterWorker:
    """
    Unlike xact pipeline (which directly downloads attachments), claimx
    requires API enrichment first to get entity data and download URLs.
    All events trigger enrichment (not just file events).
    Deterministic SHA256 event_id generation from stable Eventhouse fields.
    """

    def __init__(
        self,
        config: KafkaConfig,
        domain: str = "claimx",
        enrichment_topic: str = "",
        producer_config: Optional[KafkaConfig] = None,
    ):
        self.consumer_config = config
        self.producer_config = producer_config if producer_config else config
        self.domain = domain
        self.enrichment_topic = enrichment_topic or config.get_topic(domain, "enrichment_pending")
        self.producer: Optional[BaseKafkaProducer] = None
        self.consumer: Optional[BaseKafkaConsumer] = None

        # Background task tracking for graceful shutdown
        self._pending_tasks: Set[asyncio.Task] = set()
        self._task_counter = 0
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_deduplicated = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: Optional[asyncio.Task] = None

        # In-memory dedup cache prevents duplicate event processing when
        # Eventhouse sends duplicates or Kafka retries deliver same message twice
        self._dedup_cache: dict[str, float] = {}
        self._dedup_cache_ttl_seconds = 86400
        self._dedup_cache_max_size = 100_000
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
        return self.consumer_config

    async def start(self) -> None:
        logger.info("Starting ClaimXEventIngesterWorker")
        from kafka_pipeline.common.telemetry import initialize_telemetry
        import os

        initialize_telemetry(
            service_name=f"{self.domain}-event-ingester",
            environment=os.getenv("ENVIRONMENT", "development"),
        )

        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())
        await self.health_server.start()
        self.producer = BaseKafkaProducer(
            config=self.producer_config,
            domain=self.domain,
            worker_name="event_ingester",
        )
        await self.producer.start()
        self.consumer = BaseKafkaConsumer(
            config=self.consumer_config,
            domain=self.domain,
            worker_name="event_ingester",
            topics=[self.consumer_config.get_topic(self.domain, "events")],
            message_handler=self._handle_event_message,
        )

        self.health_server.set_ready(kafka_connected=True, api_reachable=True)
        await self.consumer.start()

    async def stop(self) -> None:
        logger.info("Stopping ClaimXEventIngesterWorker")
        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            try:
                await self._cycle_task
            except asyncio.CancelledError:
                pass

        await self._wait_for_pending_tasks(timeout_seconds=30)
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        await self.health_server.stop()

        logger.info("ClaimXEventIngesterWorker stopped successfully")

    def _create_tracked_task(
        self,
        coro,
        task_name: str,
        context: Optional[Dict[str, Any]] = None,
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

    async def _handle_event_message(self, record: ConsumerRecord) -> None:
        start_time = time.perf_counter()
        from kafka_pipeline.common.telemetry import get_tracer

        tracer = get_tracer(__name__)
        try:
            with tracer.start_active_span("event.parse") as scope:
                span = scope.span if hasattr(scope, 'span') else scope
                span.set_tag("span.kind", "internal")
                message_data = json.loads(record.value.decode("utf-8"))
                event = ClaimXEventMessage.from_eventhouse_row(message_data)
                span.set_tag("event.type", event.event_type)
                span.set_tag("event.project_id", event.project_id)
                span.set_tag("trace_id", event.event_id)
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
        if self._is_duplicate(event_id):
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
        with tracer.start_as_current_span("event.process", kind=SpanKind.INTERNAL) as span:
            span.set_attribute("event.id", event.event_id)
            span.set_attribute("event.type", event.event_type)
            span.set_attribute("project.id", event.project_id)
            await self._create_enrichment_task(event)

        self._mark_processed(event_id)
        self._cleanup_dedup_cache()
        duration = time.perf_counter() - start_time
        event_ingestion_duration_seconds.labels(domain=self.domain).observe(duration)

    async def _create_enrichment_task(self, event: ClaimXEventMessage) -> None:
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
        try:
            metadata = await self.producer.send(
                topic=self.enrichment_topic,
                key=event.event_id,
                value=enrichment_task,
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

    def _is_duplicate(self, event_id: str) -> bool:
        now = time.time()
        if event_id in self._dedup_cache:
            cached_time = self._dedup_cache[event_id]
            if now - cached_time < self._dedup_cache_ttl_seconds:
                return True
            del self._dedup_cache[event_id]

        return False

    def _mark_processed(self, event_id: str) -> None:
        now = time.time()
        if len(self._dedup_cache) >= self._dedup_cache_max_size:
            sorted_items = sorted(self._dedup_cache.items(), key=lambda x: x[1])
            evict_count = self._dedup_cache_max_size // 10
            for event_id_to_evict, _ in sorted_items[:evict_count]:
                del self._dedup_cache[event_id_to_evict]

            logger.debug(
                "Evicted old entries from event dedup cache",
                extra={
                    "evicted_count": evict_count,
                    "cache_size": len(self._dedup_cache),
                },
            )

        self._dedup_cache[event_id] = now

    def _cleanup_dedup_cache(self) -> None:
        now = time.time()
        expired_keys = [
            event_id
            for event_id, cached_time in self._dedup_cache.items()
            if now - cached_time >= self._dedup_cache_ttl_seconds
        ]

        for event_id in expired_keys:
            del self._dedup_cache[event_id]

        if expired_keys:
            logger.debug(
                "Cleaned up expired event dedup cache entries",
                extra={
                    "expired_count": len(expired_keys),
                    "cache_size": len(self._dedup_cache),
                },
            )

    async def _periodic_cycle_output(self) -> None:
        logger.info(format_cycle_output(0, 0, 0, 0, deduplicated=0))
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        try:
            while True:
                await asyncio.sleep(1)

                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= 30:
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()
                    cycle_msg = format_cycle_output(
                        cycle_count=self._cycle_count,
                        succeeded=self._records_succeeded,
                        failed=0,
                        skipped=0,
                        deduplicated=self._records_deduplicated,
                    )
                    logger.info(
                        cycle_msg,
                        extra={
                            "cycle": self._cycle_count,
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_deduplicated": self._records_deduplicated,
                            "dedup_cache_size": len(self._dedup_cache),
                            "cycle_interval_seconds": 30,
                        },
                    )

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise


__all__ = ["ClaimXEventIngesterWorker"]
