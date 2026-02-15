"""
Unified retry scheduler for header-based routing.

Consumes messages from a single retry topic per domain and routes them
to their target topics based on headers after the scheduled delay has elapsed.

Uses the transport abstraction layer (transport.py) to create producers and
consumers, supporting both Kafka and Event Hub transports. A producer pool
maps each known target topic to a dedicated producer instance.
"""

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from config.config import MessageConfig
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import detect_log_output_mode, log_startup_banner
from pipeline.common.decorators import set_log_context_from_message
from pipeline.common.health import HealthCheckServer
from pipeline.common.retry.delay_queue import DelayedMessage, DelayQueue
from pipeline.common.transport import create_consumer, create_producer
from pipeline.common.types import PipelineMessage

logger = logging.getLogger(__name__)

STATS_LOG_INTERVAL_SECONDS = 30
RETRY_REQUEUE_DELAY_SECONDS = 5
PERSISTENCE_FILE_PREFIX = "pcesdopodappv1_"


def parse_retry_count(value: str) -> int | None:
    """Parse retry count from header value. Returns None if invalid."""
    try:
        return int(value)
    except ValueError:
        return None


def parse_scheduled_time(value: str) -> datetime | None:
    """Parse scheduled retry time from header value. Returns None if invalid."""
    try:
        scheduled_time = datetime.fromisoformat(value)
        if scheduled_time.tzinfo is None:
            scheduled_time = scheduled_time.replace(tzinfo=UTC)
        return scheduled_time
    except Exception:
        return None


def encode_message_key(original_key: str | bytes | None, message: PipelineMessage) -> bytes | None:
    """Encode a message key to bytes, falling back to message.key."""
    if isinstance(original_key, str):
        return original_key.encode("utf-8")
    if isinstance(original_key, bytes):
        return original_key
    if message.key:
        return message.key if isinstance(message.key, bytes) else message.key.encode("utf-8")
    return None


class UnifiedRetryScheduler:
    """
    Unified scheduler for all retry types in a domain.

    Consumes from a single retry topic and routes messages to their target
    topics based on headers. Uses scheduled_retry_time header to determine
    when messages are ready for redelivery.

    Creates its own producers via the transport factory — one per target
    topic key plus a dedicated DLQ producer. This supports Event Hub's
    one-entity-per-connection model.

    The scheduler:
    1. Consumes from {domain}.retry
    2. Extracts routing information from headers
    3. Checks if retry_count >= max_retries (routes to DLQ if exhausted)
    4. Checks scheduled_retry_time
    5. If ready NOW: routes to target_topic from header immediately
    6. If not ready: stores in in-memory queue (persisted to disk every 10s)
    7. Malformed or unroutable messages: routes to DLQ
    8. Background task processes delayed messages when ready
    9. Offsets committed immediately (makes progress through queue)

    Crash safety:
    - In-memory queue persisted to disk every 10 seconds
    - Queue restored on startup
    - On crash, at most 10 seconds of delayed messages may be lost
    - This is acceptable trade-off vs blocking entire queue

    Usage:
        >>> config = MessageConfig.from_env()
        >>> scheduler = UnifiedRetryScheduler(
        ...     config=config,
        ...     domain="verisk",
        ...     target_topic_keys=["downloads_pending", "enrichment_pending", "downloads_results"],
        ... )
        >>> await scheduler.start()
        >>> # Scheduler runs until stopped
        >>> await scheduler.stop()
    """

    def __init__(
        self,
        config: MessageConfig,
        domain: str,
        target_topic_keys: list[str],
        persistence_interval_seconds: int = 10,
        health_port: int = 8095,
        persistence_dir: str | None = None,
    ):
        self.config = config
        self.domain = domain
        self.worker_name = "unified_retry_scheduler"
        self._target_topic_keys = target_topic_keys

        # Single retry topic per domain
        self.retry_topic = config.get_retry_topic(domain)
        self._dlq_topic = config.get_topic(domain, "dlq")
        self._max_retries = config.get_max_retries(domain)

        # Reverse mapping: resolved topic name → topic_key
        # Lets us find the right producer when a message header says
        # e.g. "verisk.downloads_pending"
        self._topic_name_to_key: dict[str, str] = {}
        for key in target_topic_keys:
            resolved = config.get_topic(domain, key)
            self._topic_name_to_key[resolved] = key

        # Producer pool and consumer (created in start())
        self._producer_pool: dict[str, Any] = {}  # resolved topic name → producer
        self._dlq_producer: Any | None = None
        self._consumer: Any | None = None
        self._running = False

        # Background tasks
        self._processor_task: asyncio.Task | None = None
        self._persistence_task: asyncio.Task | None = None
        self._persistence_interval = persistence_interval_seconds
        self._stats_logger: PeriodicStatsLogger | None = None

        base_dir = persistence_dir or "/tmp"
        persistence_file = Path(base_dir) / f"{PERSISTENCE_FILE_PREFIX}{domain}_retry_queue.json"
        self._delay_queue = DelayQueue(domain, persistence_file)

        # Metrics
        self._messages_routed = 0
        self._messages_delayed = 0
        self._messages_malformed = 0
        self._messages_exhausted = 0
        self._messages_restored = 0
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None

        # Health check server
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name=f"{domain}-retry-scheduler",
        )

    async def start(self) -> None:
        """Start the unified retry scheduler.

        Creates a producer pool (one per target topic), a DLQ producer,
        and a consumer for the retry topic. Then begins message routing.
        """
        if self._running:
            logger.warning("Scheduler already running, ignoring duplicate start call")
            return

        await self.health_server.start()

        # Create producer pool: one producer per target topic key
        for key in self._target_topic_keys:
            resolved = self.config.get_topic(self.domain, key)
            producer = create_producer(
                self.config, self.domain, self.worker_name, topic_key=key
            )
            await producer.start()
            self._producer_pool[resolved] = producer
            logger.info(
                "Producer started for target topic",
                extra={"topic_key": key, "resolved_topic": resolved},
            )

        # Create DLQ producer
        self._dlq_producer = create_producer(
            self.config, self.domain, self.worker_name, topic_key="dlq"
        )
        await self._dlq_producer.start()

        # Restore delayed messages from disk if available
        self._messages_restored = self._delay_queue.restore_from_disk()

        # Start background processor for delayed messages
        self._processor_task = asyncio.create_task(self._process_delayed_messages())

        # Start periodic persistence task
        self._persistence_task = asyncio.create_task(self._periodic_persistence())

        # Create consumer for retry topic via transport factory
        self._consumer = await create_consumer(
            config=self.config,
            domain=self.domain,
            worker_name=self.worker_name,
            topics=[self.retry_topic],
            message_handler=self._handle_retry_message,
            topic_key="retry",
        )

        self._running = True

        # Observability
        log_startup_banner(
            logger,
            worker_name=f"{self.domain} Retry Scheduler",
            domain=self.domain,
            input_topic=self.retry_topic,
            health_port=self.health_server.actual_port,
            log_output_mode=detect_log_output_mode(),
        )

        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=STATS_LOG_INTERVAL_SECONDS,
            get_stats=self._get_cycle_stats,
            stage=f"{self.domain}-retry-scheduler",
            worker_id=self.worker_name,
        )
        self._stats_logger.start()

        # Mark health server as ready after successful initialization
        self.health_server.set_ready(transport_connected=True)

        logger.info(
            "UnifiedRetryScheduler ready",
            extra={
                "health_port": self.health_server.actual_port,
                "restored_messages": self._messages_restored,
                "target_topics": list(self._producer_pool.keys()),
            },
        )

        # Start consumer (this blocks until stopped)
        try:
            await self._consumer.start()
        except asyncio.CancelledError:
            logger.info("Scheduler consumer cancelled")
            raise
        except Exception as e:
            logger.error(
                "Scheduler consumer failed",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        """Stop the unified retry scheduler gracefully."""
        if not self._running:
            logger.debug("Scheduler not running or already stopped")
            return

        logger.info(
            "Stopping UnifiedRetryScheduler",
            extra={
                "messages_routed": self._messages_routed,
                "messages_delayed": self._messages_delayed,
                "messages_malformed": self._messages_malformed,
                "messages_exhausted": self._messages_exhausted,
                "queue_size": len(self._delay_queue),
            },
        )

        self._running = False

        # Stop stats logger
        if self._stats_logger:
            await self._stats_logger.stop()
            self._stats_logger = None

        # Stop background tasks
        if self._processor_task:
            self._processor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._processor_task

        if self._persistence_task:
            self._persistence_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._persistence_task

        # Persist delayed queue before shutdown
        self._delay_queue.persist_to_disk()

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        # Stop all producers in the pool
        for producer in self._producer_pool.values():
            await producer.stop()
        self._producer_pool.clear()

        # Stop DLQ producer
        if self._dlq_producer:
            await self._dlq_producer.stop()
            self._dlq_producer = None

        await self.health_server.stop()

        logger.info("UnifiedRetryScheduler stopped successfully")

    @set_log_context_from_message
    async def _handle_retry_message(self, message: PipelineMessage) -> None:
        """Handle a message from the retry topic.

        Extracts routing information from headers, checks if delay has elapsed,
        and routes to the target topic if ready. If not ready, stores in
        in-memory queue for later processing.
        """
        headers = self._parse_headers(message)

        ts = message.timestamp
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

        # Validate required headers
        required_headers = ["scheduled_retry_time", "target_topic", "retry_count"]
        missing_headers = [h for h in required_headers if h not in headers]

        if missing_headers:
            logger.error(
                "Message missing required headers, routing to DLQ | "
                "missing=%s available=%s raw_type=%s raw_len=%s",
                missing_headers,
                list(headers.keys()),
                type(message.headers).__name__,
                len(message.headers) if message.headers else 0,
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "missing_headers": missing_headers,
                    "available_headers": list(headers.keys()),
                },
            )
            self._messages_malformed += 1
            await self._send_to_dlq(
                message,
                f"Missing required headers: {missing_headers}",
                headers,
            )
            return

        target_topic = headers["target_topic"]
        retry_count_str = headers["retry_count"]
        original_key = headers.get("original_key", message.key)
        worker_type = headers.get("worker_type", "unknown")

        # Parse and validate retry count
        retry_count = parse_retry_count(retry_count_str)
        if retry_count is None:
            logger.error(
                "Invalid retry_count in header, routing to DLQ",
                extra={
                    "retry_count": retry_count_str,
                    "target_topic": target_topic,
                },
            )
            self._messages_malformed += 1
            await self._send_to_dlq(
                message,
                f"Invalid retry_count: {retry_count_str}",
                headers,
            )
            return

        logger.debug(
            "Processing retry message",
            extra={
                "target_topic": target_topic,
                "retry_count": retry_count,
                "worker_type": worker_type,
                "scheduled_retry_time": headers["scheduled_retry_time"],
            },
        )

        # Check if retries exhausted
        if retry_count >= self._max_retries:
            logger.warning(
                "Retries exhausted, routing to DLQ",
                extra={
                    "retry_count": retry_count,
                    "max_retries": self._max_retries,
                    "target_topic": target_topic,
                    "worker_type": worker_type,
                },
            )
            self._messages_exhausted += 1
            await self._send_to_dlq(
                message,
                f"Retries exhausted ({retry_count}/{self._max_retries})",
                headers,
            )
            return

        # Parse and validate scheduled time
        scheduled_time = parse_scheduled_time(headers["scheduled_retry_time"])
        if scheduled_time is None:
            logger.error(
                "Failed to parse scheduled_retry_time, routing to DLQ",
                extra={
                    "scheduled_retry_time": headers["scheduled_retry_time"],
                },
                exc_info=True,
            )
            self._messages_malformed += 1
            await self._send_to_dlq(
                message,
                f"Invalid scheduled_retry_time: {headers['scheduled_retry_time']}",
                headers,
            )
            return

        # Not ready yet — add to delay queue
        now = datetime.now(UTC)
        if now < scheduled_time:
            seconds_remaining = (scheduled_time - now).total_seconds()
            logger.debug(
                "Retry delay not elapsed, adding to in-memory queue",
                extra={
                    "scheduled_retry_time": scheduled_time.isoformat(),
                    "seconds_remaining": seconds_remaining,
                    "target_topic": target_topic,
                },
            )

            delayed_msg = DelayedMessage(
                scheduled_time=scheduled_time,
                target_topic=target_topic,
                retry_count=retry_count,
                worker_type=worker_type,
                message_key=encode_message_key(original_key, message),
                message_value=bytes(message.value),
                headers=headers,
            )
            self._delay_queue.push(delayed_msg)
            self._messages_delayed += 1

            logger.debug(
                "Message added to delay queue",
                extra={
                    "queue_size": len(self._delay_queue),
                    "target_topic": target_topic,
                },
            )
            return

        # Ready — route to target topic immediately
        logger.info(
            "Routing message to target topic",
            extra={
                "target_topic": target_topic,
                "retry_count": retry_count,
                "worker_type": worker_type,
            },
        )

        await self._route_to_target(
            target_topic=target_topic,
            message_key=(original_key if isinstance(original_key, (str, bytes)) else message.key),
            message_value=message.value,
            retry_count=retry_count,
            worker_type=worker_type,
            headers=headers,
            original_topic=message.topic,
        )

    def _parse_headers(self, message: PipelineMessage) -> dict[str, str]:
        """Parse message headers into a dictionary."""
        headers = {}
        if message.headers:
            if isinstance(message.headers, dict):
                header_items = message.headers.items()
            else:
                header_items = message.headers

            for entry in header_items:
                try:
                    if isinstance(entry, tuple) and len(entry) == 2:
                        key, value = entry
                    else:
                        logger.warning(
                            "Skipping malformed header entry",
                            extra={
                                "entry_type": type(entry).__name__,
                                "entry_preview": repr(entry)[:200],
                            },
                        )
                        continue

                    decoded_key = (
                        key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)
                    )
                    decoded_value = (
                        value.decode("utf-8") if isinstance(value, bytes) else str(value)
                    )
                    headers[decoded_key] = decoded_value
                except Exception as e:
                    logger.warning(
                        "Failed to decode header",
                        extra={
                            "key": key if 'key' in locals() else None,
                            "error": str(e),
                        },
                    )
            if not headers:
                logger.warning(
                    "Headers empty after parsing | raw_type=%s raw_len=%s raw_sample=%s",
                    type(message.headers).__name__,
                    len(message.headers),
                    repr(message.headers[:3]) if len(message.headers) > 0 else "[]",
                )
        return headers

    @staticmethod
    def _resolve_send_topic(producer: Any, configured_topic: str) -> str:
        """Resolve topic/entity name to avoid transport-specific mismatch warnings."""
        name = getattr(producer, "eventhub_name", None)
        return name if isinstance(name, str) else configured_topic

    async def _send_to_dlq(
        self,
        message: PipelineMessage,
        reason: str,
        headers: dict[str, str],
    ) -> None:
        """Send malformed message to DLQ."""
        logger.error(
            "Sending malformed retry message to DLQ",
            extra={
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "reason": reason,
            },
        )

        try:
            dlq_topic = self._resolve_send_topic(self._dlq_producer, self._dlq_topic)
            await self._dlq_producer.send(
                topic=dlq_topic,
                key=message.key,
                value=message.value,
                headers={
                    "dlq_reason": reason,
                    "original_topic": message.topic,
                    "original_partition": str(message.partition),
                    "original_offset": str(message.offset),
                    "failed": "true",
                    **{k: v for k, v in headers.items() if isinstance(v, str)},
                },
            )

            logger.info(
                "Malformed message sent to DLQ",
                extra={"dlq_topic": dlq_topic},
            )

        except Exception as e:
            logger.error(
                "Failed to send malformed message to DLQ",
                extra={
                    "error": str(e),
                    "dlq_topic": self._dlq_topic,
                },
                exc_info=True,
            )
            # Re-raise to prevent commit
            raise

    async def _route_to_target(
        self,
        target_topic: str,
        message_key: bytes | None,
        message_value: bytes,
        retry_count: int,
        worker_type: str,
        headers: dict[str, str],
        original_topic: str,
    ) -> None:
        """Route a message to its target topic using the producer pool.

        If target_topic is unknown (no matching producer), routes to DLQ.
        """
        producer = self._producer_pool.get(target_topic)
        if producer is None:
            logger.error(
                "Unknown target topic, routing to DLQ",
                extra={
                    "target_topic": target_topic,
                    "known_topics": list(self._producer_pool.keys()),
                    "worker_type": worker_type,
                },
            )
            self._messages_malformed += 1
            await self._dlq_producer.send(
                topic=self._resolve_send_topic(self._dlq_producer, self._dlq_topic),
                key=message_key,
                value=message_value,
                headers={
                    "dlq_reason": f"Unknown target topic: {target_topic}",
                    "original_topic": original_topic,
                    "target_topic": target_topic,
                    "failed": "true",
                    **{k: v for k, v in headers.items() if isinstance(v, str)},
                },
            )
            return

        try:
            # Build redelivery headers (all values must be strings)
            redelivery_headers = {
                "redelivered_from": original_topic,
                "retry_count": str(retry_count),
                "worker_type": worker_type,
            }

            # Preserve additional context headers if present
            for header_key in ["error_category", "domain"]:
                if header_key in headers:
                    redelivery_headers[header_key] = headers[header_key]

            await producer.send(
                topic=self._resolve_send_topic(producer, target_topic),
                key=message_key,
                value=message_value,
                headers=redelivery_headers,
            )

            self._messages_routed += 1

            logger.info(
                "Message routed successfully",
                extra={
                    "target_topic": target_topic,
                    "messages_routed": self._messages_routed,
                },
            )

        except Exception as e:
            logger.error(
                f"Failed to route message to target topic '{target_topic}': {type(e).__name__}: {str(e)}",
                exc_info=True,
            )
            # Re-raise to prevent commit for immediately ready messages
            raise

    async def _process_delayed_messages(self) -> None:
        """Background task that continuously processes delayed messages when ready."""
        logger.info("Started delayed message processor")

        while self._running:
            try:
                now = datetime.now(UTC)
                ready_messages = self._delay_queue.pop_ready(now)

                for delayed_msg in ready_messages:
                    logger.debug(
                        "Processing delayed message",
                        extra={
                            "target_topic": delayed_msg.target_topic,
                            "retry_count": delayed_msg.retry_count,
                            "scheduled_time": delayed_msg.scheduled_time.isoformat(),
                        },
                    )

                    try:
                        await self._route_to_target(
                            target_topic=delayed_msg.target_topic,
                            message_key=delayed_msg.message_key,
                            message_value=delayed_msg.message_value,
                            retry_count=delayed_msg.retry_count,
                            worker_type=delayed_msg.worker_type,
                            headers=delayed_msg.headers,
                            original_topic=self.retry_topic,
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to route delayed message to '{delayed_msg.target_topic}', will retry in 5s: {type(e).__name__}: {str(e)}",
                            exc_info=True,
                        )
                        self._delay_queue.requeue_with_delay(delayed_msg, delay_seconds=RETRY_REQUEUE_DELAY_SECONDS)

                # Sleep until next message is ready (or max 1 second)
                next_time = self._delay_queue.next_scheduled_time
                if next_time is not None:
                    sleep_seconds = max(0.1, (next_time - now).total_seconds())
                    await asyncio.sleep(min(sleep_seconds, 1.0))
                else:
                    await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                logger.info("Delayed message processor cancelled")
                raise
            except Exception:
                logger.error(
                    "Error in delayed message processor",
                    exc_info=True,
                )
                await asyncio.sleep(1.0)

    async def _periodic_persistence(self) -> None:
        """Background task that periodically persists the delayed queue to disk."""
        logger.info(
            "Started periodic persistence",
            extra={"interval_seconds": self._persistence_interval},
        )

        while self._running:
            try:
                await asyncio.sleep(self._persistence_interval)
                self._delay_queue.persist_to_disk()
            except asyncio.CancelledError:
                logger.info("Periodic persistence cancelled")
                raise
            except Exception:
                logger.error(
                    "Error in periodic persistence",
                    exc_info=True,
                )

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        """Stats callback for PeriodicStatsLogger."""
        extra = {
            "messages_routed": self._messages_routed,
            "messages_delayed": self._messages_delayed,
            "messages_malformed": self._messages_malformed,
            "messages_exhausted": self._messages_exhausted,
            "queue_size": len(self._delay_queue),
            "cycle_offset_start_ts": self._cycle_offset_start_ts,
            "cycle_offset_end_ts": self._cycle_offset_end_ts,
        }
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        return "", extra

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running and self._consumer is not None

    @property
    def stats(self) -> dict[str, Any]:
        """Get scheduler statistics."""
        return {
            "messages_routed": self._messages_routed,
            "messages_delayed": self._messages_delayed,
            "messages_malformed": self._messages_malformed,
            "messages_exhausted": self._messages_exhausted,
            "messages_restored": self._messages_restored,
            "queue_size": len(self._delay_queue),
        }


__all__ = ["UnifiedRetryScheduler"]
