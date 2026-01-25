# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Unified retry scheduler for header-based routing.

Consumes messages from a single retry topic per domain and routes them
to their target topics based on headers after the scheduled delay has elapsed.
"""

import asyncio
import base64
import heapq
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

from aiokafka.structs import ConsumerRecord

from config.config import KafkaConfig
from core.logging.setup import get_logger
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer

logger = get_logger(__name__)


@dataclass
class DelayedMessage:
    """In-memory representation of a delayed retry message."""

    scheduled_time: datetime
    target_topic: str
    retry_count: int
    worker_type: str
    message_key: Optional[bytes]
    message_value: bytes
    headers: Dict[str, str]

    def __lt__(self, other: "DelayedMessage") -> bool:
        """Compare by scheduled_time for heap ordering."""
        return self.scheduled_time < other.scheduled_time


class UnifiedRetryScheduler:
    """
    Unified scheduler for all retry types in a domain.

    Consumes from a single retry topic and routes messages to their target
    topics based on headers. Uses scheduled_retry_time header to determine
    when messages are ready for redelivery.

    The scheduler:
    1. Consumes from {domain}.retry
    2. Extracts routing information from headers
    3. Checks if retry_count >= max_retries (routes to DLQ if exhausted)
    4. Checks scheduled_retry_time
    5. If ready NOW: routes to target_topic from header immediately
    6. If not ready: stores in in-memory queue (persisted to disk every 10s)
    7. Malformed messages: routes to DLQ
    8. Background task processes delayed messages when ready
    9. Offsets committed immediately (makes progress through queue)

    Crash safety:
    - In-memory queue persisted to disk every 10 seconds
    - Queue restored on startup
    - On crash, at most 10 seconds of delayed messages may be lost
    - This is acceptable trade-off vs blocking entire queue

    Usage:
        >>> config = KafkaConfig.from_env()
        >>> producer = BaseKafkaProducer(
        ...     config=config,
        ...     domain="xact",
        ...     worker_name="unified_retry_scheduler"
        ... )
        >>> await producer.start()
        >>>
        >>> scheduler = UnifiedRetryScheduler(
        ...     config=config,
        ...     producer=producer,
        ...     domain="xact"
        ... )
        >>> await scheduler.start()
        >>> # Scheduler runs until stopped
        >>> await scheduler.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        producer: BaseKafkaProducer,
        domain: str,
        persistence_interval_seconds: int = 10,
    ):
        """
        Initialize unified retry scheduler.

        Args:
            config: Kafka configuration
            producer: Kafka producer for routing messages
            domain: Domain name ("xact" or "claimx")
            persistence_interval_seconds: How often to persist queue to disk (default 10s)
        """
        self.config = config
        self.producer = producer
        self.domain = domain
        self.worker_name = "unified_retry_scheduler"

        # Single retry topic per domain
        self.retry_topic = config.get_retry_topic(domain)
        self._dlq_topic = config.get_topic(domain, "dlq")
        self._max_retries = config.get_max_retries(domain)

        self._consumer: Optional[BaseKafkaConsumer] = None
        self._running = False

        # In-memory delay queue (min-heap by scheduled_time)
        self._delayed_queue: List[DelayedMessage] = []

        # Background tasks
        self._processor_task: Optional[asyncio.Task] = None
        self._persistence_task: Optional[asyncio.Task] = None
        self._persistence_interval = persistence_interval_seconds

        # Persistence file path
        self._persistence_file = Path(f"/tmp/vpipe_{domain}_retry_queue.json")

        # Metrics
        self._messages_routed = 0
        self._messages_delayed = 0
        self._messages_malformed = 0
        self._messages_exhausted = 0
        self._messages_restored = 0

        logger.info(
            "Initialized UnifiedRetryScheduler",
            extra={
                "domain": domain,
                "retry_topic": self.retry_topic,
                "dlq_topic": self._dlq_topic,
                "max_retries": self._max_retries,
                "persistence_file": str(self._persistence_file),
                "persistence_interval": self._persistence_interval,
            },
        )

    async def start(self) -> None:
        """
        Start the unified retry scheduler.

        Creates consumer for the retry topic and begins message routing.

        Raises:
            RuntimeError: If producer not started
            Exception: If consumer fails to start
        """
        if not self.producer.is_started:
            raise RuntimeError("Producer must be started before scheduler")

        if self._running:
            logger.warning("Scheduler already running, ignoring duplicate start call")
            return

        logger.info(
            "Starting UnifiedRetryScheduler",
            extra={"retry_topic": self.retry_topic},
        )

        # Restore delayed messages from disk if available
        await self._restore_from_disk()

        # Start background processor for delayed messages
        self._processor_task = asyncio.create_task(self._process_delayed_messages())

        # Start periodic persistence task
        self._persistence_task = asyncio.create_task(self._periodic_persistence())

        # Create consumer for retry topic
        self._consumer = BaseKafkaConsumer(
            config=self.config,
            domain=self.domain,
            worker_name=self.worker_name,
            topics=[self.retry_topic],
            message_handler=self._handle_retry_message,
        )

        self._running = True

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
        """
        Stop the unified retry scheduler gracefully.
        """
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
                "queue_size": len(self._delayed_queue),
            },
        )

        self._running = False

        # Stop background tasks
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass

        if self._persistence_task:
            self._persistence_task.cancel()
            try:
                await self._persistence_task
            except asyncio.CancelledError:
                pass

        # Persist delayed queue before shutdown
        await self._persist_to_disk()

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        logger.info("UnifiedRetryScheduler stopped successfully")

    async def _handle_retry_message(self, message: ConsumerRecord) -> None:
        """
        Handle a message from the retry topic.

        Extracts routing information from headers, checks if delay has elapsed,
        and routes to the target topic if ready. If not ready, stores in
        in-memory queue for later processing.

        Args:
            message: ConsumerRecord from retry topic

        Note:
            This method commits offsets immediately after processing, whether
            the message is routed immediately or stored for later. Messages
            stored in the in-memory queue are persisted to disk periodically.
        """
        try:
            await self._handle_retry_message_impl(message)
        except Exception as e:
            logger.error(
                "SCHEDULER: Unhandled exception in retry message handler",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                },
                exc_info=True,
            )
            raise

    async def _handle_retry_message_impl(self, message: ConsumerRecord) -> None:
        """Implementation of retry message handling."""
        # Parse headers
        headers = self._parse_headers(message)

        # Validate required headers
        required_headers = ["scheduled_retry_time", "target_topic", "retry_count"]
        missing_headers = [h for h in required_headers if h not in headers]

        if missing_headers:
            logger.error(
                "Message missing required headers, routing to DLQ",
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

        # Extract routing information
        scheduled_retry_time_str = headers["scheduled_retry_time"]
        target_topic = headers["target_topic"]
        retry_count_str = headers["retry_count"]
        original_key = headers.get("original_key", message.key)
        worker_type = headers.get("worker_type", "unknown")

        # Parse retry count
        try:
            retry_count = int(retry_count_str)
        except ValueError:
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
                "scheduled_retry_time": scheduled_retry_time_str,
            },
        )

        # Check if retries exhausted - route to DLQ
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

        # Parse scheduled time
        try:
            scheduled_time = datetime.fromisoformat(scheduled_retry_time_str)
            # Ensure timezone-aware
            if scheduled_time.tzinfo is None:
                scheduled_time = scheduled_time.replace(tzinfo=timezone.utc)
        except Exception as e:
            logger.error(
                "Failed to parse scheduled_retry_time, routing to DLQ",
                extra={
                    "scheduled_retry_time": scheduled_retry_time_str,
                    "error": str(e),
                },
                exc_info=True,
            )
            self._messages_malformed += 1
            await self._send_to_dlq(
                message,
                f"Invalid scheduled_retry_time: {e}",
                headers,
            )
            return

        # Check if ready for redelivery
        now = datetime.now(timezone.utc)
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

            # Add to in-memory delay queue
            # Convert key to bytes if needed
            if isinstance(original_key, str):
                msg_key = original_key.encode("utf-8")
            elif isinstance(original_key, bytes):
                msg_key = original_key
            elif message.key:
                msg_key = (
                    message.key if isinstance(message.key, bytes) else message.key.encode("utf-8")
                )
            else:
                msg_key = None

            delayed_msg = DelayedMessage(
                scheduled_time=scheduled_time,
                target_topic=target_topic,
                retry_count=retry_count,
                worker_type=worker_type,
                message_key=msg_key,
                message_value=bytes(message.value),
                headers=headers,
            )
            heapq.heappush(self._delayed_queue, delayed_msg)
            self._messages_delayed += 1

            logger.debug(
                "Message added to delay queue",
                extra={
                    "queue_size": len(self._delayed_queue),
                    "target_topic": target_topic,
                },
            )
            # Return normally - offset will be committed (we've accepted responsibility)
            return

        # Ready - route to target topic immediately
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
            message_key=original_key if isinstance(original_key, (str, bytes)) else message.key,
            message_value=message.value,
            retry_count=retry_count,
            worker_type=worker_type,
            headers=headers,
            original_topic=message.topic,
        )

    def _parse_headers(self, message: ConsumerRecord) -> Dict[str, str]:
        """
        Parse Kafka message headers into a dictionary.

        Args:
            message: ConsumerRecord with headers

        Returns:
            Dictionary of header key-value pairs
        """
        headers = {}
        if message.headers:
            for key, value in message.headers:
                try:
                    # Decode header value (Kafka headers are bytes)
                    decoded_value = (
                        value.decode("utf-8") if isinstance(value, bytes) else str(value)
                    )
                    headers[key] = decoded_value
                except Exception as e:
                    logger.warning(
                        "Failed to decode header",
                        extra={
                            "key": key,
                            "error": str(e),
                        },
                    )
        return headers

    async def _send_to_dlq(
        self,
        message: ConsumerRecord,
        reason: str,
        headers: Dict[str, str],
    ) -> None:
        """
        Send malformed message to DLQ.

        Args:
            message: Original ConsumerRecord
            reason: Reason for DLQ routing
            headers: Parsed headers (may be incomplete)
        """
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
            await self.producer.send(
                topic=self._dlq_topic,
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
                extra={"dlq_topic": self._dlq_topic},
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
        message_key: Optional[bytes],
        message_value: bytes,
        retry_count: int,
        worker_type: str,
        headers: Dict[str, str],
        original_topic: str,
    ) -> None:
        """
        Route a message to its target topic.

        Args:
            target_topic: Destination topic
            message_key: Message key (bytes or None)
            message_value: Message value (bytes)
            retry_count: Current retry count
            worker_type: Type of worker that will process message
            headers: Original headers
            original_topic: Source topic (for logging)
        """
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

            await self.producer.send(
                topic=target_topic,
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
        """
        Background task that continuously processes delayed messages when ready.
        """
        logger.info("Started delayed message processor")

        while self._running:
            try:
                now = datetime.now(timezone.utc)

                # Process all messages that are ready
                while self._delayed_queue and self._delayed_queue[0].scheduled_time <= now:
                    delayed_msg = heapq.heappop(self._delayed_queue)

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
                        # Put back in queue with a small delay to avoid tight loop
                        delayed_msg.scheduled_time = datetime.now(timezone.utc).replace(
                            microsecond=0
                        ) + timedelta(seconds=5)
                        heapq.heappush(self._delayed_queue, delayed_msg)

                # Sleep until next message is ready (or max 1 second)
                if self._delayed_queue:
                    next_time = self._delayed_queue[0].scheduled_time
                    sleep_seconds = max(0.1, (next_time - now).total_seconds())
                    await asyncio.sleep(min(sleep_seconds, 1.0))
                else:
                    await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                logger.info("Delayed message processor cancelled")
                raise
            except Exception as e:
                logger.error(
                    "Error in delayed message processor",
                    exc_info=True,
                )
                await asyncio.sleep(1.0)

    async def _periodic_persistence(self) -> None:
        """
        Background task that periodically persists the delayed queue to disk.
        """
        logger.info(
            "Started periodic persistence",
            extra={"interval_seconds": self._persistence_interval},
        )

        while self._running:
            try:
                await asyncio.sleep(self._persistence_interval)
                await self._persist_to_disk()
            except asyncio.CancelledError:
                logger.info("Periodic persistence cancelled")
                raise
            except Exception as e:
                logger.error(
                    "Error in periodic persistence",
                    exc_info=True,
                )

    async def _persist_to_disk(self) -> None:
        """
        Persist the in-memory delayed queue to disk as JSON.
        """
        if not self._delayed_queue:
            logger.debug("Delayed queue empty, skipping persistence")
            return

        try:
            # Convert queue to serializable format
            messages_data = []
            for msg in self._delayed_queue:
                messages_data.append(
                    {
                        "scheduled_time": msg.scheduled_time.isoformat(),
                        "target_topic": msg.target_topic,
                        "retry_count": msg.retry_count,
                        "worker_type": msg.worker_type,
                        "message_key": (
                            base64.b64encode(msg.message_key).decode("utf-8")
                            if msg.message_key
                            else None
                        ),
                        "message_value": base64.b64encode(msg.message_value).decode("utf-8"),
                        "headers": msg.headers,
                    }
                )

            data = {
                "version": 1,
                "domain": self.domain,
                "last_persisted": datetime.now(timezone.utc).isoformat(),
                "messages": messages_data,
            }

            # Write atomically (write to temp file, then rename)
            temp_file = self._persistence_file.with_suffix(".tmp")
            with open(temp_file, "w") as f:
                json.dump(data, f, indent=2)

            temp_file.replace(self._persistence_file)

            logger.debug(
                "Persisted delayed queue to disk",
                extra={
                    "queue_size": len(self._delayed_queue),
                    "file": str(self._persistence_file),
                },
            )

        except Exception as e:
            logger.error(
                "Failed to persist delayed queue to disk",
                extra={
                    "file": str(self._persistence_file),
                    "error": str(e),
                },
                exc_info=True,
            )

    async def _restore_from_disk(self) -> None:
        """
        Restore the in-memory delayed queue from disk if available.
        """
        if not self._persistence_file.exists():
            logger.debug(
                "No persistence file found, starting with empty queue",
                extra={"file": str(self._persistence_file)},
            )
            return

        try:
            with open(self._persistence_file, "r") as f:
                data = json.load(f)

            if data.get("version") != 1:
                logger.warning(
                    "Unknown persistence file version, ignoring",
                    extra={"version": data.get("version")},
                )
                return

            if data.get("domain") != self.domain:
                logger.warning(
                    "Persistence file domain mismatch, ignoring",
                    extra={
                        "expected_domain": self.domain,
                        "file_domain": data.get("domain"),
                    },
                )
                return

            # Restore messages
            now = datetime.now(timezone.utc)
            restored_count = 0
            expired_count = 0

            for msg_data in data.get("messages", []):
                scheduled_time = datetime.fromisoformat(msg_data["scheduled_time"])

                # Skip messages that are too old (more than max retry delay past due)
                if (now - scheduled_time).total_seconds() > 300:  # 5 minutes grace period
                    expired_count += 1
                    logger.debug(
                        "Skipping expired message from persistence",
                        extra={
                            "scheduled_time": scheduled_time.isoformat(),
                            "target_topic": msg_data["target_topic"],
                        },
                    )
                    continue

                delayed_msg = DelayedMessage(
                    scheduled_time=scheduled_time,
                    target_topic=msg_data["target_topic"],
                    retry_count=msg_data["retry_count"],
                    worker_type=msg_data["worker_type"],
                    message_key=(
                        base64.b64decode(msg_data["message_key"])
                        if msg_data["message_key"]
                        else None
                    ),
                    message_value=base64.b64decode(msg_data["message_value"]),
                    headers=msg_data["headers"],
                )
                heapq.heappush(self._delayed_queue, delayed_msg)
                restored_count += 1

            self._messages_restored = restored_count

            logger.info(
                "Restored delayed queue from disk",
                extra={
                    "restored_count": restored_count,
                    "expired_count": expired_count,
                    "file": str(self._persistence_file),
                    "last_persisted": data.get("last_persisted"),
                },
            )

            # Delete persistence file after successful restore
            self._persistence_file.unlink()

        except Exception as e:
            logger.error(
                "Failed to restore delayed queue from disk",
                extra={
                    "file": str(self._persistence_file),
                    "error": str(e),
                },
                exc_info=True,
            )

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running and self._consumer is not None

    @property
    def stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        return {
            "messages_routed": self._messages_routed,
            "messages_delayed": self._messages_delayed,
            "messages_malformed": self._messages_malformed,
            "messages_exhausted": self._messages_exhausted,
            "messages_restored": self._messages_restored,
            "queue_size": len(self._delayed_queue),
        }


__all__ = ["UnifiedRetryScheduler"]
