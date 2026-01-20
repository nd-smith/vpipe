"""
Unified retry scheduler for header-based routing.

Consumes messages from a single retry topic per domain and routes them
to their target topics based on headers after the scheduled delay has elapsed.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from aiokafka.structs import ConsumerRecord

from config.config import KafkaConfig
from core.logging.setup import get_logger
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer

logger = get_logger(__name__)


class UnifiedRetryScheduler:
    """
    Unified scheduler for all retry types in a domain.

    Consumes from a single retry topic and routes messages to their target
    topics based on headers. Uses scheduled_retry_time header to determine
    when messages are ready for redelivery.

    The scheduler:
    1. Consumes from {domain}.retry
    2. Extracts routing information from headers
    3. Checks scheduled_retry_time
    4. If ready: routes to target_topic from header
    5. If not ready: raises exception (prevents commit, message reprocessed later)
    6. Malformed messages: routes to DLQ

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
    ):
        """
        Initialize unified retry scheduler.

        Args:
            config: Kafka configuration
            producer: Kafka producer for routing messages
            domain: Domain name ("xact" or "claimx")
        """
        self.config = config
        self.producer = producer
        self.domain = domain
        self.worker_name = "unified_retry_scheduler"

        # Single retry topic per domain
        self.retry_topic = config.get_retry_topic(domain)
        self._dlq_topic = config.get_topic(domain, "dlq")

        self._consumer: Optional[BaseKafkaConsumer] = None
        self._running = False

        # Metrics
        self._messages_routed = 0
        self._messages_delayed = 0
        self._messages_malformed = 0

        logger.info(
            "Initialized UnifiedRetryScheduler",
            extra={
                "domain": domain,
                "retry_topic": self.retry_topic,
                "dlq_topic": self._dlq_topic,
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
            },
        )

        self._running = False

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        logger.info("UnifiedRetryScheduler stopped successfully")

    async def _handle_retry_message(self, message: ConsumerRecord) -> None:
        """
        Handle a message from the retry topic.

        Extracts routing information from headers, checks if delay has elapsed,
        and routes to the target topic if ready.

        Args:
            message: ConsumerRecord from retry topic

        Raises:
            RuntimeError: If delay not elapsed (prevents commit)
            ValueError: If message is malformed
            Exception: If routing fails
        """
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
        retry_count = headers["retry_count"]
        original_key = headers.get("original_key", message.key)
        worker_type = headers.get("worker_type", "unknown")

        logger.debug(
            "Processing retry message",
            extra={
                "target_topic": target_topic,
                "retry_count": retry_count,
                "worker_type": worker_type,
                "scheduled_retry_time": scheduled_retry_time_str,
            },
        )

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
                "Retry delay not elapsed, message will be reprocessed",
                extra={
                    "scheduled_retry_time": scheduled_time.isoformat(),
                    "seconds_remaining": seconds_remaining,
                    "target_topic": target_topic,
                },
            )
            self._messages_delayed += 1
            # Raise to prevent commit - message will retry later
            raise RuntimeError(
                f"Delay not elapsed, {seconds_remaining:.0f}s remaining"
            )

        # Ready - route to target topic
        logger.info(
            "Routing message to target topic",
            extra={
                "target_topic": target_topic,
                "retry_count": retry_count,
                "worker_type": worker_type,
            },
        )

        try:
            # Preserve original message value (treat as opaque bytes)
            # Build redelivery headers
            redelivery_headers = {
                "redelivered_from": message.topic,
                "retry_count": retry_count,
                "worker_type": worker_type,
            }

            # Preserve additional context headers if present
            for header_key in ["error_category", "domain"]:
                if header_key in headers:
                    redelivery_headers[header_key] = headers[header_key]

            await self.producer.send(
                topic=target_topic,
                key=original_key if isinstance(original_key, (str, bytes)) else message.key,
                value=message.value,  # Forward original message unchanged
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
                "Failed to route message to target topic",
                extra={
                    "target_topic": target_topic,
                    "error": str(e),
                },
                exc_info=True,
            )
            # Re-raise to prevent commit - message will retry later
            raise

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
                    decoded_value = value.decode("utf-8") if isinstance(value, bytes) else str(value)
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
        }


__all__ = ["UnifiedRetryScheduler"]
