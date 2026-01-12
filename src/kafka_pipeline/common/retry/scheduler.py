"""
Delayed redelivery scheduler for retry topics.

Consumes messages from retry topics and redelivers them to the pending topic
after the configured delay has elapsed. Monitors pending topic lag to avoid
overwhelming downstream consumers.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from aiokafka.structs import ConsumerRecord

from config.config import KafkaConfig
from kafka_pipeline.common.consumer import BaseKafkaConsumer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage

logger = logging.getLogger(__name__)


class DelayedRedeliveryScheduler:
    """
    Scheduler for delayed message redelivery from retry topics.

    Consumes messages from all retry topics and redelivers them to the
    pending topic after the configured delay has elapsed. Pauses redelivery
    if the pending topic has high lag to prevent overwhelming downstream
    consumers.

    The scheduler:
    1. Consumes messages from all retry topics (*.retry.5m, *.retry.10m, etc.)
    2. Checks retry_at timestamp in message metadata
    3. If delay elapsed: sends to pending topic and commits offset
    4. If delay not elapsed: doesn't commit (message reprocessed later)
    5. Monitors pending topic lag and pauses if threshold exceeded

    Usage:
        >>> config = load_config()
        >>> producer = BaseKafkaProducer(config, domain="xact", worker_name="retry_scheduler")
        >>> await producer.start()
        >>>
        >>> scheduler = DelayedRedeliveryScheduler(
        ...     config=config,
        ...     producer=producer,
        ...     domain="xact",
        ...     lag_threshold=1000
        ... )
        >>> await scheduler.start()
        >>> # Scheduler runs until stopped
        >>> await scheduler.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        producer: BaseKafkaProducer,
        domain: str = "xact",
        lag_threshold: int = 1000,
        check_interval_seconds: int = 60,
    ):
        """
        Initialize delayed redelivery scheduler.

        Args:
            config: Kafka configuration with retry topic settings
            producer: Kafka producer for sending messages to pending topic
            domain: Domain name ("xact" or "claimx")
            lag_threshold: Pause redelivery if pending topic lag exceeds this
            check_interval_seconds: How often to check pending topic lag
        """
        self.config = config
        self.producer = producer
        self.domain = domain
        self.lag_threshold = lag_threshold
        self.check_interval_seconds = check_interval_seconds

        # Get topic configuration from domain
        self._pending_topic = config.get_topic(domain, "downloads_pending")
        self._max_retries = config.get_max_retries(domain)

        # Build list of retry topics to consume
        self.retry_topics = [
            config.get_retry_topic(domain, i) for i in range(self._max_retries)
        ]

        self._consumer: Optional[BaseKafkaConsumer] = None
        self._running = False
        self._paused = False
        self._lag_check_task: Optional[asyncio.Task] = None

        logger.info(
            "Initialized DelayedRedeliveryScheduler",
            extra={
                "domain": domain,
                "retry_topics": self.retry_topics,
                "pending_topic": self._pending_topic,
                "lag_threshold": lag_threshold,
                "check_interval": check_interval_seconds,
            },
        )

    async def start(self) -> None:
        """
        Start the delayed redelivery scheduler.

        Creates consumer for retry topics and starts message processing.
        Also starts background task to monitor pending topic lag.

        Raises:
            RuntimeError: If producer not started
            Exception: If consumer fails to start
        """
        if not self.producer.is_started:
            raise RuntimeError("Producer must be started before scheduler")

        if self._running:
            logger.warning("Scheduler already running, ignoring duplicate start call")
            return

        logger.info("Starting DelayedRedeliveryScheduler")

        # Create consumer for all retry topics
        self._consumer = BaseKafkaConsumer(
            config=self.config,
            domain=self.domain,
            worker_name="retry_scheduler",
            topics=self.retry_topics,
            message_handler=self._handle_retry_message,
        )

        self._running = True

        # Start background lag monitoring task
        self._lag_check_task = asyncio.create_task(self._monitor_pending_lag())

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
        Stop the delayed redelivery scheduler.

        Stops consumer and background lag monitoring task gracefully.
        Safe to call multiple times.
        """
        if not self._running:
            logger.debug("Scheduler not running or already stopped")
            return

        logger.info("Stopping DelayedRedeliveryScheduler")
        self._running = False

        # Cancel lag monitoring task
        if self._lag_check_task and not self._lag_check_task.done():
            self._lag_check_task.cancel()
            try:
                await self._lag_check_task
            except asyncio.CancelledError:
                pass
            self._lag_check_task = None

        # Stop consumer
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        logger.info("DelayedRedeliveryScheduler stopped successfully")

    async def _handle_retry_message(self, message: ConsumerRecord) -> None:
        """
        Handle a message from a retry topic.

        Checks if delay has elapsed and redelivers to pending topic if ready.
        If delay not elapsed, raises exception to prevent commit (message
        will be reprocessed on next poll).

        Args:
            message: ConsumerRecord from retry topic

        Raises:
            ValueError: If message is malformed or delay not elapsed
            Exception: If redelivery to pending topic fails
        """
        # Check if scheduler is paused due to high lag
        if self._paused:
            logger.debug(
                "Scheduler paused due to high lag, message will be reprocessed",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                },
            )
            # Raise exception to prevent commit - message will retry later
            raise RuntimeError("Scheduler paused due to high pending topic lag")

        # Parse message as DownloadTaskMessage
        try:
            task = DownloadTaskMessage.model_validate_json(message.value)
        except Exception as e:
            logger.error(
                "Failed to parse retry message as DownloadTaskMessage",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            # Re-raise to prevent commit - malformed messages need investigation
            raise ValueError(f"Invalid DownloadTaskMessage: {e}") from e

        # Extract retry_at timestamp from metadata
        retry_at_str = task.metadata.get("retry_at")
        if not retry_at_str:
            logger.error(
                "Message missing retry_at in metadata",
                extra={
                    "trace_id": task.trace_id,
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                },
            )
            # Re-raise to prevent commit - missing timestamp needs investigation
            raise ValueError("Message missing retry_at timestamp in metadata")

        # Parse retry_at timestamp
        try:
            retry_at = datetime.fromisoformat(retry_at_str)
            # Ensure timezone-aware
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
        except Exception as e:
            logger.error(
                "Failed to parse retry_at timestamp",
                extra={
                    "trace_id": task.trace_id,
                    "retry_at_str": retry_at_str,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise ValueError(f"Invalid retry_at timestamp: {e}") from e

        # Check if delay has elapsed
        now = datetime.now(timezone.utc)
        if now < retry_at:
            # Delay not elapsed - don't commit, message will be reprocessed
            seconds_remaining = (retry_at - now).total_seconds()
            logger.debug(
                "Retry delay not elapsed, message will be reprocessed",
                extra={
                    "trace_id": task.trace_id,
                    "retry_at": retry_at.isoformat(),
                    "now": now.isoformat(),
                    "seconds_remaining": seconds_remaining,
                },
            )
            # Raise exception to prevent commit
            raise RuntimeError(
                f"Delay not elapsed, {seconds_remaining:.0f}s remaining"
            )

        # Delay elapsed - redeliver to pending topic
        logger.info(
            "Redelivering message to pending topic",
            extra={
                "trace_id": task.trace_id,
                "retry_count": task.retry_count,
                "retry_at": retry_at.isoformat(),
                "topic": message.topic,
            },
        )

        try:
            await self.producer.send(
                topic=self._pending_topic,
                key=task.trace_id,
                value=task,
                headers={
                    "redelivered_from": message.topic,
                    "retry_count": str(task.retry_count),
                },
            )

            logger.info(
                "Message redelivered successfully",
                extra={
                    "trace_id": task.trace_id,
                    "pending_topic": self._pending_topic,
                },
            )

        except Exception as e:
            logger.error(
                "Failed to redeliver message to pending topic",
                extra={
                    "trace_id": task.trace_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            # Re-raise to prevent commit - message will retry later
            raise

    async def _monitor_pending_lag(self) -> None:
        """
        Background task to monitor pending topic lag.

        Periodically checks lag on the pending topic and pauses/resumes
        redelivery based on the lag threshold. This prevents overwhelming
        downstream consumers when they're already backlogged.
        """
        logger.info("Starting pending topic lag monitoring")

        while self._running:
            try:
                # Get lag for pending topic
                # For now, we use a simple heuristic: get lag from consumer assignment
                # In production, this could query Kafka broker metrics
                lag = await self._get_pending_topic_lag()

                if lag is not None:
                    logger.debug(
                        "Pending topic lag check",
                        extra={
                            "lag": lag,
                            "threshold": self.lag_threshold,
                            "paused": self._paused,
                        },
                    )

                    # Pause if lag exceeds threshold
                    if lag > self.lag_threshold and not self._paused:
                        self._paused = True
                        logger.warning(
                            "Pausing redelivery due to high pending topic lag",
                            extra={
                                "lag": lag,
                                "threshold": self.lag_threshold,
                            },
                        )

                    # Resume if lag drops below threshold
                    elif lag <= self.lag_threshold and self._paused:
                        self._paused = False
                        logger.info(
                            "Resuming redelivery, pending topic lag acceptable",
                            extra={
                                "lag": lag,
                                "threshold": self.lag_threshold,
                            },
                        )

                # Wait before next check
                await asyncio.sleep(self.check_interval_seconds)

            except asyncio.CancelledError:
                logger.info("Lag monitoring cancelled")
                raise
            except Exception as e:
                logger.error(
                    "Error monitoring pending topic lag",
                    extra={"error": str(e)},
                    exc_info=True,
                )
                # Continue monitoring even on error
                await asyncio.sleep(self.check_interval_seconds)

    async def _get_pending_topic_lag(self) -> Optional[int]:
        """
        Get current lag for the pending topic.

        This is a simple implementation that returns None for now.
        In production, this would query Kafka broker metrics or use
        consumer group lag from the download worker consumer group.

        Returns:
            Current lag or None if unable to determine
        """
        # TODO: Implement actual lag monitoring
        # Options:
        # 1. Query Kafka broker metrics API
        # 2. Use aiokafka admin client to get consumer group lag
        # 3. Monitor Prometheus metrics if available
        #
        # For now, return 0 to allow scheduler to run
        # This will be improved in a future work package
        return 0

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running and self._consumer is not None

    @property
    def is_paused(self) -> bool:
        """Check if scheduler is paused due to high lag."""
        return self._paused


__all__ = [
    "DelayedRedeliveryScheduler",
]
