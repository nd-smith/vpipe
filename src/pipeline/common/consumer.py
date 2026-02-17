"""Message consumer with circuit breaker, auth, error classification, and DLQ routing (WP-211)."""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, TopicPartition

from config.config import MessageConfig
from core.errors.exceptions import ErrorCategory
from core.errors.transport_classifier import TransportErrorClassifier
from core.logging import MessageLogContext
from core.utils import generate_worker_id
from pipeline.common.kafka_config import build_kafka_security_config
from pipeline.common.metrics import (
    message_processing_duration_seconds,
    record_message_consumed,
    record_processing_error,
    update_assigned_partitions,
    update_connection_status,
    update_consumer_lag,
    update_consumer_offset,
)
from pipeline.common.types import PipelineMessage, from_consumer_record

logger = logging.getLogger(__name__)


class MessageConsumer:
    """Async message consumer with circuit breaker, auth, worker-specific config, and DLQ routing."""

    def __init__(
        self,
        config: MessageConfig,
        domain: str,
        worker_name: str,
        topics: list[str],
        message_handler: Callable[[PipelineMessage], Awaitable[None]],
        enable_message_commit: bool = True,
        instance_id: str | None = None,
    ):
        if not topics:
            raise ValueError("At least one topic must be specified")

        self.config = config
        self.domain = domain
        self.worker_name = worker_name
        self.instance_id = instance_id
        self.topics = topics
        self.message_handler = message_handler
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

        prefix = f"{domain}-{worker_name}"
        if instance_id:
            prefix = f"{prefix}-{instance_id}"
        self.worker_id = generate_worker_id(prefix)

        self.consumer_config = config.get_worker_config(domain, worker_name, "consumer")
        self.group_id = config.get_consumer_group(domain, worker_name)

        processing_config = config.get_worker_config(domain, worker_name, "processing")
        self.max_batches = processing_config.get("max_batches")
        self._batch_count = 0

        self._enable_message_commit = enable_message_commit

        # Local import to avoid circular dependency through pipeline.common.dlq.__init__
        from pipeline.common.dlq.producer import DLQProducer

        # Lazy-initialized DLQ producer (WP-211)
        self._dlq_producer = DLQProducer(
            config=config,
            domain=domain,
            worker_name=worker_name,
            group_id=self.group_id,
            worker_id=self.worker_id,
        )

        logger.info(
            "Initialized message consumer",
            extra={
                "domain": domain,
                "worker_name": worker_name,
                "topics": topics,
                "group_id": self.group_id,
                "bootstrap_servers": config.bootstrap_servers,
                "max_batches": self.max_batches,
                "enable_message_commit": enable_message_commit,
                "consumer_config": self.consumer_config,
            },
        )

    async def start(self) -> None:
        if self._running:
            logger.warning("Consumer already running, ignoring duplicate start call")
            return

        logger.info(
            "Starting message consumer",
            extra={
                "topics": self.topics,
                "group_id": self.group_id,
            },
        )

        kafka_consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": self.group_id,
            "client_id": (
                f"{self.domain}-{self.worker_name}-{self.instance_id}"
                if self.instance_id
                else f"{self.domain}-{self.worker_name}"
            ),
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        kafka_consumer_config.update(
            {
                "enable_auto_commit": self.consumer_config.get("enable_auto_commit", False),
                "auto_offset_reset": self.consumer_config.get("auto_offset_reset", "earliest"),
                "max_poll_records": self.consumer_config.get("max_poll_records", 100),
                "max_poll_interval_ms": self.consumer_config.get("max_poll_interval_ms", 300000),
                "session_timeout_ms": self.consumer_config.get("session_timeout_ms", 30000),
            }
        )

        if "heartbeat_interval_ms" in self.consumer_config:
            kafka_consumer_config["heartbeat_interval_ms"] = self.consumer_config[
                "heartbeat_interval_ms"
            ]
        if "fetch_min_bytes" in self.consumer_config:
            kafka_consumer_config["fetch_min_bytes"] = self.consumer_config["fetch_min_bytes"]
        if "fetch_max_wait_ms" in self.consumer_config:
            kafka_consumer_config["fetch_max_wait_ms"] = self.consumer_config["fetch_max_wait_ms"]
        if "partition_assignment_strategy" in self.consumer_config:
            kafka_consumer_config["partition_assignment_strategy"] = self.consumer_config[
                "partition_assignment_strategy"
            ]

        kafka_consumer_config.update(build_kafka_security_config(self.config))

        self._consumer = AIOKafkaConsumer(*self.topics, **kafka_consumer_config)

        await self._consumer.start()
        self._running = True

        update_connection_status("consumer", connected=True)
        assignment = self._consumer.assignment()
        partition_count = len(assignment)
        update_assigned_partitions(self.group_id, partition_count)

        # Log starting offsets so crash recovery gaps are visible
        partition_offsets = {}
        for tp in assignment:
            try:
                pos = await self._consumer.position(tp)
                partition_offsets[f"{tp.topic}:{tp.partition}"] = pos
            except Exception:
                partition_offsets[f"{tp.topic}:{tp.partition}"] = "unknown"

        logger.info(
            "Message consumer started successfully",
            extra={
                "topics": self.topics,
                "group_id": self.group_id,
                "partitions": partition_count,
                "resuming_from_offsets": partition_offsets,
            },
        )

        try:
            await self._consume_loop()
        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled, shutting down")
            raise
        except Exception:
            logger.error(
                "Consumer loop terminated with error",
                exc_info=True,
            )
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        if not self._running or self._consumer is None:
            logger.debug("Consumer not running or already stopped")
            return

        logger.info("Stopping message consumer")
        self._running = False

        try:
            if self._consumer:
                await self._consumer.commit()
                await self._consumer.stop()

            await self._dlq_producer.stop()

            logger.info("Message consumer stopped successfully")
        except Exception:
            logger.error(
                "Error stopping message consumer",
                exc_info=True,
            )
            raise
        finally:
            update_connection_status("consumer", connected=False)
            update_assigned_partitions(self.group_id, 0)
            self._consumer = None

    async def commit(self) -> None:
        """For batch processing. Call after successfully processing/writing a batch."""
        if self._consumer is None:
            logger.warning("Cannot commit: consumer not started")
            return

        await self._consumer.commit()
        logger.debug(
            "Committed offsets",
            extra={
                "group_id": self.group_id,
            },
        )

    async def _consume_loop(self) -> None:
        logger.info(
            "Starting message consumption loop",
            extra={
                "max_batches": self.max_batches,
                "topics": self.topics,
                "group_id": self.group_id,
            },
        )

        _logged_waiting_for_assignment = False
        _logged_assignment_received = False

        while self._running and self._consumer:
            try:
                if self.max_batches is not None and self._batch_count >= self.max_batches:
                    logger.info(
                        "Reached max_batches limit, stopping consumer",
                        extra={
                            "max_batches": self.max_batches,
                            "batches_processed": self._batch_count,
                        },
                    )
                    return

                # Avoid blocking during rebalance: getmany() can hang if called before partition assignment
                assignment = self._consumer.assignment()
                if not assignment:
                    if not _logged_waiting_for_assignment:
                        logger.info(
                            "Waiting for partition assignment (consumer group rebalance in progress)",
                            extra={
                                "group_id": self.group_id,
                                "topics": self.topics,
                            },
                        )
                        _logged_waiting_for_assignment = True
                    await asyncio.sleep(0.5)
                    continue

                if not _logged_assignment_received:
                    partition_info = [f"{tp.topic}:{tp.partition}" for tp in assignment]
                    logger.info(
                        "Partition assignment received, starting message consumption",
                        extra={
                            "group_id": self.group_id,
                            "partition_count": len(assignment),
                            "partitions": partition_info,
                        },
                    )
                    _logged_assignment_received = True
                    update_assigned_partitions(self.group_id, len(assignment))

                # Fetch messages with timeout
                data = await self._consumer.getmany(timeout_ms=1000)

                if data:
                    self._batch_count += 1

                for _topic_partition, messages in data.items():
                    for message in messages:
                        if not self._running:
                            logger.info("Consumer stopped, breaking message loop")
                            return

                        await self._process_message(message)

            except asyncio.CancelledError:
                logger.info("Consumption loop cancelled")
                raise
            except Exception:
                logger.error(
                    "Error in consumption loop",
                    exc_info=True,
                )
                await asyncio.sleep(1)

    async def _process_message(self, message: ConsumerRecord) -> None:
        with MessageLogContext(
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            key=message.key.decode("utf-8") if message.key else None,
            consumer_group=self.group_id,
        ):
            start_time = time.perf_counter()
            message_size = len(message.value) if message.value else 0

            try:
                # Convert ConsumerRecord to PipelineMessage for handler
                pipeline_message = from_consumer_record(message)
                await self.message_handler(pipeline_message)

                duration = time.perf_counter() - start_time
                message_processing_duration_seconds.labels(
                    topic=message.topic, consumer_group=self.group_id
                ).observe(duration)

                if self._enable_message_commit:
                    await self._consumer.commit()

                self._update_partition_metrics(message)

                record_message_consumed(message.topic, self.group_id, message_size, success=True)

            except Exception as e:
                duration = time.perf_counter() - start_time
                message_processing_duration_seconds.labels(
                    topic=message.topic, consumer_group=self.group_id
                ).observe(duration)

                record_message_consumed(message.topic, self.group_id, message_size, success=False)

                # Pass PipelineMessage to error handler
                pipeline_message = from_consumer_record(message)
                await self._handle_processing_error(pipeline_message, message, e, duration)

    async def _handle_processing_error(
        self,
        pipeline_message: PipelineMessage,
        message: ConsumerRecord,
        error: Exception,
        duration: float,
    ) -> None:
        """Error classification with DLQ routing for PERMANENT errors, retry for others."""
        classified_error = TransportErrorClassifier.classify_consumer_error(
            error,
            context={
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "group_id": self.group_id,
            },
        )

        error_category = classified_error.category
        record_processing_error(message.topic, self.group_id, error_category.value)

        common_context = {
            "error_category": error_category.value,
            "classified_as": type(classified_error).__name__,
            "duration_ms": round(duration * 1000, 2),
        }

        if error_category == ErrorCategory.PERMANENT:
            logger.error(
                "Permanent error processing message - routing to DLQ",
                extra={**common_context},
                exc_info=True,
            )

            try:
                await self._dlq_producer.send(pipeline_message, error, error_category)

                # Commit offset after DLQ routing to advance past poison pill
                if self._enable_message_commit:
                    await self._consumer.commit()
                    logger.info(
                        "Offset committed after DLQ routing - partition can advance",
                        extra={
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                        },
                    )

            except Exception:
                logger.error(
                    "DLQ routing failed - message will be retried",
                    extra={**common_context},
                    exc_info=True,
                )

        elif error_category == ErrorCategory.TRANSIENT:
            logger.warning(
                "Transient error - will reprocess message",
                extra={**common_context},
                exc_info=True,
            )

        elif error_category == ErrorCategory.AUTH:
            logger.warning(
                "Authentication error - will reprocess after token refresh",
                extra={**common_context},
                exc_info=True,
            )

        elif error_category == ErrorCategory.CIRCUIT_OPEN:
            logger.warning(
                "Circuit breaker open - will reprocess when circuit closes",
                extra={**common_context},
                exc_info=True,
            )

        else:
            logger.error(
                f"Unhandled error category '{error_category.value}' - "
                f"routing to DLQ: {type(error).__name__}: {error}",
                extra={**common_context},
                exc_info=True,
            )

            try:
                await self._dlq_producer.send(pipeline_message, error, error_category)

                if self._enable_message_commit:
                    await self._consumer.commit()
                    logger.info(
                        "Offset committed after DLQ routing for unknown error",
                        extra={
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                            "error_category": error_category.value,
                        },
                    )

            except Exception:
                logger.error(
                    "DLQ routing failed for unknown error - message will be retried",
                    extra={**common_context},
                    exc_info=True,
                )

    def _update_partition_metrics(self, message: ConsumerRecord) -> None:
        if not self._consumer:
            return

        try:
            update_consumer_offset(message.topic, message.partition, self.group_id, message.offset)

            tp = TopicPartition(message.topic, message.partition)
            partition_metadata = self._consumer.highwater(tp)

            if partition_metadata is not None:
                lag = partition_metadata - (message.offset + 1)
                update_consumer_lag(message.topic, message.partition, self.group_id, lag)

        except Exception as e:
            logger.debug(
                "Failed to update partition metrics",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "error": str(e),
                },
            )

    @property
    def is_running(self) -> bool:
        return self._running and self._consumer is not None


__all__ = [
    "MessageConsumer",
    "AIOKafkaConsumer",
    "ConsumerRecord",
]
