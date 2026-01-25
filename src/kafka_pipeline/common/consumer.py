# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""Kafka consumer with circuit breaker, auth, error classification, and DLQ routing (WP-211)."""

import asyncio
import json
import logging
import socket
import time
from typing import Awaitable, Callable, List, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord, TopicPartition

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging import get_logger, log_with_context, log_exception, KafkaLogContext
from core.errors.exceptions import CircuitOpenError, ErrorCategory
from core.errors.kafka_classifier import KafkaErrorClassifier
from config.config import KafkaConfig
from kafka_pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_connection_status,
    update_assigned_partitions,
    update_consumer_lag,
    update_consumer_offset,
    message_processing_duration_seconds,
)

logger = get_logger(__name__)


class BaseKafkaConsumer:
    """Async Kafka consumer with circuit breaker, auth, worker-specific config, and DLQ routing."""

    def __init__(
        self,
        config: KafkaConfig,
        domain: str,
        worker_name: str,
        topics: List[str],
        message_handler: Callable[[ConsumerRecord], Awaitable[None]],
        enable_message_commit: bool = True,
        instance_id: Optional[str] = None,
    ):
        if not topics:
            raise ValueError("At least one topic must be specified")

        self.config = config
        self.domain = domain
        self.worker_name = worker_name
        self.instance_id = instance_id
        self.topics = topics
        self.message_handler = message_handler
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._dlq_producer: Optional[AIOKafkaProducer] = (
            None  # Lazy-initialized for DLQ routing (WP-211)
        )

        self.consumer_config = config.get_worker_config(domain, worker_name, "consumer")
        self.group_id = config.get_consumer_group(domain, worker_name)

        processing_config = config.get_worker_config(domain, worker_name, "processing")
        self.max_batches = processing_config.get("max_batches")
        self._batch_count = 0

        self._enable_message_commit = enable_message_commit

        log_with_context(
            logger,
            logging.INFO,
            "Initialized Kafka consumer",
            domain=domain,
            worker_name=worker_name,
            topics=topics,
            group_id=self.group_id,
            bootstrap_servers=config.bootstrap_servers,
            max_batches=self.max_batches,
            enable_message_commit=enable_message_commit,
            consumer_config=self.consumer_config,
        )

    async def start(self) -> None:
        if self._running:
            logger.warning("Consumer already running, ignoring duplicate start call")
            return

        log_with_context(
            logger,
            logging.INFO,
            "Starting Kafka consumer",
            topics=self.topics,
            group_id=self.group_id,
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

        if self.config.security_protocol != "PLAINTEXT":
            kafka_consumer_config["security_protocol"] = self.config.security_protocol
            kafka_consumer_config["sasl_mechanism"] = self.config.sasl_mechanism

            # Create SSL context for SSL/SASL_SSL connections
            if "SSL" in self.config.security_protocol:
                import ssl

                ssl_context = ssl.create_default_context()
                kafka_consumer_config["ssl_context"] = ssl_context

            if self.config.sasl_mechanism == "OAUTHBEARER":
                oauth_callback = create_kafka_oauth_callback()
                kafka_consumer_config["sasl_oauth_token_provider"] = oauth_callback
            elif self.config.sasl_mechanism == "PLAIN":
                kafka_consumer_config["sasl_plain_username"] = self.config.sasl_plain_username
                kafka_consumer_config["sasl_plain_password"] = self.config.sasl_plain_password

        self._consumer = AIOKafkaConsumer(*self.topics, **kafka_consumer_config)

        await self._consumer.start()
        self._running = True

        update_connection_status("consumer", connected=True)
        partition_count = len(self._consumer.assignment())
        update_assigned_partitions(self.group_id, partition_count)

        log_with_context(
            logger,
            logging.INFO,
            "Kafka consumer started successfully",
            topics=self.topics,
            group_id=self.group_id,
            partitions=partition_count,
        )

        try:
            await self._consume_loop()
        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled, shutting down")
            raise
        except Exception as e:
            log_exception(
                logger,
                e,
                "Consumer loop terminated with error",
            )
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        if not self._running or self._consumer is None:
            logger.debug("Consumer not running or already stopped")
            return

        logger.info("Stopping Kafka consumer")
        self._running = False

        try:
            if self._consumer:
                await self._consumer.commit()
                await self._consumer.stop()

            if self._dlq_producer is not None:
                try:
                    await self._dlq_producer.flush()
                    await self._dlq_producer.stop()
                    logger.info("DLQ producer stopped successfully")
                except Exception as dlq_error:
                    log_exception(
                        logger,
                        dlq_error,
                        "Error stopping DLQ producer",
                    )
                finally:
                    self._dlq_producer = None

            logger.info("Kafka consumer stopped successfully")
        except Exception as e:
            log_exception(
                logger,
                e,
                "Error stopping Kafka consumer",
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
        log_with_context(
            logger,
            logging.DEBUG,
            "Committed offsets",
            group_id=self.group_id,
        )

    async def _consume_loop(self) -> None:
        log_with_context(
            logger,
            logging.INFO,
            "Starting message consumption loop",
            max_batches=self.max_batches,
            topics=self.topics,
            group_id=self.group_id,
        )

        _logged_waiting_for_assignment = False
        _logged_assignment_received = False

        while self._running and self._consumer:
            try:
                if self.max_batches is not None and self._batch_count >= self.max_batches:
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Reached max_batches limit, stopping consumer",
                        max_batches=self.max_batches,
                        batches_processed=self._batch_count,
                    )
                    return

                # Avoid blocking during rebalance: getmany() can hang if called before partition assignment
                assignment = self._consumer.assignment()
                if not assignment:
                    if not _logged_waiting_for_assignment:
                        log_with_context(
                            logger,
                            logging.INFO,
                            "Waiting for partition assignment (consumer group rebalance in progress)",
                            group_id=self.group_id,
                            topics=self.topics,
                        )
                        _logged_waiting_for_assignment = True
                    await asyncio.sleep(0.5)
                    continue

                if not _logged_assignment_received:
                    partition_info = [f"{tp.topic}:{tp.partition}" for tp in assignment]
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Partition assignment received, starting message consumption",
                        group_id=self.group_id,
                        partition_count=len(assignment),
                        partitions=partition_info,
                    )
                    _logged_assignment_received = True
                    update_assigned_partitions(self.group_id, len(assignment))

                # Fetch messages with timeout
                data = await self._consumer.getmany(timeout_ms=1000)

                if data:
                    self._batch_count += 1

                for topic_partition, messages in data.items():
                    for message in messages:
                        if not self._running:
                            logger.info("Consumer stopped, breaking message loop")
                            return

                        await self._process_message(message)

            except asyncio.CancelledError:
                logger.info("Consumption loop cancelled")
                raise
            except Exception as e:
                log_exception(
                    logger,
                    e,
                    "Error in consumption loop",
                )
                await asyncio.sleep(1)

    async def _process_message(self, message: ConsumerRecord) -> None:
        from kafka_pipeline.common.telemetry import get_tracer

        # Extract trace context from headers (optional - graceful degradation)
        parent_context = None
        try:
            import opentracing

            carrier = {}
            if message.headers:
                for key, value in message.headers:
                    k = key.decode("utf-8") if isinstance(key, bytes) else key
                    v = value.decode("utf-8") if isinstance(value, bytes) else value
                    carrier[k] = v
            tracer = get_tracer(__name__)
            if hasattr(tracer, "extract"):
                parent_context = tracer.extract(opentracing.Format.TEXT_MAP, carrier)
        except Exception:
            pass  # Tracing not available

        tracer = get_tracer(__name__)
        with tracer.start_active_span("kafka.message.process", child_of=parent_context) as scope:
            span = scope.span if hasattr(scope, "span") else scope
            span.set_tag("messaging.system", "kafka")
            span.set_tag("messaging.destination", message.topic)
            span.set_tag("messaging.kafka.partition", message.partition)
            span.set_tag("messaging.kafka.offset", message.offset)
            span.set_tag("messaging.kafka.consumer_group", self.group_id)
            span.set_tag(
                "messaging.message.id", message.key.decode("utf-8") if message.key else None
            )
            span.set_tag("span.kind", "consumer")

            with KafkaLogContext(
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                key=message.key.decode("utf-8") if message.key else None,
                consumer_group=self.group_id,
            ):
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "Processing message",
                    message_size=len(message.value) if message.value else 0,
                )

                start_time = time.perf_counter()
                message_size = len(message.value) if message.value else 0

                try:
                    await self.message_handler(message)

                    duration = time.perf_counter() - start_time
                    message_processing_duration_seconds.labels(
                        topic=message.topic, consumer_group=self.group_id
                    ).observe(duration)

                    if self._enable_message_commit:
                        await self._consumer.commit()

                    self._update_partition_metrics(message)

                    record_message_consumed(
                        message.topic, self.group_id, message_size, success=True
                    )

                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Message processed successfully",
                        duration_ms=round(duration * 1000, 2),
                    )

                except Exception as e:
                    duration = time.perf_counter() - start_time
                    message_processing_duration_seconds.labels(
                        topic=message.topic, consumer_group=self.group_id
                    ).observe(duration)

                    record_message_consumed(
                        message.topic, self.group_id, message_size, success=False
                    )

                    span.set_tag("error", True)
                    span.log_kv({"event": "error", "error.object": str(e)})

                    await self._handle_processing_error(message, e, duration)

    async def _handle_processing_error(
        self, message: ConsumerRecord, error: Exception, duration: float
    ) -> None:
        """Error classification with DLQ routing for PERMANENT errors, retry for others."""
        classified_error = KafkaErrorClassifier.classify_consumer_error(
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
            log_exception(
                logger,
                error,
                "Permanent error processing message - routing to DLQ",
                **common_context,
            )

            try:
                await self._send_to_dlq(message, error, error_category)

                # Commit offset after DLQ routing to advance past poison pill
                if self._enable_message_commit:
                    await self._consumer.commit()
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Offset committed after DLQ routing - partition can advance",
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                    )

            except Exception as dlq_error:
                log_exception(
                    logger,
                    dlq_error,
                    "DLQ routing failed - message will be retried",
                    **common_context,
                )

        elif error_category == ErrorCategory.TRANSIENT:
            log_exception(
                logger,
                error,
                "Transient error - will reprocess message",
                level=logging.WARNING,
                **common_context,
            )

        elif error_category == ErrorCategory.AUTH:
            log_exception(
                logger,
                error,
                "Authentication error - will reprocess after token refresh",
                level=logging.WARNING,
                **common_context,
            )

        elif error_category == ErrorCategory.CIRCUIT_OPEN:
            log_exception(
                logger,
                error,
                "Circuit breaker open - will reprocess when circuit closes",
                level=logging.WARNING,
                **common_context,
            )

        else:
            log_exception(
                logger,
                error,
                "Unknown error category - applying conservative retry",
                **common_context,
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
            log_with_context(
                logger,
                logging.DEBUG,
                "Failed to update partition metrics",
                topic=message.topic,
                partition=message.partition,
                error=str(e),
            )

    async def _ensure_dlq_producer(self) -> None:
        """Lazy-initialize DLQ producer to avoid unnecessary connections."""
        if self._dlq_producer is not None:
            return

        log_with_context(
            logger,
            logging.INFO,
            "Initializing DLQ producer for permanent error routing",
            domain=self.domain,
            worker_name=self.worker_name,
        )

        dlq_producer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "value_serializer": lambda v: v,
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
            "acks": "all",
            "enable_idempotence": True,
            "retry_backoff_ms": 1000,
        }

        if self.config.security_protocol != "PLAINTEXT":
            dlq_producer_config["security_protocol"] = self.config.security_protocol
            dlq_producer_config["sasl_mechanism"] = self.config.sasl_mechanism

            # Create SSL context for SSL/SASL_SSL connections
            if "SSL" in self.config.security_protocol:
                import ssl

                ssl_context = ssl.create_default_context()
                dlq_producer_config["ssl_context"] = ssl_context

            if self.config.sasl_mechanism == "OAUTHBEARER":
                oauth_callback = create_kafka_oauth_callback()
                dlq_producer_config["sasl_oauth_token_provider"] = oauth_callback
            elif self.config.sasl_mechanism == "PLAIN":
                dlq_producer_config["sasl_plain_username"] = self.config.sasl_plain_username
                dlq_producer_config["sasl_plain_password"] = self.config.sasl_plain_password

        self._dlq_producer = AIOKafkaProducer(**dlq_producer_config)
        await self._dlq_producer.start()

        log_with_context(
            logger,
            logging.INFO,
            "DLQ producer started successfully",
            bootstrap_servers=self.config.bootstrap_servers,
        )

    async def _send_to_dlq(
        self, message: ConsumerRecord, error: Exception, error_category: ErrorCategory
    ) -> None:
        """Send failed message to {topic}.dlq with full context (original message + error details)."""
        await self._ensure_dlq_producer()

        dlq_topic = f"{message.topic}.dlq"

        try:
            worker_id = socket.gethostname()
        except Exception:
            worker_id = "unknown"

        dlq_message = {
            "original_topic": message.topic,
            "original_partition": message.partition,
            "original_offset": message.offset,
            "original_key": message.key.decode("utf-8") if message.key else None,
            "original_value": message.value.decode("utf-8") if message.value else None,
            "original_headers": {
                k: v.decode("utf-8") if isinstance(v, bytes) else v
                for k, v in (message.headers or [])
            },
            "original_timestamp": message.timestamp,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_category": error_category.value,
            "consumer_group": self.group_id,
            "worker_id": worker_id,
            "domain": self.domain,
            "worker_name": self.worker_name,
            "dlq_timestamp": time.time(),
        }

        dlq_value = json.dumps(dlq_message).encode("utf-8")
        dlq_key = message.key or f"dlq-{message.offset}".encode("utf-8")

        dlq_headers = [
            ("dlq_source_topic", message.topic.encode("utf-8")),
            ("dlq_error_category", error_category.value.encode("utf-8")),
            ("dlq_consumer_group", self.group_id.encode("utf-8")),
        ]

        try:
            metadata = await self._dlq_producer.send_and_wait(
                dlq_topic,
                key=dlq_key,
                value=dlq_value,
                headers=dlq_headers,
            )

            log_with_context(
                logger,
                logging.INFO,
                "Message sent to DLQ successfully",
                dlq_topic=dlq_topic,
                dlq_partition=metadata.partition,
                dlq_offset=metadata.offset,
                original_topic=message.topic,
                original_partition=message.partition,
                original_offset=message.offset,
                error_category=error_category.value,
                error_type=type(error).__name__,
            )

            if error_category == ErrorCategory.PERMANENT:
                record_dlq_permanent(message.topic, self.group_id)
            else:
                record_dlq_transient(message.topic, self.group_id)

        except Exception as dlq_error:
            log_exception(
                logger,
                dlq_error,
                "Failed to send message to DLQ - message will be retried",
                dlq_topic=dlq_topic,
                original_topic=message.topic,
                original_partition=message.partition,
                original_offset=message.offset,
                error_category=error_category.value,
            )

    @property
    def is_running(self):
        return self._running and self._consumer is not None


__all__ = [
    "BaseKafkaConsumer",
    "AIOKafkaConsumer",
    "ConsumerRecord",
    "create_kafka_oauth_callback",
]
