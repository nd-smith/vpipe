"""
Kafka consumer with circuit breaker integration.

Provides async Kafka consumer functionality with:
- Manual offset commit for at-least-once processing
- Circuit breaker protection for resilience
- OAUTHBEARER authentication for Azure EventHub
- Multiple topic subscription
- Graceful shutdown handling
- Message handler pattern for processing logic
- itelligent error classification and routing
- Dead letter queue (DLQ) routing for permanent errors (WP-211)
"""

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
from core.resilience.circuit_breaker import (
    CircuitBreaker,
    KAFKA_CIRCUIT_CONFIG,
    get_circuit_breaker,
)
from config.config import KafkaConfig
from kafka_pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_connection_status,
    update_assigned_partitions,
    update_consumer_lag,
    update_consumer_offset,
    message_processing_duration_seconds,
    record_consumer_shutdown,
    record_consumer_shutdown_error,
)

logger = get_logger(__name__)


class BaseKafkaConsumer:
    """
    Async Kafka consumer with circuit breaker and authentication.

    Provides reliable message consumption with:
    - Manual offset commit for at-least-once processing
    - Circuit breaker protection against broker failures
    - Azure AD authentication via OAUTHBEARER
    - Multiple topic subscription
    - Graceful shutdown handling
    - Custom message handler pattern
    - Worker-specific configuration from hierarchical config

    Usage:
        >>> config = load_config()
        >>> async def handle_message(record: ConsumerRecord):
        ...     # Process message
        ...     print(f"Received: {record.value}")
        >>>
        >>> consumer = BaseKafkaConsumer(
        ...     config=config,
        ...     domain="xact",
        ...     worker_name="download_worker",
        ...     topics=["xact.downloads.pending"],
        ...     message_handler=handle_message
        ... )
        >>> await consumer.start()
        >>> # Consumer runs until stopped
        >>> await consumer.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        domain: str,
        worker_name: str,
        topics: List[str],
        message_handler: Callable[[ConsumerRecord], Awaitable[None]],
        circuit_breaker: Optional[CircuitBreaker] = None,
        enable_message_commit: bool = True,
    ):
        """
        Initialize Kafka consumer with worker-specific configuration.

        Args:
            config: Kafka configuration (loads from config.yaml)
            domain: Domain name ("xact" or "claimx")
            worker_name: Worker name (e.g., "download_worker", "event_ingester")
            topics: List of topics to subscribe to
            message_handler: Async callback function to process messages
            circuit_breaker: Optional custom circuit breaker (uses default if None)
            enable_message_commit: If True (default), commit offset after each message.
                        Set to False for batch processing where handler manages commits.

        Note:
            Consumer settings (max_poll_records, session_timeout_ms, etc.) are loaded
            from config using config.get_worker_config(domain, worker_name, "consumer").
            This merges worker-specific settings with defaults.
        """
        if not topics:
            raise ValueError("At least one topic must be specified")

        self.config = config
        self.domain = domain
        self.worker_name = worker_name
        self.topics = topics
        self.message_handler = message_handler
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

        # DLQ producer for routing permanent errors (WP-211)
        # Created lazily when first DLQ message needs to be sent
        self._dlq_producer: Optional[AIOKafkaProducer] = None

        # Get worker-specific consumer config (merged with defaults)
        self.consumer_config = config.get_worker_config(domain, worker_name, "consumer")

        # Get consumer group name (from worker config or generated)
        self.group_id = config.get_consumer_group(domain, worker_name)

        # Get max_batches from processing config if available
        processing_config = config.get_worker_config(domain, worker_name, "processing")
        self.max_batches = processing_config.get("max_batches")
        self._batch_count = 0

        # Circuit breaker
        self._circuit_breaker = circuit_breaker or get_circuit_breaker(
            f"kafka_consumer_{self.group_id}", KAFKA_CIRCUIT_CONFIG
        )

        # Commit control - disable for batch processing
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
        """
        Start the Kafka consumer and begin processing messages.

        Creates the underlying aiokafka consumer, connects to the cluster,
        and starts the message consumption loop. This method runs until
        stop() is called.

        The consumer will:
        1. Connect to Kafka cluster with authentication
        2. Subscribe to configured topics
        3. Consume messages and call message_handler for each
        4. Manually commit offsets after successful processing

        Raises:
            Exception: If consumer fails to start or connect
        """
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

        # Build aiokafka consumer configuration from merged settings
        kafka_consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": self.group_id,
            # Connection timeout settings (from shared connection config)
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        # Apply worker-specific consumer settings (merged defaults + overrides)
        # These come from config.get_worker_config(domain, worker_name, "consumer")
        kafka_consumer_config.update({
            "enable_auto_commit": self.consumer_config.get("enable_auto_commit", False),
            "auto_offset_reset": self.consumer_config.get("auto_offset_reset", "earliest"),
            "max_poll_records": self.consumer_config.get("max_poll_records", 100),
            "max_poll_interval_ms": self.consumer_config.get("max_poll_interval_ms", 300000),
            "session_timeout_ms": self.consumer_config.get("session_timeout_ms", 30000),
        })

        # Optional consumer settings (only add if present)
        if "heartbeat_interval_ms" in self.consumer_config:
            kafka_consumer_config["heartbeat_interval_ms"] = self.consumer_config["heartbeat_interval_ms"]
        if "fetch_min_bytes" in self.consumer_config:
            kafka_consumer_config["fetch_min_bytes"] = self.consumer_config["fetch_min_bytes"]
        if "fetch_max_wait_ms" in self.consumer_config:
            kafka_consumer_config["fetch_max_wait_ms"] = self.consumer_config["fetch_max_wait_ms"]
        if "partition_assignment_strategy" in self.consumer_config:
            kafka_consumer_config["partition_assignment_strategy"] = self.consumer_config["partition_assignment_strategy"]

        # Configure security based on protocol
        if self.config.security_protocol != "PLAINTEXT":
            kafka_consumer_config["security_protocol"] = self.config.security_protocol
            kafka_consumer_config["sasl_mechanism"] = self.config.sasl_mechanism

            # Add authentication based on mechanism
            if self.config.sasl_mechanism == "OAUTHBEARER":
                oauth_callback = create_kafka_oauth_callback()
                kafka_consumer_config["sasl_oauth_token_provider"] = oauth_callback
            elif self.config.sasl_mechanism == "PLAIN":
                # SASL_PLAIN for Event Hubs or basic auth
                kafka_consumer_config["sasl_plain_username"] = self.config.sasl_plain_username
                kafka_consumer_config["sasl_plain_password"] = self.config.sasl_plain_password

        # Create aiokafka consumer
        self._consumer = AIOKafkaConsumer(*self.topics, **kafka_consumer_config)

        await self._consumer.start()
        self._running = True

        # Update connection status and partition assignment metrics
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

        # Start message consumption loop
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
        """
        Stop the Kafka consumer and cleanup resources.

        Commits any pending offsets and closes the connection gracefully.
        Also stops the DLQ producer if it was created.
        Safe to call multiple times.
        """
        if not self._running or self._consumer is None:
            logger.debug("Consumer not running or already stopped")
            return

        logger.info("Stopping Kafka consumer")
        self._running = False

        try:
            # Commit any pending offsets
            if self._consumer:
                await self._consumer.commit()
                # Stop the consumer
                await self._consumer.stop()

            # Stop DLQ producer if it exists
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
            # Update connection status metrics
            update_connection_status("consumer", connected=False)
            update_assigned_partitions(self.group_id, 0)
            self._consumer = None

    async def commit(self) -> None:
        """
        Commit current offsets to Kafka.

        Use this for batch processing where enable_message_commit=False.
        Should be called after successfully processing/writing a batch.

        Raises:
            Exception: If commit fails
        """
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
        """
        Main message consumption loop.

        Continuously fetches messages, processes them through the handler,
        and commits offsets after successful processing.

        If max_batches is set, exits after processing that many batches.
        """
        log_with_context(
            logger,
            logging.INFO,
            "Starting message consumption loop",
            max_batches=self.max_batches,
            topics=self.topics,
            group_id=self.group_id,
        )

        # Track partition assignment status for logging
        _logged_waiting_for_assignment = False
        _logged_assignment_received = False

        while self._running and self._consumer:
            try:
                # Check if we've reached max_batches limit
                if self.max_batches is not None and self._batch_count >= self.max_batches:
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Reached max_batches limit, stopping consumer",
                        max_batches=self.max_batches,
                        batches_processed=self._batch_count,
                    )
                    return

                # Check partition assignment before fetching
                # During consumer group rebalances, getmany() can block indefinitely
                # waiting for partition assignment (ignoring timeout_ms).
                # This check ensures we don't hang during startup with multiple consumers.
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
                    # Sleep briefly and retry - don't call getmany() during rebalance
                    await asyncio.sleep(0.5)
                    continue

                # Log when we first receive partition assignment
                if not _logged_assignment_received:
                    partition_info = [
                        f"{tp.topic}:{tp.partition}" for tp in assignment
                    ]
                    log_with_context(
                        logger,
                        logging.INFO,
                        "Partition assignment received, starting message consumption",
                        group_id=self.group_id,
                        partition_count=len(assignment),
                        partitions=partition_info,
                    )
                    _logged_assignment_received = True
                    # Update partition assignment metric
                    update_assigned_partitions(self.group_id, len(assignment))

                # Fetch messages with timeout
                # Note: We call getmany() directly instead of through the circuit breaker
                # because the circuit breaker uses threading.RLock which can cause deadlocks
                # when multiple async consumers share the same lock in a single event loop.
                try:
                    data = await self._consumer.getmany(timeout_ms=1000)
                    self._circuit_breaker.record_success()
                except Exception as fetch_error:
                    self._circuit_breaker.record_failure(fetch_error)
                    raise

                # Count this as a batch if we got any messages
                if data:
                    self._batch_count += 1

                # Process messages from all partitions
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
                # Continue processing - the circuit breaker will handle repeated failures
                await asyncio.sleep(1)

    async def _process_message(self, message: ConsumerRecord) -> None:
        """
        Process a single message with error classification and routing.

        Handles errors according to their category:
        - TRANSIENT: Will be retried (don't commit, message reprocessed)
        - PERMANENT: Should go to DLQ (don't commit for now, logged)
        - AUTH: Token refresh needed (don't commit, will reprocess)
        - CIRCUIT_OPEN: Circuit breaker open (don't commit, will reprocess)

        Args:
            message: ConsumerRecord to process
        """
        # Extract OpenTelemetry trace context from Kafka headers
        from opentelemetry.propagate import extract
        from opentelemetry import trace
        from opentelemetry.trace import SpanKind, StatusCode

        # Convert Kafka headers to dict for context extraction
        carrier = {}
        if message.headers:
            for key, value in message.headers:
                k = key.decode("utf-8") if isinstance(key, bytes) else key
                v = value.decode("utf-8") if isinstance(value, bytes) else value
                carrier[k] = v

        # Extract trace context and create processing span
        ctx = extract(carrier)
        tracer = trace.get_tracer(__name__)

        with tracer.start_as_current_span(
            "kafka.message.process",
            context=ctx,
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination": message.topic,
                "messaging.kafka.partition": message.partition,
                "messaging.kafka.offset": message.offset,
                "messaging.kafka.consumer_group": self.group_id,
                "messaging.message.id": message.key.decode("utf-8") if message.key else None,
            },
        ) as span:
            # Use KafkaLogContext to automatically include Kafka context in all logs
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

                # Track processing time
                start_time = time.perf_counter()
                message_size = len(message.value) if message.value else 0

                try:
                    # Call user-provided message handler
                    await self.message_handler(message)

                    # Record processing duration
                    duration = time.perf_counter() - start_time
                    message_processing_duration_seconds.labels(
                        topic=message.topic, consumer_group=self.group_id
                    ).observe(duration)

                    # Commit offset after successful processing (at-least-once semantics)
                    # Skip if enable_message_commit=False (batch processing mode)
                    if self._enable_message_commit:
                        await self._consumer.commit()

                    # Update offset and lag metrics
                    self._update_partition_metrics(message)

                    # Record successful message consumption
                    record_message_consumed(
                        message.topic, self.group_id, message_size, success=True
                    )

                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "Message processed successfully",
                        duration_ms=round(duration * 1000, 2),
                    )

                    span.set_status(StatusCode.OK)

                except Exception as e:
                    # Record processing duration even for failures
                    duration = time.perf_counter() - start_time
                    message_processing_duration_seconds.labels(
                        topic=message.topic, consumer_group=self.group_id
                    ).observe(duration)

                    # Record failed message consumption
                    record_message_consumed(
                        message.topic, self.group_id, message_size, success=False
                    )

                    # Record exception in span
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(e)

                    # Classify the error to determine routing
                    await self._handle_processing_error(message, e, duration)

    async def _handle_processing_error(
        self, message: ConsumerRecord, error: Exception, duration: float
    ) -> None:
        """
        Handle message processing errors with intelligent routing.

        Classifies the error and determines appropriate action:
        - PERMANENT: Send to DLQ and commit offset (no retry - poison pill removed)
        - TRANSIENT: Log and don't commit (message will be retried on next poll)
        - AUTH: Log and don't commit (will reprocess after token refresh)
        - CIRCUIT_OPEN: Log and don't commit (will reprocess when circuit closes)
        - UNKNOWN: Log and don't commit (conservative retry)

        Args:
            message: The ConsumerRecord that failed processing
            error: The exception that occurred during processing
            duration: Processing duration in seconds before error occurred
        """
        # Classify the error using Kafka error classifier
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

        # Record error metric
        record_processing_error(message.topic, self.group_id, error_category.value)

        # Common log context - Kafka context is automatically included via KafkaLogContext
        common_context = {
            "error_category": error_category.value,
            "classified_as": type(classified_error).__name__,
            "duration_ms": round(duration * 1000, 2),
        }

        # Route based on error category
        if error_category == ErrorCategory.PERMANENT:
            log_exception(
                logger,
                error,
                "Transient error processing message - will retry on next poll",
                level=logging.WARNING,
                **common_context,
            )
            # Don't commit offset - message will be reprocessed
            # TODO (WP-209): Send to retry topic with exponential backoff

        if error_category == ErrorCategory.PERMANENT:
            log_exception(
                logger,
                error,
                "Permanent error processing message - routing to DLQ",
                **common_context,
            )

            # Send to DLQ topic (WP-211)
            try:
                await self._send_to_dlq(message, error, error_category)

                # Commit offset after successful DLQ routing to advance past poison pill
                # This prevents the message from blocking the partition forever
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
                # If DLQ send failed, log but don't commit offset
                # Message will be reprocessed (same as before DLQ implementation)
                log_exception(
                    logger,
                    dlq_error,
                    "DLQ routing failed - message will be retried",
                    **common_context,
                )

        elif error_category == ErrorCategory.TRANSIENT:
            # Transient errors - don't commit, will retry
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
            # Don't commit offset - message will be reprocessed after auth refresh
            # The token cache will refresh automatically on next attempt

        elif error_category == ErrorCategory.CIRCUIT_OPEN:
            log_exception(
                logger,
                error,
                "Circuit breaker open - will reprocess when circuit closes",
                level=logging.WARNING,
                **common_context,
            )
            # Don't commit offset - message will be reprocessed when circuit recovers
            # Circuit breaker will track failure rate and open/close accordingly

        else:  # UNKNOWN or other categories
            log_exception(
                logger,
                error,
                "Unknown error category - applying conservative retry",
                **common_context,
            )
            # Don't commit offset - conservative retry for unknown errors

        # Note: We don't re-raise the exception here because we want to continue
        # processing other messages. The offset won't be committed (except for PERMANENT
        # errors routed to DLQ), so this message will be retried on the next poll.

    def _update_partition_metrics(self, message: ConsumerRecord) -> None:
        """
        Update offset and lag metrics for the partition.

        Args:
            message: The consumed message with partition and offset info
        """
        if not self._consumer:
            return

        try:
            # Update current offset
            update_consumer_offset(
                message.topic, message.partition, self.group_id, message.offset
            )

            # Calculate and update lag (high watermark - current offset)
            # Get high watermark for the partition
            tp = TopicPartition(message.topic, message.partition)
            partition_metadata = self._consumer.highwater(tp)

            if partition_metadata is not None:
                # Lag = high watermark - (current offset + 1)
                # +1 because we've consumed this message
                lag = partition_metadata - (message.offset + 1)
                update_consumer_lag(
                    message.topic, message.partition, self.group_id, lag
                )

        except Exception as e:
            # Don't fail message processing due to metrics issues
            log_with_context(
                logger,
                logging.DEBUG,
                "Failed to update partition metrics",
                topic=message.topic,
                partition=message.partition,
                error=str(e),
            )


    async def _ensure_dlq_producer(self) -> None:
        """
        Ensure DLQ producer is initialized.

        Creates a Kafka producer for sending messages to DLQ topics if it doesn't exist.
        The producer is created lazily when the first DLQ message needs to be sent.

        This avoids creating unnecessary connections if DLQ is never used.
        """
        if self._dlq_producer is not None:
            return

        log_with_context(
            logger,
            logging.INFO,
            "Initializing DLQ producer for permanent error routing",
            domain=self.domain,
            worker_name=self.worker_name,
        )

        # Build DLQ producer configuration (similar to consumer connection config)
        dlq_producer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "value_serializer": lambda v: v,  # We'll handle serialization manually
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
            # DLQ producer settings - prefer reliability over throughput
            "acks": "all",  # Wait for all replicas
            "enable_idempotence": True,  # Prevent duplicates
            "retry_backoff_ms": 1000,
        }

        # Configure security based on protocol
        if self.config.security_protocol != "PLAINTEXT":
            dlq_producer_config["security_protocol"] = self.config.security_protocol
            dlq_producer_config["sasl_mechanism"] = self.config.sasl_mechanism

            # Add authentication based on mechanism
            if self.config.sasl_mechanism == "OAUTHBEARER":
                oauth_callback = create_kafka_oauth_callback()
                dlq_producer_config["sasl_oauth_token_provider"] = oauth_callback
            elif self.config.sasl_mechanism == "PLAIN":
                dlq_producer_config["sasl_plain_username"] = self.config.sasl_plain_username
                dlq_producer_config["sasl_plain_password"] = self.config.sasl_plain_password

        # Create and start DLQ producer
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
        """
        Send a failed message to the dead-letter queue with full context.

        Creates a DLQ message containing:
        - Original message key, value, headers, topic, partition, offset
        - Error details: type, message, category
        - Metadata: timestamp, worker ID, consumer group

        DLQ messages are sent to topic "{original_topic}.dlq"

        Args:
            message: The original ConsumerRecord that failed processing
            error: The exception that occurred during processing
            error_category: Classification of the error (PERMANENT, TRANSIENT, etc.)

        Raises:
            Exception: If DLQ producer fails to send message (logged but not re-raised)
        """
        # Ensure DLQ producer is initialized
        await self._ensure_dlq_producer()

        # Build DLQ topic name
        dlq_topic = f"{message.topic}.dlq"

        # Get hostname for worker identification
        try:
            worker_id = socket.gethostname()
        except Exception:
            worker_id = "unknown"

        # Build DLQ message with full context
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

        # Serialize DLQ message to JSON
        dlq_value = json.dumps(dlq_message).encode("utf-8")

        # Use original message key for DLQ (maintains partitioning)
        dlq_key = message.key or f"dlq-{message.offset}".encode("utf-8")

        # Add DLQ headers
        dlq_headers = [
            ("dlq_source_topic", message.topic.encode("utf-8")),
            ("dlq_error_category", error_category.value.encode("utf-8")),
            ("dlq_consumer_group", self.group_id.encode("utf-8")),
        ]

        try:
            # Send to DLQ through producer
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

            # Record DLQ metric based on error category
            if error_category == ErrorCategory.PERMANENT:
                record_dlq_permanent(message.topic, self.group_id)
            else:
                # TRANSIENT errors that exhausted retries
                record_dlq_transient(message.topic, self.group_id)

        except Exception as dlq_error:
            # Log DLQ send failure but don't re-raise
            # We don't want DLQ failures to crash the consumer
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
            # Note: Since we couldn't send to DLQ, offset won't be committed,
            # so message will be reprocessed (same as before DLQ implementation)

    @property
    def is_running(self) -> bool:
        """Check if consumer is running and processing messages."""
        return self._running and self._consumer is not None


__all__ = [
    "BaseKafkaConsumer",
    "AIOKafkaConsumer",
    "get_circuit_breaker",
    "CircuitBreaker",
    "ConsumerRecord",
    "create_kafka_oauth_callback",
]
