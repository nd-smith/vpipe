"""
Kafka producer with circuit breaker integration.

Provides async Kafka producer functionality with:
- Circuit breaker protection for resilience
- OAUTHBEARER authentication for Azure EventHub
- Batch sending support
- Header support for message routing
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from aiokafka import AIOKafkaProducer
from aiokafka.structs import RecordMetadata
from pydantic import BaseModel

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging import get_logger, log_with_context, log_exception
from core.resilience.circuit_breaker import (
    CircuitBreaker,
    KAFKA_CIRCUIT_CONFIG,
    get_circuit_breaker,
)
from config.config import KafkaConfig
from kafka_pipeline.common.metrics import (
    record_message_produced,
    record_producer_error,
    update_connection_status,
    batch_processing_duration_seconds,
)

logger = get_logger(__name__)


class BaseKafkaProducer:
    """
    Async Kafka producer with circuit breaker and authentication.

    Provides reliable message production with:
    - Circuit breaker protection against broker failures
    - Azure AD authentication via OAUTHBEARER
    - Batching support for efficient throughput
    - Message headers for routing metadata
    - Worker-specific configuration from hierarchical config

    Usage:
        >>> config = load_config()
        >>> producer = BaseKafkaProducer(
        ...     config=config,
        ...     domain="xact",
        ...     worker_name="download_worker"
        ... )
        >>> await producer.start()
        >>> try:
        ...     metadata = await producer.send(
        ...         topic="xact.downloads.cached",
        ...         key="key-123",
        ...         value=my_pydantic_model,
        ...         headers={"trace_id": "evt-456"}
        ...     )
        ... finally:
        ...     await producer.stop()
    """

    def __init__(
        self,
        config: KafkaConfig,
        domain: str,
        worker_name: str,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        """
        Initialize Kafka producer with worker-specific configuration.

        Args:
            config: Kafka configuration (loads from config.yaml)
            domain: Domain name ("xact" or "claimx")
            worker_name: Worker name (e.g., "download_worker", "event_ingester")
            circuit_breaker: Optional custom circuit breaker (uses default if None)

        Note:
            Producer settings (acks, retries, compression, etc.) are loaded
            from config using config.get_worker_config(domain, worker_name, "producer").
            This merges worker-specific settings with defaults.
        """
        self.config = config
        self.domain = domain
        self.worker_name = worker_name
        self._producer: Optional[AIOKafkaProducer] = None
        self._circuit_breaker = circuit_breaker or get_circuit_breaker(
            f"kafka_producer_{domain}_{worker_name}", KAFKA_CIRCUIT_CONFIG
        )
        self._started = False

        # Get worker-specific producer config (merged with defaults)
        self.producer_config = config.get_worker_config(domain, worker_name, "producer")

        log_with_context(
            logger,
            logging.INFO,
            "Initialized Kafka producer",
            domain=domain,
            worker_name=worker_name,
            bootstrap_servers=config.bootstrap_servers,
            security_protocol=config.security_protocol,
            sasl_mechanism=config.sasl_mechanism,
            producer_config=self.producer_config,
        )

    async def start(self) -> None:
        """
        Start the Kafka producer and establish connection.

        Creates the underlying aiokafka producer with authentication
        and connects to the Kafka cluster.

        Raises:
            Exception: If producer fails to start or connect
        """
        if self._started:
            logger.warning("Producer already started, ignoring duplicate start call")
            return

        logger.info("Starting Kafka producer")

        # Build aiokafka producer configuration from merged settings
        kafka_producer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "value_serializer": lambda v: v,  # We'll handle serialization in send()
            # Connection timeout settings (from shared connection config)
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        # Apply worker-specific producer settings (merged defaults + overrides)
        # These come from config.get_worker_config(domain, worker_name, "producer")
        # Note: aiokafka doesn't have a 'retries' parameter like kafka-python
        # It handles retries internally based on retry_backoff_ms and request_timeout_ms
        # Convert acks to int if numeric string (aiokafka requires int for 0/1, or "all")
        acks_value = self.producer_config.get("acks", "all")
        if isinstance(acks_value, str) and acks_value.isdigit():
            acks_value = int(acks_value)
        kafka_producer_config.update({
            "acks": acks_value,
            "retry_backoff_ms": self.producer_config.get("retry_backoff_ms", 1000),
        })

        # Optional producer settings (only add if present)
        # Note: aiokafka uses 'max_batch_size', config uses 'batch_size' for compatibility
        if "batch_size" in self.producer_config:
            kafka_producer_config["max_batch_size"] = self.producer_config["batch_size"]
        if "linger_ms" in self.producer_config:
            kafka_producer_config["linger_ms"] = self.producer_config["linger_ms"]
        if "compression_type" in self.producer_config:
            # Convert string "none" to Python None for aiokafka compatibility
            compression = self.producer_config["compression_type"]
            kafka_producer_config["compression_type"] = None if compression == "none" else compression
        # Note: max_in_flight_requests_per_connection is not supported by aiokafka
        # (it's a kafka-python/Java client parameter). aiokafka handles concurrency internally.
        # Note: buffer_memory is not supported by aiokafka (it's a kafka-python/Java client
        # parameter). aiokafka manages its internal buffer differently.
        if "max_request_size" in self.producer_config:
            kafka_producer_config["max_request_size"] = self.producer_config["max_request_size"]
        if "max_request_size" not in self.producer_config:
            # Set default max_request_size to 10MB if not specified
            kafka_producer_config["max_request_size"] = 10 * 1024 * 1024

        # Configure security based on protocol
        if self.config.security_protocol != "PLAINTEXT":
            kafka_producer_config["security_protocol"] = self.config.security_protocol
            kafka_producer_config["sasl_mechanism"] = self.config.sasl_mechanism

            # Add authentication based on mechanism
            if self.config.sasl_mechanism == "OAUTHBEARER":
                oauth_callback = create_kafka_oauth_callback()
                kafka_producer_config["sasl_oauth_token_provider"] = oauth_callback
            elif self.config.sasl_mechanism == "PLAIN":
                # SASL_PLAIN for Event Hubs or basic auth
                kafka_producer_config["sasl_plain_username"] = self.config.sasl_plain_username
                kafka_producer_config["sasl_plain_password"] = self.config.sasl_plain_password

        # Create aiokafka producer
        self._producer = AIOKafkaProducer(**kafka_producer_config)

        await self._producer.start()
        self._started = True

        # Update connection status metric
        update_connection_status("producer", connected=True)

        log_with_context(
            logger,
            logging.INFO,
            "Kafka producer started successfully",
            bootstrap_servers=self.config.bootstrap_servers,
            acks=self.producer_config.get("acks", "all"),
            compression_type=self.producer_config.get("compression_type", "none"),
        )

    async def stop(self) -> None:
        """
        Stop the Kafka producer and cleanup resources.

        Flushes any pending messages and closes the connection gracefully.
        Safe to call multiple times.

        Note: Errors during stop are logged but not re-raised, since cleanup
        errors (especially during exception handling) should not mask the
        original exception or prevent other cleanup from completing.
        """
        import asyncio

        if not self._started or self._producer is None:
            logger.debug("Producer not started or already stopped")
            return

        logger.info("Stopping Kafka producer")

        try:
            # Check if we have a running event loop before attempting async operations.
            # During shutdown (GeneratorExit, KeyboardInterrupt), the loop may already
            # be closed, and aiokafka's stop() will fail trying to create tasks.
            try:
                loop = asyncio.get_running_loop()
                if loop.is_closed():
                    logger.warning(
                        "Event loop is closed, skipping graceful producer shutdown"
                    )
                    return
            except RuntimeError:
                # No running event loop - can't perform async cleanup
                logger.warning(
                    "No running event loop, skipping graceful producer shutdown"
                )
                return

            # Flush any pending messages
            await self._producer.flush()
            # Stop the producer
            await self._producer.stop()
            logger.info("Kafka producer stopped successfully")
        except Exception as e:
            # Log but don't re-raise - cleanup errors should not propagate.
            # This is especially important during GeneratorExit or CancelledError
            # cleanup, where aiokafka's stop() may fail due to event loop state.
            log_exception(
                logger,
                e,
                "Error stopping Kafka producer",
            )
        finally:
            # Update connection status metric
            update_connection_status("producer", connected=False)
            self._producer = None
            self._started = False

    async def send(
        self,
        topic: str,
        key: str,
        value: Union[BaseModel, Dict[str, Any]],
        headers: Optional[Dict[str, str]] = None,
    ) -> RecordMetadata:
        """
        Send a single message to Kafka topic.

        The message is serialized to JSON and sent with the specified key.
        Headers can be provided for routing metadata.

        Args:
            topic: Kafka topic name
            key: Message key (used for partitioning)
            value: Pydantic model or dict to serialize as message value
            headers: Optional key-value pairs for message headers

        Returns:
            RecordMetadata with topic, partition, offset information

        Raises:
            CircuitOpenError: If circuit breaker is open
            Exception: If send operation fails
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        # Serialize value to JSON bytes (handle both BaseModel and dict)
        if isinstance(value, BaseModel):
            value_bytes = value.model_dump_json().encode("utf-8")
        else:
            value_bytes = json.dumps(value).encode("utf-8")

        # Convert headers to list of tuples with byte values
        headers_list = None
        if headers:
            headers_list = [(k, v.encode("utf-8")) for k, v in headers.items()]

        log_with_context(
            logger,
            logging.DEBUG,
            "Sending message to Kafka",
            topic=topic,
            key=key,
            headers=headers,
            value_size=len(value_bytes),
        )

        # Send through circuit breaker
        async def _send():
            return await self._producer.send_and_wait(
                topic,
                key=key.encode("utf-8"),
                value=value_bytes,
                headers=headers_list,
            )

        try:
            metadata = await self._circuit_breaker.call_async(_send)

            # Record successful message production
            record_message_produced(topic, len(value_bytes), success=True)

            log_with_context(
                logger,
                logging.DEBUG,
                "Message sent successfully",
                topic=metadata.topic,
                partition=metadata.partition,
                offset=metadata.offset,
            )

            return metadata

        except Exception as e:
            # Record failed message production
            record_message_produced(topic, len(value_bytes), success=False)
            record_producer_error(topic, type(e).__name__)

            log_exception(
                logger,
                e,
                "Failed to send message",
                topic=topic,
                key=key,
            )
            raise

    async def send_batch(
        self,
        topic: str,
        messages: List[Tuple[str, BaseModel]],
        headers: Optional[Dict[str, str]] = None,
    ) -> List[RecordMetadata]:
        """
        Send a batch of messages to Kafka topic.

        All messages are sent to the same topic. Each message can have
        a different key for partitioning. Headers are applied to all messages.

        Args:
            topic: Kafka topic name
            messages: List of (key, value) tuples to send
            headers: Optional headers applied to all messages

        Returns:
            List of RecordMetadata in same order as input messages

        Raises:
            CircuitOpenError: If circuit breaker is open
            Exception: If any send operation fails
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        if not messages:
            logger.warning("send_batch called with empty message list")
            return []

        log_with_context(
            logger,
            logging.INFO,
            "Sending batch to Kafka",
            topic=topic,
            message_count=len(messages),
            headers=headers,
        )

        # Convert headers to list of tuples
        headers_list = None
        if headers:
            headers_list = [(k, v.encode("utf-8")) for k, v in headers.items()]

        # Track batch processing time
        start_time = time.perf_counter()

        # Send all messages and collect futures
        futures = []
        total_bytes = 0
        for key, value in messages:
            # Serialize value to JSON bytes
            value_bytes = value.model_dump_json().encode("utf-8")
            total_bytes += len(value_bytes)

            # Send message (returns Future)
            future = await self._producer.send(
                topic,
                key=key.encode("utf-8"),
                value=value_bytes,
                headers=headers_list,
            )
            futures.append(future)

        # Wait for all sends to complete through circuit breaker
        async def _wait_for_batch():
            # Await all futures
            results = []
            for future in futures:
                metadata = await future
                results.append(metadata)
            return results

        try:
            results = await self._circuit_breaker.call_async(_wait_for_batch)

            # Record batch metrics
            duration = time.perf_counter() - start_time
            batch_processing_duration_seconds.labels(topic=topic).observe(duration)

            # Record each message as successfully produced
            for _ in results:
                record_message_produced(topic, total_bytes // len(results), success=True)

            log_with_context(
                logger,
                logging.INFO,
                "Batch sent successfully",
                topic=topic,
                message_count=len(results),
                partitions=list({r.partition for r in results}),
                duration_ms=round(duration * 1000, 2),
            )

            return results

        except Exception as e:
            # Record batch failure
            duration = time.perf_counter() - start_time
            batch_processing_duration_seconds.labels(topic=topic).observe(duration)

            # Record failures for each message
            for _ in messages:
                record_message_produced(topic, total_bytes // len(messages), success=False)
            record_producer_error(topic, type(e).__name__)

            log_exception(
                logger,
                e,
                "Failed to send batch",
                topic=topic,
                message_count=len(messages),
                duration_ms=round(duration * 1000, 2),
            )
            raise

    async def flush(self) -> None:
        """
        Flush any pending messages to Kafka.

        Blocks until all buffered messages have been sent.

        Raises:
            RuntimeError: If producer not started
        """
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        logger.debug("Flushing producer")
        await self._producer.flush()

    @property
    def is_started(self) -> bool:
        """Check if producer is started and ready to send messages."""
        return self._started and self._producer is not None


__all__ = [
    "BaseKafkaProducer",
    "AIOKafkaProducer",
    "get_circuit_breaker",
    "CircuitBreaker",
    "RecordMetadata",
    "create_kafka_oauth_callback",
]
