"""Kafka producer with circuit breaker integration and Azure AD authentication."""

import json
import logging
import time
from typing import Any

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel

from config.config import KafkaConfig
from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging import get_logger, log_exception, log_with_context
from core.utils.json_serializers import json_serializer
from kafka_pipeline.common.metrics import (
    message_processing_duration_seconds,
    record_message_produced,
    record_producer_error,
    update_connection_status,
)
from kafka_pipeline.common.types import ProduceResult

logger = get_logger(__name__)


class BaseKafkaProducer:
    """Async Kafka producer with circuit breaker, Azure AD auth, and worker-specific config."""

    def __init__(
        self,
        config: KafkaConfig,
        domain: str,
        worker_name: str,
    ):
        # Producer settings loaded via config.get_worker_config(domain, worker_name, "producer")
        # which merges worker-specific overrides with defaults
        self.config = config
        self.domain = domain
        self.worker_name = worker_name
        self._producer: AIOKafkaProducer | None = None
        self._started = False
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
        if self._started:
            logger.warning("Producer already started, ignoring duplicate start call")
            return

        logger.info("Starting Kafka producer")

        kafka_producer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "value_serializer": lambda v: v,
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        # aiokafka requires int for acks 0/1, or string "all"
        acks_value = self.producer_config.get("acks", "all")
        if isinstance(acks_value, str) and acks_value.isdigit():
            acks_value = int(acks_value)

        # Idempotent producer prevents duplicate messages during retries.
        # Kafka assigns Producer ID and tracks sequence numbers for deduplication.
        enable_idempotence = self.producer_config.get("enable_idempotence", True)
        if enable_idempotence and acks_value != "all":
            log_with_context(
                logger,
                logging.WARNING,
                "Overriding acks to 'all' because enable_idempotence=True requires it",
                configured_acks=acks_value,
                domain=self.domain,
                worker_name=self.worker_name,
            )
            acks_value = "all"

        kafka_producer_config.update(
            {
                "acks": acks_value,
                "retry_backoff_ms": self.producer_config.get("retry_backoff_ms", 1000),
                "enable_idempotence": enable_idempotence,
            }
        )

        # aiokafka uses 'max_batch_size', config uses 'batch_size' for compatibility
        if "batch_size" in self.producer_config:
            kafka_producer_config["max_batch_size"] = self.producer_config["batch_size"]
        if "linger_ms" in self.producer_config:
            kafka_producer_config["linger_ms"] = self.producer_config["linger_ms"]
        if "compression_type" in self.producer_config:
            compression = self.producer_config["compression_type"]
            kafka_producer_config["compression_type"] = (
                None if compression == "none" else compression
            )
        if "max_request_size" in self.producer_config:
            kafka_producer_config["max_request_size"] = self.producer_config[
                "max_request_size"
            ]
        if "max_request_size" not in self.producer_config:
            kafka_producer_config["max_request_size"] = 10 * 1024 * 1024
        if self.config.security_protocol != "PLAINTEXT":
            kafka_producer_config["security_protocol"] = self.config.security_protocol
            kafka_producer_config["sasl_mechanism"] = self.config.sasl_mechanism

            # Create SSL context for SSL/SASL_SSL connections
            if "SSL" in self.config.security_protocol:
                import ssl

                ssl_context = ssl.create_default_context()
                kafka_producer_config["ssl_context"] = ssl_context

            if self.config.sasl_mechanism == "OAUTHBEARER":
                oauth_callback = create_kafka_oauth_callback()
                kafka_producer_config["sasl_oauth_token_provider"] = oauth_callback
            elif self.config.sasl_mechanism == "PLAIN":
                kafka_producer_config["sasl_plain_username"] = (
                    self.config.sasl_plain_username
                )
                kafka_producer_config["sasl_plain_password"] = (
                    self.config.sasl_plain_password
                )
            elif self.config.sasl_mechanism == "GSSAPI":
                kafka_producer_config["sasl_kerberos_service_name"] = (
                    self.config.sasl_kerberos_service_name
                )

        # Debug: log connection settings (mask secrets)
        logger.info(
            "Producer connection config: bootstrap_servers=%s, security_protocol=%s, "
            "sasl_mechanism=%s, sasl_username=%s, sasl_password_set=%s, ssl_context_set=%s",
            kafka_producer_config.get("bootstrap_servers"),
            kafka_producer_config.get("security_protocol", "PLAINTEXT"),
            kafka_producer_config.get("sasl_mechanism", "N/A"),
            kafka_producer_config.get("sasl_plain_username", "N/A"),
            bool(kafka_producer_config.get("sasl_plain_password")),
            "ssl_context" in kafka_producer_config,
        )

        self._producer = AIOKafkaProducer(**kafka_producer_config)
        await self._producer.start()
        self._started = True
        update_connection_status("producer", connected=True)

        log_with_context(
            logger,
            logging.INFO,
            "Kafka producer started successfully",
            bootstrap_servers=self.config.bootstrap_servers,
            acks=self.producer_config.get("acks", "all"),
            compression_type=self.producer_config.get("compression_type", "none"),
            enable_idempotence=enable_idempotence,
        )

    async def stop(self) -> None:
        # Errors during stop are logged but not re-raised to avoid masking original exceptions
        import asyncio

        if not self._started or self._producer is None:
            logger.debug("Producer not started or already stopped")
            return

        logger.info("Stopping Kafka producer")

        try:
            # During shutdown, event loop may be closed and aiokafka's stop() will fail
            try:
                loop = asyncio.get_running_loop()
                if loop.is_closed():
                    logger.warning(
                        "Event loop is closed, skipping graceful producer shutdown"
                    )
                    return
            except RuntimeError:
                logger.warning(
                    "No running event loop, skipping graceful producer shutdown"
                )
                return

            await self._producer.flush()
            await self._producer.stop()
            logger.info("Kafka producer stopped successfully")
        except Exception as e:
            log_exception(logger, e, "Error stopping Kafka producer")
        finally:
            update_connection_status("producer", connected=False)
            self._producer = None
            self._started = False

    async def send(
        self,
        topic: str,
        key: str | bytes | None,
        value: BaseModel | dict[str, Any] | bytes,
        headers: dict[str, str] | None = None,
    ) -> ProduceResult:
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        if isinstance(value, bytes):
            # Pass-through for raw bytes (e.g., retry routing)
            value_bytes = value
        elif isinstance(value, BaseModel):
            value_bytes = value.model_dump_json().encode("utf-8")
        else:
            value_bytes = json.dumps(value, default=json_serializer).encode("utf-8")

        headers_list = None
        if headers:
            headers_list = [(k, v.encode("utf-8")) for k, v in headers.items()]

        # Note: Distributed tracing has been removed or no active span

        log_with_context(
            logger,
            logging.DEBUG,
            "Sending message to Kafka",
            topic=topic,
            key=key,
            headers=headers,
            value_size=len(value_bytes),
        )

        try:
            # Handle key encoding
            if key is None:
                key_bytes = None
            elif isinstance(key, bytes):
                key_bytes = key
            else:
                key_bytes = key.encode("utf-8")

            metadata = await self._producer.send_and_wait(
                topic,
                key=key_bytes,
                value=value_bytes,
                headers=headers_list,
            )
            record_message_produced(topic, len(value_bytes), success=True)

            log_with_context(
                logger,
                logging.DEBUG,
                "Message sent successfully",
                topic=metadata.topic,
                partition=metadata.partition,
                offset=metadata.offset,
            )

            # Convert RecordMetadata to transport-agnostic ProduceResult
            return ProduceResult(
                topic=metadata.topic,
                partition=metadata.partition,
                offset=metadata.offset,
            )

        except Exception as e:
            record_message_produced(topic, len(value_bytes), success=False)
            record_producer_error(topic, type(e).__name__)
            log_exception(logger, e, "Failed to send message", topic=topic, key=key)
            raise

    async def send_batch(
        self,
        topic: str,
        messages: list[tuple[str, BaseModel]],
        headers: dict[str, str] | None = None,
    ) -> list[ProduceResult]:
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

        headers_list = None
        if headers:
            headers_list = [(k, v.encode("utf-8")) for k, v in headers.items()]

        start_time = time.perf_counter()
        futures = []
        total_bytes = 0
        for key, value in messages:
            value_bytes = value.model_dump_json().encode("utf-8")
            total_bytes += len(value_bytes)
            future = await self._producer.send(
                topic,
                key=key.encode("utf-8"),
                value=value_bytes,
                headers=headers_list,
            )
            futures.append(future)

        try:
            results = []
            for future in futures:
                metadata = await future
                # Convert RecordMetadata to transport-agnostic ProduceResult
                result = ProduceResult(
                    topic=metadata.topic,
                    partition=metadata.partition,
                    offset=metadata.offset,
                )
                results.append(result)

            duration = time.perf_counter() - start_time
            message_processing_duration_seconds.labels(topic=topic).observe(duration)
            for _ in results:
                record_message_produced(
                    topic, total_bytes // len(results), success=True
                )

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
            duration = time.perf_counter() - start_time
            message_processing_duration_seconds.labels(topic=topic).observe(duration)
            for _ in messages:
                record_message_produced(
                    topic, total_bytes // len(messages), success=False
                )
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
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")
        logger.debug("Flushing producer")
        await self._producer.flush()

    @property
    def is_started(self) -> bool:
        return self._started and self._producer is not None


__all__ = [
    "BaseKafkaProducer",
    "AIOKafkaProducer",
    "ProduceResult",
    "create_kafka_oauth_callback",
]
