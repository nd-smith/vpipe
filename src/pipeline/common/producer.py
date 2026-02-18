"""Message producer with circuit breaker integration and Azure AD authentication."""

import json
import logging
import time
from typing import Any

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel

from config.config import MessageConfig
from core.utils.json_serializers import json_serializer
from pipeline.common.kafka_config import build_kafka_security_config
from pipeline.common.metrics import (
    message_processing_duration_seconds,
    record_message_produced,
    record_producer_error,
    update_connection_status,
)
from pipeline.common.types import ProduceResult

logger = logging.getLogger(__name__)


class MessageProducer:
    """Async message producer with circuit breaker, Azure AD auth, and worker-specific config."""

    def __init__(
        self,
        config: MessageConfig,
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

        logger.info(
            "Initialized message producer",
            extra={
                "domain": domain,
                "worker_name": worker_name,
                "bootstrap_servers": config.bootstrap_servers,
                "security_protocol": config.security_protocol,
                "sasl_mechanism": config.sasl_mechanism,
                "producer_config": self.producer_config,
            },
        )

    def _resolve_acks_and_idempotence(self) -> tuple[Any, bool]:
        """Resolve acks value and idempotence setting, enforcing mutual constraints."""
        acks_value = self.producer_config.get("acks", "all")
        if isinstance(acks_value, str) and acks_value.isdigit():
            acks_value = int(acks_value)

        enable_idempotence = self.producer_config.get("enable_idempotence", True)
        if enable_idempotence and acks_value != "all":
            logger.warning(
                "Overriding acks to 'all' because enable_idempotence=True requires it",
                extra={"configured_acks": acks_value, "domain": self.domain, "worker_name": self.worker_name},
            )
            acks_value = "all"

        return acks_value, enable_idempotence

    def _apply_optional_producer_config(self, kafka_config: dict[str, Any]) -> None:
        """Apply optional producer settings from worker config to kafka config dict."""
        # Config key -> kafka key mapping (direct pass-through)
        _DIRECT_KEYS = {"linger_ms": "linger_ms"}
        for cfg_key, kafka_key in _DIRECT_KEYS.items():
            if cfg_key in self.producer_config:
                kafka_config[kafka_key] = self.producer_config[cfg_key]

        if "batch_size" in self.producer_config:
            kafka_config["max_batch_size"] = self.producer_config["batch_size"]

        if "compression_type" in self.producer_config:
            compression = self.producer_config["compression_type"]
            kafka_config["compression_type"] = None if compression == "none" else compression

        kafka_config["max_request_size"] = self.producer_config.get(
            "max_request_size", 10 * 1024 * 1024
        )

    async def start(self) -> None:
        if self._started:
            logger.warning("Producer already started, ignoring duplicate start call")
            return

        logger.info("Starting message producer")

        acks_value, enable_idempotence = self._resolve_acks_and_idempotence()

        kafka_producer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "value_serializer": lambda v: v,
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
            "acks": acks_value,
            "retry_backoff_ms": self.producer_config.get("retry_backoff_ms", 1000),
            "enable_idempotence": enable_idempotence,
        }

        self._apply_optional_producer_config(kafka_producer_config)
        kafka_producer_config.update(build_kafka_security_config(self.config))

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

        logger.info(
            "Message producer started successfully",
            extra={
                "bootstrap_servers": self.config.bootstrap_servers,
                "acks": self.producer_config.get("acks", "all"),
                "compression_type": self.producer_config.get("compression_type", "none"),
                "enable_idempotence": enable_idempotence,
            },
        )

    async def stop(self) -> None:
        # Errors during stop are logged but not re-raised to avoid masking original exceptions
        import asyncio

        if self._producer is None:
            logger.debug("Producer already stopped")
            return

        logger.info("Stopping message producer")

        try:
            # During shutdown, event loop may be closed and aiokafka's stop() will fail
            try:
                loop = asyncio.get_running_loop()
                if loop.is_closed():
                    logger.warning("Event loop is closed, skipping graceful producer shutdown")
                    return
            except RuntimeError:
                logger.warning("No running event loop, skipping graceful producer shutdown")
                return

            # Only flush if the producer was fully started (connected successfully)
            if self._started:
                await self._producer.flush()
            await self._producer.stop()
            logger.info("Message producer stopped successfully")
        except Exception as e:
            logger.error(
                "Error stopping message producer",
                extra={"error": str(e)},
                exc_info=True,
            )
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

        logger.debug(
            "Sending message",
            extra={
                "topic": topic,
                "key": key,
                "headers": headers,
                "value_size": len(value_bytes),
            },
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

            logger.debug(
                "Message sent successfully",
                extra={
                    "topic": metadata.topic,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                },
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
            logger.error(
                "Failed to send message",
                extra={"topic": topic, "key": key, "error": str(e)},
                exc_info=True,
            )
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

        logger.info(
            "Sending batch",
            extra={"topic": topic, "message_count": len(messages), "headers": headers},
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
                results.append(ProduceResult(
                    topic=metadata.topic, partition=metadata.partition, offset=metadata.offset,
                ))

            self._record_batch_metrics(topic, results, total_bytes, start_time, success=True)
            return results

        except Exception as e:
            self._record_batch_metrics(topic, messages, total_bytes, start_time, success=False, error=e)
            raise

    def _record_batch_metrics(
        self, topic: str, items: list, total_bytes: int, start_time: float,
        *, success: bool, error: Exception | None = None,
    ) -> None:
        """Record metrics and log for a batch send (success or failure)."""
        duration = time.perf_counter() - start_time
        message_processing_duration_seconds.labels(topic=topic).observe(duration)
        avg_bytes = total_bytes // len(items) if items else 0
        for _ in items:
            record_message_produced(topic, avg_bytes, success=success)

        if success:
            logger.info(
                "Batch sent successfully",
                extra={
                    "topic": topic, "message_count": len(items),
                    "partitions": list({r.partition for r in items}) if items and hasattr(items[0], "partition") else [],
                    "duration_ms": round(duration * 1000, 2),
                },
            )
        else:
            record_producer_error(topic, type(error).__name__ if error else "unknown")
            logger.error(
                "Failed to send batch",
                extra={"topic": topic, "message_count": len(items), "duration_ms": round(duration * 1000, 2), "error": str(error)},
                exc_info=True,
            )

    async def flush(self) -> None:
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")
        logger.debug("Flushing producer")
        await self._producer.flush()

    @property
    def is_started(self) -> bool:
        return self._started and self._producer is not None


__all__ = [
    "MessageProducer",
    "AIOKafkaProducer",
    "ProduceResult",
]
