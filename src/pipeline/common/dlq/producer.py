"""DLQ producer for routing permanently failed messages."""

import json
import logging
import time

from aiokafka import AIOKafkaProducer

from config.config import MessageConfig
from core.types import ErrorCategory
from pipeline.common.kafka_config import build_kafka_security_config
from pipeline.common.metrics import record_dlq_message
from pipeline.common.types import PipelineMessage

logger = logging.getLogger(__name__)


class DLQProducer:
    """Lazy-initialized Kafka producer for DLQ routing.

    Only connects on first send — avoids unnecessary connections when
    no permanent errors occur.
    """

    def __init__(
        self, config: MessageConfig, domain: str, worker_name: str, group_id: str, worker_id: str
    ):
        self._config = config
        self._domain = domain
        self._worker_name = worker_name
        self._group_id = group_id
        self._worker_id = worker_id
        self._producer: AIOKafkaProducer | None = None

    async def _ensure_started(self) -> None:
        if self._producer is not None:
            return

        logger.info(
            "Initializing DLQ producer for permanent error routing",
            extra={
                "domain": self._domain,
                "worker_name": self._worker_name,
            },
        )

        producer_config = {
            "bootstrap_servers": self._config.bootstrap_servers,
            "value_serializer": lambda v: v,
            "request_timeout_ms": self._config.request_timeout_ms,
            "metadata_max_age_ms": self._config.metadata_max_age_ms,
            "connections_max_idle_ms": self._config.connections_max_idle_ms,
            "acks": "all",
            "enable_idempotence": True,
            "retry_backoff_ms": 1000,
        }

        producer_config.update(build_kafka_security_config(self._config))

        self._producer = AIOKafkaProducer(**producer_config)
        await self._producer.start()

        logger.info(
            "DLQ producer started successfully",
            extra={"bootstrap_servers": self._config.bootstrap_servers},
        )

    async def send(
        self, pipeline_message: PipelineMessage, error: Exception, error_category: ErrorCategory
    ) -> None:
        """Send failed message to {topic}.dlq with full context."""
        await self._ensure_started()

        dlq_topic = f"{pipeline_message.topic}.dlq"

        dlq_message = {
            "original_topic": pipeline_message.topic,
            "original_partition": pipeline_message.partition,
            "original_offset": pipeline_message.offset,
            "original_key": (
                pipeline_message.key.decode("utf-8") if pipeline_message.key else None
            ),
            "original_value": (
                pipeline_message.value.decode("utf-8") if pipeline_message.value else None
            ),
            "original_headers": {
                k: v.decode("utf-8") if isinstance(v, bytes) else v
                for k, v in (pipeline_message.headers or [])
            },
            "original_timestamp": pipeline_message.timestamp,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_category": error_category.value,
            "consumer_group": self._group_id,
            "worker_id": self._worker_id,
            "domain": self._domain,
            "worker_name": self._worker_name,
            "dlq_timestamp": time.time(),
        }

        dlq_value = json.dumps(dlq_message).encode("utf-8")
        dlq_key = pipeline_message.key or f"dlq-{pipeline_message.offset}".encode()

        dlq_headers = [
            ("dlq_source_topic", pipeline_message.topic.encode("utf-8")),
            ("dlq_error_category", error_category.value.encode("utf-8")),
            ("dlq_consumer_group", self._group_id.encode("utf-8")),
        ]

        try:
            metadata = await self._producer.send_and_wait(
                dlq_topic,
                key=dlq_key,
                value=dlq_value,
                headers=dlq_headers,
            )

            logger.info(
                "Message sent to DLQ successfully",
                extra={
                    "dlq_topic": dlq_topic,
                    "dlq_partition": metadata.partition,
                    "dlq_offset": metadata.offset,
                    "original_topic": pipeline_message.topic,
                    "original_partition": pipeline_message.partition,
                    "original_offset": pipeline_message.offset,
                    "error_category": error_category.value,
                    "error_type": type(error).__name__,
                },
            )

            record_dlq_message(self._domain, error_category.value)

        except Exception:
            logger.error(
                "Failed to send message to DLQ - message will be retried",
                extra={
                    "dlq_topic": dlq_topic,
                    "original_topic": pipeline_message.topic,
                    "original_partition": pipeline_message.partition,
                    "original_offset": pipeline_message.offset,
                    "error_category": error_category.value,
                },
                exc_info=True,
            )

    async def stop(self) -> None:
        if self._producer is None:
            return

        try:
            await self._producer.flush()
            await self._producer.stop()
            logger.info("DLQ producer stopped successfully")
        except Exception:
            logger.error("Error stopping DLQ producer", exc_info=True)
        finally:
            self._producer = None
