# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""Kafka producer with circuit breaker integration and Azure AD authentication."""

import json
import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple, Union


def _json_serializer(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


from aiokafka import AIOKafkaProducer
from aiokafka.structs import RecordMetadata
from pydantic import BaseModel

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging import get_logger, log_with_context, log_exception
from config.config import KafkaConfig
from kafka_pipeline.common.metrics import (
    record_message_produced,
    record_producer_error,
    update_connection_status,
    message_processing_duration_seconds,
)

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
        self._producer: Optional[AIOKafkaProducer] = None
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
            kafka_producer_config["max_request_size"] = self.producer_config["max_request_size"]
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
                kafka_producer_config["sasl_plain_username"] = self.config.sasl_plain_username
                kafka_producer_config["sasl_plain_password"] = self.config.sasl_plain_password

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
                    logger.warning("Event loop is closed, skipping graceful producer shutdown")
                    return
            except RuntimeError:
                logger.warning("No running event loop, skipping graceful producer shutdown")
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
        key: Optional[Union[str, bytes]],
        value: Union[BaseModel, Dict[str, Any], bytes],
        headers: Optional[Dict[str, str]] = None,
    ) -> RecordMetadata:
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")

        if isinstance(value, bytes):
            # Pass-through for raw bytes (e.g., retry routing)
            value_bytes = value
        elif isinstance(value, BaseModel):
            value_bytes = value.model_dump_json().encode("utf-8")
        else:
            value_bytes = json.dumps(value, default=_json_serializer).encode("utf-8")

        headers_list = None
        if headers:
            headers_list = [(k, v.encode("utf-8")) for k, v in headers.items()]

        # Inject trace context into headers (optional - graceful degradation)
        try:
            import opentracing
            from kafka_pipeline.common.telemetry import get_tracer

            tracer = get_tracer(__name__)
            if hasattr(tracer, "inject") and opentracing.tracer.active_span:
                carrier: dict = {}
                tracer.inject(
                    opentracing.tracer.active_span.context, opentracing.Format.TEXT_MAP, carrier
                )
                if not headers_list:
                    headers_list = []
                headers_list.extend([(k, v.encode("utf-8")) for k, v in carrier.items()])
        except Exception:
            pass  # Tracing not available or no active span

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

            return metadata

        except Exception as e:
            record_message_produced(topic, len(value_bytes), success=False)
            record_producer_error(topic, type(e).__name__)
            log_exception(logger, e, "Failed to send message", topic=topic, key=key)
            raise

    async def send_batch(
        self,
        topic: str,
        messages: List[Tuple[str, BaseModel]],
        headers: Optional[Dict[str, str]] = None,
    ) -> List[RecordMetadata]:
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
                results.append(metadata)

            duration = time.perf_counter() - start_time
            message_processing_duration_seconds.labels(topic=topic).observe(duration)
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
            duration = time.perf_counter() - start_time
            message_processing_duration_seconds.labels(topic=topic).observe(duration)
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
        if not self._started or self._producer is None:
            raise RuntimeError("Producer not started. Call start() first.")
        logger.debug("Flushing producer")
        await self._producer.flush()

    @property
    def is_started(self):
        return self._started and self._producer is not None


__all__ = [
    "BaseKafkaProducer",
    "AIOKafkaProducer",
    "RecordMetadata",
    "create_kafka_oauth_callback",
]
