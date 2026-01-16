"""
Upload worker for processing cached downloads and uploading to OneLake.

Consumes CachedDownloadMessage from downloads.cached topic,
uploads files to OneLake, and produces DownloadResultMessage.

This worker is decoupled from the Download Worker to allow:
- Independent scaling of download vs upload workers
- Downloads not blocked by slow OneLake uploads
- Cache buffer if OneLake has temporary issues

Architecture:
- Download Worker: downloads → local cache → CachedDownloadMessage
- Upload Worker: CachedDownloadMessage → OneLake → DownloadResultMessage
"""

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging.context import set_log_context
from core.logging.setup import get_logger
from config.config import KafkaConfig
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.xact.schemas.cached import CachedDownloadMessage
from kafka_pipeline.xact.schemas.results import DownloadResultMessage
from kafka_pipeline.common.storage import OneLakeClient
from kafka_pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_connection_status,
    update_assigned_partitions,
    update_assigned_partitions,
    update_consumer_lag,
    update_consumer_offset,
    update_uploads_concurrent,
    message_processing_duration_seconds,
)

logger = get_logger(__name__)


@dataclass
class UploadResult:
    """Result of processing a single upload task."""
    message: ConsumerRecord
    cached_message: CachedDownloadMessage
    processing_time_ms: int
    success: bool
    error: Optional[Exception] = None


class UploadWorker:
    """
    Worker that uploads cached downloads to OneLake.

    Consumes CachedDownloadMessage from downloads.cached topic,
    uploads files to OneLake, produces DownloadResultMessage,
    and cleans up the local cache.

    Concurrent Processing:
    - Fetches batches of messages using Kafka's getmany()
    - Processes uploads concurrently with configurable parallelism
    - Uses semaphore to control max concurrent uploads (default: 10)
    - Tracks in-flight uploads for graceful shutdown

    For each message:
    1. Parse CachedDownloadMessage from Kafka
    2. Verify cached file exists
    3. Upload to OneLake
    4. Produce DownloadResultMessage to results topic
    5. Delete local cached file
    6. Commit offsets after batch processing

    Usage:
        config = KafkaConfig.from_env()
        worker = UploadWorker(config)
        await worker.start()  # Runs until stopped
        await worker.stop()
    """

    WORKER_NAME = "upload_worker"

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = 30

    def __init__(self, config: KafkaConfig, domain: str = "xact"):
        """
        Initialize upload worker.

        Args:
            config: Kafka configuration
            domain: Domain identifier (default: "xact")

        Raises:
            ValueError: If no OneLake path is configured (neither domain paths nor base path)
        """
        self.config = config
        self.domain = domain

        # Validate OneLake configuration - need either domain paths or base path
        if not config.onelake_domain_paths and not config.onelake_base_path:
            raise ValueError(
                "OneLake path configuration required. Set either:\n"
                "  - onelake_domain_paths in config.yaml (preferred), or\n"
                "  - ONELAKE_XACT_PATH / ONELAKE_CLAIMX_PATH env vars, or\n"
                "  - ONELAKE_BASE_PATH env var (fallback for all domains)"
            )

        # Get worker-specific processing config
        processing_config = config.get_worker_config(domain, self.WORKER_NAME, "processing")
        self.concurrency = processing_config.get("concurrency", 10)
        self.batch_size = processing_config.get("batch_size", 20)

        # Topic to consume from
        self.topic = config.get_topic(domain, "downloads_cached")

        # Consumer will be created in start()
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

        # Concurrency control
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._in_flight_tasks: Set[str] = set()  # Track by trace_id
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: Optional[asyncio.Event] = None

        # Create producer for result messages
        self.producer = BaseKafkaProducer(
            config=config,
            domain=domain,
            worker_name=self.WORKER_NAME,
        )

        # OneLake clients by domain (lazy initialized in start())
        self.onelake_clients: Dict[str, OneLakeClient] = {}

        # Health check server - use worker-specific port from config
        health_port = processing_config.get("health_port", 8091)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="xact-uploader",
        )

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._bytes_uploaded = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        # Log configured domains
        configured_domains = list(config.onelake_domain_paths.keys())
        logger.info(
            "Initialized upload worker",
            extra={
                "domain": domain,
                "worker_name": self.WORKER_NAME,
                "consumer_group": config.get_consumer_group(domain, self.WORKER_NAME),
                "topic": self.topic,
                "configured_domains": configured_domains,
                "fallback_path": config.onelake_base_path or "(none)",
                "upload_concurrency": self.concurrency,
                "upload_batch_size": self.batch_size,
            },
        )

    async def start(self) -> None:
        """
        Start the upload worker with concurrent processing.

        Begins consuming messages from cached topic.
        Processes messages in concurrent batches.
        Runs until stop() is called or error occurs.

        Raises:
            Exception: If consumer or producer fails to start
        """
        if self._running:
            logger.warning("Worker already running, ignoring duplicate start call")
            return

        logger.info(
            "Starting upload worker",
            extra={
                "upload_concurrency": self.concurrency,
                "upload_batch_size": self.batch_size,
            },
        )

        # Initialize OpenTelemetry
        from kafka_pipeline.common.telemetry import initialize_telemetry
        import os

        initialize_telemetry(
            service_name=f"{self.domain}-upload-worker",
            environment=os.getenv("ENVIRONMENT", "development"),
        )

        # Start health check server first
        await self.health_server.start()

        # Initialize concurrency control
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Start producer
        await self.producer.start()

        # Initialize OneLake clients for each configured domain
        for domain, path in self.config.onelake_domain_paths.items():
            client = OneLakeClient(path)
            await client.__aenter__()
            self.onelake_clients[domain] = client
            logger.info(
                "Initialized OneLake client for domain",
                extra={
                    "domain": domain,
                    "onelake_base_path": path,
                },
            )

        # Initialize fallback client if base_path is configured
        if self.config.onelake_base_path:
            fallback_client = OneLakeClient(self.config.onelake_base_path)
            await fallback_client.__aenter__()
            self.onelake_clients["_fallback"] = fallback_client
            logger.info(
                "Initialized fallback OneLake client",
                extra={"onelake_base_path": self.config.onelake_base_path},
            )

        # Create Kafka consumer
        await self._create_consumer()

        self._running = True

        # Update health check readiness
        self.health_server.set_ready(kafka_connected=True)

        # Update connection status
        update_connection_status("consumer", connected=True)

        logger.info("Upload worker started successfully")

        try:
            await self._consume_batch_loop()
        except asyncio.CancelledError:
            logger.info("Upload worker cancelled")
        except Exception as e:
            logger.error(f"Upload worker error: {e}", exc_info=True)
            raise
        finally:
            self._running = False

    async def request_shutdown(self) -> None:
        """
        Request graceful shutdown after current batch completes.

        Sets the running flag to False so the batch loop will exit after
        completing its current batch. This allows in-progress uploads
        to finish and offsets to be committed before stopping.

        Unlike stop(), this does not immediately clean up resources -
        it allows the batch loop to exit naturally.
        """
        if not self._running:
            logger.debug("Worker not running, shutdown request ignored")
            return

        logger.info(
            "Graceful shutdown requested, will stop after current batch completes"
        )
        self._running = False

    async def stop(self) -> None:
        """
        Stop the upload worker and clean up resources.

        Waits for in-flight uploads to complete before stopping.
        Safe to call multiple times. Will clean up resources even if
        request_shutdown() was called first.
        """
        # Check if already fully stopped (consumer is None)
        if self._consumer is None and not self.onelake_clients:
            logger.debug("Worker already stopped")
            return

        logger.info("Stopping upload worker...")
        self._running = False

        # Signal shutdown
        if self._shutdown_event:
            self._shutdown_event.set()

        # Wait for in-flight uploads (with timeout)
        if self._in_flight_tasks:
            logger.info(
                f"Waiting for {len(self._in_flight_tasks)} in-flight uploads to complete..."
            )
            wait_start = time.time()
            while self._in_flight_tasks and (time.time() - wait_start) < 30:
                await asyncio.sleep(0.5)

            if self._in_flight_tasks:
                logger.warning(
                    f"Forcing shutdown with {len(self._in_flight_tasks)} uploads still in progress"
                )

        # Stop consumer
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

        # Stop producer
        await self.producer.stop()

        # Close all OneLake clients
        for domain, client in self.onelake_clients.items():
            try:
                await client.close()
                logger.debug(f"Closed OneLake client for domain '{domain}'")
            except Exception as e:
                logger.warning(f"Error closing OneLake client for '{domain}': {e}")
        self.onelake_clients.clear()

        # Stop health check server
        await self.health_server.stop()

        # Update metrics
        update_connection_status("consumer", connected=False)
        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        update_assigned_partitions(consumer_group, 0)

        logger.info("Upload worker stopped")

    async def _create_consumer(self) -> None:
        """Create and start Kafka consumer."""
        # Get worker-specific consumer config (merged with defaults)
        consumer_config_dict = self.config.get_worker_config(self.domain, self.WORKER_NAME, "consumer")
        
        consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": self.config.get_consumer_group(self.domain, self.WORKER_NAME),
            "auto_offset_reset": consumer_config_dict.get("auto_offset_reset", "earliest"),
            "enable_auto_commit": False,
            "max_poll_records": self.batch_size,
            "session_timeout_ms": consumer_config_dict.get("session_timeout_ms", 60000),
            "max_poll_interval_ms": consumer_config_dict.get("max_poll_interval_ms", 300000),
            # Connection timeout settings
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        # Add optional consumer settings if present in worker config
        if "heartbeat_interval_ms" in consumer_config_dict:
            consumer_config["heartbeat_interval_ms"] = consumer_config_dict["heartbeat_interval_ms"]
        if "fetch_min_bytes" in consumer_config_dict:
            consumer_config["fetch_min_bytes"] = consumer_config_dict["fetch_min_bytes"]
        if "fetch_max_wait_ms" in consumer_config_dict:
            consumer_config["fetch_max_wait_ms"] = consumer_config_dict["fetch_max_wait_ms"]

        # Add security configuration
        if self.config.security_protocol != "PLAINTEXT":
            consumer_config["security_protocol"] = self.config.security_protocol
            consumer_config["sasl_mechanism"] = self.config.sasl_mechanism

            if self.config.sasl_mechanism == "OAUTHBEARER":
                consumer_config["sasl_oauth_token_provider"] = create_kafka_oauth_callback()
            elif self.config.sasl_mechanism == "PLAIN":
                consumer_config["sasl_plain_username"] = self.config.sasl_plain_username
                consumer_config["sasl_plain_password"] = self.config.sasl_plain_password

        self._consumer = AIOKafkaConsumer(self.topic, **consumer_config)
        await self._consumer.start()

        logger.info(
            "Consumer started",
            extra={
                "topic": self.topic,
                "consumer_group": self.config.get_consumer_group(self.domain, self.WORKER_NAME),
            },
        )

    async def _consume_batch_loop(self) -> None:
        """Main consumption loop with batch processing."""
        assert self._consumer is not None

        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)

        # Track partition assignment status for logging
        _logged_waiting_for_assignment = False
        _logged_assignment_received = False

        # Log initial cycle 0
        logger.info(
            "Cycle 0: processed=0 (succeeded=0, failed=0), bytes=0, in_flight=0 "
            "[cycle output every %ds]",
            self.CYCLE_LOG_INTERVAL_SECONDS,
        )
        self._last_cycle_log = time.monotonic()

        while self._running:
            try:
                # Check partition assignment before fetching
                # During consumer group rebalances, getmany() can block indefinitely
                # waiting for partition assignment (ignoring timeout_ms).
                # This check ensures we don't hang during startup with multiple consumers.
                assignment = self._consumer.assignment()
                if not assignment:
                    if not _logged_waiting_for_assignment:
                        logger.info(
                            "Waiting for partition assignment (consumer group rebalance in progress)",
                            extra={
                                "consumer_group": consumer_group,
                                "topic": self.topic,
                            },
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
                    logger.info(
                        "Partition assignment received, starting message consumption",
                        extra={
                            "consumer_group": consumer_group,
                            "partition_count": len(assignment),
                            "partitions": partition_info,
                        },
                    )
                    _logged_assignment_received = True
                    # Update partition assignment metric
                    update_assigned_partitions(consumer_group, len(assignment))

                # Fetch batch of messages
                batch: Dict[str, List[ConsumerRecord]] = await self._consumer.getmany(
                    timeout_ms=1000,
                    max_records=self.batch_size,
                )

                # Log cycle output at regular intervals
                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= self.CYCLE_LOG_INTERVAL_SECONDS:
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()
                    in_flight = len(self._in_flight_tasks)
                    logger.info(
                        f"Cycle {self._cycle_count}: processed={self._records_processed} "
                        f"(succeeded={self._records_succeeded}, failed={self._records_failed}), "
                        f"bytes={self._bytes_uploaded}, in_flight={in_flight}",
                        extra={
                            "cycle": self._cycle_count,
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_failed": self._records_failed,
                            "bytes_uploaded": self._bytes_uploaded,
                            "in_flight": in_flight,
                            "cycle_interval_seconds": self.CYCLE_LOG_INTERVAL_SECONDS,
                        },
                    )

                if not batch:
                    continue

                # Flatten messages from all partitions
                messages = []
                for topic_partition, records in batch.items():
                    messages.extend(records)

                if messages:
                    await self._process_batch(messages)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consume loop: {e}", exc_info=True)
                record_processing_error(self.topic, consumer_group, "consume_error")
                await asyncio.sleep(1)  # Brief pause before retry  # Brief pause before retry

    async def _process_batch(self, messages: List[ConsumerRecord]) -> None:
        """Process a batch of messages concurrently."""
        assert self._consumer is not None
        assert self._semaphore is not None

        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)

        logger.debug(f"Processing batch of {len(messages)} messages")

        # Update concurrent uploads metric
        update_uploads_concurrent(self.WORKER_NAME, len(messages))

        # Process all messages concurrently
        tasks = [
            asyncio.create_task(self._process_single_with_semaphore(msg))
            for msg in messages
        ]

        results: List[UploadResult] = await asyncio.gather(*tasks, return_exceptions=True)

        # Update concurrent uploads metric - all complete
        update_uploads_concurrent(self.WORKER_NAME, 0)

        # Handle any exceptions
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Unexpected error in upload: {result}", exc_info=True)
                record_processing_error(self.topic, consumer_group, "unexpected_error")

        # Commit offsets after batch
        try:
            await self._consumer.commit()
        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}", exc_info=True)

    async def _process_single_with_semaphore(self, message: ConsumerRecord) -> UploadResult:
        """Process single message with semaphore for concurrency control."""
        assert self._semaphore is not None

        async with self._semaphore:
            return await self._process_single_upload(message)

    async def _process_single_upload(self, message: ConsumerRecord) -> UploadResult:
        """Process a single cached download message."""
        start_time = time.time()
        trace_id = "unknown"
        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        cached_message: Optional[CachedDownloadMessage] = None

        try:
            # Parse message
            cached_message = CachedDownloadMessage.model_validate_json(message.value)
            trace_id = cached_message.trace_id

            # Set logging context for correlation
            set_log_context(trace_id=trace_id)

            # Track messages received for cycle output
            self._records_processed += 1

            # Track in-flight
            async with self._in_flight_lock:
                self._in_flight_tasks.add(trace_id)

            record_message_consumed(
                self.topic, consumer_group, len(message.value), success=True
            )

            # Verify cached file exists
            cache_path = Path(cached_message.local_cache_path)
            if not cache_path.exists():
                raise FileNotFoundError(f"Cached file not found: {cache_path}")

            # Get domain from event_type to route to correct OneLake path
            # event_type should be the domain (e.g., "xact", "claimx") set by EventIngester
            raw_event_type = cached_message.event_type
            domain = raw_event_type.lower()

            # Log domain lookup for debugging
            available_domains = list(self.onelake_clients.keys())
            logger.debug(
                "Domain lookup for upload",
                extra={
                    "trace_id": trace_id,
                    "media_id": cached_message.media_id,
                    "raw_event_type": raw_event_type,
                    "domain": domain,
                    "available_domains": available_domains,
                },
            )

            # Get appropriate OneLake client for this domain
            onelake_client = self.onelake_clients.get(domain)
            if onelake_client is None:
                # Fall back to default client
                onelake_client = self.onelake_clients.get("_fallback")
                if onelake_client is None:
                    raise ValueError(
                        f"No OneLake client configured for domain '{domain}' "
                        f"(event_type='{raw_event_type}') and no fallback configured. "
                        f"Available domains: {available_domains}"
                    )
                logger.warning(
                    "Using fallback OneLake client - domain not found",
                    extra={
                        "trace_id": trace_id,
                        "media_id": cached_message.media_id,
                        "raw_event_type": raw_event_type,
                        "domain": domain,
                        "available_domains": available_domains,
                    },
                )

            # Upload to OneLake (domain-specific path)
            from opentelemetry import trace
            from opentelemetry.trace import SpanKind

            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span(
                "onelake.upload",
                kind=SpanKind.CLIENT,
                attributes={
                    "trace_id": trace_id,
                    "media_id": cached_message.media_id,
                    "domain": domain,
                    "destination_path": cached_message.destination_path,
                    "bytes": cached_message.bytes_downloaded,
                },
            ) as span:
                blob_path = await onelake_client.upload_file(
                    relative_path=cached_message.destination_path,
                    local_path=cache_path,
                    overwrite=True,
                )
                span.set_attribute("upload.success", True)
                span.set_attribute("blob_path", blob_path)

            logger.info(
                "Uploaded file to OneLake",
                extra={
                    "trace_id": trace_id,
                    "media_id": cached_message.media_id,
                    "domain": domain,
                    "raw_event_type": raw_event_type,
                    "destination_path": cached_message.destination_path,
                    "onelake_base_path": onelake_client.base_path,
                    "blob_path": blob_path,
                    "bytes": cached_message.bytes_downloaded,
                },
            )

            # Calculate processing time
            processing_time_ms = int((time.time() - start_time) * 1000)

            # Produce success result
            result_message = DownloadResultMessage(
                media_id=cached_message.media_id,
                trace_id=trace_id,
                attachment_url=cached_message.attachment_url,
                blob_path=cached_message.destination_path,
                status_subtype=cached_message.status_subtype,
                file_type=cached_message.file_type,
                assignment_id=cached_message.assignment_id,
                status="completed",
                bytes_downloaded=cached_message.bytes_downloaded,
                created_at=datetime.now(timezone.utc),
            )

            await self.producer.send(
                topic=self.config.get_topic(self.domain, "downloads_results"),
                key=trace_id,
                value=result_message,
            )

            # Clean up cached file
            await self._cleanup_cache_file(cache_path)

            # Track successful upload for cycle output
            self._records_succeeded += 1
            self._bytes_uploaded += cached_message.bytes_downloaded

            # Record processing duration metric
            duration = time.time() - start_time
            message_processing_duration_seconds.labels(
                topic=self.topic, consumer_group=consumer_group
            ).observe(duration)

            return UploadResult(
                message=message,
                cached_message=cached_message,
                processing_time_ms=processing_time_ms,
                success=True,
            )

        except Exception as e:
            processing_time_ms = int((time.time() - start_time) * 1000)

            # Track failed upload for cycle output
            self._records_failed += 1

            logger.error(
                f"Upload failed: {e}",
                extra={
                    "trace_id": trace_id,
                    "media_id": cached_message.media_id if cached_message else None,
                },
                exc_info=True,
            )
            record_processing_error(self.topic, consumer_group, "upload_error")

            # For upload failures, we produce a failure result
            # The file stays in cache for manual review/retry
            try:
                # Re-parse message in case it wasn't parsed yet (e.g., JSON parsing failed)
                if cached_message is None:
                    cached_message = CachedDownloadMessage.model_validate_json(message.value)

                result_message = DownloadResultMessage(
                    media_id=cached_message.media_id,
                    trace_id=cached_message.trace_id,
                    attachment_url=cached_message.attachment_url,
                    blob_path=cached_message.destination_path,
                    status_subtype=cached_message.status_subtype,
                    file_type=cached_message.file_type,
                    assignment_id=cached_message.assignment_id,
                    status="failed_permanent",
                    bytes_downloaded=0,
                    error_message=str(e)[:500],
                    created_at=datetime.now(timezone.utc),
                )

                await self.producer.send(
                    topic=self.config.get_topic(self.domain, "downloads_results"),
                    key=cached_message.trace_id,
                    value=result_message,
                )
            except Exception as produce_error:
                logger.error(
                    f"Failed to produce failure result: {produce_error}",
                    extra={
                        "trace_id": trace_id,
                        "media_id": cached_message.media_id if cached_message else None,
                    },
                )

            return UploadResult(
                message=message,
                cached_message=cached_message,
                processing_time_ms=processing_time_ms,
                success=False,
                error=e,
            )

        finally:
            # Remove from in-flight tracking
            async with self._in_flight_lock:
                self._in_flight_tasks.discard(trace_id)

    async def _cleanup_cache_file(self, cache_path: Path) -> None:
        """Clean up cached file and its parent directory if empty."""
        try:
            # Delete the file
            if cache_path.exists():
                await asyncio.to_thread(os.remove, str(cache_path))

            # Try to remove parent directory if empty
            parent = cache_path.parent
            if parent.exists():
                try:
                    await asyncio.to_thread(parent.rmdir)
                except OSError:
                    pass  # Directory not empty

            logger.debug(f"Cleaned up cache file: {cache_path}")

        except Exception as e:
            logger.warning(f"Failed to clean up cache file {cache_path}: {e}")
