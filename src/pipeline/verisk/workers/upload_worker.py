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
import contextlib
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord

from config.config import KafkaConfig
from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging.context import set_log_context
from core.logging.setup import get_logger
from core.logging.utilities import format_cycle_output, log_worker_error
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    message_processing_duration_seconds,
    record_message_consumed,
    record_processing_error,
    update_assigned_partitions,
    update_connection_status,
)
from pipeline.common.producer import BaseKafkaProducer
from pipeline.common.storage import OneLakeClient
from pipeline.common.telemetry import initialize_worker_telemetry
from pipeline.common.types import PipelineMessage, from_consumer_record
from pipeline.verisk.schemas.cached import CachedDownloadMessage
from pipeline.verisk.schemas.results import DownloadResultMessage
from pipeline.verisk.workers.consumer_factory import create_consumer
from pipeline.verisk.workers.periodic_logger import PeriodicStatsLogger
from pipeline.verisk.workers.worker_defaults import WorkerDefaults

logger = get_logger(__name__)


@dataclass
class UploadResult:
    message: PipelineMessage
    cached_message: CachedDownloadMessage
    processing_time_ms: int
    success: bool
    error: Exception | None = None


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
    CYCLE_LOG_INTERVAL_SECONDS = WorkerDefaults.CYCLE_LOG_INTERVAL_SECONDS

    def __init__(
        self,
        config: KafkaConfig,
        domain: str = "verisk",
        instance_id: str | None = None,
        storage_client: Any | None = None,
    ):
        self.config = config
        self.domain = domain
        self.instance_id = instance_id

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = (
                f"{self.WORKER_NAME}-{instance_id}"  # e.g., "upload_worker-happy-tiger"
            )
        else:
            self.worker_id = self.WORKER_NAME

        # Store injected storage client (for simulation mode)
        self._injected_storage_client = storage_client

        if storage_client is not None:
            logger.info(
                "Using injected storage client (simulation mode)",
                extra={"storage_type": type(storage_client).__name__},
            )

        # Validate OneLake configuration - need either domain paths or base path (only if not using injected client)
        if (
            storage_client is None
            and not config.onelake_domain_paths
            and not config.onelake_base_path
        ):
            raise ValueError(
                "OneLake path configuration required. Set either:\n"
                "  - onelake_domain_paths in config.yaml (preferred), or\n"
                "  - ONELAKE_XACT_PATH / ONELAKE_CLAIMX_PATH env vars, or\n"
                "  - ONELAKE_BASE_PATH env var (fallback for all domains)"
            )

        # Get worker-specific processing config
        processing_config = config.get_worker_config(
            domain, self.WORKER_NAME, "processing"
        )
        self.concurrency = processing_config.get("concurrency", WorkerDefaults.CONCURRENCY)
        self.batch_size = processing_config.get("batch_size", WorkerDefaults.BATCH_SIZE)

        # Topic to consume from
        self.topic = config.get_topic(domain, "downloads_cached")

        # Consumer will be created in start()
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

        # Concurrency control
        self._semaphore: asyncio.Semaphore | None = None
        self._in_flight_tasks: set[str] = set()  # Track by trace_id
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: asyncio.Event | None = None

        # Create producer for result messages
        self.producer = BaseKafkaProducer(
            config=config,
            domain=domain,
            worker_name=self.WORKER_NAME,
        )

        # OneLake clients by domain (lazy initialized in start())
        self.onelake_clients: dict[str, OneLakeClient] = {}

        # Health check server - use worker-specific port from config
        health_port = processing_config.get("health_port", 8091)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name=self.WORKER_NAME,
        )

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._bytes_uploaded = 0
        self._stats_logger: PeriodicStatsLogger | None = None

        # Log configured domains
        configured_domains = list(config.onelake_domain_paths.keys())
        logger.info(
            "Initialized upload worker",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "consumer_group": config.get_consumer_group(domain, self.WORKER_NAME),
                "topic": self.topic,
                "configured_domains": configured_domains,
                "fallback_path": config.onelake_base_path or "(none)",
                "upload_concurrency": self.concurrency,
                "upload_batch_size": self.batch_size,
            },
        )

    async def start(self) -> None:
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

        initialize_worker_telemetry(self.domain, "upload-worker")

        # Start health check server first
        await self.health_server.start()

        # Initialize concurrency control
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Start producer
        await self.producer.start()

        # Initialize storage clients (use injected client or create OneLake clients)
        if self._injected_storage_client is not None:
            # Use injected storage client for this domain (simulation mode)
            # The XACT worker supports multi-domain routing, so we need to set it as the domain client
            self.onelake_clients[self.domain] = self._injected_storage_client

            # If the injected client is an async context manager, enter it
            if hasattr(self._injected_storage_client, "__aenter__"):
                await self._injected_storage_client.__aenter__()

            logger.info(
                "Using injected storage client for domain",
                extra={
                    "domain": self.domain,
                    "storage_type": type(self._injected_storage_client).__name__,
                },
            )
        else:
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

        # Start periodic stats logger
        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=self.CYCLE_LOG_INTERVAL_SECONDS,
            get_stats=self._get_cycle_stats,
            stage="upload",
            worker_id=self.worker_id,
        )
        self._stats_logger.start()

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
            logger.exception("Upload worker error", extra={"error": str(e)})
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

        # Stop periodic stats logger
        if self._stats_logger:
            await self._stats_logger.stop()

        # Signal shutdown
        if self._shutdown_event:
            self._shutdown_event.set()

        # Wait for in-flight uploads (with timeout)
        if self._in_flight_tasks:
            logger.info(
                "Waiting for in-flight uploads to complete",
                extra={"in_flight_count": len(self._in_flight_tasks)},
            )
            wait_start = time.time()
            while self._in_flight_tasks and (time.time() - wait_start) < 30:
                await asyncio.sleep(0.5)

            if self._in_flight_tasks:
                logger.warning(
                    "Forcing shutdown with uploads still in progress",
                    extra={"in_flight_count": len(self._in_flight_tasks)},
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
                logger.debug("Closed OneLake client", extra={"domain": domain})
            except Exception as e:
                logger.warning(
                    "Error closing OneLake client",
                    extra={"domain": domain, "error": str(e)},
                )
        self.onelake_clients.clear()

        # Stop health check server
        await self.health_server.stop()

        # Update metrics
        update_connection_status("consumer", connected=False)
        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        update_assigned_partitions(consumer_group, 0)

        logger.info("Upload worker stopped")

    async def _create_consumer(self) -> None:
        self._consumer = create_consumer(
            config=self.config,
            domain=self.domain,
            worker_name=self.WORKER_NAME,
            topics=self.topic,
            instance_id=self.instance_id,
            max_poll_records=self.batch_size,
        )
        await self._consumer.start()

        logger.info(
            "Consumer started",
            extra={
                "topic": self.topic,
                "consumer_group": self.config.get_consumer_group(
                    self.domain, self.WORKER_NAME
                ),
            },
        )

    async def _consume_batch_loop(self) -> None:
        if self._consumer is None:
            raise RuntimeError("Consumer not initialized - call start() first")

        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)

        # Track partition assignment status for logging
        _logged_waiting_for_assignment = False
        _logged_assignment_received = False

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
                    partition_info = [f"{tp.topic}:{tp.partition}" for tp in assignment]
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
                batch: dict[str, list[ConsumerRecord]] = await self._consumer.getmany(
                    timeout_ms=1000,
                    max_records=self.batch_size,
                )

                if not batch:
                    continue

                # Flatten messages from all partitions and convert to PipelineMessage
                messages = []
                for _topic_partition, records in batch.items():
                    messages.extend([from_consumer_record(r) for r in records])

                if messages:
                    await self._process_batch(messages)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Error in consume loop", extra={"error": str(e)}, exc_info=True
                )
                record_processing_error(self.topic, consumer_group, "consume_error")
                await asyncio.sleep(1)

    async def _process_batch(self, messages: list[PipelineMessage]) -> None:
        if self._consumer is None:
            raise RuntimeError("Consumer not initialized - call start() first")
        if self._semaphore is None:
            raise RuntimeError("Semaphore not initialized - call start() first")

        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)

        logger.debug("Processing message batch", extra={"batch_size": len(messages)})

        # Process all messages concurrently
        tasks = [
            asyncio.create_task(self._process_single_with_semaphore(msg))
            for msg in messages
        ]

        results: list[UploadResult] = await asyncio.gather(
            *tasks, return_exceptions=True
        )

        # Handle any exceptions
        for upload_result in results:
            if isinstance(upload_result, Exception):
                logger.error(
                    "Unexpected error in upload",
                    extra={"error": str(upload_result)},
                    exc_info=True,
                )
                record_processing_error(self.topic, consumer_group, "unexpected_error")

        # Commit offsets after batch
        try:
            await self._consumer.commit()
        except Exception as e:
            logger.error(
                "Failed to commit offsets", extra={"error": str(e)}, exc_info=True
            )

    async def _process_single_with_semaphore(
        self, message: PipelineMessage
    ) -> UploadResult:
        if self._semaphore is None:
            raise RuntimeError("Semaphore not initialized - call start() first")

        async with self._semaphore:
            return await self._process_single_upload(message)

    async def _process_single_upload(self, message: PipelineMessage) -> UploadResult:
        start_time = time.time()
        trace_id = "unknown"
        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        cached_message: CachedDownloadMessage | None = None

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
            blob_path = await onelake_client.async_upload_file(
                relative_path=cached_message.destination_path,
                local_path=cache_path,
                overwrite=True,
            )

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
                created_at=datetime.now(UTC),
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

            log_worker_error(
                logger,
                "Upload failed",
                error_category="TRANSIENT",
                exc=e,
                trace_id=trace_id,
                media_id=cached_message.media_id if cached_message else None,
            )
            record_processing_error(self.topic, consumer_group, "upload_error")

            # For upload failures, we produce a failure result
            # The file stays in cache for manual review/retry
            try:
                # Re-parse message in case it wasn't parsed yet (e.g., JSON parsing failed)
                if cached_message is None:
                    cached_message = CachedDownloadMessage.model_validate_json(
                        message.value
                    )

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
                    created_at=datetime.now(UTC),
                )

                await self.producer.send(
                    topic=self.config.get_topic(self.domain, "downloads_results"),
                    key=cached_message.trace_id,
                    value=result_message,
                )
            except Exception as produce_error:
                logger.error(
                    "Failed to produce failure result",
                    extra={
                        "error": str(produce_error),
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

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        """Get cycle statistics for periodic logging."""
        msg = format_cycle_output(
            cycle_count=cycle_count,
            succeeded=self._records_succeeded,
            failed=self._records_failed,
        )
        extra = {
            "records_processed": self._records_processed,
            "records_succeeded": self._records_succeeded,
            "records_failed": self._records_failed,
            "bytes_uploaded": self._bytes_uploaded,
            "in_flight": len(self._in_flight_tasks),
        }
        return msg, extra

    async def _cleanup_cache_file(self, cache_path: Path) -> None:
        try:
            if cache_path.exists():
                await asyncio.to_thread(os.remove, str(cache_path))

            parent = cache_path.parent
            if parent.exists():
                with contextlib.suppress(OSError):
                    await asyncio.to_thread(parent.rmdir)

            logger.debug("Cleaned up cache file", extra={"cache_path": str(cache_path)})

        except Exception as e:
            logger.warning(
                "Failed to clean up cache file",
                extra={"cache_path": str(cache_path), "error": str(e)},
            )
