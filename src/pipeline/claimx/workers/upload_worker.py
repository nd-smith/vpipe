"""ClaimX upload worker for OneLake uploads.

Uploads cached files to OneLake with concurrent processing.
Decoupled from download worker for independent scaling.
"""

import asyncio
import contextlib
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from config.config import KafkaConfig
from core.logging.utilities import format_cycle_output, log_worker_error
from pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from pipeline.claimx.schemas.results import ClaimXUploadResultMessage
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_assigned_partitions,
    update_connection_status,
)
from pipeline.common.storage import OneLakeClient
from pipeline.common.transport import create_batch_consumer, create_producer
from pipeline.common.types import PipelineMessage

logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    message: PipelineMessage
    cached_message: ClaimXCachedDownloadMessage
    processing_time_ms: int
    success: bool
    error: Exception | None = None


class ClaimXUploadWorker:
    """
    Worker that uploads ClaimX cached downloads to OneLake.

    Consumes ClaimXCachedDownloadMessage from claimx.downloads.cached topic,
    uploads files to OneLake, produces ClaimXUploadResultMessage,
    and cleans up the local cache.

    Concurrent Processing:
    - Fetches batches of messages using Kafka's getmany()
    - Processes uploads concurrently with configurable parallelism
    - Uses semaphore to control max concurrent uploads (default: 10)
    - Tracks in-flight uploads for graceful shutdown

    For each message:
    1. Parse ClaimXCachedDownloadMessage from Kafka
    2. Verify cached file exists
    3. Upload to OneLake
    4. Produce ClaimXUploadResultMessage to results topic
    5. Delete local cached file
    6. Commit offsets after batch processing

    Usage:
        config = KafkaConfig.from_env()
        worker = ClaimXUploadWorker(config)
        await worker.start()  # Runs until stopped
        await worker.stop()
    """

    WORKER_NAME = "upload_worker"

    def __init__(
        self,
        config: KafkaConfig,
        domain: str = "claimx",
        instance_id: str | None = None,
        storage_client: Any | None = None,
    ):
        self.config = config
        self.domain = domain
        self.instance_id = instance_id

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        self._injected_storage_client = storage_client

        # Validate OneLake configuration for claimx domain (only if not using injected client)
        if (
            storage_client is None
            and not config.onelake_domain_paths
            and not config.onelake_base_path
        ):
            raise ValueError(
                "OneLake path configuration required. Set either:\n"
                "  - onelake_domain_paths in config.yaml (preferred), or\n"
                "  - ONELAKE_CLAIMX_PATH env var, or\n"
                "  - ONELAKE_BASE_PATH env var (fallback for all domains)"
            )

        # Get worker-specific processing config
        processing_config = config.get_worker_config(
            domain, self.WORKER_NAME, "processing"
        )
        self.concurrency = processing_config.get("concurrency", 10)
        self.batch_size = processing_config.get("batch_size", 20)

        # Topics to consume from
        self.topics = [config.get_topic(domain, "downloads_cached")]
        self.results_topic = config.get_topic(domain, "downloads_results")

        # Consumer will be created in start()
        self._consumer = None
        self._running = False

        # Concurrency control
        self._semaphore: asyncio.Semaphore | None = None
        self._in_flight_tasks: set[str] = set()  # Track by media_id
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: asyncio.Event | None = None

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: asyncio.Task | None = None

        # Cycle-specific metrics (reset each cycle)
        self._last_cycle_processed = 0
        self._last_cycle_failed = 0

        # Create producer for result messages
        self.producer = create_producer(
            config=config,
            domain=domain,
            worker_name=self.WORKER_NAME,
            topic_key="downloads_results",
        )

        # OneLake client (lazy initialized in start())
        self.onelake_client: OneLakeClient | None = None

        # Health check server - use worker-specific port from config
        health_port = processing_config.get("health_port", 8083)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-uploader",
        )

        logger.info(
            "Initialized ClaimX upload worker",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "consumer_group": config.get_consumer_group(domain, self.WORKER_NAME),
                "topics": self.topics,
                "results_topic": self.results_topic,
                "upload_concurrency": self.concurrency,
                "upload_batch_size": self.batch_size,
            },
        )

    async def start(self) -> None:
        if self._running:
            logger.warning("Worker already running, ignoring duplicate start call")
            return

        logger.info(
            "Starting ClaimX upload worker",
            extra={
                "upload_concurrency": self.concurrency,
                "upload_batch_size": self.batch_size,
            },
        )

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "upload-worker")

        # Start health check server first
        await self.health_server.start()

        # Initialize concurrency control
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Start producer
        await self.producer.start()

        if hasattr(self.producer, "eventhub_name"):
            self.results_topic = self.producer.eventhub_name

        # Initialize storage client (use injected client or create OneLake client)
        if self._injected_storage_client is not None:
            # Use injected storage client
            self.onelake_client = self._injected_storage_client

            # If the injected client is an async context manager, enter it
            if hasattr(self.onelake_client, "__aenter__"):
                await self.onelake_client.__aenter__()

            logger.info(
                "Using injected storage client for claimx domain",
                extra={
                    "domain": self.domain,
                    "storage_type": type(self.onelake_client).__name__,
                },
            )
        else:
            # Initialize OneLake client for claimx domain with proper error handling
            onelake_path = self.config.onelake_domain_paths.get(self.domain)
            if not onelake_path:
                # Fall back to base path
                onelake_path = self.config.onelake_base_path
                if not onelake_path:
                    raise ValueError(
                        f"No OneLake path configured for domain '{self.domain}' and no fallback base path configured"
                    )
                logger.warning(
                    "Using fallback OneLake base path for claimx domain",
                    extra={"onelake_base_path": onelake_path},
                )

            # Use proper error handling with cleanup on failure
            try:
                self.onelake_client = OneLakeClient(onelake_path)
                await self.onelake_client.__aenter__()
                logger.info(
                    "Initialized OneLake client for claimx domain",
                    extra={
                        "domain": self.domain,
                        "onelake_path": onelake_path,
                    },
                )
            except Exception as e:
                logger.error(
                    "Failed to initialize OneLake client",
                    extra={"error": str(e)},
                    exc_info=True,
                )
                # Clean up producer and health server since we're failing after they started
                await self.producer.stop()
                await self.health_server.stop()
                raise

        # Create batch consumer from transport layer
        try:
            self._consumer = await create_batch_consumer(
                config=self.config,
                domain=self.domain,
                worker_name=self.WORKER_NAME,
                topics=self.topics,
                batch_handler=self._process_batch,
                batch_size=self.batch_size,
                batch_timeout_ms=1000,
                instance_id=self.instance_id,
                topic_key="downloads_cached",
            )
        except Exception as e:
            logger.error(
                "Failed to create batch consumer",
                extra={"error": str(e)},
                exc_info=True,
            )
            # Clean up OneLake client, producer, and health server on consumer creation failure
            if self.onelake_client is not None:
                try:
                    await self.onelake_client.close()
                except Exception as cleanup_error:
                    logger.warning(
                        "Error cleaning up OneLake client",
                        extra={"error": str(cleanup_error)},
                    )
                finally:
                    self.onelake_client = None
            await self.producer.stop()
            await self.health_server.stop()
            raise

        self._running = True

        # Start cycle output background task
        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        # Update health check readiness (upload worker doesn't use API)
        self.health_server.set_ready(kafka_connected=True, api_reachable=True)

        logger.info("ClaimX upload worker started successfully")

        try:
            # Transport layer handles the consume loop
            await self._consumer.start()
        except asyncio.CancelledError:
            logger.info("ClaimX upload worker cancelled")
        except Exception as e:
            logger.error(
                "ClaimX upload worker error", extra={"error": str(e)}, exc_info=True
            )
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
        if self._consumer is None and self.onelake_client is None:
            logger.debug("Worker already stopped")
            return

        logger.info("Stopping ClaimX upload worker...")
        self._running = False

        # Cancel cycle output task
        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cycle_task

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
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.error(
                    "Error stopping consumer",
                    extra={"error": str(e)},
                    exc_info=True,
                )
            finally:
                self._consumer = None

        # Stop producer
        await self.producer.stop()

        # Stop health check server
        await self.health_server.stop()

        # Close OneLake client
        if self.onelake_client is not None:
            try:
                await self.onelake_client.close()
                logger.debug("Closed OneLake client")
            except Exception as e:
                logger.warning("Error closing OneLake client", extra={"error": str(e)})
            finally:
                self.onelake_client = None

        # Update metrics
        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        update_connection_status("consumer", connected=False)
        update_assigned_partitions(consumer_group, 0)

        logger.info("ClaimX upload worker stopped")

    async def _process_batch(self, messages: list[PipelineMessage]) -> bool:
        """
        Process a batch of messages concurrently.

        CRITICAL (Issue #38): Verifies all uploads succeeded before committing offsets.
        Failed uploads are tracked and offsets are not committed for those messages.

        Returns:
            True to commit batch, False to skip commit (upload failures)
        """
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

        # CRITICAL (Issue #38): Verify all uploads succeeded before committing offsets
        failed_count = 0
        success_count = 0
        exception_count = 0

        for upload_result in results:
            if isinstance(upload_result, Exception):
                logger.error(
                    "Unexpected error in upload",
                    extra={"error": str(upload_result)},
                    exc_info=True,
                )
                record_processing_error(self.topics[0], consumer_group, "unexpected_error")
                exception_count += 1
            elif isinstance(upload_result, UploadResult):
                if upload_result.success:
                    success_count += 1
                else:
                    failed_count += 1
            else:
                logger.warning(
                    "Unexpected result type",
                    extra={"result_type": str(type(upload_result))},
                )
                exception_count += 1

        # Only commit offsets if ALL uploads in batch succeeded
        # This ensures at-least-once semantics: failed uploads will be retried
        if failed_count == 0 and exception_count == 0:
            logger.debug(
                "All uploads succeeded - committing batch",
                extra={
                    "batch_size": len(messages),
                    "success_count": success_count,
                },
            )
            return True
        else:
            logger.warning(
                "Upload failures in batch - not committing offsets",
                extra={
                    "batch_size": len(messages),
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "exception_count": exception_count,
                },
            )
            return False

    async def _process_single_with_semaphore(
        self, message: PipelineMessage
    ) -> UploadResult:
        if self._semaphore is None:
            raise RuntimeError("Semaphore not initialized - call start() first")

        async with self._semaphore:
            return await self._process_single_upload(message)

    async def _process_single_upload(self, message: PipelineMessage) -> UploadResult:
        start_time = time.time()
        media_id = "unknown"

        try:
            # Parse message
            cached_message = ClaimXCachedDownloadMessage.model_validate_json(
                message.value
            )
            media_id = cached_message.media_id

            # Track in-flight
            async with self._in_flight_lock:
                self._in_flight_tasks.add(media_id)

            consumer_group = self.config.get_consumer_group(
                self.domain, self.WORKER_NAME
            )
            record_message_consumed(
                message.topic, consumer_group, len(message.value), success=True
            )

            # Track records processed
            self._records_processed += 1

            # Verify cached file exists
            cache_path = Path(cached_message.local_cache_path)
            if not cache_path.exists():
                raise FileNotFoundError(f"Cached file not found: {cache_path}")

            # Upload to OneLake (using claimx domain-specific path)
            if self.onelake_client is None:
                raise RuntimeError(
                    "OneLake client not initialized - call start() first"
                )
            blob_path = await self.onelake_client.async_upload_file(
                relative_path=cached_message.destination_path,
                local_path=cache_path,
                overwrite=True,
            )

            # Calculate processing time
            processing_time_ms = int((time.time() - start_time) * 1000)

            logger.debug(
                "Uploaded file to OneLake",
                extra={
                    "correlation_id": cached_message.source_event_id,
                    "media_id": media_id,
                    "project_id": cached_message.project_id,
                    "domain": self.domain,
                    "destination_path": cached_message.destination_path,
                    "blob_path": blob_path,
                    "bytes_uploaded": cached_message.bytes_downloaded,
                    "processing_time_ms": processing_time_ms,
                },
            )

            self._records_succeeded += 1

            # Produce success result
            result_message = ClaimXUploadResultMessage(
                media_id=media_id,
                project_id=cached_message.project_id,
                download_url=cached_message.download_url,
                blob_path=cached_message.destination_path,
                file_type=cached_message.file_type,
                file_name=cached_message.file_name,
                source_event_id=cached_message.source_event_id,
                status="completed",
                bytes_uploaded=cached_message.bytes_downloaded,
                created_at=datetime.now(UTC),
            )

            # Use source_event_id as key for consistent partitioning across all ClaimX topics
            await self.producer.send(
                value=result_message,
                key=cached_message.source_event_id,
            )

            # Clean up cached file
            await self._cleanup_cache_file(cache_path)

            return UploadResult(
                message=message,
                cached_message=cached_message,
                processing_time_ms=processing_time_ms,
                success=True,
            )

        except Exception as e:
            processing_time_ms = int((time.time() - start_time) * 1000)

            # Build error log extra fields
            # Extract event_id and project_id if cached_message was parsed
            event_id = None
            project_id = None
            if "cached_message" in locals() and cached_message is not None:
                event_id = cached_message.source_event_id
                project_id = cached_message.project_id

            # Use standardized error logging
            log_worker_error(
                logger,
                "Upload failed",
                event_id=event_id,
                error_category="permanent",  # Upload failures are typically permanent
                exc=e,
                media_id=media_id,
                project_id=project_id,
                processing_time_ms=processing_time_ms,
            )
            consumer_group = self.config.get_consumer_group(
                self.domain, self.WORKER_NAME
            )
            record_processing_error(message.topic, consumer_group, "upload_error")
            self._records_failed += 1

            # For upload failures, we produce a failure result
            # The file stays in cache for manual review/retry
            try:
                # Re-parse message in case it wasn't parsed yet
                if "cached_message" not in locals() or cached_message is None:
                    cached_message = ClaimXCachedDownloadMessage.model_validate_json(
                        message.value
                    )

                result_message = ClaimXUploadResultMessage(
                    media_id=cached_message.media_id,
                    project_id=cached_message.project_id,
                    download_url=cached_message.download_url,
                    blob_path=cached_message.destination_path,
                    file_type=cached_message.file_type,
                    file_name=cached_message.file_name,
                    source_event_id=cached_message.source_event_id,
                    status="failed_permanent",
                    bytes_uploaded=0,
                    error_message=str(e)[:500],
                    created_at=datetime.now(UTC),
                )

                # Use source_event_id as key for consistent partitioning across all ClaimX topics
                await self.producer.send(
                    topic=self.results_topic,
                    key=cached_message.source_event_id,
                    value=result_message,
                )
            except Exception as produce_error:
                logger.error(
                    "Failed to produce failure result",
                    extra={"error": str(produce_error)},
                )

            return UploadResult(
                message=message,
                cached_message=cached_message if "cached_message" in locals() else None,
                processing_time_ms=processing_time_ms,
                success=False,
                error=e,
            )

        finally:
            # Remove from in-flight tracking
            async with self._in_flight_lock:
                self._in_flight_tasks.discard(media_id)

    async def _periodic_cycle_output(self) -> None:
        # Initial cycle output
        logger.info(
            format_cycle_output(
                cycle_count=0,
                succeeded=0,
                failed=0,
                skipped=0,
                deduplicated=0,
            ),
            extra={
                "worker_id": self.worker_id,
                "stage": "upload",
                "cycle": 0,
                "cycle_id": "cycle-0",
            },
        )
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        try:
            while True:  # Runs until cancelled
                await asyncio.sleep(1)

                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= 30:  # 30 matches standard interval
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()

                    async with self._in_flight_lock:
                        in_flight = len(self._in_flight_tasks)

                    logger.info(
                        format_cycle_output(
                            cycle_count=self._cycle_count,
                            succeeded=self._records_succeeded,
                            failed=self._records_failed,
                            skipped=self._records_skipped,
                            deduplicated=0,
                        ),
                        extra={
                            "worker_id": self.worker_id,
                            "stage": "upload",
                            "cycle": self._cycle_count,
                            "cycle_id": f"cycle-{self._cycle_count}",
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_failed": self._records_failed,
                            "records_skipped": self._records_skipped,
                            "in_flight": in_flight,
                            "cycle_interval_seconds": 30,
                        },
                    )

                    # Update last cycle counters
                    self._last_cycle_processed = self._records_processed
                    self._last_cycle_failed = self._records_failed

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise

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
