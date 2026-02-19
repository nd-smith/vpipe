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

from config.config import MessageConfig
from core.logging.context import set_log_context
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import format_cycle_output, log_worker_error
from pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from pipeline.claimx.schemas.results import ClaimXUploadResultMessage
from pipeline.common.worker_defaults import CYCLE_LOG_INTERVAL_SECONDS
from pipeline.common.decorators import set_log_context_from_message
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    message_processing_duration_seconds,
    record_message_consumed,
    record_processing_error,
    update_assigned_partitions,
    update_connection_status,
    update_disk_usage,
)
from pipeline.common.stale_file_cleaner import StaleFileCleaner
from pipeline.common.storage import OneLakeClient
from pipeline.common.telemetry import initialize_worker_telemetry
from pipeline.common.consumer_config import ConsumerConfig
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
        config = MessageConfig.from_env()
        worker = ClaimXUploadWorker(config)
        await worker.start()  # Runs until stopped
        await worker.stop()
    """

    WORKER_NAME = "upload_worker"

    def __init__(
        self,
        config: MessageConfig,
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
                "  - ONELAKE_CLAIMX_PATH env var"
            )

        self.concurrency = 10
        self.batch_size = 100

        # Topics to consume from
        self.topics = [config.get_topic(domain, "downloads_cached")]
        self.results_topic = config.get_topic(domain, "downloads_results")

        self.cache_dir = Path(config.cache_dir) / domain
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Consumer will be created in start()
        self._consumer = None
        self._consumer_group: str | None = None
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
        self._bytes_uploaded = 0
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None

        self._stats_logger: PeriodicStatsLogger | None = None
        self._stale_cleaner = StaleFileCleaner(
            scan_dir=self.cache_dir,
            in_flight_lock=self._in_flight_lock,
            in_flight_tasks=self._in_flight_tasks,
        )

        # Create producer for result messages
        self.producer = create_producer(
            config=config,
            domain=domain,
            worker_name=self.WORKER_NAME,
            topic_key="downloads_results",
        )

        # OneLake client (lazy initialized in start())
        self.onelake_client: OneLakeClient | None = None

        # Health check server
        health_port = 8083
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

    async def _init_storage_client(self) -> None:
        """Initialize the OneLake storage client (injected or created from config)."""
        if self._injected_storage_client is not None:
            self.onelake_client = self._injected_storage_client
            if hasattr(self.onelake_client, "__aenter__"):
                await self.onelake_client.__aenter__()
            logger.info(
                "Using injected storage client for claimx domain",
                extra={"domain": self.domain, "storage_type": type(self.onelake_client).__name__},
            )
            return

        onelake_path = self.config.onelake_domain_paths.get(self.domain)
        if not onelake_path:
            onelake_path = self.config.onelake_base_path
            if not onelake_path:
                raise ValueError(
                    f"No OneLake path configured for domain '{self.domain}' and no fallback base path configured"
                )
            logger.warning(
                "Using fallback OneLake base path for claimx domain",
                extra={"onelake_base_path": onelake_path},
            )

        try:
            self.onelake_client = OneLakeClient(onelake_path, max_pool_size=self.concurrency)
            await self.onelake_client.__aenter__()
            logger.info(
                "Initialized OneLake client for claimx domain",
                extra={"domain": self.domain, "onelake_path": onelake_path},
            )
        except Exception as e:
            logger.error("Failed to initialize OneLake client", extra={"error": str(e)}, exc_info=True)
            await self.producer.stop()
            await self.health_server.stop()
            raise

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

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        initialize_worker_telemetry(self.domain, "upload-worker")

        # Initialize concurrency control
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Start producer
        await self.producer.start()

        if hasattr(self.producer, "eventhub_name"):
            self.results_topic = self.producer.eventhub_name

        # Initialize storage client (use injected client or create OneLake client)
        await self._init_storage_client()

        # Size the default thread pool to match upload concurrency so
        # asyncio.to_thread calls (OneLake uploads, file I/O) aren't bottlenecked
        # by the default min(32, cpu+4) executor.
        import concurrent.futures

        loop = asyncio.get_running_loop()
        loop.set_default_executor(
            concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency + 4)
        )

        # Create batch consumer from transport layer
        try:
            self._consumer = await create_batch_consumer(
                config=self.config,
                domain=self.domain,
                worker_name=self.WORKER_NAME,
                topics=self.topics,
                batch_handler=self._process_batch,
                topic_key="downloads_cached",
                consumer_config=ConsumerConfig(
                    batch_size=self.batch_size,
                    batch_timeout_ms=1000,
                    instance_id=self.instance_id,
                ),
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
                    await asyncio.to_thread(self.onelake_client.close)
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

        self._consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        self._running = True

        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=CYCLE_LOG_INTERVAL_SECONDS,
            get_stats=self._get_cycle_stats,
            stage="upload",
            worker_id=self.worker_id,
        )
        self._stats_logger.start()

        # Update health check readiness (upload worker doesn't use API)
        self.health_server.set_ready(transport_connected=True, api_reachable=True)

        await self._stale_cleaner.start()

        logger.info("ClaimX upload worker started successfully")

        try:
            # Transport layer handles the consume loop
            await self._consumer.start()
        except asyncio.CancelledError:
            logger.info("ClaimX upload worker cancelled")
        except Exception as e:
            logger.error("ClaimX upload worker error", extra={"error": str(e)}, exc_info=True)
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

        logger.info("Graceful shutdown requested, will stop after current batch completes")
        self._running = False

    async def _close_resource(self, name: str, method: str = "stop", *, clear: bool = False) -> None:
        """Close a resource by attribute name, logging errors. Optionally set to None."""
        resource = getattr(self, name, None)
        if resource is None:
            return
        try:
            await getattr(resource, method)()
        except asyncio.CancelledError:
            logger.warning(f"Cancelled while stopping {name}")
        except Exception as e:
            logger.error(f"Error stopping {name}", extra={"error": str(e)})
        finally:
            if clear:
                setattr(self, name, None)

    async def _close_onelake_client(self) -> None:
        """Close the OneLake client (sync close via thread)."""
        if self.onelake_client is None:
            return
        try:
            await asyncio.to_thread(self.onelake_client.close)
            logger.debug("Closed OneLake client")
        except Exception as e:
            logger.warning("Error closing OneLake client", extra={"error": str(e)})
        finally:
            self.onelake_client = None

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

        await self._close_resource("_stats_logger")
        await self._close_resource("_stale_cleaner")

        # Signal shutdown
        if self._shutdown_event:
            self._shutdown_event.set()

        try:
            await self._wait_for_in_flight(timeout=30.0)
        except asyncio.CancelledError:
            logger.warning("Interrupted while waiting for in-flight uploads")

        await self._close_resource("_consumer", clear=True)
        await self._close_resource("producer")
        await self._close_resource("health_server")
        await self._close_onelake_client()

        # Update metrics
        update_connection_status("consumer", connected=False)
        update_assigned_partitions(self._consumer_group, 0)

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

        logger.debug("Processing message batch", extra={"batch_size": len(messages)})

        # Process all messages concurrently
        tasks = [asyncio.create_task(self._process_single_with_semaphore(msg)) for msg in messages]

        results: list[UploadResult] = await asyncio.gather(*tasks, return_exceptions=True)

        # CRITICAL (Issue #38): Verify all uploads succeeded before committing offsets
        success_count, failed_count, exception_count = self._tally_upload_results(results)

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

    def _tally_upload_results(
        self, results: list[UploadResult],
    ) -> tuple[int, int, int]:
        """Classify upload results into success, failed, and exception counts."""
        success_count = 0
        failed_count = 0
        exception_count = 0

        for upload_result in results:
            if isinstance(upload_result, Exception):
                logger.error(
                    "Unexpected error in upload",
                    extra={"error": str(upload_result)},
                    exc_info=True,
                )
                record_processing_error(self.topics[0], self._consumer_group, "unexpected_error")
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

        return success_count, failed_count, exception_count

    async def _process_single_with_semaphore(self, message: PipelineMessage) -> UploadResult:
        if self._semaphore is None:
            raise RuntimeError("Semaphore not initialized - call start() first")

        async with self._semaphore:
            return await self._process_single_upload(message)

    def _update_cycle_offsets(self, ts: int | None) -> None:
        """Update cycle offset tracking with a message timestamp."""
        if ts is None:
            return
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

    async def _produce_failure_result(
        self, message: PipelineMessage, cached_message: ClaimXCachedDownloadMessage | None, error: Exception,
    ) -> None:
        """Produce a failure result message for a failed upload."""
        try:
            if cached_message is None:
                cached_message = ClaimXCachedDownloadMessage.model_validate_json(message.value)

            result_message = ClaimXUploadResultMessage(
                media_id=cached_message.media_id,
                project_id=cached_message.project_id,
                download_url=cached_message.download_url,
                blob_path=cached_message.destination_path,
                file_type=cached_message.file_type,
                file_name=cached_message.file_name,
                trace_id=cached_message.trace_id,
                status="failed_permanent",
                bytes_uploaded=0,
                error_message=str(error)[:500],
                created_at=datetime.now(UTC),
            )
            await self.producer.send(key=cached_message.trace_id, value=result_message)
        except Exception as produce_error:
            logger.error("Failed to produce failure result", extra={"error": str(produce_error)})

    @set_log_context_from_message
    async def _process_single_upload(self, message: PipelineMessage) -> UploadResult:
        start_time = time.perf_counter()
        media_id = "unknown"
        cached_message: ClaimXCachedDownloadMessage | None = None

        try:
            # Parse message
            cached_message = ClaimXCachedDownloadMessage.model_validate_json(message.value)
            media_id = cached_message.media_id

            # Set logging context for correlation
            set_log_context(trace_id=cached_message.trace_id, media_id=cached_message.media_id)

            # Track in-flight
            async with self._in_flight_lock:
                self._in_flight_tasks.add(media_id)

            record_message_consumed(
                message.topic, self._consumer_group, len(message.value), success=True
            )

            self._records_processed += 1
            self._update_cycle_offsets(message.timestamp)

            # Verify cached file exists
            cache_path = Path(cached_message.local_cache_path)
            if not cache_path.exists():
                raise FileNotFoundError(f"Cached file not found: {cache_path}")

            # Upload to OneLake (using claimx domain-specific path)
            if self.onelake_client is None:
                raise RuntimeError("OneLake client not initialized - call start() first")
            blob_path = await self.onelake_client.async_upload_file(
                relative_path=cached_message.destination_path,
                local_path=cache_path,
                overwrite=True,
            )

            processing_time_ms = int((time.perf_counter() - start_time) * 1000)

            logger.info(
                "Uploaded file to OneLake",
                extra={
                    "trace_id": cached_message.trace_id,
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
            self._bytes_uploaded += cached_message.bytes_downloaded

            # Produce success result
            result_message = ClaimXUploadResultMessage(
                media_id=media_id,
                project_id=cached_message.project_id,
                download_url=cached_message.download_url,
                blob_path=cached_message.destination_path,
                file_type=cached_message.file_type,
                file_name=cached_message.file_name,
                trace_id=cached_message.trace_id,
                status="completed",
                bytes_uploaded=cached_message.bytes_downloaded,
                created_at=datetime.now(UTC),
            )
            await self.producer.send(value=result_message, key=cached_message.trace_id)

            await self._cleanup_cache_file(cache_path)

            duration = time.perf_counter() - start_time
            message_processing_duration_seconds.labels(
                topic=self.topics[0], consumer_group=self._consumer_group
            ).observe(duration)

            return UploadResult(
                message=message,
                cached_message=cached_message,
                processing_time_ms=processing_time_ms,
                success=True,
            )

        except Exception as e:
            processing_time_ms = int((time.perf_counter() - start_time) * 1000)

            log_worker_error(
                logger,
                "Upload failed",
                error_category="permanent",
                trace_id=cached_message.trace_id if cached_message else None,
                exc=e,
                media_id=media_id,
                project_id=cached_message.project_id if cached_message else None,
                processing_time_ms=processing_time_ms,
            )
            record_processing_error(message.topic, self._consumer_group, "upload_error")
            self._records_failed += 1

            await self._produce_failure_result(message, cached_message, e)

            return UploadResult(
                message=message,
                cached_message=cached_message,
                processing_time_ms=processing_time_ms,
                success=False,
                error=e,
            )

        finally:
            async with self._in_flight_lock:
                self._in_flight_tasks.discard(media_id)

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        """Get cycle statistics for periodic logging."""
        update_disk_usage(str(self.cache_dir))

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
            "stale_files_removed": self._stale_cleaner.total_removed,
            "cycle_offset_start_ts": self._cycle_offset_start_ts,
            "cycle_offset_end_ts": self._cycle_offset_end_ts,
        }
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        return msg, extra

    async def _wait_for_in_flight(self, timeout: float = 30.0) -> None:
        start_time = time.perf_counter()
        while True:
            async with self._in_flight_lock:
                count = len(self._in_flight_tasks)

            if count == 0:
                logger.info("All in-flight uploads completed")
                return

            elapsed = time.perf_counter() - start_time
            if elapsed >= timeout:
                logger.warning(
                    "Timeout waiting for in-flight uploads",
                    extra={
                        "remaining_tasks": count,
                        "timeout_seconds": timeout,
                    },
                )
                return

            logger.debug(
                "Waiting for in-flight uploads",
                extra={"remaining_tasks": count},
            )
            await asyncio.sleep(0.5)

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
