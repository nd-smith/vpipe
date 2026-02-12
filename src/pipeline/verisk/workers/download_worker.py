"""
Download worker for processing download tasks with concurrent processing.

Consumes DownloadTaskMessage from pending and retry topics,
downloads attachments using AttachmentDownloader, caches to local
filesystem, and produces CachedDownloadMessage for upload worker.

Architecture:
- Downloads are cached locally before upload (decoupled from upload worker)
- Upload worker consumes from downloads.cached topic
- This allows independent scaling of download vs upload workers

Concurrent Processing (WP-313):
- Fetches batches of messages from Kafka
- Processes downloads concurrently using asyncio.Semaphore
- Uses HTTP connection pooling via shared aiohttp.ClientSession
- Configurable concurrency via DOWNLOAD_CONCURRENCY (default: 10, max: 50)
- Graceful shutdown waits for in-flight downloads to complete
"""

import asyncio
import contextlib
import logging
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp

from config.config import MessageConfig
from core.download.downloader import AttachmentDownloader
from core.download.models import DownloadOutcome, DownloadTask
from core.errors.exceptions import CircuitOpenError
from core.logging.context import set_log_context
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import format_cycle_output, log_worker_error
from core.types import ErrorCategory
from pipeline.common.decorators import set_log_context_from_message
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_assigned_partitions,
    update_connection_status,
    update_disk_usage,
)
from pipeline.common.telemetry import initialize_worker_telemetry
from pipeline.common.transport import create_batch_consumer, create_producer
from pipeline.common.types import PipelineMessage
from pipeline.verisk.retry.download_handler import RetryHandler
from pipeline.verisk.schemas.cached import CachedDownloadMessage
from pipeline.verisk.schemas.results import DownloadResultMessage
from pipeline.verisk.schemas.tasks import DownloadTaskMessage
from pipeline.verisk.workers.worker_defaults import WorkerDefaults

logger = logging.getLogger(__name__)


@dataclass
class TaskResult:
    message: PipelineMessage
    task_message: DownloadTaskMessage
    outcome: DownloadOutcome
    processing_time_ms: int
    success: bool
    error: Exception | None = None


class DownloadWorker:
    """
    Worker that processes download tasks from Kafka with concurrent processing.

    Consumes DownloadTaskMessage from:
    - downloads.pending (new tasks from event ingester)
    - downloads.retry.* (retried tasks with exponential backoff)

    Architecture:
    - Downloads files to local cache directory
    - Produces CachedDownloadMessage to downloads.cached topic
    - Upload Worker (separate) handles OneLake uploads
    - This decoupling allows independent scaling of download vs upload

    Concurrent Processing (WP-313 - FR-2.6):
    - Fetches batches of messages using Kafka's getmany()
    - Processes downloads concurrently with configurable parallelism
    - Uses semaphore to control max concurrent downloads (default: 10)
    - Shares HTTP connection pool across concurrent downloads
    - Tracks in-flight downloads for graceful shutdown

    For each task:
    1. Parse DownloadTaskMessage from Kafka
    2. Convert to DownloadTask for AttachmentDownloader
    3. Download attachment to cache location (concurrent)
    4. Produce CachedDownloadMessage to cached topic
    5. Commit offsets after batch processing

    Usage:
        config = MessageConfig.from_env()
        worker = DownloadWorker(config)
        await worker.start()  # Runs until stopped
        await worker.stop()
    """

    WORKER_NAME = "download_worker"

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = WorkerDefaults.CYCLE_LOG_INTERVAL_SECONDS

    def __init__(
        self,
        config: MessageConfig,
        domain: str = "verisk",
        temp_dir: Path | None = None,
        instance_id: str | None = None,
    ):
        self.config = config
        self.domain = domain
        self.instance_id = instance_id

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = (
                f"{self.WORKER_NAME}-{instance_id}"  # e.g., "download_worker-happy-tiger"
            )
        else:
            self.worker_id = self.WORKER_NAME

        self.temp_dir = (temp_dir or Path(tempfile.gettempdir()) / "pipeline_temp") / domain
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.cache_dir = Path(config.cache_dir) / domain
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Only consume from pending topic
        # Unified retry scheduler handles routing retry messages back to pending
        self.topics = [config.get_topic(domain, "downloads_pending")]

        self._consumer = None
        self._consumer_group: str | None = None
        self._running = False
        self._initialized = False

        # Worker-specific processing config
        processing_config = config.get_worker_config(domain, self.WORKER_NAME, "processing")
        self.concurrency = processing_config.get("concurrency", WorkerDefaults.CONCURRENCY)
        self.batch_size = processing_config.get("batch_size", WorkerDefaults.BATCH_SIZE)
        self.timeout_seconds = processing_config.get("timeout_seconds", 60)

        # Concurrency control (WP-313)
        self._semaphore: asyncio.Semaphore | None = None
        self._in_flight_tasks: set[str] = set()  # media_id tracking
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: asyncio.Event | None = None

        self._http_session: aiohttp.ClientSession | None = None

        self.cached_topic = config.get_topic(domain, "downloads_cached")
        self.results_topic = config.get_topic(domain, "downloads_results")

        self.producer = create_producer(
            config=config,
            domain=domain,
            worker_name=self.WORKER_NAME,
            topic_key="downloads_cached",
        )

        self.downloader = AttachmentDownloader()

        self.retry_handler: RetryHandler | None = None

        # Health check server
        health_port = processing_config.get("health_port", 8090)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name=self.WORKER_NAME,
        )

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._bytes_downloaded = 0
        self._stale_files_removed = 0
        self._stats_logger: PeriodicStatsLogger | None = None
        self._cleanup_task: asyncio.Task | None = None

        logger.info(
            "Initialized download worker with concurrent processing",
            extra={
                "domain": domain,
                "worker_id": self.worker_id,
                "worker_name": self.WORKER_NAME,
                "instance_id": instance_id,
                "consumer_group": config.get_consumer_group(domain, self.WORKER_NAME),
                "topics": self.topics,
                "temp_dir": str(self.temp_dir),
                "cache_dir": str(self.cache_dir),
                "download_concurrency": self.concurrency,
                "download_batch_size": self.batch_size,
            },
        )

    async def start(self) -> None:
        """
        Start the download worker with concurrent processing.
        Runs until stop() is called or error occurs.
        """
        if self._running:
            logger.warning("Worker already running, ignoring duplicate start call")
            return

        logger.info(
            "Starting download worker with concurrent processing",
            extra={
                "download_concurrency": self.concurrency,
                "download_batch_size": self.batch_size,
            },
        )

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        initialize_worker_telemetry(self.domain, "download-worker")

        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Shared HTTP session with comprehensive timeout (Issue #41)
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=10,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )

        timeout = aiohttp.ClientTimeout(
            total=300,
            connect=30,
            sock_read=60,
            sock_connect=30,
        )

        self._http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        )

        self.downloader = AttachmentDownloader(session=self._http_session)

        await self.producer.start()

        # Sync topic with producer's actual entity name (Event Hub entity may
        # differ from the Kafka topic name resolved by get_topic()).
        if hasattr(self.producer, "eventhub_name"):
            self.cached_topic = self.producer.eventhub_name

        self.retry_handler = RetryHandler(self.config)
        await self.retry_handler.start()

        # Create batch consumer from transport layer
        self._consumer = await create_batch_consumer(
            config=self.config,
            domain=self.domain,
            worker_name=self.WORKER_NAME,
            topics=self.topics,
            batch_handler=self._process_batch,
            batch_size=self.batch_size,
            batch_timeout_ms=1000,
            instance_id=self.instance_id,
            topic_key="downloads_pending",
        )
        self._consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)

        self._running = True
        self._initialized = True

        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=self.CYCLE_LOG_INTERVAL_SECONDS,
            get_stats=self._get_cycle_stats,
            stage="download",
            worker_id=self.worker_id,
        )
        self._stats_logger.start()

        self.health_server.set_ready(kafka_connected=True)

        # Update metrics
        update_connection_status("consumer", connected=True)

        self._cleanup_task = asyncio.create_task(self._periodic_stale_cleanup())

        logger.info(
            "Download worker started successfully",
            extra={
                "topics": self.topics,
            },
        )

        try:
            await self._consumer.start()
        except asyncio.CancelledError:
            logger.info("Worker cancelled, shutting down")
            raise
        except Exception as e:
            logger.error(
                "Worker terminated with error",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise
        finally:
            self._running = False

    async def request_shutdown(self) -> None:
        """
        Request graceful shutdown after current batch completes.
        Unlike stop(), this does not clean up resources immediately.
        """
        if not self._running:
            logger.debug("Worker not running, shutdown request ignored")
            return

        logger.info("Graceful shutdown requested, will stop after current batch completes")
        self._running = False

    async def stop(self) -> None:
        """
        Stop the download worker and clean up resources.
        Safe to call multiple times.
        """
        if self._consumer is None and self._http_session is None:
            logger.debug("Worker already stopped")
            return

        logger.info("Stopping download worker, waiting for in-flight downloads")
        self._running = False

        if self._stats_logger:
            await self._stats_logger.stop()

        # Cancel stale file cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task
            self._cleanup_task = None

        if self._shutdown_event:
            self._shutdown_event.set()

        await self._wait_for_in_flight(timeout=30.0)
        if self._consumer:
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

        if self._http_session:
            await self._http_session.close()
            self._http_session = None

        await self.producer.stop()

        if self.retry_handler:
            await self.retry_handler.stop()
            self.retry_handler = None

        await self.health_server.stop()
        update_connection_status("consumer", connected=False)
        update_assigned_partitions(self._consumer_group, 0)

        logger.info("Download worker stopped successfully")

    async def _wait_for_in_flight(self, timeout: float = 30.0) -> None:
        start_time = time.perf_counter()
        while True:
            async with self._in_flight_lock:
                count = len(self._in_flight_tasks)

            if count == 0:
                logger.info("All in-flight downloads completed")
                return

            elapsed = time.perf_counter() - start_time
            if elapsed >= timeout:
                logger.warning(
                    "Timeout waiting for in-flight downloads",
                    extra={
                        "remaining_tasks": count,
                        "timeout_seconds": timeout,
                    },
                )
                return

            logger.debug(
                "Waiting for in-flight downloads",
                extra={"remaining_tasks": count},
            )
            await asyncio.sleep(0.5)

    async def _process_batch(self, messages: list[PipelineMessage]) -> bool:
        """
        Process batch of download messages concurrently.
        Returns True to commit batch, False to skip commit (circuit breaker errors).
        """

        async def bounded_process(message: PipelineMessage) -> TaskResult:
            async with self._semaphore:
                return await self._process_single_task(message)

        tasks = [bounded_process(msg) for msg in messages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed_results: list[TaskResult] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                message = messages[i]
                logger.error(
                    "Unhandled exception processing message",
                    extra={
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "error": str(result),
                    },
                    exc_info=result,
                )
                processed_results.append(None)  # type: ignore
            else:
                processed_results.append(result)

        succeeded = sum(1 for r in processed_results if r and r.success)
        failed = sum(1 for r in processed_results if r and not r.success)
        errors = sum(1 for r in processed_results if r is None)

        # Check for circuit breaker errors
        circuit_errors = [
            r for r in processed_results if r and r.error and isinstance(r.error, CircuitOpenError)
        ]

        # Log stats
        logger.debug(
            "Batch processing complete",
            extra={
                "batch_size": len(messages),
                "records_succeeded": succeeded,
                "records_failed": failed,
                "records_errored": errors,
                "circuit_errors": len(circuit_errors),
            },
        )

        self._records_succeeded += succeeded
        self._records_failed += failed
        self._records_failed += errors

        # Return commit decision
        if circuit_errors:
            logger.warning(
                "Circuit breaker errors in batch - not committing offsets",
                extra={"circuit_error_count": len(circuit_errors)},
            )
            return False

        return True

    @set_log_context_from_message
    async def _process_single_task(self, message: PipelineMessage) -> TaskResult:
        start_time = time.perf_counter()

        try:
            task_message = DownloadTaskMessage.model_validate_json(message.value)
        except Exception as e:
            logger.error(
                "Failed to parse DownloadTaskMessage",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": str(e),
                },
                exc_info=True,
            )
            return TaskResult(
                message=message,
                task_message=None,  # type: ignore
                outcome=DownloadOutcome(
                    success=False,
                    error_message=f"Failed to parse message: {str(e)}",
                    error_category=ErrorCategory.PERMANENT,
                ),
                processing_time_ms=int((time.perf_counter() - start_time) * 1000),
                success=False,
                error=e,
            )

        set_log_context(trace_id=task_message.trace_id)

        self._records_processed += 1

        async with self._in_flight_lock:
            self._in_flight_tasks.add(task_message.media_id)

        try:
            logger.info(
                "Processing Xact download task",
                extra={
                    "trace_id": task_message.trace_id,
                    "media_id": task_message.media_id,
                    "assignment_id": task_message.assignment_id,
                    "attachment_url": task_message.attachment_url,
                    "destination_path": task_message.blob_path,
                    "retry_count": task_message.retry_count,
                    "topic": message.topic,
                },
            )

            download_task = self._convert_to_download_task(task_message)

            outcome = await self.downloader.download(download_task)

            processing_time_ms = int((time.perf_counter() - start_time) * 1000)

            if outcome.success:
                await self._handle_success(task_message, outcome, processing_time_ms)
                record_message_consumed(
                    message.topic,
                    self._consumer_group,
                    len(message.value),
                    success=True,
                )

                self._bytes_downloaded += outcome.bytes_downloaded or 0

                return TaskResult(
                    message=message,
                    task_message=task_message,
                    outcome=outcome,
                    processing_time_ms=processing_time_ms,
                    success=True,
                )
            else:
                await self._handle_failure(task_message, outcome, processing_time_ms)
                record_message_consumed(
                    message.topic,
                    self._consumer_group,
                    len(message.value),
                    success=False,
                )

                await self._cleanup_empty_temp_dir(download_task.destination.parent)

                is_circuit_error = outcome.error_category == ErrorCategory.CIRCUIT_OPEN
                return TaskResult(
                    message=message,
                    task_message=task_message,
                    outcome=outcome,
                    processing_time_ms=processing_time_ms,
                    success=False,
                    error=(CircuitOpenError("download_worker", 60.0) if is_circuit_error else None),
                )

        finally:
            async with self._in_flight_lock:
                self._in_flight_tasks.discard(task_message.media_id)

    def _convert_to_download_task(self, task_message: DownloadTaskMessage) -> DownloadTask:
        destination_filename = Path(task_message.blob_path).name
        temp_file = self.temp_dir / task_message.trace_id / destination_filename

        return DownloadTask(
            url=task_message.attachment_url,
            destination=temp_file,
            timeout=60,
            validate_url=True,
            validate_file_type=True,
            check_expiration=True,  # Xact S3 URLs cannot be refreshed
            allow_localhost=False,
            allowed_domains=None,
            allowed_extensions=None,
            max_size=None,
        )

    async def _handle_success(
        self,
        task_message: DownloadTaskMessage,
        outcome: DownloadOutcome,
        processing_time_ms: int,
    ) -> None:
        if outcome.file_path is None:
            raise ValueError("File path missing in successful outcome")

        logger.info(
            "Download completed successfully",
            extra={
                "trace_id": task_message.trace_id,
                "attachment_url": task_message.attachment_url,
                "bytes_downloaded": outcome.bytes_downloaded,
                "content_type": outcome.content_type,
                "processing_time_ms": processing_time_ms,
                "local_path": str(outcome.file_path),
            },
        )

        cache_subdir = self.cache_dir / task_message.trace_id
        cache_subdir.mkdir(parents=True, exist_ok=True)

        filename = Path(task_message.blob_path).name
        cache_path = cache_subdir / filename

        await asyncio.to_thread(shutil.move, str(outcome.file_path), str(cache_path))

        logger.info(
            "Cached file for upload",
            extra={
                "trace_id": task_message.trace_id,
                "cache_path": str(cache_path),
            },
        )

        try:
            if outcome.file_path.parent.exists():
                await asyncio.to_thread(outcome.file_path.parent.rmdir)
        except OSError:
            pass

        cached_message = CachedDownloadMessage(
            media_id=task_message.media_id,
            trace_id=task_message.trace_id,
            attachment_url=task_message.attachment_url,
            destination_path=task_message.blob_path,
            local_cache_path=str(cache_path),
            bytes_downloaded=outcome.bytes_downloaded or 0,
            content_type=outcome.content_type,
            event_type=task_message.event_type,
            event_subtype=task_message.event_subtype,
            status_subtype=task_message.status_subtype,
            file_type=task_message.file_type,
            assignment_id=task_message.assignment_id,
            original_timestamp=task_message.original_timestamp,
            downloaded_at=datetime.now(UTC),
            metadata=task_message.metadata,
        )

        await self.producer.send(
            value=cached_message,
            key=task_message.trace_id,
        )

        logger.info(
            "Produced cached download message",
            extra={
                "trace_id": task_message.trace_id,
                "topic": self.cached_topic,
                "cache_path": str(cache_path),
            },
        )

    async def _handle_failure(
        self,
        task_message: DownloadTaskMessage,
        outcome: DownloadOutcome,
        processing_time_ms: int,
    ) -> None:
        """
        Route failures based on error category:
        - CIRCUIT_OPEN: Don't process (retried on next poll)
        - PERMANENT: DLQ (no retry)
        - TRANSIENT/AUTH/UNKNOWN: Retry topic
        """
        if self.retry_handler is None:
            raise RuntimeError("RetryHandler not initialized - call start() first")

        error_category = outcome.error_category or ErrorCategory.UNKNOWN

        log_worker_error(
            logger,
            outcome.error_message or "Download failed",
            error_category=error_category.value,
            trace_id=task_message.trace_id,
            attachment_url=task_message.attachment_url,
            status_code=outcome.status_code,
            processing_time_ms=processing_time_ms,
            retry_count=task_message.retry_count,
        )

        record_processing_error(
            self.config.get_topic(self.domain, "downloads_pending"),
            self._consumer_group,
            error_category.value,
        )

        if error_category == ErrorCategory.CIRCUIT_OPEN:
            logger.warning(
                "Circuit breaker open - will reprocess on next poll",
                extra={
                    "trace_id": task_message.trace_id,
                    "attachment_url": task_message.attachment_url,
                },
            )
            if outcome.file_path:
                await self._cleanup_temp_file(outcome.file_path)
            return

        try:
            error_message = outcome.error_message or "Download failed"
            error = Exception(error_message)

            await self.retry_handler.handle_failure(
                task=task_message,
                error=error,
                error_category=error_category,
            )

            logger.info(
                "Routed failed task through retry handler",
                extra={
                    "trace_id": task_message.trace_id,
                    "error_category": error_category.value,
                    "retry_count": task_message.retry_count,
                },
            )

        except Exception as e:
            logger.error(
                "Failed to route task through retry handler",
                extra={
                    "trace_id": task_message.trace_id,
                    "error": str(e),
                },
                exc_info=True,
            )

        status = "failed_permanent" if error_category == ErrorCategory.PERMANENT else "failed"

        result_message = DownloadResultMessage(
            media_id=task_message.media_id,
            trace_id=task_message.trace_id,
            attachment_url=task_message.attachment_url,
            blob_path=task_message.blob_path,
            status_subtype=task_message.status_subtype,
            file_type=task_message.file_type,
            assignment_id=task_message.assignment_id,
            status=status,
            bytes_downloaded=0,
            error_message=outcome.error_message,
            retry_count=task_message.retry_count,
            created_at=datetime.now(UTC),
        )

        await self.producer.send(
            value=result_message,
            key=task_message.trace_id,
        )

        logger.info(
            "Produced failure result message",
            extra={
                "trace_id": task_message.trace_id,
                "status": status,
                "topic": self.results_topic,
            },
        )

        if outcome.file_path:
            await self._cleanup_temp_file(outcome.file_path)

    STALE_FILE_MAX_AGE_HOURS = 24

    async def _periodic_stale_cleanup(self) -> None:
        """Periodically clean up stale files in temp directory."""
        while self._running:
            try:
                await asyncio.sleep(3600)
                await self._cleanup_stale_files()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning(
                    "Error in periodic stale file cleanup",
                    extra={"error": str(e)},
                )

    async def _cleanup_stale_files(self) -> None:
        """Remove files older than STALE_FILE_MAX_AGE_HOURS, skipping in-flight tasks."""
        scan_dir = self.temp_dir
        max_age_seconds = self.STALE_FILE_MAX_AGE_HOURS * 3600

        async with self._in_flight_lock:
            in_flight = set(self._in_flight_tasks)

        def _scan_and_remove() -> int:
            count = 0
            now = time.time()
            if not scan_dir.exists():
                return 0
            for path in scan_dir.rglob("*"):
                if not path.is_file():
                    continue
                if path.parent.name in in_flight:
                    continue
                try:
                    age = now - path.stat().st_mtime
                    if age > max_age_seconds:
                        path.unlink()
                        count += 1
                        logger.warning(
                            "Removed stale file",
                            extra={
                                "file_path": str(path),
                                "age_hours": round(age / 3600, 1),
                            },
                        )
                except OSError:
                    pass
            return count

        removed = await asyncio.to_thread(_scan_and_remove)
        self._stale_files_removed += removed

    async def _cleanup_temp_file(self, file_path: Path) -> None:
        try:

            def _delete() -> None:
                if file_path.exists():
                    file_path.unlink()
                    logger.debug(
                        "Deleted temporary file",
                        extra={"file_path": str(file_path)},
                    )

                parent = file_path.parent
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
                    logger.debug(
                        "Deleted empty temporary directory",
                        extra={"directory": str(parent)},
                    )

            await asyncio.to_thread(_delete)

        except Exception as e:
            logger.warning(
                "Failed to clean up temporary file",
                extra={
                    "file_path": str(file_path),
                    "error": str(e),
                },
            )

    async def _cleanup_empty_temp_dir(self, dir_path: Path) -> None:
        try:

            def _delete_if_empty() -> None:
                if dir_path.exists() and dir_path.is_dir() and not any(dir_path.iterdir()):
                    dir_path.rmdir()
                    logger.debug(
                        "Deleted empty temporary directory after failed download",
                        extra={"directory": str(dir_path)},
                    )

            await asyncio.to_thread(_delete_if_empty)

        except Exception as e:
            logger.warning(
                "Failed to clean up empty temporary directory",
                extra={
                    "directory": str(dir_path),
                    "error": str(e),
                },
            )

    def _get_cycle_stats(self, cycle_count: int) -> tuple[str, dict[str, Any]]:
        """Get cycle statistics for periodic logging."""
        update_disk_usage(str(self.temp_dir))
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
            "bytes_downloaded": self._bytes_downloaded,
            "in_flight": len(self._in_flight_tasks),
            "stale_files_removed": self._stale_files_removed,
        }
        return msg, extra

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def in_flight_count(self) -> int:
        return len(self._in_flight_tasks)


__all__ = ["DownloadWorker"]
