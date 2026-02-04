"""ClaimX download worker with concurrent processing.

Downloads media files using presigned S3 URLs and caches locally for upload worker.
Decoupled architecture allows independent scaling of download vs upload.
"""

import asyncio
import contextlib
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
from config.config import KafkaConfig
from core.download.downloader import AttachmentDownloader
from core.download.models import DownloadOutcome, DownloadTask
from core.errors.exceptions import CircuitOpenError
from core.logging.context import set_log_context
from core.logging.setup import get_logger
from core.logging.utilities import format_cycle_output, log_worker_error
from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiClient
from pipeline.claimx.retry import DownloadRetryHandler
from pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from pipeline.claimx.schemas.tasks import ClaimXDownloadTask
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_assigned_partitions,
    update_connection_status,
)
from pipeline.common.transport import create_batch_consumer, create_producer
from pipeline.common.types import PipelineMessage

logger = get_logger(__name__)


@dataclass
class TaskResult:
    message: PipelineMessage
    task_message: ClaimXDownloadTask
    outcome: DownloadOutcome
    processing_time_ms: int
    success: bool
    error: Exception | None = None


class ClaimXDownloadWorker:
    """
    Worker that processes ClaimX download tasks from Kafka with concurrent processing.

    Consumes ClaimXDownloadTask from:
    - claimx.downloads.pending (new tasks from enrichment worker)
    - claimx.downloads.retry.* (retried tasks with exponential backoff)

    Architecture:
    - Downloads files to local cache directory
    - Produces ClaimXCachedDownloadMessage to claimx.downloads.cached topic
    - Upload Worker (separate) handles OneLake uploads
    - This decoupling allows independent scaling of download vs upload

    Concurrent Processing:
    - Fetches batches of messages using Kafka's getmany()
    - Processes downloads concurrently with configurable parallelism
    - Uses semaphore to control max concurrent downloads (default: 10)
    - Shares HTTP connection pool across concurrent downloads
    - Tracks in-flight downloads for graceful shutdown

    For each task:
    1. Parse ClaimXDownloadTask from Kafka
    2. Convert to DownloadTask for AttachmentDownloader
    3. Download media file to cache location (concurrent)
    4. Produce ClaimXCachedDownloadMessage to cached topic
    5. Commit offsets after batch processing

    Usage:
        config = KafkaConfig.from_env()
        worker = ClaimXDownloadWorker(config)
        await worker.start()  # Runs until stopped
        await worker.stop()
    """

    WORKER_NAME = "download_worker"

    def __init__(
        self,
        config: KafkaConfig,
        domain: str = "claimx",
        temp_dir: Path | None = None,
        instance_id: str | None = None,
    ):
        self.config = config
        self.domain = domain
        self.instance_id = instance_id

        # Create worker_id with instance suffix (ordinal) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        self.temp_dir = (
            temp_dir or Path(tempfile.gettempdir()) / "claimx_download_worker"
        )
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.cache_dir = Path(config.cache_dir) / domain
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        processing_config = config.get_worker_config(
            domain, self.WORKER_NAME, "processing"
        )
        self.concurrency = processing_config.get("concurrency", 10)
        self.batch_size = processing_config.get("batch_size", 20)

        # Only consume from pending topic
        # Unified retry scheduler handles routing retry messages back to pending
        self.topics = [config.get_topic(domain, "downloads_pending")]

        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

        self._semaphore: asyncio.Semaphore | None = None
        self._in_flight_tasks: set[str] = set()
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: asyncio.Event | None = None

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

        self._http_session: aiohttp.ClientSession | None = None

        self.cached_topic = config.get_topic(domain, "downloads_cached")

        self.producer = create_producer(
            config=config,
            domain=domain,
            worker_name=self.WORKER_NAME,
            topic_key="downloads_cached",
        )

        self.downloader = AttachmentDownloader()
        self.api_client: ClaimXApiClient | None = None
        self.retry_handler: DownloadRetryHandler | None = None

        health_port = processing_config.get("health_port", 8082)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="claimx-downloader",
        )

        logger.info(
            "Initialized ClaimX download worker with concurrent processing",
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
        if self._running:
            logger.warning("Worker already running, ignoring duplicate start call")
            return

        logger.info(
            "Starting ClaimX download worker with concurrent processing",
            extra={
                "download_concurrency": self.concurrency,
                "download_batch_size": self.batch_size,
            },
        )

        from pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "download-worker")

        await self.health_server.start()

        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        connector = aiohttp.TCPConnector(
            limit=self.concurrency,
            limit_per_host=self.concurrency,
        )
        self._http_session = aiohttp.ClientSession(connector=connector)

        self.downloader = AttachmentDownloader(session=self._http_session)

        await self.producer.start()

        if hasattr(self.producer, "eventhub_name"):
            self.cached_topic = self.producer.eventhub_name

        self.api_client = ClaimXApiClient(
            base_url=self.config.claimx_api_url
            or "https://api.test.claimxperience.com",
            token=self.config.claimx_api_token,
            timeout_seconds=self.config.claimx_api_timeout_seconds,
            max_concurrent=self.config.claimx_api_concurrency,
        )

        self.retry_handler = DownloadRetryHandler(
            config=self.config,
            api_client=self.api_client,
        )
        await self.retry_handler.start()

        # Create batch consumer from transport layer
        # The consumer will call _process_batch() for each batch of messages
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

        self._running = True

        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        api_reachable = not self.api_client.is_circuit_open
        self.health_server.set_ready(
            kafka_connected=True,
            api_reachable=api_reachable,
            circuit_open=self.api_client.is_circuit_open,
        )

        logger.info(
            "ClaimX download worker started successfully",
            extra={
                "topics": self.topics,
                "batch_size": self.batch_size,
                "concurrency": self.concurrency,
            },
        )

        try:
            # Transport layer handles the consume loop
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
        """Request graceful shutdown after current batch completes."""
        if not self._running:
            logger.debug("Worker not running, shutdown request ignored")
            return

        logger.info(
            "Graceful shutdown requested, will stop after current batch completes"
        )
        self._running = False

    async def stop(self) -> None:
        """Gracefully stop worker, wait for in-flight downloads, commit offsets, clean up resources."""
        if self._consumer is None and self._http_session is None:
            logger.debug("Worker already stopped")
            return

        logger.info("Stopping ClaimX download worker, waiting for in-flight downloads")
        self._running = False

        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cycle_task

        if self._shutdown_event:
            self._shutdown_event.set()

        await self._wait_for_in_flight(timeout=30.0)

        if self._consumer:
            try:
                # Batch consumer's stop() method flushes remaining batches
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

        if self.api_client:
            await self.api_client.close()
            self.api_client = None

        if self.retry_handler:
            await self.retry_handler.stop()
            self.retry_handler = None

        await self.health_server.stop()

        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        update_connection_status("consumer", connected=False)
        update_assigned_partitions(consumer_group, 0)

        logger.info("ClaimX download worker stopped successfully")

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
        """Process batch of download messages concurrently.

        Returns:
            True to commit batch, False to skip commit (circuit breaker errors)
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

        # Check for circuit breaker errors (transient errors that need immediate retry)
        circuit_errors = [
            r for r in processed_results
            if r and r.error and isinstance(r.error, CircuitOpenError)
        ]

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
        self._records_failed += errors  # Count errors as failed too

        # Return commit decision
        if circuit_errors:
            logger.warning(
                "Circuit breaker errors in batch - not committing offsets",
                extra={"circuit_error_count": len(circuit_errors)},
            )
            return False  # Don't commit - batch will be reprocessed

        return True  # Commit batch

    async def _process_single_task(self, message: PipelineMessage) -> TaskResult:
        start_time = time.perf_counter()

        try:
            task_message = ClaimXDownloadTask.model_validate_json(message.value)
        except Exception as e:
            logger.error(
                "Failed to parse ClaimXDownloadTask",
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

        self._records_processed += 1

        async with self._in_flight_lock:
            self._in_flight_tasks.add(task_message.media_id)

        set_log_context(trace_id=task_message.source_event_id)

        try:
            logger.debug(
                "Processing ClaimX download task",
                extra={
                    "event_id": task_message.source_event_id,
                    "media_id": task_message.media_id,
                    "project_id": task_message.project_id,
                    "download_url": task_message.download_url,
                    "destination_path": task_message.blob_path,
                    "retry_count": task_message.retry_count,
                    "topic": message.topic,
                },
            )

            download_task = self._convert_to_download_task(task_message)

            outcome = await self.downloader.download(download_task)

            processing_time_ms = int((time.perf_counter() - start_time) * 1000)

            consumer_group = self.config.get_consumer_group(
                self.domain, self.WORKER_NAME
            )

            if outcome.success:
                await self._handle_success(task_message, outcome, processing_time_ms)
                record_message_consumed(
                    message.topic, consumer_group, len(message.value), success=True
                )
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
                    message.topic, consumer_group, len(message.value), success=False
                )

                await self._cleanup_empty_temp_dir(download_task.destination.parent)

                is_circuit_error = outcome.error_category == ErrorCategory.CIRCUIT_OPEN
                return TaskResult(
                    message=message,
                    task_message=task_message,
                    outcome=outcome,
                    processing_time_ms=processing_time_ms,
                    success=False,
                    error=(
                        CircuitOpenError("claimx_download_worker", 60.0)
                        if is_circuit_error
                        else None
                    ),
                )

        finally:
            async with self._in_flight_lock:
                self._in_flight_tasks.discard(task_message.media_id)


    def _convert_to_download_task(
        self, task_message: ClaimXDownloadTask
    ) -> DownloadTask:
        """Creates temp file path using media_id directory to avoid concurrent download conflicts."""
        destination_filename = Path(task_message.blob_path).name
        temp_file = self.temp_dir / task_message.media_id / destination_filename

        return DownloadTask(
            url=task_message.download_url,
            destination=temp_file,
            timeout=60,
            validate_url=True,
            validate_file_type=True,
            allow_localhost=False,
            allowed_domains=None,
            allowed_extensions=None,
            max_size=None,
        )

    async def _periodic_cycle_output(self) -> None:
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
                "stage": "download",
                "cycle": 0,
                "cycle_id": "cycle-0",
            },
        )
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        try:
            while True:
                await asyncio.sleep(1)

                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= 30:
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()

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
                            "stage": "download",
                            "cycle": self._cycle_count,
                            "cycle_id": f"cycle-{self._cycle_count}",
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_failed": self._records_failed,
                            "records_skipped": self._records_skipped,
                            "cycle_interval_seconds": 30,
                        },
                    )

                    # Update last cycle counters
                    self._last_cycle_processed = self._records_processed
                    self._last_cycle_failed = self._records_failed

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise

    async def _handle_success(
        self,
        task_message: ClaimXDownloadTask,
        outcome: DownloadOutcome,
        processing_time_ms: int,
    ) -> None:
        """Move downloaded file to cache directory and produce ClaimXCachedDownloadMessage for upload worker."""
        if outcome.file_path is None:
            raise ValueError("File path missing in successful outcome")

        logger.debug(
            "ClaimX download completed successfully",
            extra={
                "event_id": task_message.source_event_id,
                "media_id": task_message.media_id,
                "project_id": task_message.project_id,
                "download_url": task_message.download_url,
                "bytes_downloaded": outcome.bytes_downloaded,
                "content_type": outcome.content_type,
                "processing_time_ms": processing_time_ms,
                "local_path": str(outcome.file_path),
            },
        )

        cache_subdir = self.cache_dir / task_message.media_id
        cache_subdir.mkdir(parents=True, exist_ok=True)

        filename = Path(task_message.blob_path).name
        cache_path = cache_subdir / filename

        await asyncio.to_thread(shutil.move, str(outcome.file_path), str(cache_path))

        logger.debug(
            "Cached file for upload",
            extra={
                "media_id": task_message.media_id,
                "cache_path": str(cache_path),
            },
        )

        try:
            if outcome.file_path.parent.exists():
                await asyncio.to_thread(outcome.file_path.parent.rmdir)
        except OSError:
            pass

        cached_message = ClaimXCachedDownloadMessage(
            media_id=task_message.media_id,
            project_id=task_message.project_id,
            download_url=task_message.download_url,
            destination_path=task_message.blob_path,
            local_cache_path=str(cache_path),
            bytes_downloaded=outcome.bytes_downloaded or 0,
            content_type=outcome.content_type,
            file_type=task_message.file_type,
            file_name=task_message.file_name,
            source_event_id=task_message.source_event_id,
            downloaded_at=datetime.now(UTC),
        )

        await self.producer.send(
            value=cached_message,
            key=task_message.source_event_id,
        )

        logger.debug(
            "Produced ClaimX cached download message",
            extra={
                "media_id": task_message.media_id,
                "topic": self.cached_topic,
                "cache_path": str(cache_path),
            },
        )

    async def _handle_failure(
        self,
        task_message: ClaimXDownloadTask,
        outcome: DownloadOutcome,
        processing_time_ms: int,
    ) -> None:
        """Route failures based on error category: CIRCUIT_OPEN (reprocess), PERMANENT (DLQ), others (retry)."""
        if self.retry_handler is None:
            raise RuntimeError("RetryHandler not initialized - call start() first")

        error_category = outcome.error_category or ErrorCategory.UNKNOWN

        log_worker_error(
            logger,
            "Download failed",
            event_id=task_message.source_event_id,
            error_category=error_category.value,
            media_id=task_message.media_id,
            project_id=task_message.project_id,
            download_url=task_message.download_url,
            failure_reason=outcome.error_message,
            status_code=outcome.status_code,
            processing_time_ms=processing_time_ms,
            retry_count=task_message.retry_count,
        )

        pending_topic = self.config.get_topic(self.domain, "downloads_pending")
        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        record_processing_error(
            pending_topic,
            consumer_group,
            error_category.value,
        )

        if error_category == ErrorCategory.CIRCUIT_OPEN:
            logger.warning(
                "Circuit breaker open - will reprocess on next poll",
                extra={
                    "media_id": task_message.media_id,
                    "download_url": task_message.download_url,
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

            logger.debug(
                "Routed failed task through retry handler",
                extra={
                    "media_id": task_message.media_id,
                    "error_category": error_category.value,
                    "retry_count": task_message.retry_count,
                },
            )

        except Exception as e:
            logger.error(
                "Failed to route task through retry handler",
                extra={
                    "media_id": task_message.media_id,
                    "error": str(e),
                },
                exc_info=True,
            )

        if outcome.file_path:
            await self._cleanup_temp_file(outcome.file_path)

    async def _cleanup_temp_file(self, file_path: Path) -> None:
        try:

            def _delete():
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
        """Clean up empty temp directory created before failed download."""
        try:

            def _delete_if_empty():
                if dir_path.exists() and dir_path.is_dir():
                    if not any(dir_path.iterdir()):
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

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def in_flight_count(self) -> int:
        return len(self._in_flight_tasks)


__all__ = ["ClaimXDownloadWorker"]
