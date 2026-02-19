"""ClaimX download worker with concurrent processing.

Downloads media files using presigned S3 URLs and caches locally for upload worker.
Decoupled architecture allows independent scaling of download vs upload.
"""

import asyncio
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
from core.download.range_download import (
    LARGE_FILE_THRESHOLD,
    download_file_in_ranges,
)
from core.errors.exceptions import CircuitOpenError
from core.logging.context import set_log_context
from core.logging.periodic_logger import PeriodicStatsLogger
from core.logging.utilities import format_cycle_output, log_worker_error
from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiClient
from pipeline.claimx.retry import DownloadRetryHandler
from pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from pipeline.claimx.schemas.tasks import ClaimXDownloadTask
from pipeline.common.worker_defaults import (
    BATCH_SIZE,
    CONCURRENCY,
    CYCLE_LOG_INTERVAL_SECONDS,
)
from pipeline.common.decorators import set_log_context_from_message
from pipeline.common.health import HealthCheckServer
from pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_assigned_partitions,
    update_connection_status,
    update_disk_usage,
)
from pipeline.common.stale_file_cleaner import StaleFileCleaner
from pipeline.common.telemetry import initialize_worker_telemetry
from pipeline.common.consumer_config import ConsumerConfig
from pipeline.common.transport import create_batch_consumer, create_producer
from pipeline.common.types import PipelineMessage

logger = logging.getLogger(__name__)


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
        config = MessageConfig.from_env()
        worker = ClaimXDownloadWorker(config)
        await worker.start()  # Runs until stopped
        await worker.stop()
    """

    WORKER_NAME = "download_worker"

    def __init__(
        self,
        config: MessageConfig,
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

        self.temp_dir = (temp_dir or Path(tempfile.gettempdir()) / "pipeline_temp") / domain
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.cache_dir = Path(config.cache_dir) / domain
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.concurrency = CONCURRENCY
        self.batch_size = BATCH_SIZE

        # Only consume from pending topic
        # Unified retry scheduler handles routing retry messages back to pending
        self.topics = [config.get_topic(domain, "downloads_pending")]

        self._consumer = None
        self._consumer_group: str | None = None
        self._running = False

        self._semaphore: asyncio.Semaphore | None = None
        self._in_flight_tasks: set[str] = set()
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: asyncio.Event | None = None

        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._bytes_downloaded = 0
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None

        self._stats_logger: PeriodicStatsLogger | None = None
        self._stale_cleaner = StaleFileCleaner(
            scan_dir=self.temp_dir,
            in_flight_lock=self._in_flight_lock,
            in_flight_tasks=self._in_flight_tasks,
        )

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

        health_port = 8082
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

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        initialize_worker_telemetry(self.domain, "download-worker")

        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Close resources from a previous failed start attempt to prevent leak.
        await self._cleanup_stale_resources()

        connector = aiohttp.TCPConnector(
            limit=self.concurrency,
            limit_per_host=self.concurrency,
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

        if hasattr(self.producer, "eventhub_name"):
            self.cached_topic = self.producer.eventhub_name

        self.api_client = ClaimXApiClient(
            base_url=self.config.claimx_api_url or "https://api.test.claimxperience.com",
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
            topic_key="downloads_pending",
            consumer_config=ConsumerConfig(
                batch_size=self.batch_size,
                batch_timeout_ms=1000,
                instance_id=self.instance_id,
            ),
        )

        self._consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        self._running = True

        self._stats_logger = PeriodicStatsLogger(
            interval_seconds=CYCLE_LOG_INTERVAL_SECONDS,
            get_stats=self._get_cycle_stats,
            stage="download",
            worker_id=self.worker_id,
        )
        self._stats_logger.start()

        api_reachable = not self.api_client.is_circuit_open
        self.health_server.set_ready(
            transport_connected=True,
            api_reachable=api_reachable,
            circuit_open=self.api_client.is_circuit_open,
        )

        await self._stale_cleaner.start()

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

        logger.info("Graceful shutdown requested, will stop after current batch completes")
        self._running = False

    async def stop(self) -> None:
        """Gracefully stop worker, wait for in-flight downloads, commit offsets, clean up resources."""
        if self._consumer is None and self._http_session is None and self.api_client is None:
            logger.debug("Worker already stopped")
            return

        logger.info("Stopping ClaimX download worker, waiting for in-flight downloads")
        self._running = False

        await self._close_resource("_stats_logger", clear=False)
        await self._close_resource("_stale_cleaner", clear=False)

        if self._shutdown_event:
            self._shutdown_event.set()

        try:
            await self._wait_for_in_flight(timeout=30.0)
        except asyncio.CancelledError:
            logger.warning("Interrupted while waiting for in-flight downloads")

        await self._close_resource("_consumer")
        await self._close_http_session()
        await self._close_resource("producer", clear=False)
        await self._close_resource("api_client", method="close")
        await self._close_resource("retry_handler")
        await self._close_resource("health_server", clear=False)

        update_connection_status("consumer", connected=False)
        update_assigned_partitions(self._consumer_group, 0)

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

    async def _close_resource(
        self, attr: str, method: str = "stop", *, clear: bool = True
    ) -> None:
        """Execute a cleanup action on a resource attribute, logging any errors."""
        resource = getattr(self, attr, None)
        if resource is None:
            return
        try:
            await getattr(resource, method)()
        except asyncio.CancelledError:
            logger.warning("Cancelled while closing %s", attr)
        except Exception as e:
            logger.error(
                "Error closing %s", attr, extra={"error": str(e)}, exc_info=True
            )
        finally:
            if clear:
                setattr(self, attr, None)

    async def _close_http_session(self) -> None:
        """Close HTTP session with connector cleanup grace period."""
        if self._http_session is None:
            return
        try:
            await self._http_session.close()
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            logger.warning("Cancelled while closing HTTP session")
        except Exception as e:
            logger.error(
                "Error closing HTTP session",
                extra={"error": str(e)},
                exc_info=True,
            )
        finally:
            self._http_session = None

    async def _cleanup_stale_resources(self) -> None:
        """Close resources from a previous failed start attempt to prevent leaks."""
        if self._http_session and not self._http_session.closed:
            try:
                await self._http_session.close()
                await asyncio.sleep(0)
            except Exception as e:
                logger.warning(
                    "Error closing stale HTTP session", extra={"error": str(e)}
                )
            finally:
                self._http_session = None

        if self.api_client:
            try:
                await self.api_client.close()
            except Exception as e:
                logger.warning(
                    "Error closing stale API client", extra={"error": str(e)}
                )
            finally:
                self.api_client = None

        if self.retry_handler:
            try:
                await self.retry_handler.stop()
            except Exception as e:
                logger.warning(
                    "Error stopping stale retry handler", extra={"error": str(e)}
                )
            finally:
                self.retry_handler = None

    def _process_gather_results(
        self,
        messages: list[PipelineMessage],
        raw_results: list[TaskResult | BaseException],
    ) -> tuple[int, int, int, list[TaskResult]]:
        """Process asyncio.gather results: log exceptions, tally outcomes.

        Returns (succeeded, failed, errors, circuit_errors).
        """
        succeeded = 0
        failed = 0
        errors = 0
        circuit_errors: list[TaskResult] = []
        for i, result in enumerate(raw_results):
            if isinstance(result, Exception):
                msg = messages[i]
                logger.error(
                    "Unhandled exception processing message",
                    extra={
                        "topic": msg.topic,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "error": str(result),
                    },
                    exc_info=result,
                )
                errors += 1
            elif result.success:
                succeeded += 1
            else:
                failed += 1
                if result.error and isinstance(result.error, CircuitOpenError):
                    circuit_errors.append(result)
        return succeeded, failed, errors, circuit_errors

    def _update_cycle_offsets(self, ts: int | None) -> None:
        """Track earliest/latest message timestamps for cycle logging."""
        if ts is None:
            return
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

    async def _finalize_task(
        self,
        message: PipelineMessage,
        task_message: ClaimXDownloadTask,
        outcome: DownloadOutcome,
        download_task: DownloadTask,
        processing_time_ms: int,
        preserve_partial: bool = False,
    ) -> TaskResult:
        """Record metrics and build TaskResult from download outcome."""
        if outcome.success:
            await self._handle_success(task_message, outcome, processing_time_ms)
            record_message_consumed(
                message.topic, self._consumer_group, len(message.value), success=True
            )
            self._bytes_downloaded += outcome.bytes_downloaded or 0
            return TaskResult(
                message=message,
                task_message=task_message,
                outcome=outcome,
                processing_time_ms=processing_time_ms,
                success=True,
            )

        await self._handle_failure(task_message, outcome, processing_time_ms, preserve_partial)
        record_message_consumed(
            message.topic, self._consumer_group, len(message.value), success=False
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

    async def _process_batch(self, messages: list[PipelineMessage]) -> bool:
        """Process batch of download messages concurrently.

        Returns:
            True to commit batch, False to skip commit (circuit breaker errors)
        """

        async def bounded_process(message: PipelineMessage) -> TaskResult:
            async with self._semaphore:
                return await self._process_single_task(message)

        tasks = [bounded_process(msg) for msg in messages]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        succeeded, failed, errors, circuit_errors = self._process_gather_results(
            messages, raw_results
        )

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
        self._records_failed += failed + errors

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
            task_message = ClaimXDownloadTask.model_validate_json(message.value)
        except Exception as e:
            raw_preview = message.value[:1000].decode("utf-8", errors="replace") if message.value else ""
            logger.error(
                "Failed to parse ClaimXDownloadTask",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": str(e),
                    "raw_payload_preview": raw_preview,
                    "raw_payload_bytes": len(message.value) if message.value else 0,
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
        self._update_cycle_offsets(message.timestamp)

        async with self._in_flight_lock:
            self._in_flight_tasks.add(task_message.media_id)

        set_log_context(trace_id=task_message.trace_id, media_id=task_message.media_id)

        try:
            logger.info(
                "Processing ClaimX download task",
                extra={
                    "trace_id": task_message.trace_id,
                    "media_id": task_message.media_id,
                    "project_id": task_message.project_id,
                    "download_url": task_message.download_url,
                    "destination_path": task_message.blob_path,
                    "retry_count": task_message.retry_count,
                    "topic": message.topic,
                },
            )

            download_task = self._convert_to_download_task(task_message)

            # HEAD request to determine download strategy
            content_length = await self._head_content_length(task_message.download_url)
            use_range = content_length is not None and content_length > LARGE_FILE_THRESHOLD

            t0 = time.perf_counter()

            if use_range:
                temp_file = download_task.destination
                resume_from = temp_file.stat().st_size if temp_file.exists() else 0
                outcome = await self._download_large_file(
                    task_message, download_task, temp_file, content_length, resume_from
                )
            else:
                outcome = await self.downloader.download(download_task)

            download_ms = int((time.perf_counter() - t0) * 1000)

            processing_time_ms = int((time.perf_counter() - start_time) * 1000)

            logger.info(
                f"Download phase completed in {download_ms}ms: "
                f"success={outcome.success}, status_code={outcome.status_code}, "
                f"error_message={outcome.error_message}, "
                f"validation_error={outcome.validation_error}, "
                f"bytes={outcome.bytes_downloaded}, range={use_range}",
                extra={
                    "media_id": task_message.media_id,
                    "trace_id": task_message.trace_id,
                    "error_message": outcome.error_message,
                    "error_category": outcome.error_category.value if outcome.error_category else None,
                    "status_code": outcome.status_code,
                    "bytes_downloaded": outcome.bytes_downloaded,
                    "processing_time_ms": download_ms,
                    "range_download": use_range,
                },
            )

            return await self._finalize_task(
                message, task_message, outcome, download_task, processing_time_ms,
                preserve_partial=use_range,
            )

        finally:
            async with self._in_flight_lock:
                self._in_flight_tasks.discard(task_message.media_id)

    def _convert_to_download_task(self, task_message: ClaimXDownloadTask) -> DownloadTask:
        """Creates temp file path using media_id directory to avoid concurrent download conflicts."""
        destination_filename = Path(task_message.blob_path).name
        temp_file = self.temp_dir / task_message.media_id / destination_filename

        return DownloadTask(
            url=task_message.download_url,
            destination=temp_file,
            timeout=300,
            validate_url=True,
            validate_file_type=True,
            allow_localhost=False,
            allowed_domains=None,
            allowed_extensions=None,
            max_size=None,
            skip_head=True,
        )

    async def _head_content_length(self, url: str) -> int | None:
        """Quick HEAD request to get Content-Length. Returns None on any failure."""
        if self._http_session is None:
            return None
        try:
            async with self._http_session.head(
                url,
                timeout=aiohttp.ClientTimeout(total=30, sock_read=10),
                allow_redirects=True,
            ) as response:
                return response.content_length
        except Exception:
            return None

    async def _download_large_file(
        self,
        task_message: ClaimXDownloadTask,
        download_task: DownloadTask,
        dest: Path,
        total_size: int,
        resume_from: int,
    ) -> DownloadOutcome:
        """Download large file via range requests, with fallback to streaming."""
        # Run pre-download validation (URL, file type) before range download
        pre_error = self.downloader._validate_pre_download(download_task)
        if pre_error:
            return pre_error

        await asyncio.to_thread(dest.parent.mkdir, parents=True, exist_ok=True)

        result, error = await download_file_in_ranges(
            url=task_message.download_url,
            output_path=dest,
            session=self._http_session,
            total_size=total_size,
            resume_from_bytes=resume_from,
        )

        if error is not None:
            # Range not supported (server returned 200) — fall back to streaming
            if error.status_code == 200:
                logger.info(
                    "Server does not support Range requests, falling back to streaming",
                    extra={"media_id": task_message.media_id},
                )
                return await self.downloader.download(download_task)

            # Include file_path + bytes so _handle_failure can preserve partial file
            return DownloadOutcome(
                success=False,
                file_path=dest if error.bytes_written_so_far > 0 else None,
                bytes_downloaded=error.bytes_written_so_far,
                error_message=error.error_message,
                error_category=error.error_category,
                status_code=error.status_code,
            )

        return DownloadOutcome.success_outcome(
            file_path=dest,
            bytes_downloaded=result.bytes_written,
            content_type=result.content_type,
            status_code=206,
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
            "stale_files_removed": self._stale_cleaner.total_removed,
            "cycle_offset_start_ts": self._cycle_offset_start_ts,
            "cycle_offset_end_ts": self._cycle_offset_end_ts,
        }
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        return msg, extra

    async def _handle_success(
        self,
        task_message: ClaimXDownloadTask,
        outcome: DownloadOutcome,
        processing_time_ms: int,
    ) -> None:
        """Move downloaded file to cache directory and produce ClaimXCachedDownloadMessage for upload worker."""
        if outcome.file_path is None:
            raise ValueError("File path missing in successful outcome")

        logger.info(
            "ClaimX download completed successfully",
            extra={
                "trace_id": task_message.trace_id,
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
            trace_id=task_message.trace_id,
            downloaded_at=datetime.now(UTC),
        )

        await self.producer.send(
            value=cached_message,
            key=task_message.trace_id,
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
        preserve_partial: bool = False,
    ) -> None:
        """Route failures based on error category: CIRCUIT_OPEN (reprocess), PERMANENT (DLQ), others (retry)."""
        if self.retry_handler is None:
            raise RuntimeError("RetryHandler not initialized - call start() first")

        error_category = outcome.error_category or ErrorCategory.UNKNOWN

        logger.warning(
            f"Download outcome details: error_message={outcome.error_message}, "
            f"validation_error={outcome.validation_error}, "
            f"error_category={error_category.value}, "
            f"status_code={outcome.status_code}, "
            f"bytes={outcome.bytes_downloaded}, "
            f"processing_time_ms={processing_time_ms}",
            extra={
                "trace_id": task_message.trace_id,
                "media_id": task_message.media_id,
                "error_message": outcome.error_message,
                "error_category": error_category.value,
                "status_code": outcome.status_code,
                "bytes_downloaded": outcome.bytes_downloaded,
                "processing_time_ms": processing_time_ms,
            },
        )

        log_worker_error(
            logger,
            "Download failed",
            error_category=error_category.value,
            trace_id=task_message.trace_id,
            media_id=task_message.media_id,
            project_id=task_message.project_id,
            download_url=task_message.download_url,
            failure_reason=outcome.error_message,
            status_code=outcome.status_code,
            processing_time_ms=processing_time_ms,
            retry_count=task_message.retry_count,
        )

        pending_topic = self.config.get_topic(self.domain, "downloads_pending")
        record_processing_error(
            pending_topic,
            self._consumer_group,
            error_category.value,
        )

        if error_category == ErrorCategory.CIRCUIT_OPEN:
            logger.warning(
                "Circuit breaker open - will reprocess on next poll",
                extra={
                    "trace_id": task_message.trace_id,
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
                    "trace_id": task_message.trace_id,
                    "media_id": task_message.media_id,
                    "error_category": error_category.value,
                    "retry_count": task_message.retry_count,
                },
            )

        except Exception as e:
            logger.error(
                "Failed to route task through retry handler",
                extra={
                    "trace_id": task_message.trace_id,
                    "media_id": task_message.media_id,
                    "error": str(e),
                },
                exc_info=True,
            )

        if outcome.file_path:
            if preserve_partial and error_category == ErrorCategory.TRANSIENT:
                logger.info(
                    "Preserving partial file for resume on retry",
                    extra={
                        "media_id": task_message.media_id,
                        "file_path": str(outcome.file_path),
                        "bytes_downloaded": outcome.bytes_downloaded,
                    },
                )
            else:
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

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def in_flight_count(self) -> int:
        return len(self._in_flight_tasks)


__all__ = ["ClaimXDownloadWorker"]
