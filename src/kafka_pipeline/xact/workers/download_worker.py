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
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Set

import aiohttp
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging.context import set_log_context
from core.logging.setup import get_logger
from core.download.downloader import AttachmentDownloader
from core.download.models import DownloadTask, DownloadOutcome
from core.errors.exceptions import CircuitOpenError
from core.types import ErrorCategory
from config.config import KafkaConfig
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.xact.retry.download_handler import RetryHandler
from kafka_pipeline.xact.schemas.cached import CachedDownloadMessage
from kafka_pipeline.xact.schemas.results import DownloadResultMessage
from kafka_pipeline.xact.schemas.tasks import DownloadTaskMessage
from kafka_pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_connection_status,
    update_assigned_partitions,
    update_downloads_concurrent,
    update_downloads_batch_size,
    message_processing_duration_seconds,
)

logger = get_logger(__name__)


@dataclass
class TaskResult:
    """Result of processing a single download task."""
    message: ConsumerRecord
    task_message: DownloadTaskMessage
    outcome: DownloadOutcome
    processing_time_ms: int
    success: bool
    error: Optional[Exception] = None


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
        config = KafkaConfig.from_env()
        worker = DownloadWorker(config)
        await worker.start()  # Runs until stopped
        await worker.stop()
    """

    CONSUMER_GROUP = "xact-download-worker"
    WORKER_NAME = "download_worker"

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = 30

    def __init__(self, config: KafkaConfig, domain: str = "xact", temp_dir: Optional[Path] = None):
        """
        Initialize download worker.

        Args:
            config: Kafka configuration
            domain: Domain identifier (default: "xact")
            temp_dir: Optional directory for temporary downloads (None = system temp)
        """
        self.config = config
        self.domain = domain

        # Temp dir for in-progress downloads
        self.temp_dir = temp_dir or Path(tempfile.gettempdir()) / "download_worker"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        # Cache dir for completed downloads awaiting upload
        self.cache_dir = Path(config.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Build list of topics to consume from (pending + retry topics)
        retry_delays = config.get_retry_delays(domain)
        retry_topics = [
            config.get_retry_topic(domain, i) for i in range(len(retry_delays))
        ]
        self.topics = [config.get_topic(domain, "downloads_pending")] + retry_topics

        # Consumer will be created in start()
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

        # Get worker-specific processing config
        processing_config = config.get_worker_config(domain, self.WORKER_NAME, "processing")
        self.concurrency = processing_config.get("concurrency", 10)
        self.batch_size = processing_config.get("batch_size", 20)
        self.timeout_seconds = processing_config.get("timeout_seconds", 60)

        # Concurrency control (WP-313)
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._in_flight_tasks: Set[str] = set()  # Track by media_id (unique per attachment)
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: Optional[asyncio.Event] = None

        # Simple in-memory dedup cache (replaces complex Delta-based dedup)
        # Maps media_id -> timestamp for TTL-based eviction
        # Prevents duplicate downloads when Eventhouse sends duplicates
        self._dedup_cache: dict[str, float] = {}
        self._dedup_cache_ttl_seconds = 86400  # 24 hours
        self._dedup_cache_max_size = 100_000  # ~1MB memory for 100k entries

        # Shared HTTP session for connection pooling (WP-313)
        self._http_session: Optional[aiohttp.ClientSession] = None

        # Create producer for result messages
        self.producer = BaseKafkaProducer(
            config=config,
            domain=domain,
            worker_name=self.WORKER_NAME,
        )

        # Create downloader instance (reused across tasks)
        self.downloader = AttachmentDownloader()

        # Create retry handler for error routing (lazy initialized in start())
        self.retry_handler: Optional[RetryHandler] = None

        # Health check server - use worker-specific port from config
        health_port = processing_config.get("health_port", 8090)
        self.health_server = HealthCheckServer(
            port=health_port,
            worker_name="xact-downloader",
        )

        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._bytes_downloaded = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: Optional[asyncio.Task] = None

        logger.info(
            "Initialized download worker with concurrent processing",
            extra={
                "domain": domain,
                "worker_name": self.WORKER_NAME,
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

        Begins consuming messages from pending and retry topics.
        Processes messages in concurrent batches.
        Runs until stop() is called or error occurs.

        Raises:
            Exception: If consumer or producer fails to start
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

        # Start health check server first
        await self.health_server.start()

        # Initialize concurrency control
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Create shared HTTP session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=self.concurrency,
            limit_per_host=self.concurrency,
        )
        self._http_session = aiohttp.ClientSession(connector=connector)

        # Re-initialize downloader with shared session for connection pooling
        self.downloader = AttachmentDownloader(session=self._http_session)

        # Start producer
        await self.producer.start()

        # Initialize retry handler (requires producer to be started)
        self.retry_handler = RetryHandler(self.config, self.producer)

        # Create Kafka consumer
        await self._create_consumer()

        self._running = True

        # Start cycle output background task
        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        # Update health check readiness
        self.health_server.set_ready(kafka_connected=True)

        # Update connection status
        update_connection_status("consumer", connected=True)
        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        partition_count = len(self._consumer.assignment()) if self._consumer else 0
        update_assigned_partitions(consumer_group, partition_count)

        logger.info(
            "Download worker started successfully",
            extra={
                "topics": self.topics,
                "partitions": partition_count,
            },
        )

        # Start batch consumption loop
        try:
            await self._consume_batch_loop()
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

    async def _create_consumer(self) -> None:
        """Create and start the Kafka consumer."""
        # Get worker-specific consumer config (merged with defaults)
        consumer_config_dict = self.config.get_worker_config(self.domain, self.WORKER_NAME, "consumer")

        consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": self.config.get_consumer_group(self.domain, self.WORKER_NAME),
            "enable_auto_commit": False,  # Manual commit after batch processing
            "auto_offset_reset": consumer_config_dict.get("auto_offset_reset", "earliest"),
            "max_poll_records": self.batch_size,
            "max_poll_interval_ms": consumer_config_dict.get("max_poll_interval_ms", 300000),
            "session_timeout_ms": consumer_config_dict.get("session_timeout_ms", 60000),
            # Connection timeout settings
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        # Add optional consumer settings if present
        if "heartbeat_interval_ms" in consumer_config_dict:
            consumer_config["heartbeat_interval_ms"] = consumer_config_dict["heartbeat_interval_ms"]
        if "fetch_min_bytes" in consumer_config_dict:
            consumer_config["fetch_min_bytes"] = consumer_config_dict["fetch_min_bytes"]
        if "fetch_max_wait_ms" in consumer_config_dict:
            consumer_config["fetch_max_wait_ms"] = consumer_config_dict["fetch_max_wait_ms"]

        # Configure security based on protocol
        if self.config.security_protocol != "PLAINTEXT":
            consumer_config["security_protocol"] = self.config.security_protocol
            consumer_config["sasl_mechanism"] = self.config.sasl_mechanism

            if self.config.sasl_mechanism == "OAUTHBEARER":
                oauth_callback = create_kafka_oauth_callback()
                consumer_config["sasl_oauth_token_provider"] = oauth_callback
            elif self.config.sasl_mechanism == "PLAIN":
                consumer_config["sasl_plain_username"] = self.config.sasl_plain_username
                consumer_config["sasl_plain_password"] = self.config.sasl_plain_password

        self._consumer = AIOKafkaConsumer(*self.topics, **consumer_config)
        await self._consumer.start()

    async def request_shutdown(self) -> None:
        """
        Request graceful shutdown after current batch completes.

        Sets the running flag to False so the batch loop will exit after
        completing its current batch. This allows in-progress downloads
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
        Stop the download worker and clean up resources.

        Performs graceful shutdown:
        1. Signals shutdown to stop accepting new batches
        2. Waits for in-flight downloads to complete (up to 30s)
        3. Commits final offsets
        4. Closes all resources

        Safe to call multiple times. Will clean up resources even if
        request_shutdown() was called first.
        """
        # Check if already fully stopped (consumer is None)
        if self._consumer is None and self._http_session is None:
            logger.debug("Worker already stopped")
            return

        logger.info("Stopping download worker, waiting for in-flight downloads")
        self._running = False

        # Cancel cycle output task
        if self._cycle_task and not self._cycle_task.done():
            self._cycle_task.cancel()
            try:
                await self._cycle_task
            except asyncio.CancelledError:
                pass

        # Signal shutdown
        if self._shutdown_event:
            self._shutdown_event.set()

        # Wait for in-flight downloads to complete (with timeout)
        await self._wait_for_in_flight(timeout=30.0)

        # Stop consumer
        if self._consumer:
            try:
                await self._consumer.commit()
                await self._consumer.stop()
            except Exception as e:
                logger.error(
                    "Error stopping consumer",
                    extra={"error": str(e)},
                    exc_info=True,
                )
            finally:
                self._consumer = None

        # Close HTTP session
        if self._http_session:
            await self._http_session.close()
            self._http_session = None

        # Stop producer
        await self.producer.stop()

        # Clear retry handler reference
        self.retry_handler = None

        # Stop health check server
        await self.health_server.stop()

        # Update metrics
        update_connection_status("consumer", connected=False)
        update_assigned_partitions(self.CONSUMER_GROUP, 0)
        update_downloads_concurrent(self.WORKER_NAME, 0)
        update_downloads_batch_size(self.WORKER_NAME, 0)

        logger.info("Download worker stopped successfully")

    async def _wait_for_in_flight(self, timeout: float = 30.0) -> None:
        """
        Wait for in-flight downloads to complete.

        Args:
            timeout: Maximum time to wait in seconds
        """
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

    async def _consume_batch_loop(self) -> None:
        """
        Main batch consumption loop.

        Fetches batches of messages and processes them concurrently.
        """
        logger.info("Starting batch consumption loop")

        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)

        # Track partition assignment status for logging
        _logged_waiting_for_assignment = False
        _logged_assignment_received = False

        while self._running and self._consumer:
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
                                "topics": self.topics,
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
                data = await self._consumer.getmany(
                    timeout_ms=1000,
                    max_records=self.batch_size,
                )

                if not data:
                    continue

                # Flatten messages from all partitions
                messages: List[ConsumerRecord] = []
                for topic_partition, records in data.items():
                    messages.extend(records)

                if not messages:
                    continue

                # Update batch size metric
                update_downloads_batch_size(self.WORKER_NAME, len(messages))

                logger.info(
                    "Processing message batch",
                    extra={
                        "batch_size": len(messages),
                        "download_concurrency": self.concurrency,
                    },
                )

                # Process batch concurrently
                results = await self._process_batch(messages)

                # Handle results and determine commit strategy
                should_commit = await self._handle_batch_results(results)

                if should_commit:
                    await self._consumer.commit()
                    logger.debug(
                        "Committed offsets for batch",
                        extra={"batch_size": len(messages)},
                    )

                # Periodic cleanup of expired dedup cache entries
                self._cleanup_dedup_cache()

                # Reset batch size metric
                update_downloads_batch_size(self.WORKER_NAME, 0)

            except asyncio.CancelledError:
                logger.info("Batch consumption loop cancelled")
                raise
            except Exception as e:
                logger.error(
                    "Error in batch consumption loop",
                    extra={"error": str(e)},
                    exc_info=True,
                )
                await asyncio.sleep(1)

    async def _process_batch(self, messages: List[ConsumerRecord]) -> List[TaskResult]:
        """
        Process a batch of messages concurrently.

        Uses asyncio.Semaphore to control concurrency and asyncio.gather()
        to process downloads in parallel.

        Args:
            messages: List of ConsumerRecords to process

        Returns:
            List of TaskResult objects with processing outcomes
        """
        async def bounded_process(message: ConsumerRecord) -> TaskResult:
            """Process a single message with semaphore control."""
            async with self._semaphore:
                return await self._process_single_task(message)

        # Update concurrent downloads metric as tasks start
        update_downloads_concurrent(self.WORKER_NAME, len(messages))

        # Process all messages concurrently
        tasks = [bounded_process(msg) for msg in messages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to TaskResult objects
        processed_results: List[TaskResult] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Create error result for unhandled exceptions
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
                # We can't create a full TaskResult without parsing, so we'll handle this
                # as a transient error that will be retried on next poll
                processed_results.append(None)  # type: ignore
            else:
                processed_results.append(result)

        # Update metric - all downloads complete
        update_downloads_concurrent(self.WORKER_NAME, 0)

        # Log batch summary
        succeeded = sum(1 for r in processed_results if r and r.success)
        failed = sum(1 for r in processed_results if r and not r.success)
        errors = sum(1 for r in processed_results if r is None)

        logger.info(
            "Batch processing complete",
            extra={
                "batch_size": len(messages),
                "records_succeeded": succeeded,
                "records_failed": failed,
                "records_errored": errors,
            },
        )

        return [r for r in processed_results if r is not None]

    async def _process_single_task(self, message: ConsumerRecord) -> TaskResult:
        """
        Process a single download task message.

        Args:
            message: ConsumerRecord with DownloadTaskMessage as value

        Returns:
            TaskResult with processing outcome
        """
        start_time = time.perf_counter()

        # Parse message value as DownloadTaskMessage
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
            # Return error result
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

        # Set logging context for this request
        set_log_context(trace_id=task_message.trace_id)

        # Track records processed
        self._records_processed += 1

        # Track in-flight task by media_id (unique per attachment)
        async with self._in_flight_lock:
            self._in_flight_tasks.add(task_message.media_id)

        try:
            # Check dedup cache (simple in-memory duplicate prevention)
            if self._is_duplicate(task_message.media_id):
                logger.info(
                    "Skipping duplicate download (already processed recently)",
                    extra={
                        "trace_id": task_message.trace_id,
                        "media_id": task_message.media_id,
                        "attachment_url": task_message.attachment_url,
                    },
                )
                # Return success result without downloading (already processed)
                return TaskResult(
                    message=message,
                    task_message=task_message,
                    outcome=DownloadOutcome(
                        success=True,
                        error_message=None,
                        error_category=None,
                    ),
                    processing_time_ms=int((time.perf_counter() - start_time) * 1000),
                    success=True,
                )

            # Convert to download task
            download_task = self._convert_to_download_task(task_message)

            # Perform download
            outcome = await self.downloader.download(download_task)

            processing_time_ms = int((time.perf_counter() - start_time) * 1000)

            # Handle outcome: upload and produce result
            if outcome.success:
                await self._handle_success(task_message, outcome, processing_time_ms)
                # Mark as processed in dedup cache to prevent future duplicates
                self._mark_processed(task_message.media_id)
                record_message_consumed(
                    message.topic, self.CONSUMER_GROUP, len(message.value), success=True
                )

                # Track successful download for cycle output
                self._records_succeeded += 1
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
                    message.topic, self.CONSUMER_GROUP, len(message.value), success=False
                )

                # Clean up empty temp directory even if outcome.file_path is None
                # The directory may have been created before download failed
                await self._cleanup_empty_temp_dir(download_task.destination.parent)

                # Track failed download for cycle output
                self._records_failed += 1
                # Check if this is a circuit breaker error that should prevent commit
                is_circuit_error = outcome.error_category == ErrorCategory.CIRCUIT_OPEN
                return TaskResult(
                    message=message,
                    task_message=task_message,
                    outcome=outcome,
                    processing_time_ms=processing_time_ms,
                    success=False,
                    error=CircuitOpenError("download_worker", 60.0) if is_circuit_error else None,
                )

        finally:
            # Remove from in-flight tracking
            async with self._in_flight_lock:
                self._in_flight_tasks.discard(task_message.media_id)

    async def _handle_batch_results(self, results: List[TaskResult]) -> bool:
        """
        Handle batch results and determine if offsets should be committed.

        Returns False if any result has a circuit breaker error, which means
        those messages should be reprocessed when the circuit closes.

        Args:
            results: List of TaskResult from batch processing

        Returns:
            True if offsets should be committed, False otherwise
        """
        # Check for circuit breaker errors
        circuit_errors = [r for r in results if r.error and isinstance(r.error, CircuitOpenError)]

        if circuit_errors:
            logger.warning(
                "Circuit breaker errors in batch - not committing offsets",
                extra={"circuit_error_count": len(circuit_errors)},
            )
            return False

        return True

    def _convert_to_download_task(self, task_message: DownloadTaskMessage) -> DownloadTask:
        """
        Convert DownloadTaskMessage to DownloadTask for downloader.

        Creates a temporary file path based on destination_path to avoid
        conflicts between concurrent downloads.

        Args:
            task_message: Kafka message with download task details

        Returns:
            DownloadTask configured for AttachmentDownloader
        """
        # Create temporary file path (unique per trace_id)
        # Use blob_path to preserve file extension
        destination_filename = Path(task_message.blob_path).name
        temp_file = self.temp_dir / task_message.trace_id / destination_filename

        return DownloadTask(
            url=task_message.attachment_url,
            destination=temp_file,
            timeout=60,  # TODO: Make configurable
            validate_url=True,
            validate_file_type=True,
            check_expiration=True,  # Xact S3 URLs cannot be refreshed
            # Use default allowed domains and extensions from security module
            allowed_domains=None,
            allowed_extensions=None,
            max_size=None,  # TODO: Make configurable
        )

    async def _handle_success(
        self,
        task_message: DownloadTaskMessage,
        outcome: DownloadOutcome,
        processing_time_ms: int,
    ) -> None:
        """
        Handle successful download: move to cache and produce cached message.

        Moves the downloaded file to the cache directory and produces a
        CachedDownloadMessage for the Upload Worker to process.

        Args:
            task_message: Original task message
            outcome: Download outcome with file path
            processing_time_ms: Total processing time in milliseconds

        Raises:
            Exception: On cache move or produce failures
        """
        assert outcome.file_path is not None, "File path missing in successful outcome"

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

        # Move file to cache directory with stable path
        # Path structure: cache_dir/trace_id/filename
        cache_subdir = self.cache_dir / task_message.trace_id
        cache_subdir.mkdir(parents=True, exist_ok=True)

        # Use original filename from blob_path
        filename = Path(task_message.blob_path).name
        cache_path = cache_subdir / filename

        # Move file from temp to cache (atomic on same filesystem)
        await asyncio.to_thread(shutil.move, str(outcome.file_path), str(cache_path))

        logger.info(
            "Cached file for upload",
            extra={
                "trace_id": task_message.trace_id,
                "cache_path": str(cache_path),
            },
        )

        # Clean up empty temp directory
        try:
            if outcome.file_path.parent.exists():
                await asyncio.to_thread(outcome.file_path.parent.rmdir)
        except OSError:
            pass  # Directory not empty or already removed

        # Produce cached message for upload worker
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
            downloaded_at=datetime.now(timezone.utc),
            metadata=task_message.metadata,
        )

        await self.producer.send(
            topic=self.config.get_topic(self.domain, "downloads_cached"),
            key=task_message.trace_id,
            value=cached_message,
        )

        logger.info(
            "Produced cached download message",
            extra={
                "trace_id": task_message.trace_id,
                "topic": self.config.get_topic(self.domain, "downloads_cached"),
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
        Handle failed download: route to retry/DLQ and produce result.

        Routes failures based on error category:
        - CIRCUIT_OPEN: Don't process further (will be retried on next poll)
        - PERMANENT: Send to DLQ and commit offset (no retry)
        - TRANSIENT: Send to retry topic and commit offset
        - AUTH: Send to retry topic and commit offset (credentials may refresh)
        - UNKNOWN: Send to retry topic and commit offset (conservative retry)

        Args:
            task_message: Original task message
            outcome: Download outcome with error details
            processing_time_ms: Total processing time in milliseconds
        """
        assert self.retry_handler is not None, "RetryHandler not initialized"

        error_category = outcome.error_category or ErrorCategory.UNKNOWN

        logger.warning(
            "Download failed",
            extra={
                "trace_id": task_message.trace_id,
                "attachment_url": task_message.attachment_url,
                "error_message": outcome.error_message,
                "error_category": error_category.value,
                "status_code": outcome.status_code,
                "processing_time_ms": processing_time_ms,
                "retry_count": task_message.retry_count,
            },
        )

        # Record error metric
        record_processing_error(
            self.config.get_topic(self.domain, "downloads_pending"),
            self.config.get_consumer_group(self.domain, self.WORKER_NAME),
            error_category.value,
        )

        # Handle circuit breaker errors specially - don't route, will reprocess
        if error_category == ErrorCategory.CIRCUIT_OPEN:
            logger.warning(
                "Circuit breaker open - will reprocess on next poll",
                extra={
                    "trace_id": task_message.trace_id,
                    "attachment_url": task_message.attachment_url,
                },
            )
            # Clean up temporary file before returning
            if outcome.file_path:
                await self._cleanup_temp_file(outcome.file_path)
            return

        # For all other errors, route through RetryHandler
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
            # Don't re-raise - we'll still produce result message

        # Determine status for result message
        # Schema allows: completed, failed, failed_permanent
        if error_category == ErrorCategory.PERMANENT:
            status = "failed_permanent"
        else:
            status = "failed"  # Transient failures are just "failed"

        # Produce result message for observability
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
            created_at=datetime.now(timezone.utc),
        )

        await self.producer.send(
            topic=self.config.get_topic(self.domain, "downloads_results"),
            key=task_message.trace_id,
            value=result_message,
        )

        logger.info(
            "Produced failure result message",
            extra={
                "trace_id": task_message.trace_id,
                "status": status,
                "topic": self.config.get_topic(self.domain, "downloads_results"),
            },
        )

        # Clean up temporary file if it exists
        if outcome.file_path:
            await self._cleanup_temp_file(outcome.file_path)

    async def _cleanup_temp_file(self, file_path: Path) -> None:
        """
        Clean up temporary download file and parent directory.

        Args:
            file_path: Path to temporary file

        Note:
            Runs in thread pool since file deletion is blocking I/O.
            Errors are logged but not raised.
        """
        try:
            def _delete():
                if file_path.exists():
                    file_path.unlink()
                    logger.debug(
                        "Deleted temporary file",
                        extra={"file_path": str(file_path)},
                    )

                # Clean up parent directory if empty (trace_id directory)
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
        """
        Clean up empty temporary directory.

        Called after download failures to remove directories that were created
        before the download started but left empty when the download failed.

        Args:
            dir_path: Path to temporary directory (e.g., temp_dir/trace_id)

        Note:
            Only removes the directory if it exists and is empty.
            Errors are logged but not raised.
        """
        try:
            def _delete_if_empty():
                if dir_path.exists() and dir_path.is_dir():
                    # Only remove if empty
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

    async def _periodic_cycle_output(self) -> None:
        """
        Background task for periodic cycle logging.
        """
        logger.info(
            "Cycle 0: processed=0 (succeeded=0, failed=0), bytes=0, in_flight=0 "
            "[cycle output every %ds]",
            self.CYCLE_LOG_INTERVAL_SECONDS,
        )
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0

        try:
            while True:  # Runs until cancelled
                await asyncio.sleep(1)

                cycle_elapsed = time.monotonic() - self._last_cycle_log
                if cycle_elapsed >= self.CYCLE_LOG_INTERVAL_SECONDS:
                    self._cycle_count += 1
                    self._last_cycle_log = time.monotonic()
                    in_flight = len(self._in_flight_tasks)

                    logger.info(
                        f"Cycle {self._cycle_count}: processed={self._records_processed} "
                        f"(succeeded={self._records_succeeded}, failed={self._records_failed}), "
                        f"bytes={self._bytes_downloaded}, in_flight={in_flight}",
                        extra={
                            "cycle": self._cycle_count,
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_failed": self._records_failed,
                            "bytes_downloaded": self._bytes_downloaded,
                            "in_flight": in_flight,
                            "cycle_interval_seconds": self.CYCLE_LOG_INTERVAL_SECONDS,
                        },
                    )

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise

    def _is_duplicate(self, media_id: str) -> bool:
        """
        Check if media_id is in dedup cache (already processed recently).

        Args:
            media_id: Media ID to check (unique per attachment)

        Returns:
            True if duplicate (already in cache), False otherwise
        """
        now = time.time()

        # Check if in cache and not expired
        if media_id in self._dedup_cache:
            cached_time = self._dedup_cache[media_id]
            if now - cached_time < self._dedup_cache_ttl_seconds:
                return True
            # Expired - remove from cache
            del self._dedup_cache[media_id]

        return False

    def _mark_processed(self, media_id: str) -> None:
        """
        Add media_id to dedup cache to prevent re-processing.

        Implements simple LRU eviction if cache is full.

        Args:
            media_id: Media ID to mark as processed (unique per attachment)
        """
        now = time.time()

        # If cache is full, evict oldest entries (simple LRU)
        if len(self._dedup_cache) >= self._dedup_cache_max_size:
            # Sort by timestamp and remove oldest 10%
            sorted_items = sorted(self._dedup_cache.items(), key=lambda x: x[1])
            evict_count = self._dedup_cache_max_size // 10
            for media_id_to_evict, _ in sorted_items[:evict_count]:
                del self._dedup_cache[media_id_to_evict]

            logger.debug(
                "Evicted old entries from dedup cache",
                extra={
                    "evicted_count": evict_count,
                    "cache_size": len(self._dedup_cache),
                },
            )

        # Add to cache
        self._dedup_cache[media_id] = now

    def _cleanup_dedup_cache(self) -> None:
        """Remove expired entries from dedup cache (TTL-based cleanup)."""
        now = time.time()
        expired_keys = [
            media_id
            for media_id, cached_time in self._dedup_cache.items()
            if now - cached_time >= self._dedup_cache_ttl_seconds
        ]

        for media_id in expired_keys:
            del self._dedup_cache[media_id]

        if expired_keys:
            logger.debug(
                "Cleaned up expired dedup cache entries",
                extra={
                    "expired_count": len(expired_keys),
                    "cache_size": len(self._dedup_cache),
                },
            )

    @property
    def is_running(self) -> bool:
        """Check if worker is running and processing messages."""
        return self._running

    @property
    def in_flight_count(self) -> int:
        """Return the number of downloads currently in progress."""
        return len(self._in_flight_tasks)


__all__ = ["DownloadWorker"]
