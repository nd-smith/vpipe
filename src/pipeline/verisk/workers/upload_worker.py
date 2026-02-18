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
from pipeline.common.transport import create_batch_consumer, create_producer
from pipeline.common.types import PipelineMessage
from pipeline.verisk.schemas.cached import CachedDownloadMessage
from pipeline.verisk.schemas.results import DownloadResultMessage
from pipeline.common.worker_defaults import (
    BATCH_SIZE,
    CONCURRENCY,
    CYCLE_LOG_INTERVAL_SECONDS,
)

logger = logging.getLogger(__name__)


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
        config = MessageConfig.from_env()
        worker = UploadWorker(config)
        await worker.start()  # Runs until stopped
        await worker.stop()
    """

    WORKER_NAME = "upload_worker"

    # Cycle output configuration
    CYCLE_LOG_INTERVAL_SECONDS = CYCLE_LOG_INTERVAL_SECONDS

    def __init__(
        self,
        config: MessageConfig,
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

        # Store injected storage client
        self._injected_storage_client = storage_client

        if storage_client is not None:
            logger.info(
                "Using injected storage client",
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
                "  - ONELAKE_XACT_PATH / ONELAKE_CLAIMX_PATH env vars"
            )

        # Get worker-specific processing config
        processing_config = config.get_worker_config(domain, self.WORKER_NAME, "processing")
        self.concurrency = processing_config.get("concurrency", CONCURRENCY)
        self.batch_size = processing_config.get("batch_size", BATCH_SIZE)

        # Topic to consume from
        self.topics = [config.get_topic(domain, "downloads_cached")]

        self.cache_dir = Path(config.cache_dir) / domain
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Consumer will be created in start()
        self._consumer = None
        self._consumer_group: str | None = None
        self._running = False

        # Concurrency control
        self._semaphore: asyncio.Semaphore | None = None
        self._in_flight_tasks: set[str] = set()  # Track by trace_id
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: asyncio.Event | None = None

        # Create producer for result messages
        self.results_topic = config.get_topic(domain, "downloads_results")
        self.producer = create_producer(
            config=config,
            domain=domain,
            worker_name=self.WORKER_NAME,
            topic_key="downloads_results",
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
        self._cycle_offset_start_ts = None
        self._cycle_offset_end_ts = None
        self._stats_logger: PeriodicStatsLogger | None = None
        self._stale_cleaner = StaleFileCleaner(
            scan_dir=self.cache_dir,
            in_flight_lock=self._in_flight_lock,
            in_flight_tasks=self._in_flight_tasks,
        )

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
                "topics": self.topics,
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

        # Start health server first for immediate liveness probe response
        await self.health_server.start()

        initialize_worker_telemetry(self.domain, "upload-worker")

        # Initialize concurrency control
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Start producer
        await self.producer.start()

        # Sync topic with producer's actual entity name (Event Hub entity may
        # differ from the Kafka topic name resolved by get_topic()).
        if hasattr(self.producer, "eventhub_name"):
            self.results_topic = self.producer.eventhub_name

        # Initialize storage clients (use injected client or create OneLake clients)
        if self._injected_storage_client is not None:
            # Use injected storage client for this domain
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
                client = OneLakeClient(path, max_pool_size=self.concurrency)
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
                fallback_client = OneLakeClient(self.config.onelake_base_path, max_pool_size=self.concurrency)
                await fallback_client.__aenter__()
                self.onelake_clients["_fallback"] = fallback_client
                logger.info(
                    "Initialized fallback OneLake client",
                    extra={"onelake_base_path": self.config.onelake_base_path},
                )

        # Size the default thread pool to match upload concurrency so
        # asyncio.to_thread calls (OneLake uploads, file I/O) aren't bottlenecked
        # by the default min(32, cpu+4) executor.
        import concurrent.futures

        loop = asyncio.get_running_loop()
        loop.set_default_executor(
            concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency + 4)
        )

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
            topic_key="downloads_cached",
        )

        self._consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
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
        self.health_server.set_ready(transport_connected=True)

        # Update connection status
        update_connection_status("consumer", connected=True)

        await self._stale_cleaner.start()

        logger.info("Upload worker started successfully")

        try:
            await self._consumer.start()
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

        logger.info("Graceful shutdown requested, will stop after current batch completes")
        self._running = False

    async def stop(self) -> None:
        """
        Stop the upload worker and clean up resources.

        Waits for in-flight uploads to complete before stopping.
        Safe to call multiple times. Will clean up resources even if
        request_shutdown() was called first.
        """
        if self._consumer is None and not self.onelake_clients:
            logger.debug("Worker already stopped")
            return

        logger.info("Stopping upload worker...")
        self._running = False

        if self._shutdown_event:
            self._shutdown_event.set()

        await self._close_resource("stats logger", self._stop_stats_logger)
        await self._close_resource("stale cleaner", self._stale_cleaner.stop)
        await self._close_resource("in-flight uploads", self._drain_in_flight)
        await self._close_resource("consumer", self._stop_consumer)
        await self._close_resource("producer", self.producer.stop)
        await self._close_onelake_clients()
        await self._close_resource("health server", self.health_server.stop)

        update_connection_status("consumer", connected=False)
        update_assigned_partitions(self._consumer_group, 0)

        logger.info("Upload worker stopped")

    async def _close_resource(self, name: str, method) -> None:
        try:
            await method()
        except Exception as e:
            logger.error(f"Error stopping {name}", extra={"error": str(e)})

    async def _stop_stats_logger(self) -> None:
        if self._stats_logger:
            await self._stats_logger.stop()

    async def _drain_in_flight(self) -> None:
        await self._wait_for_in_flight(timeout=30.0)

    async def _stop_consumer(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def _close_onelake_clients(self) -> None:
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

    async def _process_batch(self, messages: list[PipelineMessage]) -> bool:
        """Process batch of upload messages concurrently.

        Returns:
            True to commit batch, False to skip commit (circuit breaker errors)
        """
        if self._consumer is None:
            raise RuntimeError("Consumer not initialized - call start() first")
        if self._semaphore is None:
            raise RuntimeError("Semaphore not initialized - call start() first")

        logger.debug("Processing message batch", extra={"batch_size": len(messages)})

        tasks = [asyncio.create_task(self._process_single_with_semaphore(msg)) for msg in messages]
        results: list[UploadResult] = await asyncio.gather(*tasks, return_exceptions=True)

        self._log_batch_exceptions(results)
        return self._check_batch_commit(results)

    def _log_batch_exceptions(self, results: list) -> None:
        topic = self.topics[0]
        for upload_result in results:
            if isinstance(upload_result, Exception):
                logger.error(
                    "Unexpected error in upload",
                    extra={"error": str(upload_result)},
                    exc_info=True,
                )
                record_processing_error(topic, self._consumer_group, "unexpected_error")

    def _check_batch_commit(self, results: list) -> bool:
        circuit_errors = [
            r for r in results
            if isinstance(r, UploadResult)
            and r.error
            and r.error.__class__.__name__ == "CircuitOpenError"
        ]
        if circuit_errors:
            logger.warning(
                "Circuit breaker errors - not committing",
                extra={"count": len(circuit_errors)},
            )
            return False
        return True

    async def _process_single_with_semaphore(self, message: PipelineMessage) -> UploadResult:
        if self._semaphore is None:
            raise RuntimeError("Semaphore not initialized - call start() first")

        async with self._semaphore:
            return await self._process_single_upload(message)

    def _update_cycle_offsets(self, ts) -> None:
        if self._cycle_offset_start_ts is None or ts < self._cycle_offset_start_ts:
            self._cycle_offset_start_ts = ts
        if self._cycle_offset_end_ts is None or ts > self._cycle_offset_end_ts:
            self._cycle_offset_end_ts = ts

    def _resolve_onelake_client(self, cached_message: CachedDownloadMessage):
        """Resolve the OneLake client for the message's domain."""
        domain = cached_message.event_type.lower()
        available_domains = list(self.onelake_clients.keys())

        logger.debug(
            "Domain lookup for upload",
            extra={
                "trace_id": cached_message.trace_id,
                "media_id": cached_message.media_id,
                "raw_event_type": cached_message.event_type,
                "domain": domain,
                "available_domains": available_domains,
            },
        )

        client = self.onelake_clients.get(domain)
        if client is not None:
            return client, domain

        client = self.onelake_clients.get("_fallback")
        if client is None:
            raise ValueError(
                f"No OneLake client configured for domain '{domain}' "
                f"(event_type='{cached_message.event_type}') and no fallback configured. "
                f"Available domains: {available_domains}"
            )
        logger.warning(
            "Using fallback OneLake client - domain not found",
            extra={
                "trace_id": cached_message.trace_id,
                "media_id": cached_message.media_id,
                "raw_event_type": cached_message.event_type,
                "domain": domain,
                "available_domains": available_domains,
            },
        )
        return client, domain

    async def _produce_failure_result(
        self, message: PipelineMessage, cached_message: CachedDownloadMessage | None,
        trace_id: str, error: Exception,
    ) -> None:
        try:
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
                error_message=str(error)[:500],
                created_at=datetime.now(UTC),
            )

            await self.producer.send(key=cached_message.trace_id, value=result_message)
        except Exception as produce_error:
            logger.error(
                "Failed to produce failure result",
                extra={
                    "error": str(produce_error),
                    "trace_id": trace_id,
                    "media_id": cached_message.media_id if cached_message else None,
                },
            )

    @set_log_context_from_message
    async def _process_single_upload(self, message: PipelineMessage) -> UploadResult:
        start_time = time.perf_counter()
        trace_id = "unknown"
        cached_message: CachedDownloadMessage | None = None

        try:
            cached_message = CachedDownloadMessage.model_validate_json(message.value)
            trace_id = cached_message.trace_id

            set_log_context(trace_id=trace_id, media_id=cached_message.media_id)

            self._records_processed += 1
            self._update_cycle_offsets(message.timestamp)

            async with self._in_flight_lock:
                self._in_flight_tasks.add(trace_id)

            record_message_consumed(
                self.topics[0], self._consumer_group, len(message.value), success=True
            )

            cache_path = Path(cached_message.local_cache_path)
            if not cache_path.exists():
                raise FileNotFoundError(f"Cached file not found: {cache_path}")

            onelake_client, domain = self._resolve_onelake_client(cached_message)

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
                    "raw_event_type": cached_message.event_type,
                    "destination_path": cached_message.destination_path,
                    "onelake_base_path": onelake_client.base_path,
                    "blob_path": blob_path,
                    "bytes": cached_message.bytes_downloaded,
                },
            )

            processing_time_ms = int((time.perf_counter() - start_time) * 1000)

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

            await self.producer.send(value=result_message, key=trace_id)
            await self._cleanup_cache_file(cache_path)

            self._records_succeeded += 1
            self._bytes_uploaded += cached_message.bytes_downloaded

            duration = time.perf_counter() - start_time
            message_processing_duration_seconds.labels(
                topic=self.topics[0], consumer_group=self._consumer_group
            ).observe(duration)

            return UploadResult(
                message=message, cached_message=cached_message,
                processing_time_ms=processing_time_ms, success=True,
            )

        except Exception as e:
            processing_time_ms = int((time.perf_counter() - start_time) * 1000)
            self._records_failed += 1

            log_worker_error(
                logger, "Upload failed", error_category="TRANSIENT", exc=e,
                trace_id=trace_id,
                media_id=cached_message.media_id if cached_message else None,
            )
            record_processing_error(self.topics[0], self._consumer_group, "upload_error")

            await self._produce_failure_result(message, cached_message, trace_id, e)

            return UploadResult(
                message=message, cached_message=cached_message,
                processing_time_ms=processing_time_ms, success=False, error=e,
            )

        finally:
            async with self._in_flight_lock:
                self._in_flight_tasks.discard(trace_id)

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
