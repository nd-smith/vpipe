# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
ClaimX download worker for processing download tasks with concurrent processing.

Consumes ClaimXDownloadTask from pending and retry topics,
downloads media files using AttachmentDownloader, caches to local
filesystem, and produces ClaimXCachedDownloadMessage for upload worker.

This implementation follows the xact download worker pattern but adapted for ClaimX:
- Uses ClaimXDownloadTask (media_id, project_id, download_url)
- Downloads from S3 presigned URLs (from API enrichment)
- Caches files locally before upload (decoupled from upload worker)
- Upload worker consumes from claimx.downloads.cached topic

Concurrent Processing:
- Fetches batches of messages from Kafka
- Processes downloads concurrently using asyncio.Semaphore
- Uses HTTP connection pooling via shared aiohttp.ClientSession
- Configurable concurrency via DOWNLOAD_CONCURRENCY (default: 10, max: 50)
- Graceful shutdown waits for in-flight downloads to complete
"""

import asyncio
import base64
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
from core.logging.utilities import format_cycle_output, log_worker_error
from core.download.downloader import AttachmentDownloader
from core.download.models import DownloadTask, DownloadOutcome
from core.errors.exceptions import CircuitOpenError
from core.types import ErrorCategory
from config.config import KafkaConfig
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.claimx.api_client import ClaimXApiClient
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.claimx.retry import DownloadRetryHandler
from kafka_pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from kafka_pipeline.claimx.schemas.tasks import ClaimXDownloadTask
from kafka_pipeline.simulation.config import is_simulation_mode, get_simulation_config
from kafka_pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_connection_status,
    update_assigned_partitions,
)

logger = get_logger(__name__)


@dataclass
class TaskResult:
    message: ConsumerRecord
    task_message: ClaimXDownloadTask
    outcome: DownloadOutcome
    processing_time_ms: int
    success: bool
    error: Optional[Exception] = None


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
        temp_dir: Optional[Path] = None,
        instance_id: Optional[str] = None,
    ):
        self.config = config
        self.domain = domain
        self.instance_id = instance_id

        # Create worker_id with instance suffix (coolname) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        self.temp_dir = temp_dir or Path(tempfile.gettempdir()) / "claimx_download_worker"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.cache_dir = Path(config.cache_dir) / domain
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        processing_config = config.get_worker_config(domain, self.WORKER_NAME, "processing")
        self.concurrency = processing_config.get("concurrency", 10)
        self.batch_size = processing_config.get("batch_size", 20)

        # Only consume from pending topic
        # Unified retry scheduler handles routing retry messages back to pending
        self.topics = [config.get_topic(domain, "downloads_pending")]

        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

        self._semaphore: Optional[asyncio.Semaphore] = None
        self._in_flight_tasks: Set[str] = set()
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: Optional[asyncio.Event] = None

        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: Optional[asyncio.Task] = None

        # Cycle-specific metrics (reset each cycle)
        self._last_cycle_processed = 0
        self._last_cycle_failed = 0

        self._http_session: Optional[aiohttp.ClientSession] = None

        self.producer = BaseKafkaProducer(
            config=config,
            domain=domain,
            worker_name=self.WORKER_NAME,
        )

        self.downloader = AttachmentDownloader()
        self.api_client: Optional[ClaimXApiClient] = None
        self.retry_handler: Optional[DownloadRetryHandler] = None

        # Check if simulation mode is enabled for localhost URL support
        self._simulation_mode_enabled = is_simulation_mode()
        if self._simulation_mode_enabled:
            try:
                self._simulation_config = get_simulation_config()
                logger.info(
                    "Simulation mode enabled - localhost URLs will be allowed",
                    extra={
                        "allow_localhost_urls": self._simulation_config.allow_localhost_urls,
                    },
                )
            except RuntimeError:
                # Simulation mode check indicated true but config failed to load
                # Fall back to disabled for safety
                self._simulation_mode_enabled = False
                self._simulation_config = None
                logger.warning("Simulation mode check failed, localhost URLs will be blocked")
        else:
            self._simulation_config = None

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

        # Initialize telemetry
        from kafka_pipeline.common.telemetry import initialize_telemetry
        import os

        initialize_telemetry(
            service_name=f"{self.domain}-download-worker",
            environment=os.getenv("ENVIRONMENT", "development"),
        )

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

        if self._simulation_mode_enabled:
            # In simulation mode, use mock API client (doesn't need real credentials)
            from kafka_pipeline.simulation.claimx_api_mock import MockClaimXAPIClient

            self.api_client = MockClaimXAPIClient(fixtures_dir=self._simulation_config.fixtures_dir)
            logger.info("Using MockClaimXAPIClient for simulation mode")
        else:
            self.api_client = ClaimXApiClient(
                base_url=self.config.claimx_api_url or "https://api.test.claimxperience.com",
                token=self.config.claimx_api_token,
                timeout_seconds=self.config.claimx_api_timeout_seconds,
                max_concurrent=self.config.claimx_api_concurrency,
            )

        self.retry_handler = DownloadRetryHandler(
            config=self.config,
            producer=self.producer,
            api_client=self.api_client,
        )

        await self._create_consumer()

        self._running = True

        self._cycle_task = asyncio.create_task(self._periodic_cycle_output())

        api_reachable = not self.api_client.is_circuit_open
        self.health_server.set_ready(
            kafka_connected=True,
            api_reachable=api_reachable,
            circuit_open=self.api_client.is_circuit_open,
        )

        update_connection_status("consumer", connected=True)
        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        partition_count = len(self._consumer.assignment()) if self._consumer else 0
        update_assigned_partitions(consumer_group, partition_count)

        logger.info(
            "ClaimX download worker started successfully",
            extra={
                "topics": self.topics,
                "partitions": partition_count,
            },
        )

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
        consumer_config_dict = self.config.get_worker_config(
            self.domain, self.WORKER_NAME, "consumer"
        )

        consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": self.config.get_consumer_group(self.domain, self.WORKER_NAME),
            "client_id": (
                f"{self.domain}-{self.WORKER_NAME.replace('_', '-')}-{self.instance_id}"
                if self.instance_id
                else f"{self.domain}-{self.WORKER_NAME.replace('_', '-')}"
            ),
            "enable_auto_commit": False,
            "auto_offset_reset": consumer_config_dict.get("auto_offset_reset", "earliest"),
            "max_poll_records": self.batch_size,
            "max_poll_interval_ms": consumer_config_dict.get("max_poll_interval_ms", 300000),
            "session_timeout_ms": consumer_config_dict.get("session_timeout_ms", 60000),
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        if "heartbeat_interval_ms" in consumer_config_dict:
            consumer_config["heartbeat_interval_ms"] = consumer_config_dict["heartbeat_interval_ms"]
        if "fetch_min_bytes" in consumer_config_dict:
            consumer_config["fetch_min_bytes"] = consumer_config_dict["fetch_min_bytes"]
        if "fetch_max_wait_ms" in consumer_config_dict:
            consumer_config["fetch_max_wait_ms"] = consumer_config_dict["fetch_max_wait_ms"]

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
        """Request graceful shutdown after current batch completes."""
        if not self._running:
            logger.debug("Worker not running, shutdown request ignored")
            return

        logger.info("Graceful shutdown requested, will stop after current batch completes")
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
            try:
                await self._cycle_task
            except asyncio.CancelledError:
                pass

        if self._shutdown_event:
            self._shutdown_event.set()

        await self._wait_for_in_flight(timeout=30.0)

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

        if self._http_session:
            await self._http_session.close()
            self._http_session = None

        await self.producer.stop()

        if self.api_client:
            await self.api_client.close()
            self.api_client = None

        self.retry_handler = None

        await self.health_server.stop()

        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        update_connection_status("consumer", connected=False)
        update_assigned_partitions(consumer_group, 0)
        # Note: update_downloads_batch_size metric not implemented

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

    async def _consume_batch_loop(self) -> None:
        logger.info("Starting batch consumption loop")

        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)

        _logged_waiting_for_assignment = False
        _logged_assignment_received = False

        while self._running and self._consumer:
            try:
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
                    await asyncio.sleep(0.5)
                    continue

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
                    update_assigned_partitions(consumer_group, len(assignment))

                data = await self._consumer.getmany(
                    timeout_ms=1000,
                    max_records=self.batch_size,
                )

                if not data:
                    continue

                messages: List[ConsumerRecord] = []
                for topic_partition, records in data.items():
                    messages.extend(records)

                if not messages:
                    continue

                # Note: update_downloads_batch_size metric not implemented

                logger.debug(
                    "Processing message batch",
                    extra={
                        "batch_size": len(messages),
                        "download_concurrency": self.concurrency,
                    },
                )

                results = await self._process_batch(messages)

                should_commit = await self._handle_batch_results(results)

                if should_commit:
                    await self._consumer.commit()
                    logger.debug(
                        "Committed offsets for batch",
                        extra={"batch_size": len(messages)},
                    )

                # Note: update_downloads_batch_size metric not implemented

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
        async def bounded_process(message: ConsumerRecord) -> TaskResult:
            async with self._semaphore:
                return await self._process_single_task(message)

        tasks = [bounded_process(msg) for msg in messages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed_results: List[TaskResult] = []
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

        logger.debug(
            "Batch processing complete",
            extra={
                "batch_size": len(messages),
                "records_succeeded": succeeded,
                "records_failed": failed,
                "records_errored": errors,
            },
        )

        self._records_succeeded += succeeded
        self._records_failed += failed
        self._records_failed += errors  # Count errors as failed too

        return [r for r in processed_results if r is not None]

    async def _process_single_task(self, message: ConsumerRecord) -> TaskResult:
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

            from kafka_pipeline.common.telemetry import get_tracer

            tracer = get_tracer(__name__)
            with tracer.start_active_span("download.execute") as scope:
                span = scope.span if hasattr(scope, "span") else scope
                span.set_tag("span.kind", "client")
                span.set_tag("media.id", task_message.media_id)
                span.set_tag("project.id", task_message.project_id)
                span.set_tag("event.id", task_message.source_event_id)
                outcome = await self.downloader.download(download_task)

            processing_time_ms = int((time.perf_counter() - start_time) * 1000)

            consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
            duration = time.perf_counter() - start_time
            # Note: message_processing_duration_seconds metric not implemented
            # Note: claim_processing_seconds metric not implemented

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

    async def _handle_batch_results(self, results: List[TaskResult]) -> bool:
        """Returns False if circuit breaker errors present (don't commit, will reprocess)."""
        circuit_errors = [r for r in results if r.error and isinstance(r.error, CircuitOpenError)]

        if circuit_errors:
            logger.warning(
                "Circuit breaker errors in batch - not committing offsets",
                extra={"circuit_error_count": len(circuit_errors)},
            )
            return False

        return True

    def _convert_to_download_task(self, task_message: ClaimXDownloadTask) -> DownloadTask:
        """Creates temp file path using media_id directory to avoid concurrent download conflicts."""
        destination_filename = Path(task_message.blob_path).name
        temp_file = self.temp_dir / task_message.media_id / destination_filename

        # Determine if localhost URLs should be allowed based on simulation config
        allow_localhost = False
        if self._simulation_mode_enabled and self._simulation_config:
            allow_localhost = self._simulation_config.allow_localhost_urls

        return DownloadTask(
            url=task_message.download_url,
            destination=temp_file,
            timeout=60,
            validate_url=True,
            validate_file_type=True,
            allow_localhost=allow_localhost,
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

                    # Calculate cycle-specific deltas
                    processed_cycle = self._records_processed - self._last_cycle_processed
                    errors_cycle = self._records_failed - self._last_cycle_failed

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
        assert outcome.file_path is not None, "File path missing in successful outcome"

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

        if outcome.bytes_downloaded:
            media_type = "unknown"
            if task_message.file_type:
                file_type_lower = task_message.file_type.lower()
                if (
                    "image" in file_type_lower
                    or "jpg" in file_type_lower
                    or "png" in file_type_lower
                ):
                    media_type = "image"
                elif (
                    "video" in file_type_lower
                    or "mp4" in file_type_lower
                    or "mov" in file_type_lower
                ):
                    media_type = "video"
                elif "pdf" in file_type_lower or "doc" in file_type_lower:
                    media_type = "document"
            elif outcome.content_type:
                content_type_lower = outcome.content_type.lower()
                if "image" in content_type_lower:
                    media_type = "image"
                elif "video" in content_type_lower:
                    media_type = "video"
                elif "pdf" in content_type_lower or "document" in content_type_lower:
                    media_type = "document"

            # Note: claim_media_bytes_total metric not implemented

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
            downloaded_at=datetime.now(timezone.utc),
        )

        cached_topic = self.config.get_topic(self.domain, "downloads_cached")
        await self.producer.send(
            topic=cached_topic,
            key=task_message.source_event_id,
            value=cached_message,
        )

        logger.debug(
            "Produced ClaimX cached download message",
            extra={
                "media_id": task_message.media_id,
                "topic": cached_topic,
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
        assert self.retry_handler is not None, "RetryHandler not initialized"

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
    def is_running(self):
        return self._running

    @property
    def in_flight_count(self):
        return len(self._in_flight_tasks)


__all__ = ["ClaimXDownloadWorker"]
