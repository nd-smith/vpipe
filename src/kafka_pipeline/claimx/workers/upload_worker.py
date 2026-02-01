"""
ClaimX upload worker for uploading cached downloads to OneLake.

Consumes ClaimXCachedDownloadMessage from claimx.downloads.cached topic,
uploads files to OneLake, and produces ClaimXUploadResultMessage.

This worker is decoupled from the Download Worker to allow:
- Independent scaling of download vs upload workers
- Downloads not blocked by slow OneLake uploads
- Cache buffer if OneLake has temporary issues

Architecture:
- Download Worker: downloads → local cache → ClaimXCachedDownloadMessage
- Upload Worker: ClaimXCachedDownloadMessage → OneLake → ClaimXUploadResultMessage
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
from aiokafka.structs import ConsumerRecord as AIOConsumerRecord

from config.config import KafkaConfig
from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging.setup import get_logger
from core.logging.utilities import format_cycle_output, log_worker_error
from kafka_pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from kafka_pipeline.claimx.schemas.results import ClaimXUploadResultMessage
from kafka_pipeline.common.health import HealthCheckServer
from kafka_pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_assigned_partitions,
    update_connection_status,
)
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.common.storage import OneLakeClient
from kafka_pipeline.common.types import PipelineMessage, from_consumer_record

logger = get_logger(__name__)


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

        # Create worker_id with instance suffix (coolname) if provided
        if instance_id:
            self.worker_id = f"{self.WORKER_NAME}-{instance_id}"
        else:
            self.worker_id = self.WORKER_NAME

        # Store injected storage client (for simulation mode)
        self._injected_storage_client = storage_client

        if storage_client is not None:
            logger.info(
                "Using injected storage client (simulation mode)",
                extra={"storage_type": type(storage_client).__name__},
            )

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

        # Topic to consume from
        self.topic = config.get_topic(domain, "downloads_cached")
        self.results_topic = config.get_topic(domain, "downloads_results")

        # Consumer will be created in start()
        self._consumer: AIOKafkaConsumer | None = None
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
        self.producer = BaseKafkaProducer(
            config=config,
            domain=domain,
            worker_name=self.WORKER_NAME,
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
                "topic": self.topic,
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

        from kafka_pipeline.common.telemetry import initialize_worker_telemetry

        initialize_worker_telemetry(self.domain, "upload-worker")

        # Start health check server first
        await self.health_server.start()

        # Initialize concurrency control
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Start producer
        await self.producer.start()

        # Initialize storage client (use injected client or create OneLake client)
        if self._injected_storage_client is not None:
            # Use injected storage client (simulation mode)
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

        # Create Kafka consumer with cleanup on failure
        try:
            await self._create_consumer()
        except Exception as e:
            logger.error(
                "Failed to create Kafka consumer",
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

        # Update connection status
        update_connection_status("consumer", connected=True)

        logger.info("ClaimX upload worker started successfully")

        try:
            await self._consume_batch_loop()
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
            await self._consumer.stop()
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

    async def _create_consumer(self) -> None:
        # Get worker-specific consumer config (merged with defaults)
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
            "auto_offset_reset": consumer_config_dict.get(
                "auto_offset_reset", "earliest"
            ),
            "enable_auto_commit": False,
            "max_poll_records": self.batch_size,
            "session_timeout_ms": consumer_config_dict.get("session_timeout_ms", 60000),
            "max_poll_interval_ms": consumer_config_dict.get(
                "max_poll_interval_ms", 300000
            ),
            # Connection timeout settings
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        # Add optional consumer settings if present in worker config
        if "heartbeat_interval_ms" in consumer_config_dict:
            consumer_config["heartbeat_interval_ms"] = consumer_config_dict[
                "heartbeat_interval_ms"
            ]
        if "fetch_min_bytes" in consumer_config_dict:
            consumer_config["fetch_min_bytes"] = consumer_config_dict["fetch_min_bytes"]
        if "fetch_max_wait_ms" in consumer_config_dict:
            consumer_config["fetch_max_wait_ms"] = consumer_config_dict[
                "fetch_max_wait_ms"
            ]

        # Add security configuration
        if self.config.security_protocol != "PLAINTEXT":
            consumer_config["security_protocol"] = self.config.security_protocol
            consumer_config["sasl_mechanism"] = self.config.sasl_mechanism

            if self.config.sasl_mechanism == "OAUTHBEARER":
                consumer_config["sasl_oauth_token_provider"] = (
                    create_kafka_oauth_callback()
                )
            elif self.config.sasl_mechanism == "PLAIN":
                consumer_config["sasl_plain_username"] = self.config.sasl_plain_username
                consumer_config["sasl_plain_password"] = self.config.sasl_plain_password

        self._consumer = AIOKafkaConsumer(self.topic, **consumer_config)
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
                batch: dict[str, list[AIOConsumerRecord]] = (
                    await self._consumer.getmany(
                        timeout_ms=1000,
                        max_records=self.batch_size,
                    )
                )

                if not batch:
                    continue

                # Flatten messages from all partitions and convert to PipelineMessage
                messages = []
                for _topic_partition, records in batch.items():
                    for record in records:
                        messages.append(from_consumer_record(record))

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
        """
        Process a batch of messages concurrently.

        CRITICAL (Issue #38): Verifies all uploads succeeded before committing offsets.
        Failed uploads are tracked and offsets are not committed for those messages.
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
                record_processing_error(self.topic, consumer_group, "unexpected_error")
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
            try:
                await self._consumer.commit()
                logger.debug(
                    "Committed offsets after successful batch",
                    extra={
                        "batch_size": len(messages),
                        "success_count": success_count,
                    },
                )
            except Exception as e:
                logger.error(
                    "Failed to commit offsets", extra={"error": str(e)}, exc_info=True
                )
        else:
            logger.warning(
                "Skipping offset commit due to upload failures in batch",
                extra={
                    "batch_size": len(messages),
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "exception_count": exception_count,
                },
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
                self.topic, consumer_group, len(message.value), success=True
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
                topic=self.results_topic,
                key=cached_message.source_event_id,
                value=result_message,
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
            record_processing_error(self.topic, consumer_group, "upload_error")
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

                    # Calculate cycle-specific deltas
                    (
                        self._records_processed - self._last_cycle_processed
                    )
                    self._records_failed - self._last_cycle_failed

                    # Use standardized cycle output format
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
