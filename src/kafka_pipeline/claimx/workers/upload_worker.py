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
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord

from core.auth.kafka_oauth import create_kafka_oauth_callback
from core.logging.setup import get_logger
from config.config import KafkaConfig
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.claimx.monitoring import HealthCheckServer
from kafka_pipeline.claimx.schemas.cached import ClaimXCachedDownloadMessage
from kafka_pipeline.claimx.schemas.results import ClaimXUploadResultMessage
from kafka_pipeline.common.storage import OneLakeClient
from kafka_pipeline.common.metrics import (
    record_message_consumed,
    record_processing_error,
    update_connection_status,
    update_assigned_partitions,
    update_consumer_lag,
    update_consumer_offset,
    message_processing_duration_seconds,
)

logger = get_logger(__name__)


@dataclass
class UploadResult:
    """Result of processing a single upload task."""
    message: ConsumerRecord
    cached_message: ClaimXCachedDownloadMessage
    processing_time_ms: int
    success: bool
    error: Optional[Exception] = None


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

    def __init__(self, config: KafkaConfig, domain: str = "claimx"):
        """
        Initialize ClaimX upload worker.

        Args:
            config: Kafka configuration
            domain: Domain identifier (default: "claimx")

        Raises:
            ValueError: If no OneLake path is configured for claimx domain
        """
        self.config = config
        self.domain = domain

        # Validate OneLake configuration for claimx domain
        if not config.onelake_domain_paths and not config.onelake_base_path:
            raise ValueError(
                "OneLake path configuration required. Set either:\n"
                "  - onelake_domain_paths in config.yaml (preferred), or\n"
                "  - ONELAKE_CLAIMX_PATH env var, or\n"
                "  - ONELAKE_BASE_PATH env var (fallback for all domains)"
            )

        # Get worker-specific processing config
        processing_config = config.get_worker_config(domain, self.WORKER_NAME, "processing")
        self.concurrency = processing_config.get("concurrency", 10)
        self.batch_size = processing_config.get("batch_size", 20)

        # Topic to consume from
        self.topic = config.get_topic(domain, "downloads_cached")
        self.results_topic = config.get_topic(domain, "downloads_results")

        # Consumer will be created in start()
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

        # Concurrency control
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._in_flight_tasks: Set[str] = set()  # Track by media_id
        self._in_flight_lock = asyncio.Lock()
        self._shutdown_event: Optional[asyncio.Event] = None
        
        # Cycle output tracking
        self._records_processed = 0
        self._records_succeeded = 0
        self._records_failed = 0
        self._records_skipped = 0
        self._last_cycle_log = time.monotonic()
        self._cycle_count = 0
        self._cycle_task: Optional[asyncio.Task] = None

        # Create producer for result messages
        self.producer = BaseKafkaProducer(
            config=config,
            domain=domain,
            worker_name=self.WORKER_NAME,
        )

        # OneLake client (lazy initialized in start())
        self.onelake_client: Optional[OneLakeClient] = None

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
                "worker_name": self.WORKER_NAME,
                "consumer_group": config.get_consumer_group(domain, self.WORKER_NAME),
                "topic": self.topic,
                "results_topic": self.results_topic,
                "upload_concurrency": self.concurrency,
                "upload_batch_size": self.batch_size,
            },
        )

    async def start(self) -> None:
        """
        Start the upload worker with concurrent processing.

        Begins consuming messages from cached topic.
        Processes messages in concurrent batches.
        Runs until stop() is called or error occurs.

        Raises:
            Exception: If consumer or producer fails to start
        """
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

        # Start health check server first
        await self.health_server.start()

        # Initialize concurrency control
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._shutdown_event = asyncio.Event()
        self._in_flight_tasks = set()

        # Start producer
        await self.producer.start()

        # Initialize OneLake client for claimx domain
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

        self.onelake_client = OneLakeClient(onelake_path)
        await self.onelake_client.__aenter__()
        logger.info(
            "Initialized OneLake client for claimx domain",
            extra={
                "domain": self.domain,
                "onelake_path": onelake_path,
            },
        )

        # Create Kafka consumer
        await self._create_consumer()

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
            logger.error(f"ClaimX upload worker error: {e}", exc_info=True)
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
        # Check if already fully stopped
        if self._consumer is None and self.onelake_client is None:
            logger.debug("Worker already stopped")
            return

        logger.info("Stopping ClaimX upload worker...")
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

        # Wait for in-flight uploads (with timeout)
        if self._in_flight_tasks:
            logger.info(
                f"Waiting for {len(self._in_flight_tasks)} in-flight uploads to complete..."
            )
            wait_start = time.time()
            while self._in_flight_tasks and (time.time() - wait_start) < 30:
                await asyncio.sleep(0.5)

            if self._in_flight_tasks:
                logger.warning(
                    f"Forcing shutdown with {len(self._in_flight_tasks)} uploads still in progress"
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
                logger.warning(f"Error closing OneLake client: {e}")
            finally:
                self.onelake_client = None

        # Update metrics
        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
        update_connection_status("consumer", connected=False)
        update_assigned_partitions(consumer_group, 0)

        logger.info("ClaimX upload worker stopped")

    async def _create_consumer(self) -> None:
        """Create and start Kafka consumer."""
        # Get worker-specific consumer config (merged with defaults)
        consumer_config_dict = self.config.get_worker_config(self.domain, self.WORKER_NAME, "consumer")
        
        consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": self.config.get_consumer_group(self.domain, self.WORKER_NAME),
            "auto_offset_reset": consumer_config_dict.get("auto_offset_reset", "earliest"),
            "enable_auto_commit": False,
            "max_poll_records": self.batch_size,
            "session_timeout_ms": consumer_config_dict.get("session_timeout_ms", 60000),
            "max_poll_interval_ms": consumer_config_dict.get("max_poll_interval_ms", 300000),
            # Connection timeout settings
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        # Add optional consumer settings if present in worker config
        if "heartbeat_interval_ms" in consumer_config_dict:
            consumer_config["heartbeat_interval_ms"] = consumer_config_dict["heartbeat_interval_ms"]
        if "fetch_min_bytes" in consumer_config_dict:
            consumer_config["fetch_min_bytes"] = consumer_config_dict["fetch_min_bytes"]
        if "fetch_max_wait_ms" in consumer_config_dict:
            consumer_config["fetch_max_wait_ms"] = consumer_config_dict["fetch_max_wait_ms"]

        # Add security configuration
        if self.config.security_protocol != "PLAINTEXT":
            consumer_config["security_protocol"] = self.config.security_protocol
            consumer_config["sasl_mechanism"] = self.config.sasl_mechanism

            if self.config.sasl_mechanism == "OAUTHBEARER":
                consumer_config["sasl_oauth_token_provider"] = create_kafka_oauth_callback()
            elif self.config.sasl_mechanism == "PLAIN":
                consumer_config["sasl_plain_username"] = self.config.sasl_plain_username
                consumer_config["sasl_plain_password"] = self.config.sasl_plain_password

        self._consumer = AIOKafkaConsumer(self.topic, **consumer_config)
        await self._consumer.start()

        logger.info(
            "Consumer started",
            extra={
                "topic": self.topic,
                "consumer_group": self.config.get_consumer_group(self.domain, self.WORKER_NAME),
            },
        )

    async def _consume_batch_loop(self) -> None:
        """Main consumption loop with batch processing."""
        assert self._consumer is not None

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
                batch: Dict[str, List[ConsumerRecord]] = await self._consumer.getmany(
                    timeout_ms=1000,
                    max_records=self.batch_size,
                )

                if not batch:
                    continue

                # Flatten messages from all partitions
                messages = []
                for topic_partition, records in batch.items():
                    messages.extend(records)

                if messages:
                    await self._process_batch(messages)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consume loop: {e}", exc_info=True)
                record_processing_error(self.topic, consumer_group, "consume_error")
                await asyncio.sleep(1)  # Brief pause before retry  # Brief pause before retry

    async def _process_batch(self, messages: List[ConsumerRecord]) -> None:
        """Process a batch of messages concurrently."""
        assert self._consumer is not None
        assert self._semaphore is not None

        consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)

        logger.debug(f"Processing batch of {len(messages)} messages")

        # Process all messages concurrently
        tasks = [
            asyncio.create_task(self._process_single_with_semaphore(msg))
            for msg in messages
        ]

        results: List[UploadResult] = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Unexpected error in upload: {result}", exc_info=True)
                record_processing_error(self.topic, consumer_group, "unexpected_error")

        # Commit offsets after batch
        try:
            await self._consumer.commit()
        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}", exc_info=True)

    async def _process_single_with_semaphore(self, message: ConsumerRecord) -> UploadResult:
        """Process single message with semaphore for concurrency control."""
        assert self._semaphore is not None

        async with self._semaphore:
            return await self._process_single_upload(message)

    async def _process_single_upload(self, message: ConsumerRecord) -> UploadResult:
        """
        Process a single cached download message.
        """
        start_time = time.time()
        media_id = "unknown"

        try:
            # Parse message
            cached_message = ClaimXCachedDownloadMessage.model_validate_json(message.value)
            media_id = cached_message.media_id

            # Track in-flight
            async with self._in_flight_lock:
                self._in_flight_tasks.add(media_id)

            consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
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
            assert self.onelake_client is not None
            blob_path = await self.onelake_client.upload_file(
                relative_path=cached_message.destination_path,
                local_path=cache_path,
                overwrite=True,
            )

            # Calculate processing time
            processing_time_ms = int((time.time() - start_time) * 1000)

            logger.info(
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
                    "records_succeeded": self._records_succeeded,
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
                created_at=datetime.now(timezone.utc),
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
            error_extra = {
                "media_id": media_id,
                "processing_time_ms": processing_time_ms,
                "error_category": "permanent",  # Upload failures are typically permanent
            }

            # Add correlation_id and project_id if cached_message was parsed
            if 'cached_message' in locals() and cached_message is not None:
                error_extra["correlation_id"] = cached_message.source_event_id
                error_extra["project_id"] = cached_message.project_id

            logger.error(
                f"Upload failed: {e}",
                extra=error_extra,
                exc_info=True,
            )
            consumer_group = self.config.get_consumer_group(self.domain, self.WORKER_NAME)
            record_processing_error(self.topic, consumer_group, "upload_error")
            self._records_failed += 1

            # For upload failures, we produce a failure result
            # The file stays in cache for manual review/retry
            try:
                # Re-parse message in case it wasn't parsed yet
                if 'cached_message' not in locals() or cached_message is None:
                    cached_message = ClaimXCachedDownloadMessage.model_validate_json(message.value)

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
                    created_at=datetime.now(timezone.utc),
                )

                # Use source_event_id as key for consistent partitioning across all ClaimX topics
                await self.producer.send(
                    topic=self.results_topic,
                    key=cached_message.source_event_id,
                    value=result_message,
                )
            except Exception as produce_error:
                logger.error(f"Failed to produce failure result: {produce_error}")

            return UploadResult(
                message=message,
                cached_message=cached_message if 'cached_message' in locals() else None,
                processing_time_ms=processing_time_ms,
                success=False,
                error=e,
            )

        finally:
            # Remove from in-flight tracking
            async with self._in_flight_lock:
                self._in_flight_tasks.discard(media_id)

    async def _periodic_cycle_output(self) -> None:
        """
        Background task for periodic cycle logging.
        """
        logger.info(
            "Cycle 0: processed=0 (succeeded=0, failed=0, skipped=0), pending=0 "
            "[cycle output every %ds]",
            30,
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
                        f"Cycle {self._cycle_count}: processed={self._records_processed} "
                        f"(succeeded={self._records_succeeded}, failed={self._records_failed}, "
                        f"skipped={self._records_skipped}), in_flight={in_flight}",
                        extra={
                            "cycle": self._cycle_count,
                            "records_processed": self._records_processed,
                            "records_succeeded": self._records_succeeded,
                            "records_failed": self._records_failed,
                            "records_skipped": self._records_skipped,
                            "in_flight": in_flight,
                            "cycle_interval_seconds": 30,
                        },
                    )

        except asyncio.CancelledError:
            logger.debug("Periodic cycle output task cancelled")
            raise

    async def _cleanup_cache_file(self, cache_path: Path) -> None:
        """Clean up cached file and its parent directory if empty."""
        try:
            # Delete the file
            if cache_path.exists():
                await asyncio.to_thread(os.remove, str(cache_path))

            # Try to remove parent directory if empty
            parent = cache_path.parent
            if parent.exists():
                try:
                    await asyncio.to_thread(parent.rmdir)
                except OSError:
                    pass  # Directory not empty

            logger.debug(f"Cleaned up cache file: {cache_path}")

        except Exception as e:
            logger.warning(f"Failed to clean up cache file {cache_path}: {e}")
