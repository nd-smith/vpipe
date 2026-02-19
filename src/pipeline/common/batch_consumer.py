"""Message batch consumer for high-throughput concurrent processing.

Wrapper around AIOKafkaConsumer that provides batch-concurrent message processing.
Unlike MessageConsumer (which processes messages one-at-a-time), this consumer
fetches batches and delegates to a batch_handler for concurrent processing.

Designed for I/O-bound workers (downloads, uploads) that benefit from processing
multiple messages concurrently.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer

from config.config import MessageConfig
from pipeline.common.kafka_config import build_kafka_security_config
from pipeline.common.metrics import (
    update_assigned_partitions,
    update_connection_status,
)
from pipeline.common.types import BatchResult, PipelineMessage, from_consumer_record

logger = logging.getLogger(__name__)


class MessageBatchConsumer:
    """Message batch consumer for concurrent message processing.

    Fetches batches of messages using getmany() and processes them concurrently
    for high-throughput I/O operations.

    Batch Handler Contract:
    - Receives list[PipelineMessage] (1 to batch_size messages)
    - Returns True to commit batch, False to skip (reprocess)
    - If raises exception, batch is NOT committed (messages redelivered)
    - Handler is responsible for concurrent processing (e.g., asyncio.gather + semaphore)

    Commit Strategy:
    - All-or-nothing: commit all offsets after handler returns True
    - Skip commit if handler returns False or raises exception
    - Messages are redelivered on next poll (at-least-once semantics)
    """

    def __init__(
        self,
        config: MessageConfig,
        domain: str,
        worker_name: str,
        topics: list[str],
        batch_handler: Callable[[list[PipelineMessage]], Awaitable[bool]],
        **kwargs,
    ):
        """Initialize message batch consumer.

        Args:
            config: MessageConfig with connection details
            domain: Pipeline domain (e.g., "verisk", "claimx")
            worker_name: Worker name for logging
            topics: List of topics to consume
            batch_handler: Async function that processes message batches
            **kwargs: Optional overrides:
                batch_size (int): Target batch size (default: 20)
                max_batch_size (int): Upper bound for max_poll_records (default: batch_size)
                batch_timeout_ms (int): Timeout for getmany (default: 1000ms)
                enable_message_commit (bool): Whether to commit after processing (default: True)
                instance_id (str): Instance identifier for parallel consumers
        """
        if not topics:
            raise ValueError("At least one topic must be specified")

        self.config = config
        self.domain = domain
        self.worker_name = worker_name
        self.instance_id = kwargs.get("instance_id")
        self.topics = topics
        self.batch_handler = batch_handler
        batch_size = kwargs.get("batch_size", 20)
        self.batch_size = batch_size
        self.max_batch_size = kwargs.get("max_batch_size") or batch_size
        self.batch_timeout_ms = kwargs.get("batch_timeout_ms", 1000)
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False
        self._enable_message_commit = kwargs.get("enable_message_commit", True)

        # DLQ producer for routing permanent failures from batch handlers
        from core.utils import generate_worker_id
        from pipeline.common.dlq.producer import DLQProducer

        prefix = f"{domain}-{worker_name}"
        if self.instance_id:
            prefix = f"{prefix}-{self.instance_id}"
        self._worker_id = generate_worker_id(prefix)

        self._dlq_producer = DLQProducer(
            config=config,
            domain=domain,
            worker_name=worker_name,
            group_id=config.get_consumer_group(domain, worker_name),
            worker_id=self._worker_id,
        )

        self.consumer_config: dict = {}
        self.group_id = config.get_consumer_group(domain, worker_name)

        self.max_batches = None
        self._batch_count = 0

        logger.info(
            "Initialized message batch consumer",
            extra={
                "domain": domain,
                "worker_name": worker_name,
                "topics": topics,
                "group_id": self.group_id,
                "batch_size": batch_size,
                "batch_timeout_ms": self.batch_timeout_ms,
                "bootstrap_servers": config.bootstrap_servers,
                "max_batches": self.max_batches,
                "enable_message_commit": self._enable_message_commit,
            },
        )

    # Optional consumer config keys forwarded to AIOKafkaConsumer if present
    _OPTIONAL_CONSUMER_KEYS = (
        "heartbeat_interval_ms",
        "fetch_min_bytes",
        "fetch_max_wait_ms",
        "partition_assignment_strategy",
    )

    def _build_kafka_config(self) -> dict:
        """Build the AIOKafkaConsumer configuration dict."""
        client_id = f"{self.domain}-{self.worker_name}"
        if self.instance_id:
            client_id = f"{client_id}-{self.instance_id}"

        cfg = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": self.group_id,
            "client_id": client_id,
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
            "enable_auto_commit": self.consumer_config.get("enable_auto_commit", False),
            "auto_offset_reset": self.consumer_config.get("auto_offset_reset", "earliest"),
            "max_poll_records": self.max_batch_size,
            "max_poll_interval_ms": self.consumer_config.get("max_poll_interval_ms", 300000),
            "session_timeout_ms": self.consumer_config.get("session_timeout_ms", 30000),
        }

        for key in self._OPTIONAL_CONSUMER_KEYS:
            if key in self.consumer_config:
                cfg[key] = self.consumer_config[key]

        cfg.update(build_kafka_security_config(self.config))
        return cfg

    async def _log_starting_offsets(self) -> None:
        """Log partition offsets on startup for crash recovery visibility."""
        assignment = self._consumer.assignment()
        update_assigned_partitions(self.group_id, len(assignment))

        partition_offsets = {}
        for tp in assignment:
            try:
                pos = await self._consumer.position(tp)
                partition_offsets[f"{tp.topic}:{tp.partition}"] = pos
            except Exception:
                partition_offsets[f"{tp.topic}:{tp.partition}"] = "unknown"

        logger.info(
            "Message batch consumer started successfully",
            extra={
                "topics": self.topics,
                "group_id": self.group_id,
                "partitions": len(assignment),
                "resuming_from_offsets": partition_offsets,
            },
        )

    async def start(self) -> None:
        """Start the message batch consumer."""
        if self._running:
            logger.warning("Batch consumer already running, ignoring duplicate start call")
            return

        logger.info("Starting message batch consumer", extra={"topics": self.topics, "group_id": self.group_id, "batch_size": self.batch_size})

        self._consumer = AIOKafkaConsumer(*self.topics, **self._build_kafka_config())
        await self._consumer.start()
        self._running = True

        update_connection_status("consumer", connected=True)
        await self._log_starting_offsets()

        try:
            await self._consume_loop()
        except asyncio.CancelledError:
            logger.info("Batch consumer loop cancelled, shutting down")
            raise
        except Exception as e:
            logger.error("Batch consumer loop terminated with error", extra={"error": str(e)}, exc_info=True)
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        """Stop the message batch consumer."""
        if not self._running or self._consumer is None:
            logger.debug("Batch consumer not running or already stopped")
            return

        logger.info("Stopping message batch consumer")
        self._running = False

        try:
            if self._consumer:
                await self._consumer.commit()
                await self._consumer.stop()

            await self._dlq_producer.stop()

            logger.info("Message batch consumer stopped successfully")
        except Exception as e:
            logger.error(
                "Error stopping message batch consumer",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise
        finally:
            update_connection_status("consumer", connected=False)
            update_assigned_partitions(self.group_id, 0)
            self._consumer = None

    async def commit(self) -> None:
        """Commit offsets for processed messages.

        For batch consumer, this is called after successful batch processing.
        """
        if self._consumer is None:
            logger.warning("Cannot commit: consumer not started")
            return

        await self._consumer.commit()
        logger.debug(
            "Committed offsets",
            extra={"group_id": self.group_id},
        )

    def _collect_messages(self, data: dict) -> list[PipelineMessage]:
        """Flatten partition data from getmany() into a single message list."""
        messages: list[PipelineMessage] = []
        for partition_messages in data.values():
            messages.extend([from_consumer_record(record) for record in partition_messages])
        return messages

    async def _flush_batch(self, messages: list[PipelineMessage]) -> None:
        """Run the batch handler and commit offsets on success."""
        start_time = time.perf_counter()

        try:
            result = await self.batch_handler(messages)

            # Backward compat: bool return = existing behavior
            if isinstance(result, bool):
                should_commit = result
                permanent_failures: list[tuple[PipelineMessage, Exception]] = []
            else:
                should_commit = result.commit
                permanent_failures = result.permanent_failures

            duration = time.perf_counter() - start_time

            # Route permanent failures to DLQ
            from core.types import ErrorCategory

            for msg, error in permanent_failures:
                try:
                    await self._dlq_producer.send(msg, error, ErrorCategory.PERMANENT)
                except Exception:
                    logger.error("DLQ routing failed for message", exc_info=True)
                    should_commit = False  # Don't advance past un-DLQ'd message

            if should_commit and self._enable_message_commit:
                await self._consumer.commit()
                logger.info(
                    "Batch committed",
                    extra={
                        "batch_size": len(messages),
                        "duration_ms": round(duration * 1000, 2),
                        "dlq_routed": len(permanent_failures),
                    },
                )
            else:
                logger.info(
                    "Batch commit skipped (handler returned False) - messages will be redelivered",
                    extra={
                        "batch_size": len(messages),
                        "duration_ms": round(duration * 1000, 2),
                    },
                )

        except Exception as e:
            duration = time.perf_counter() - start_time

            logger.error(
                "Batch processing failed - messages will be redelivered",
                extra={
                    "batch_size": len(messages),
                    "duration_ms": round(duration * 1000, 2),
                    "error": str(e),
                },
                exc_info=True,
            )

    async def _wait_for_assignment(self) -> bool:
        """Wait for partition assignment, logging once. Returns True when assigned."""
        logged_waiting = False
        while self._running and self._consumer:
            assignment = self._consumer.assignment()
            if assignment:
                partition_info = [f"{tp.topic}:{tp.partition}" for tp in assignment]
                logger.info(
                    "Partition assignment received, starting batch consumption",
                    extra={"group_id": self.group_id, "partition_count": len(assignment), "partitions": partition_info},
                )
                update_assigned_partitions(self.group_id, len(assignment))
                return True
            if not logged_waiting:
                logger.info("Waiting for partition assignment (consumer group rebalance in progress)", extra={"group_id": self.group_id, "topics": self.topics})
                logged_waiting = True
            await asyncio.sleep(0.5)
        return False

    def _reached_max_batches(self) -> bool:
        """Check if the max_batches limit has been reached."""
        if self.max_batches is not None and self._batch_count >= self.max_batches:
            logger.info("Reached max_batches limit, stopping consumer", extra={"max_batches": self.max_batches, "batches_processed": self._batch_count})
            return True
        return False

    async def _fetch_and_process_batch(self) -> None:
        """Fetch a single batch from Kafka and process it."""
        data = await self._consumer.getmany(timeout_ms=self.batch_timeout_ms, max_records=self.batch_size)
        if not data:
            return

        messages = self._collect_messages(data)
        if not messages:
            return

        self._batch_count += 1
        await self._flush_batch(messages)

    async def _consume_loop(self) -> None:
        """Main consumption loop - fetches and processes batches."""
        logger.info("Starting batch consumption loop", extra={"max_batches": self.max_batches, "topics": self.topics, "group_id": self.group_id, "batch_size": self.batch_size})

        if not await self._wait_for_assignment():
            return

        while self._running and self._consumer:
            try:
                if self._reached_max_batches():
                    return
                await self._fetch_and_process_batch()
            except asyncio.CancelledError:
                logger.info("Batch consumption loop cancelled")
                raise
            except Exception as e:
                logger.error("Error in batch consumption loop", extra={"error": str(e)}, exc_info=True)
                await asyncio.sleep(1)
