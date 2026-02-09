"""Azure Event Hub batch consumer adapter for high-throughput concurrent processing.

Implements batch-concurrent message processing by accumulating messages into batches
before processing. Uses client-side buffering to convert EventHub's streaming API
(one message at a time) into batch processing.

Key differences from EventHubConsumer:
- Buffers messages per-partition until batch_size or batch_timeout_ms reached
- Calls batch_handler with list of messages instead of single message
- Handler controls concurrency (e.g., asyncio.gather with semaphore)
- Commits/checkpoints entire batch atomically after successful processing

Designed for I/O-bound workers (downloads, uploads) that benefit from concurrent
processing of multiple messages.
"""

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from azure.eventhub import EventData, TransportType
from azure.eventhub.aio import EventHubConsumerClient

from typing import Any
from pipeline.common.eventhub.consumer import EventHubConsumerRecord
from pipeline.common.metrics import (
    update_assigned_partitions,
    update_connection_status,
)
from pipeline.common.types import PipelineMessage

logger = logging.getLogger(__name__)


@dataclass
class BufferedMessage:
    """Message with its partition context for checkpointing."""

    partition_context: object  # PartitionContext from EventHub SDK
    event: EventData
    message: PipelineMessage


class EventHubBatchConsumer:
    """Event Hub batch consumer for concurrent message processing.

    Accumulates messages into batches and processes them concurrently for
    high-throughput I/O operations (downloads, uploads).

    Batch Handler Contract:
    - Receives list[PipelineMessage] (1 to batch_size messages)
    - Returns True to checkpoint batch, False to skip (reprocess)
    - If raises exception, batch is NOT checkpointed (messages redelivered)
    - Handler is responsible for concurrent processing (e.g., asyncio.gather + semaphore)

    Checkpoint Strategy:
    - All-or-nothing: checkpoint all messages after handler returns True
    - Skip checkpoint if handler returns False or raises exception
    - Messages are redelivered on next receive (at-least-once semantics)

    Batch Accumulation:
    - Per-partition buffering (prevents cross-partition ordering issues)
    - Flush when batch_size reached OR batch_timeout_ms elapsed
    - Flush incomplete batches on partition rebalance and shutdown
    """

    def __init__(
        self,
        connection_string: str,
        domain: str,
        worker_name: str,
        eventhub_name: str,
        consumer_group: str,
        batch_handler: Callable[[list[PipelineMessage]], Awaitable[bool]],
        batch_size: int = 20,
        batch_timeout_ms: int = 1000,
        enable_message_commit: bool = True,
        instance_id: str | None = None,
        checkpoint_store: Any = None,
    ):
        """Initialize Event Hub batch consumer.

        Args:
            connection_string: Namespace-level connection string (no EntityPath)
            domain: Pipeline domain (e.g., "verisk", "claimx")
            worker_name: Worker name for logging
            eventhub_name: Event Hub name
            consumer_group: Consumer group name
            batch_handler: Async function that processes message batches
            batch_size: Target batch size (default: 20)
            batch_timeout_ms: Max wait time to accumulate batch (default: 1000ms)
            enable_message_commit: Whether to checkpoint after successful processing
            instance_id: Optional instance identifier for parallel consumers
            checkpoint_store: Optional checkpoint store for durable offset persistence
        """
        self.connection_string = connection_string
        self.domain = domain
        self.worker_name = worker_name
        self.instance_id = instance_id
        self.eventhub_name = eventhub_name
        self.consumer_group = consumer_group
        self.batch_handler = batch_handler
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self.checkpoint_store = checkpoint_store
        self._consumer: EventHubConsumerClient | None = None
        self._running = False
        self._enable_message_commit = enable_message_commit

        # Per-partition batch buffers
        self._batch_buffers: dict[str, list[BufferedMessage]] = {}
        self._batch_timers: dict[str, float] = {}  # partition_id -> batch start time
        self._flush_locks: dict[str, asyncio.Lock] = {}  # Prevent concurrent flushes

        # Background task for timeout-based flushing
        self._flush_task: asyncio.Task | None = None

        logger.info(
            "Initialized Event Hub batch consumer",
            extra={
                "domain": domain,
                "worker_name": worker_name,
                "entity": eventhub_name,
                "consumer_group": consumer_group,
                "batch_size": batch_size,
                "batch_timeout_ms": batch_timeout_ms,
                "enable_message_commit": enable_message_commit,
                "checkpoint_persistence": (
                    "blob_storage" if checkpoint_store else "in_memory"
                ),
            },
        )

    async def start(self) -> None:
        """Start the Event Hub batch consumer.

        Creates EventHubConsumerClient and begins consuming messages in batches.
        """
        if self._running:
            logger.warning(
                "Batch consumer already running, ignoring duplicate start call"
            )
            return

        checkpoint_mode = (
            "with blob storage checkpoint persistence"
            if self.checkpoint_store
            else "with in-memory checkpoints only"
        )
        logger.info(
            f"Starting Event Hub batch consumer {checkpoint_mode}",
            extra={
                "entity": self.eventhub_name,
                "consumer_group": self.consumer_group,
                "batch_size": self.batch_size,
                "batch_timeout_ms": self.batch_timeout_ms,
                "checkpoint_persistence": (
                    "blob_storage" if self.checkpoint_store else "in_memory"
                ),
            },
        )

        try:
            # Apply SSL configuration for production CA bundle
            # Check for custom CA bundle (production TLS-intercepting proxy)
            ssl_kwargs = {}
            ca_bundle = (
                os.getenv("SSL_CERT_FILE")
                or os.getenv("REQUESTS_CA_BUNDLE")
                or os.getenv("CURL_CA_BUNDLE")
            )
            if ca_bundle:
                ssl_kwargs = {"connection_verify": ca_bundle}

            # Create consumer with AMQP over WebSocket transport
            self._consumer = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group=self.consumer_group,
                eventhub_name=self.eventhub_name,
                transport_type=TransportType.AmqpOverWebsocket,
                checkpoint_store=self.checkpoint_store,
                **ssl_kwargs,
            )

            self._running = True
            update_connection_status("consumer", connected=True)

            logger.info(
                "Event Hub batch consumer started successfully",
                extra={
                    "entity": self.eventhub_name,
                    "consumer_group": self.consumer_group,
                },
            )

            # Start background timeout flush task
            self._flush_task = asyncio.create_task(self._timeout_flush_loop())

            # Start consuming
            await self._consume_loop()

        except asyncio.CancelledError:
            logger.info("Batch consumer loop cancelled, shutting down")
            raise
        except Exception:
            logger.error("Batch consumer loop terminated with error", exc_info=True)
            raise
        finally:
            self._running = False

    async def stop(self) -> None:
        """Stop consumer and flush all remaining batches."""
        if not self._running or self._consumer is None:
            logger.debug("Batch consumer not running or already stopped")
            return

        logger.info("Stopping Event Hub batch consumer - flushing remaining batches")
        self._running = False

        try:
            # Cancel timeout flush task
            if self._flush_task:
                self._flush_task.cancel()
                try:
                    await self._flush_task
                except asyncio.CancelledError:
                    pass

            # Flush all partition buffers
            for partition_id in list(self._batch_buffers.keys()):
                await self._flush_partition_batch(partition_id)

            # Close consumer
            if self._consumer:
                await self._consumer.close()

            logger.info("Event Hub batch consumer stopped successfully")
        except Exception:
            logger.error("Error stopping Event Hub batch consumer", exc_info=True)
            raise
        finally:
            update_connection_status("consumer", connected=False)
            update_assigned_partitions(self.consumer_group, 0)
            self._consumer = None

            # Allow time for aiohttp sessions to close properly
            # EventHubConsumerClient uses aiohttp internally with AmqpOverWebsocket
            # and doesn't always close sessions cleanly on exit
            await asyncio.sleep(0.250)

    async def commit(self) -> None:
        """Commit offsets (for compatibility with transport layer interface).

        For batch consumer, checkpointing happens in _flush_partition_batch after
        successful batch processing. This method is a no-op.
        """
        logger.debug(
            "Commit called (no-op for batch consumer - checkpointing done per batch)",
            extra={"consumer_group": self.consumer_group},
        )

    async def _consume_loop(self) -> None:
        """Main consumption loop using Event Hub async receive."""
        logger.info(
            "Starting batch message consumption loop",
            extra={
                "entity": self.eventhub_name,
                "consumer_group": self.consumer_group,
            },
        )

        # Define event handler for each partition
        async def on_event(partition_context, event):
            """Receive single event and add to partition batch buffer."""
            if not self._running:
                return

            partition_id = partition_context.partition_id

            # Initialize partition state if needed
            if partition_id not in self._batch_buffers:
                self._batch_buffers[partition_id] = []
                self._batch_timers[partition_id] = time.time()
                self._flush_locks[partition_id] = asyncio.Lock()

            # Convert to PipelineMessage
            record_adapter = EventHubConsumerRecord(
                event, self.eventhub_name, partition_id
            )
            message = record_adapter.to_pipeline_message()

            # Add to buffer
            buffered = BufferedMessage(
                partition_context=partition_context,
                event=event,
                message=message,
            )
            self._batch_buffers[partition_id].append(buffered)

            # Check if batch is ready (size threshold)
            batch_size = len(self._batch_buffers[partition_id])
            if batch_size >= self.batch_size:
                # Flush immediately when batch_size reached
                asyncio.create_task(self._flush_partition_batch(partition_id))

        async def on_partition_initialize(partition_context):
            """Called when partition is assigned to this consumer."""
            partition_id = partition_context.partition_id
            logger.info(
                "Partition assigned",
                extra={
                    "entity": self.eventhub_name,
                    "consumer_group": self.consumer_group,
                    "partition_id": partition_id,
                },
            )
            # Update metrics
            current_count = len(self._batch_buffers)
            update_assigned_partitions(self.consumer_group, current_count + 1)

        async def on_partition_close(partition_context, reason):
            """Called when partition is revoked - flush incomplete batch."""
            partition_id = partition_context.partition_id
            logger.info(
                "Partition revoked - flushing incomplete batch",
                extra={
                    "entity": self.eventhub_name,
                    "consumer_group": self.consumer_group,
                    "partition_id": partition_id,
                    "reason": reason,
                },
            )

            # Flush incomplete batch before yielding partition
            await self._flush_partition_batch(partition_id)

            # Clean up partition state
            self._batch_buffers.pop(partition_id, None)
            self._batch_timers.pop(partition_id, None)
            self._flush_locks.pop(partition_id, None)

            # Update metrics
            update_assigned_partitions(self.consumer_group, len(self._batch_buffers))

        async def on_error(partition_context, error):
            """Called when error occurs during consumption."""
            partition_id = (
                partition_context.partition_id if partition_context else "unknown"
            )
            logger.error(
                "Error in Event Hub batch consumption",
                extra={
                    "entity": self.eventhub_name,
                    "consumer_group": self.consumer_group,
                    "partition_id": partition_id,
                },
                exc_info=True,
            )

        # Start receiving events
        try:
            async with self._consumer:
                await self._consumer.receive(
                    on_event=on_event,
                    on_partition_initialize=on_partition_initialize,
                    on_partition_close=on_partition_close,
                    on_error=on_error,
                    starting_position="-1",  # Start from beginning
                )
        except Exception:
            logger.error("Error in Event Hub batch receive loop", exc_info=True)
            raise

    async def _timeout_flush_loop(self):
        """Background task to flush batches that hit timeout threshold.

        Checks every 100ms for batches that have exceeded batch_timeout_ms.
        """
        while self._running:
            try:
                await asyncio.sleep(0.1)  # Check every 100ms

                current_time = time.time()
                for partition_id in list(self._batch_buffers.keys()):
                    if partition_id not in self._batch_timers:
                        continue

                    elapsed_ms = (
                        current_time - self._batch_timers[partition_id]
                    ) * 1000
                    batch_size = len(self._batch_buffers.get(partition_id, []))

                    # Flush if timeout exceeded and batch not empty
                    if batch_size > 0 and elapsed_ms >= self.batch_timeout_ms:
                        logger.debug(
                            "Timeout flush triggered",
                            extra={
                                "partition_id": partition_id,
                                "batch_size": batch_size,
                                "elapsed_ms": round(elapsed_ms, 1),
                                "timeout_ms": self.batch_timeout_ms,
                            },
                        )
                        asyncio.create_task(self._flush_partition_batch(partition_id))

            except asyncio.CancelledError:
                break
            except Exception:
                logger.error("Error in timeout flush loop", exc_info=True)
                await asyncio.sleep(1)  # Avoid tight loop on errors

    async def _flush_partition_batch(self, partition_id: str):
        """Flush accumulated batch for a partition.

        Args:
            partition_id: Partition to flush

        Processing steps:
        1. Acquire flush lock (prevent concurrent flushes)
        2. Get batch and reset buffer atomically
        3. Call batch_handler with messages
        4. If handler returns True, checkpoint all messages
        5. If handler returns False or raises, skip checkpoint (redelivery)
        """
        # Acquire lock to prevent concurrent flushes of same partition
        lock = self._flush_locks.get(partition_id)
        if lock is None:
            # Partition was closed, ignore flush
            return

        async with lock:
            # Get batch and reset buffer atomically
            batch = self._batch_buffers.get(partition_id, [])
            if not batch:
                return  # Empty batch, nothing to process

            # Reset buffer and timer
            self._batch_buffers[partition_id] = []
            self._batch_timers[partition_id] = time.time()

        # Extract messages for handler
        messages = [buffered.message for buffered in batch]

        logger.debug(
            "Processing batch",
            extra={
                "partition_id": partition_id,
                "batch_size": len(messages),
            },
        )

        start_time = time.perf_counter()

        try:
            # Call batch handler
            should_commit = await self.batch_handler(messages)

            duration = time.perf_counter() - start_time

            if should_commit and self._enable_message_commit:
                # Checkpoint all messages in batch
                checkpoint_errors = 0
                for buffered in batch:
                    try:
                        await buffered.partition_context.update_checkpoint(
                            buffered.event
                        )
                    except Exception:
                        # Log but continue - message will be redelivered (at-least-once)
                        logger.error(
                            "Failed to checkpoint message - will be redelivered",
                            extra={
                                "partition_id": partition_id,
                                "offset": buffered.message.offset,
                            },
                            exc_info=True,
                        )
                        checkpoint_errors += 1

                logger.debug(
                    "Batch checkpointed",
                    extra={
                        "partition_id": partition_id,
                        "batch_size": len(batch),
                        "duration_ms": round(duration * 1000, 2),
                        "checkpoint_errors": checkpoint_errors,
                    },
                )
            else:
                logger.info(
                    "Batch checkpoint skipped (handler returned False) - messages will be redelivered",
                    extra={
                        "partition_id": partition_id,
                        "batch_size": len(batch),
                        "duration_ms": round(duration * 1000, 2),
                    },
                )

        except Exception:
            duration = time.perf_counter() - start_time

            # Handler raised exception - don't checkpoint
            logger.error(
                "Batch processing failed - messages will be redelivered",
                extra={
                    "partition_id": partition_id,
                    "batch_size": len(batch),
                    "duration_ms": round(duration * 1000, 2),
                },
                exc_info=True,
            )
            # Don't re-raise - continue processing other partitions

    def get_batch_stats(self) -> dict[str, int]:
        """Get current batch buffer sizes per partition (for debugging/monitoring).

        Returns:
            Dict mapping partition_id to current buffer size
        """
        return {
            partition_id: len(buffer)
            for partition_id, buffer in self._batch_buffers.items()
        }
