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
from core.auth.eventhub_oauth import create_eventhub_oauth_callback
from pipeline.common.metrics import (
    update_assigned_partitions,
    update_connection_status,
)
from pipeline.common.types import PipelineMessage, from_consumer_record

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
        batch_size: int = 20,
        batch_timeout_ms: int = 1000,
        enable_message_commit: bool = True,
        instance_id: str | None = None,
    ):
        """Initialize message batch consumer.

        Args:
            config: MessageConfig with connection details
            domain: Pipeline domain (e.g., "verisk", "claimx")
            worker_name: Worker name for logging
            topics: List of topics to consume
            batch_handler: Async function that processes message batches
            batch_size: Target batch size (maps to max_poll_records, default: 20)
            batch_timeout_ms: Timeout for getmany (default: 1000ms)
            enable_message_commit: Whether to commit after successful processing
            instance_id: Optional instance identifier for parallel consumers
        """
        if not topics:
            raise ValueError("At least one topic must be specified")

        self.config = config
        self.domain = domain
        self.worker_name = worker_name
        self.instance_id = instance_id
        self.topics = topics
        self.batch_handler = batch_handler
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False
        self._enable_message_commit = enable_message_commit

        self.consumer_config = config.get_worker_config(domain, worker_name, "consumer")
        self.group_id = config.get_consumer_group(domain, worker_name)

        processing_config = config.get_worker_config(domain, worker_name, "processing")
        self.max_batches = processing_config.get("max_batches")
        self._batch_count = 0

        logger.info(
            "Initialized message batch consumer",
            extra={
                "domain": domain,
                "worker_name": worker_name,
                "topics": topics,
                "group_id": self.group_id,
                "batch_size": batch_size,
                "batch_timeout_ms": batch_timeout_ms,
                "bootstrap_servers": config.bootstrap_servers,
                "max_batches": self.max_batches,
                "enable_message_commit": enable_message_commit,
            },
        )

    async def start(self) -> None:
        """Start the message batch consumer."""
        if self._running:
            logger.warning(
                "Batch consumer already running, ignoring duplicate start call"
            )
            return

        logger.info(
            "Starting message batch consumer",
            extra={
                "topics": self.topics,
                "group_id": self.group_id,
                "batch_size": self.batch_size,
            },
        )

        kafka_consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": self.group_id,
            "client_id": (
                f"{self.domain}-{self.worker_name}-{self.instance_id}"
                if self.instance_id
                else f"{self.domain}-{self.worker_name}"
            ),
            "request_timeout_ms": self.config.request_timeout_ms,
            "metadata_max_age_ms": self.config.metadata_max_age_ms,
            "connections_max_idle_ms": self.config.connections_max_idle_ms,
        }

        kafka_consumer_config.update(
            {
                "enable_auto_commit": self.consumer_config.get(
                    "enable_auto_commit", False
                ),
                "auto_offset_reset": self.consumer_config.get(
                    "auto_offset_reset", "earliest"
                ),
                "max_poll_records": self.batch_size,  # Use batch_size for max_poll_records
                "max_poll_interval_ms": self.consumer_config.get(
                    "max_poll_interval_ms", 300000
                ),
                "session_timeout_ms": self.consumer_config.get(
                    "session_timeout_ms", 30000
                ),
            }
        )

        if "heartbeat_interval_ms" in self.consumer_config:
            kafka_consumer_config["heartbeat_interval_ms"] = self.consumer_config[
                "heartbeat_interval_ms"
            ]
        if "fetch_min_bytes" in self.consumer_config:
            kafka_consumer_config["fetch_min_bytes"] = self.consumer_config[
                "fetch_min_bytes"
            ]
        if "fetch_max_wait_ms" in self.consumer_config:
            kafka_consumer_config["fetch_max_wait_ms"] = self.consumer_config[
                "fetch_max_wait_ms"
            ]
        if "partition_assignment_strategy" in self.consumer_config:
            kafka_consumer_config["partition_assignment_strategy"] = (
                self.consumer_config["partition_assignment_strategy"]
            )

        if self.config.security_protocol != "PLAINTEXT":
            kafka_consumer_config["security_protocol"] = self.config.security_protocol
            kafka_consumer_config["sasl_mechanism"] = self.config.sasl_mechanism

            # Create SSL context for SSL/SASL_SSL connections
            if "SSL" in self.config.security_protocol:
                import ssl

                ssl_context = ssl.create_default_context()
                kafka_consumer_config["ssl_context"] = ssl_context

            if self.config.sasl_mechanism == "OAUTHBEARER":
                oauth_callback = create_eventhub_oauth_callback()
                kafka_consumer_config["sasl_oauth_token_provider"] = oauth_callback
            elif self.config.sasl_mechanism == "PLAIN":
                kafka_consumer_config["sasl_plain_username"] = (
                    self.config.sasl_plain_username
                )
                kafka_consumer_config["sasl_plain_password"] = (
                    self.config.sasl_plain_password
                )
            elif self.config.sasl_mechanism == "GSSAPI":
                kafka_consumer_config["sasl_kerberos_service_name"] = (
                    self.config.sasl_kerberos_service_name
                )

        self._consumer = AIOKafkaConsumer(*self.topics, **kafka_consumer_config)

        await self._consumer.start()
        self._running = True

        update_connection_status("consumer", connected=True)
        partition_count = len(self._consumer.assignment())
        update_assigned_partitions(self.group_id, partition_count)

        logger.info(
            "Message batch consumer started successfully",
            extra={
                "topics": self.topics,
                "group_id": self.group_id,
                "partitions": partition_count,
            },
        )

        try:
            await self._consume_loop()
        except asyncio.CancelledError:
            logger.info("Batch consumer loop cancelled, shutting down")
            raise
        except Exception as e:
            logger.error(
                "Batch consumer loop terminated with error",
                extra={"error": str(e)},
                exc_info=True,
            )
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

    async def _consume_loop(self) -> None:
        """Main consumption loop - fetches and processes batches."""
        logger.info(
            "Starting batch consumption loop",
            extra={
                "max_batches": self.max_batches,
                "topics": self.topics,
                "group_id": self.group_id,
                "batch_size": self.batch_size,
            },
        )

        _logged_waiting_for_assignment = False
        _logged_assignment_received = False

        while self._running and self._consumer:
            try:
                # Check max_batches limit
                if (
                    self.max_batches is not None
                    and self._batch_count >= self.max_batches
                ):
                    logger.info(
                        "Reached max_batches limit, stopping consumer",
                        extra={
                            "max_batches": self.max_batches,
                            "batches_processed": self._batch_count,
                        },
                    )
                    return

                # Wait for partition assignment
                assignment = self._consumer.assignment()
                if not assignment:
                    if not _logged_waiting_for_assignment:
                        logger.info(
                            "Waiting for partition assignment (consumer group rebalance in progress)",
                            extra={"group_id": self.group_id, "topics": self.topics},
                        )
                        _logged_waiting_for_assignment = True
                    await asyncio.sleep(0.5)
                    continue

                if not _logged_assignment_received:
                    partition_info = [f"{tp.topic}:{tp.partition}" for tp in assignment]
                    logger.info(
                        "Partition assignment received, starting batch consumption",
                        extra={
                            "group_id": self.group_id,
                            "partition_count": len(assignment),
                            "partitions": partition_info,
                        },
                    )
                    _logged_assignment_received = True
                    update_assigned_partitions(self.group_id, len(assignment))

                # Fetch batch of messages
                data = await self._consumer.getmany(
                    timeout_ms=self.batch_timeout_ms,
                    max_records=self.batch_size,
                )

                if not data:
                    continue

                # Flatten to single list of PipelineMessages
                messages: list[PipelineMessage] = []
                for partition_messages in data.values():
                    messages.extend(
                        [from_consumer_record(record) for record in partition_messages]
                    )

                if not messages:
                    continue

                self._batch_count += 1

                logger.debug(
                    "Processing batch",
                    extra={"batch_size": len(messages)},
                )

                start_time = time.perf_counter()

                try:
                    # Call batch handler
                    should_commit = await self.batch_handler(messages)

                    duration = time.perf_counter() - start_time

                    if should_commit and self._enable_message_commit:
                        await self._consumer.commit()
                        logger.debug(
                            "Batch committed",
                            extra={
                                "batch_size": len(messages),
                                "duration_ms": round(duration * 1000, 2),
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

                    # Handler raised exception - don't commit
                    logger.error(
                        "Batch processing failed - messages will be redelivered",
                        extra={
                            "batch_size": len(messages),
                            "duration_ms": round(duration * 1000, 2),
                            "error": str(e),
                        },
                        exc_info=True,
                    )
                    # Don't re-raise - continue processing next batch

            except asyncio.CancelledError:
                logger.info("Batch consumption loop cancelled")
                raise
            except Exception as e:
                logger.error(
                    "Error in batch consumption loop",
                    extra={"error": str(e)},
                    exc_info=True,
                )
                await asyncio.sleep(1)  # Avoid tight loop on errors
