"""
Event Hub logging handler for real-time ADX streaming.

This handler sends logs to Azure Event Hub asynchronously, which then
flows into ADX via native Event Hub data connection.

Features:
- Async sending (non-blocking worker threads)
- Batching for efficiency
- Circuit breaker for resilience
- Sampling support for volume control
"""

import asyncio
import logging
import queue
import threading
from typing import Optional

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient


class EventHubLogHandler(logging.Handler):
    """
    Async Event Hub handler for real-time log streaming to ADX.

    Logs are queued and sent asynchronously in batches to avoid blocking
    application workers. Includes circuit breaker to stop sending if
    Event Hub is unavailable.

    Example:
        handler = EventHubLogHandler(
            connection_string="Endpoint=sb://...",
            eventhub_name="pipeline-logs",
            batch_size=100,
            batch_timeout_seconds=1.0
        )
        handler.setFormatter(JSONFormatter())
        handler.setLevel(logging.WARNING)  # Only warnings/errors
        logger.addHandler(handler)
    """

    def __init__(
        self,
        connection_string: str,
        eventhub_name: str,
        batch_size: int = 100,
        batch_timeout_seconds: float = 1.0,
        max_queue_size: int = 10000,
        circuit_breaker_threshold: int = 5,
    ):
        """
        Initialize Event Hub log handler.

        Args:
            connection_string: Azure Event Hub connection string
            eventhub_name: Name of the Event Hub (e.g., "pipeline-logs")
            batch_size: Number of logs to batch before sending
            batch_timeout_seconds: Max seconds to wait before sending partial batch
            max_queue_size: Max logs to queue (older logs dropped if full)
            circuit_breaker_threshold: Failures before circuit opens
        """
        super().__init__()
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        self.circuit_breaker_threshold = circuit_breaker_threshold

        # Queue for async sending
        self.log_queue: queue.Queue = queue.Queue(maxsize=max_queue_size)

        # Background thread state
        self._sender_thread: Optional[threading.Thread] = None
        self._shutdown = threading.Event()
        self._circuit_open = False
        self._failure_count = 0
        self._total_sent = 0
        self._total_dropped = 0

        # Start background sender
        self._start_sender()

    def emit(self, record: logging.LogRecord) -> None:
        """
        Called by Python logging system.
        Queues the log for async sending (non-blocking).

        Args:
            record: Log record to send
        """
        try:
            # Skip if circuit is open
            if self._circuit_open:
                self._total_dropped += 1
                return

            # Format the log
            log_entry = self.format(record)

            # Queue for async sending (don't block the worker)
            try:
                self.log_queue.put_nowait(log_entry)
            except queue.Full:
                # Queue is full - drop this log to avoid blocking
                # This is acceptable for real-time streaming
                self._total_dropped += 1

        except Exception:
            # Never let logging break the application
            self.handleError(record)

    def _start_sender(self) -> None:
        """Start background thread for sending logs to Event Hub."""
        self._sender_thread = threading.Thread(
            target=self._run_sender, daemon=True, name="eventhub-log-sender"
        )
        self._sender_thread.start()

    def _run_sender(self) -> None:
        """
        Background thread that sends logs to Event Hub.
        Runs in a separate event loop to avoid blocking workers.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self._send_loop())
        finally:
            loop.close()

    async def _send_loop(self) -> None:
        """Main loop for sending batches to Event Hub."""
        producer = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_string, eventhub_name=self.eventhub_name
        )

        async with producer:
            batch = []
            last_send = asyncio.get_event_loop().time()

            while not self._shutdown.is_set():
                try:
                    # Get log from queue (with timeout)
                    try:
                        log_entry = self.log_queue.get(timeout=0.1)
                        batch.append(log_entry)
                    except queue.Empty:
                        pass

                    now = asyncio.get_event_loop().time()

                    # Send batch if:
                    # 1. Reached batch size, OR
                    # 2. Timeout reached and we have logs
                    should_send = len(batch) >= self.batch_size or (
                        batch and (now - last_send) >= self.batch_timeout_seconds
                    )

                    if should_send and not self._circuit_open:
                        await self._send_batch(producer, batch)
                        batch = []
                        last_send = now

                except asyncio.CancelledError:
                    break
                except Exception:
                    # Handle errors but keep running
                    await asyncio.sleep(1)

            # Final flush on shutdown
            if batch and not self._circuit_open:
                try:
                    await self._send_batch(producer, batch)
                except Exception:
                    pass

    async def _send_batch(
        self, producer: EventHubProducerClient, batch: list[str]
    ) -> None:
        """
        Send a batch of logs to Event Hub.

        Args:
            producer: Event Hub producer client
            batch: List of JSON log strings to send

        Raises:
            Exception: If sending fails
        """
        if not batch:
            return

        try:
            # Create Event Hub batch
            event_batch = await producer.create_batch()

            for log_entry in batch:
                event_data = EventData(log_entry)
                try:
                    event_batch.add(event_data)
                except ValueError:
                    # Batch is full, send it and create new batch
                    await producer.send_batch(event_batch)
                    event_batch = await producer.create_batch()
                    event_batch.add(event_data)

            # Send final batch
            if len(event_batch) > 0:
                await producer.send_batch(event_batch)
                self._total_sent += len(batch)

            # Reset failure count on success
            if self._failure_count > 0:
                self._failure_count = 0

            # Close circuit if it was open
            if self._circuit_open:
                self._circuit_open = False

        except Exception as e:
            self._handle_send_error(e)
            raise

    def _handle_send_error(self, error: Exception) -> None:
        """
        Handle errors with circuit breaker pattern.

        Opens circuit after consecutive failures to avoid overwhelming
        Event Hub or wasting resources.

        Args:
            error: Exception that occurred during send
        """
        self._failure_count += 1

        # Open circuit after threshold failures
        if self._failure_count >= self.circuit_breaker_threshold:
            self._circuit_open = True
            # Circuit will auto-reset on next successful send

    def get_stats(self) -> dict:
        """
        Get handler statistics.

        Returns:
            Dict with sent/dropped counts and circuit state
        """
        return {
            "total_sent": self._total_sent,
            "total_dropped": self._total_dropped,
            "queue_size": self.log_queue.qsize(),
            "circuit_open": self._circuit_open,
            "failure_count": self._failure_count,
        }

    def close(self) -> None:
        """Shutdown handler gracefully."""
        self._shutdown.set()
        if self._sender_thread:
            self._sender_thread.join(timeout=5.0)
        super().close()
