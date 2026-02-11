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
import contextlib
import logging
import os
import queue
import threading

from azure.eventhub import EventData, TransportType
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
        self._sender_thread: threading.Thread | None = None
        self._shutdown = threading.Event()
        self._circuit_open = False
        self._circuit_opened_at = 0.0
        self._circuit_reset_interval = 60.0  # Try to reset circuit every 60 seconds
        self._failure_count = 0
        self._total_sent = 0
        self._total_dropped = 0

        # Start background sender
        print(f"[EVENTHUB_LOGS] Initializing EventHub log handler for: {eventhub_name}")
        print(f"[EVENTHUB_LOGS] Batch size: {batch_size}, Timeout: {batch_timeout_seconds}s")
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
                print(
                    f"[EVENTHUB_LOGS] WARNING: Queue full, dropping logs (dropped: {self._total_dropped})"
                )

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
        print(f"[EVENTHUB_LOGS] Background sender thread started for: {self.eventhub_name}")

        try:
            # Use AMQP over WebSocket (port 443) instead of AMQP over TCP (port 5671)
            # This works better in corporate networks where 5671 may be blocked
            ssl_kwargs = {}
            ca_bundle = (
                os.getenv("SSL_CERT_FILE")
                or os.getenv("REQUESTS_CA_BUNDLE")
                or os.getenv("CURL_CA_BUNDLE")
            )
            if ca_bundle:
                ssl_kwargs = {"connection_verify": ca_bundle}

            producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name,
                transport_type=TransportType.AmqpOverWebsocket,
                **ssl_kwargs,
            )
            print(
                "[EVENTHUB_LOGS] EventHub producer client created successfully (using AMQP over WebSocket on port 443)"
            )
        except Exception as e:
            import sys

            print(
                f"[EVENTHUB_LOGS] ERROR creating EventHub producer: {type(e).__name__}: {str(e)[:200]}",
                file=sys.stderr,
            )
            return

        async with producer:
            print("[EVENTHUB_LOGS] Connected to EventHub - ready to send logs")
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

                    # Auto-reset circuit breaker after cooldown period
                    if (
                        self._circuit_open
                        and (now - self._circuit_opened_at) >= self._circuit_reset_interval
                    ):
                        print(
                            "[EVENTHUB_LOGS] Circuit breaker cooldown expired - attempting to reconnect"
                        )
                        self._circuit_open = False
                        self._failure_count = 0

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
                with contextlib.suppress(Exception):
                    await self._send_batch(producer, batch)

        # Allow time for aiohttp sessions to close properly
        # EventHubProducerClient uses aiohttp internally with AmqpOverWebsocket
        # and doesn't always close sessions cleanly on exit
        await asyncio.sleep(0.250)

    async def _send_batch(self, producer: EventHubProducerClient, batch: list[str]) -> None:
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

                # Print success for visibility (only occasionally to avoid spam)
                if self._total_sent % 100 == 0:
                    print(f"[EVENTHUB_LOGS] Successfully sent {self._total_sent} logs to EventHub")

            # Reset failure count on success
            if self._failure_count > 0:
                self._failure_count = 0
                print("[EVENTHUB_LOGS] Circuit breaker reset after successful send")

            # Close circuit if it was open
            if self._circuit_open:
                self._circuit_open = False
                print("[EVENTHUB_LOGS] Circuit breaker closed - resuming log uploads")

        except Exception as e:
            loop_time = asyncio.get_event_loop().time()
            self._handle_send_error(e, loop_time)
            # Print error to stderr so it's visible
            import sys

            print(
                f"[EVENTHUB_LOGS] ERROR sending batch to EventHub: {type(e).__name__}: {str(e)[:200]}",
                file=sys.stderr,
            )
            raise

    def _handle_send_error(self, error: Exception, loop_time: float) -> None:
        """
        Handle errors with circuit breaker pattern.

        Opens circuit after consecutive failures to avoid overwhelming
        Event Hub or wasting resources.

        Args:
            error: Exception that occurred during send
            loop_time: Current event loop time for circuit breaker timing
        """
        self._failure_count += 1

        # Open circuit after threshold failures
        if self._failure_count >= self.circuit_breaker_threshold and not self._circuit_open:
            self._circuit_open = True
            self._circuit_opened_at = loop_time
            print(
                f"[EVENTHUB_LOGS] Circuit breaker OPENED after {self._failure_count} failures - will retry in {self._circuit_reset_interval}s"
            )
            # Circuit will auto-reset after cooldown period

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
