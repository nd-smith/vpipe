"""
Event sink abstractions for decoupling event output from the poller.

Provides a Protocol-based interface for event sinks, allowing the KQLEventPoller
to write events to different destinations (message transports, JSON files, etc.)
without tight coupling to any specific implementation.
"""

import asyncio
import contextlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel

logger = logging.getLogger(__name__)


@runtime_checkable
class EventSink(Protocol):
    """
    Protocol for event sinks that receive polled events.

    Implementations must provide async context manager support (start/stop)
    and a write method for individual events.
    """

    async def start(self) -> None:
        """Initialize the sink (open connections, files, etc.)."""
        ...

    async def stop(self) -> None:
        """Gracefully shutdown the sink (flush buffers, close connections)."""
        ...

    async def write(
        self, key: str, event: BaseModel, headers: dict[str, str] | None = None
    ) -> None:
        """
        Write a single event to the sink.

        Args:
            key: Event key/identifier (e.g., trace_id)
            event: The event data as a Pydantic model
            headers: Optional metadata headers
        """
        ...

    async def write_batch(
        self,
        messages: list[tuple[str, BaseModel]],
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Write a batch of events to the sink.

        Args:
            messages: List of (key, event) tuples
            headers: Optional metadata headers applied to all messages
        """
        ...

    async def flush(self) -> None:
        """Flush any buffered data."""
        ...


@dataclass
class MessageSinkConfig:
    """Configuration for message sink."""

    message_config: Any  # MessageConfig
    domain: str
    worker_name: str = "eventhouse_poller"


class MessageSink:
    """
    Event sink that writes to message transport destinations.

    Wraps MessageProducer to provide the EventSink interface.
    Supports both Kafka and EventHub transports.
    """

    def __init__(self, config: MessageSinkConfig):
        self.config = config
        self._producer = None
        self._topic: str | None = None

    async def start(self) -> None:
        """Initialize producer via transport factory.

        Uses create_producer() which checks PIPELINE_TRANSPORT env var to
        select between aiokafka (Kafka protocol) and azure-eventhub (AMQP).
        """
        from pipeline.common.transport import create_producer

        # Resolve topic first so Event Hub producer gets the correct entity name
        self._topic = self.config.message_config.get_topic(self.config.domain, "events")

        self._producer = create_producer(
            config=self.config.message_config,
            domain=self.config.domain,
            worker_name=self.config.worker_name,
            topic=self._topic,
            topic_key="events",
        )
        await self._producer.start()

        # Sync topic with producer's actual entity name (Event Hub entity may
        # differ from the Kafka topic name resolved by get_topic()).
        if hasattr(self._producer, "eventhub_name"):
            self._topic = self._producer.eventhub_name

        logger.info(
            "MessageSink started",
            extra={"domain": self.config.domain, "topic": self._topic},
        )

    async def stop(self) -> None:
        """Shutdown message producer."""
        if self._producer:
            await self._producer.stop()
            logger.info("MessageSink stopped")

    async def write(
        self, key: str, event: BaseModel, headers: dict[str, str] | None = None
    ) -> None:
        """Write event to message transport."""
        if not self._producer or not self._topic:
            raise RuntimeError("MessageSink not started. Call start() first.")

        await self._producer.send(
            value=event,
            key=key,
            headers=headers,
        )

    async def write_batch(
        self,
        messages: list[tuple[str, BaseModel]],
        headers: dict[str, str] | None = None,
    ) -> None:
        """Write a batch of events to message transport in a single call."""
        if not self._producer or not self._topic:
            raise RuntimeError("MessageSink not started. Call start() first.")

        if not messages:
            return

        await self._producer.send_batch(
            messages=messages,
            headers=headers,
        )

    async def flush(self) -> None:
        """Flush message producer buffers."""
        if self._producer:
            await self._producer.flush()


@dataclass
class JsonFileSinkConfig:
    """Configuration for JSON file sink."""

    output_path: Path
    rotate_size_bytes: int = 100 * 1024 * 1024  # 100 MB default
    pretty_print: bool = False
    include_metadata: bool = True
    flush_interval_seconds: float = 5.0
    buffer_size: int = 100  # Number of events to buffer before auto-flush


class JsonFileSink:
    """
    Event sink that writes events to JSON Lines (.jsonl) files.

    Features:
    - JSON Lines format (one JSON object per line)
    - Optional file rotation by size
    - Buffered writes with configurable flush interval
    - Atomic file operations for crash safety
    - Optional metadata (key, timestamp, headers) in output
    """

    def __init__(self, config: JsonFileSinkConfig):
        self.config = config
        self._file = None
        self._current_path: Path | None = None
        self._current_size = 0
        self._file_index = 0
        self._buffer: list = []
        self._flush_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._running = False
        self._events_written = 0

    async def start(self) -> None:
        """Open output file and start flush task."""
        self._running = True
        self._current_path = self._get_output_path()
        self._current_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if file exists and get current size for rotation
        if self._current_path.exists():
            self._current_size = self._current_path.stat().st_size
        else:
            self._current_size = 0

        self._file = open(self._current_path, "a", encoding="utf-8")

        # Start periodic flush task
        self._flush_task = asyncio.create_task(self._periodic_flush())

        logger.info(
            "JsonFileSink started",
            extra={
                "output_path": str(self._current_path),
                "rotate_size_mb": self.config.rotate_size_bytes / (1024 * 1024),
            },
        )

    async def stop(self) -> None:
        """Flush remaining buffer and close file."""
        self._running = False

        # Cancel flush task
        if self._flush_task:
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task

        # Final flush
        await self.flush()

        # Close file
        if self._file:
            self._file.close()
            self._file = None

        logger.info(
            "JsonFileSink stopped",
            extra={
                "events_written": self._events_written,
                "output_path": str(self._current_path),
            },
        )

    async def write(
        self, key: str, event: BaseModel, headers: dict[str, str] | None = None
    ) -> None:
        """Buffer event for writing."""
        record = self._format_record(key, event, headers)

        async with self._lock:
            self._buffer.append(record)

            # Auto-flush if buffer is full
            if len(self._buffer) >= self.config.buffer_size:
                await self._flush_buffer()

    async def write_batch(
        self,
        messages: list[tuple[str, BaseModel]],
        headers: dict[str, str] | None = None,
    ) -> None:
        """Buffer a batch of events for writing."""
        for key, event in messages:
            await self.write(key=key, event=event, headers=headers)

    async def flush(self) -> None:
        """Flush buffered events to file."""
        async with self._lock:
            await self._flush_buffer()

    async def _flush_buffer(self) -> None:
        """Internal flush (must hold lock)."""
        if not self._buffer or not self._file:
            return

        # Check if rotation needed
        if self._current_size >= self.config.rotate_size_bytes:
            await self._rotate_file()

        # Write buffer to file
        lines = []
        for record in self._buffer:
            if self.config.pretty_print:
                line = json.dumps(record, indent=2, default=str)
            else:
                line = json.dumps(record, default=str)
            lines.append(line)

        content = "\n".join(lines) + "\n"
        self._file.write(content)
        self._file.flush()
        os.fsync(self._file.fileno())

        bytes_written = len(content.encode("utf-8"))
        self._current_size += bytes_written
        self._events_written += len(self._buffer)

        logger.debug(
            "Flushed events to file",
            extra={
                "events_flushed": len(self._buffer),
                "bytes_written": bytes_written,
                "total_events": self._events_written,
            },
        )

        self._buffer.clear()

    async def _rotate_file(self) -> None:
        """Rotate to a new output file."""
        if self._file:
            self._file.close()

        self._file_index += 1
        self._current_path = self._get_output_path()
        self._current_size = 0
        self._file = open(self._current_path, "a", encoding="utf-8")

        logger.info(
            "Rotated output file",
            extra={
                "new_path": str(self._current_path),
                "file_index": self._file_index,
            },
        )

    async def _periodic_flush(self) -> None:
        """Background task to periodically flush buffer."""
        while self._running:
            try:
                await asyncio.sleep(self.config.flush_interval_seconds)
                if self._buffer:
                    await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in periodic flush", extra={"error": str(e)})

    def _get_output_path(self) -> Path:
        """Generate output path, optionally with rotation index."""
        base_path = self.config.output_path

        if self._file_index == 0:
            return base_path

        # Insert index before extension: events.jsonl -> events.001.jsonl
        stem = base_path.stem
        suffix = base_path.suffix
        return base_path.parent / f"{stem}.{self._file_index:03d}{suffix}"

    def _format_record(
        self, key: str, event: BaseModel, headers: dict[str, str] | None
    ) -> dict[str, Any]:
        """Format event as output record."""
        # Get event data
        event_data = event.model_dump()

        if self.config.include_metadata:
            return {
                "_key": key,
                "_timestamp": datetime.now(UTC).isoformat(),
                "_headers": headers or {},
                **event_data,
            }
        else:
            return event_data

    @property
    def stats(self) -> dict[str, Any]:
        """Return sink statistics."""
        return {
            "events_written": self._events_written,
            "current_file": str(self._current_path),
            "current_size_bytes": self._current_size,
            "buffer_size": len(self._buffer),
            "file_index": self._file_index,
        }


def create_message_sink(
    message_config: Any, domain: str, worker_name: str = "eventhouse_poller"
) -> MessageSink:
    """Factory function to create a MessageSink."""
    config = MessageSinkConfig(
        message_config=message_config,
        domain=domain,
        worker_name=worker_name,
    )
    return MessageSink(config)


def create_json_sink(
    output_path: str | Path,
    rotate_size_mb: float = 100.0,
    pretty_print: bool = False,
    include_metadata: bool = True,
) -> JsonFileSink:
    """
    Factory function to create a JsonFileSink.

    Args:
        output_path: Path to output file (will create parent directories)
        rotate_size_mb: Rotate file when it reaches this size in MB
        pretty_print: Format JSON with indentation (larger files)
        include_metadata: Include _key, _timestamp, _headers in output

    Returns:
        Configured JsonFileSink instance
    """
    config = JsonFileSinkConfig(
        output_path=Path(output_path),
        rotate_size_bytes=int(rotate_size_mb * 1024 * 1024),
        pretty_print=pretty_print,
        include_metadata=include_metadata,
    )
    return JsonFileSink(config)


__all__ = [
    "EventSink",
    "MessageSink",
    "MessageSinkConfig",
    "JsonFileSink",
    "JsonFileSinkConfig",
    "create_message_sink",
    "create_json_sink",
]
