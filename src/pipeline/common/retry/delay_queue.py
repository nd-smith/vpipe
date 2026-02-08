"""In-memory delay queue with disk persistence for retry scheduling."""

import base64
import heapq
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

EXPIRED_MESSAGE_GRACE_SECONDS = 300


@dataclass
class DelayedMessage:
    """In-memory representation of a delayed retry message."""

    scheduled_time: datetime
    target_topic: str
    retry_count: int
    worker_type: str
    message_key: bytes | None
    message_value: bytes
    headers: dict[str, str]

    def __lt__(self, other: "DelayedMessage") -> bool:
        """Compare by scheduled_time for heap ordering."""
        return self.scheduled_time < other.scheduled_time


class DelayQueue:
    """Min-heap delay queue with periodic disk persistence.

    Messages are ordered by scheduled_time. Provides push/pop operations
    and JSON-based disk persistence for crash recovery.
    """

    def __init__(self, domain: str, persistence_file: Path):
        self._domain = domain
        self._persistence_file = persistence_file
        self._heap: list[DelayedMessage] = []

    def __len__(self) -> int:
        return len(self._heap)

    @property
    def next_scheduled_time(self) -> datetime | None:
        if not self._heap:
            return None
        return self._heap[0].scheduled_time

    def push(self, message: DelayedMessage) -> None:
        heapq.heappush(self._heap, message)

    def pop_ready(self, now: datetime) -> list[DelayedMessage]:
        """Pop all messages whose scheduled_time <= now."""
        ready = []
        while self._heap and self._heap[0].scheduled_time <= now:
            ready.append(heapq.heappop(self._heap))
        return ready

    def requeue_with_delay(self, message: DelayedMessage, delay_seconds: int = 5) -> None:
        """Put a message back with a new delay (e.g. after routing failure)."""
        message.scheduled_time = datetime.now(UTC).replace(microsecond=0) + timedelta(
            seconds=delay_seconds
        )
        heapq.heappush(self._heap, message)

    def persist_to_disk(self) -> None:
        """Persist the queue to disk as JSON. Skips if empty."""
        if not self._heap:
            logger.debug("Delayed queue empty, skipping persistence")
            return

        try:
            messages_data = []
            for msg in self._heap:
                messages_data.append(
                    {
                        "scheduled_time": msg.scheduled_time.isoformat(),
                        "target_topic": msg.target_topic,
                        "retry_count": msg.retry_count,
                        "worker_type": msg.worker_type,
                        "message_key": (
                            base64.b64encode(msg.message_key).decode("utf-8")
                            if msg.message_key
                            else None
                        ),
                        "message_value": base64.b64encode(msg.message_value).decode(
                            "utf-8"
                        ),
                        "headers": msg.headers,
                    }
                )

            data = {
                "version": 1,
                "domain": self._domain,
                "last_persisted": datetime.now(UTC).isoformat(),
                "messages": messages_data,
            }

            # Write atomically (write to temp file, then rename)
            temp_file = self._persistence_file.with_suffix(".tmp")
            with open(temp_file, "w") as f:
                json.dump(data, f, indent=2)

            temp_file.replace(self._persistence_file)

            logger.debug(
                "Persisted delayed queue to disk",
                extra={
                    "queue_size": len(self._heap),
                    "file": str(self._persistence_file),
                },
            )

        except Exception as e:
            logger.error(
                "Failed to persist delayed queue to disk",
                extra={
                    "file": str(self._persistence_file),
                    "error": str(e),
                },
                exc_info=True,
            )

    def restore_from_disk(self) -> int:
        """Restore queue from disk. Returns number of messages restored."""
        if not self._persistence_file.exists():
            logger.debug(
                "No persistence file found, starting with empty queue",
                extra={"file": str(self._persistence_file)},
            )
            return 0

        try:
            with open(self._persistence_file) as f:
                data = json.load(f)

            if data.get("version") != 1:
                logger.warning(
                    "Unknown persistence file version, ignoring",
                    extra={"version": data.get("version")},
                )
                return 0

            if data.get("domain") != self._domain:
                logger.warning(
                    "Persistence file domain mismatch, ignoring",
                    extra={
                        "expected_domain": self._domain,
                        "file_domain": data.get("domain"),
                    },
                )
                return 0

            now = datetime.now(UTC)
            restored_count = 0
            expired_count = 0

            for msg_data in data.get("messages", []):
                scheduled_time = datetime.fromisoformat(msg_data["scheduled_time"])

                # Skip messages that are too old (more than max retry delay past due)
                if (now - scheduled_time).total_seconds() > EXPIRED_MESSAGE_GRACE_SECONDS:
                    expired_count += 1
                    logger.debug(
                        "Skipping expired message from persistence",
                        extra={
                            "scheduled_time": scheduled_time.isoformat(),
                            "target_topic": msg_data["target_topic"],
                        },
                    )
                    continue

                delayed_msg = DelayedMessage(
                    scheduled_time=scheduled_time,
                    target_topic=msg_data["target_topic"],
                    retry_count=msg_data["retry_count"],
                    worker_type=msg_data["worker_type"],
                    message_key=(
                        base64.b64decode(msg_data["message_key"])
                        if msg_data["message_key"]
                        else None
                    ),
                    message_value=base64.b64decode(msg_data["message_value"]),
                    headers=msg_data["headers"],
                )
                heapq.heappush(self._heap, delayed_msg)
                restored_count += 1

            logger.info(
                "Restored delayed queue from disk",
                extra={
                    "restored_count": restored_count,
                    "expired_count": expired_count,
                    "file": str(self._persistence_file),
                    "last_persisted": data.get("last_persisted"),
                },
            )

            # Delete persistence file after successful restore
            self._persistence_file.unlink()

            return restored_count

        except Exception as e:
            logger.error(
                "Failed to restore delayed queue from disk",
                extra={
                    "file": str(self._persistence_file),
                    "error": str(e),
                },
                exc_info=True,
            )
            return 0
