"""
Unit tests for DelayQueue.

Test Coverage:
    - DelayedMessage dataclass ordering
    - Queue push/pop operations
    - pop_ready with various time scenarios
    - requeue_with_delay behavior
    - Disk persistence (persist and restore)
    - Restore skips expired messages
    - Restore handles version/domain mismatches
    - Restore handles missing/corrupt files
    - next_scheduled_time property
    - Queue length tracking
"""

import base64
import json
from datetime import UTC, datetime, timedelta

from pipeline.common.retry.delay_queue import (
    EXPIRED_MESSAGE_GRACE_SECONDS,
    DelayedMessage,
    DelayQueue,
)


def make_delayed_message(
    scheduled_time=None,
    target_topic="test.topic",
    retry_count=1,
    worker_type="download",
    message_key=b"key-1",
    message_value=b'{"data": "test"}',
    headers=None,
):
    """Create a DelayedMessage with sensible defaults."""
    if scheduled_time is None:
        scheduled_time = datetime.now(UTC) + timedelta(seconds=60)
    if headers is None:
        headers = {"retry_count": "1", "target_topic": target_topic}
    return DelayedMessage(
        scheduled_time=scheduled_time,
        target_topic=target_topic,
        retry_count=retry_count,
        worker_type=worker_type,
        message_key=message_key,
        message_value=message_value,
        headers=headers,
    )


class TestDelayedMessage:
    """Test DelayedMessage dataclass."""

    def test_ordering_by_scheduled_time(self):
        """Messages compare by scheduled_time for heap ordering."""
        earlier = make_delayed_message(scheduled_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC))
        later = make_delayed_message(scheduled_time=datetime(2024, 1, 1, 13, 0, 0, tzinfo=UTC))

        assert earlier < later
        assert not later < earlier

    def test_equal_times_are_not_less_than(self):
        """Messages with equal times are not less than each other."""
        time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        msg1 = make_delayed_message(scheduled_time=time)
        msg2 = make_delayed_message(scheduled_time=time)

        assert not msg1 < msg2
        assert not msg2 < msg1

    def test_none_message_key_allowed(self):
        """DelayedMessage accepts None as message_key."""
        msg = make_delayed_message(message_key=None)
        assert msg.message_key is None


class TestDelayQueueBasicOperations:
    """Test basic queue operations."""

    def test_empty_queue_has_zero_length(self, tmp_path):
        queue = DelayQueue("verisk", tmp_path / "queue.json")
        assert len(queue) == 0

    def test_push_increases_length(self, tmp_path):
        queue = DelayQueue("verisk", tmp_path / "queue.json")
        queue.push(make_delayed_message())
        assert len(queue) == 1

    def test_push_multiple_messages(self, tmp_path):
        queue = DelayQueue("verisk", tmp_path / "queue.json")
        queue.push(make_delayed_message())
        queue.push(make_delayed_message())
        queue.push(make_delayed_message())
        assert len(queue) == 3

    def test_next_scheduled_time_empty_queue(self, tmp_path):
        """next_scheduled_time returns None for empty queue."""
        queue = DelayQueue("verisk", tmp_path / "queue.json")
        assert queue.next_scheduled_time is None

    def test_next_scheduled_time_returns_earliest(self, tmp_path):
        """next_scheduled_time returns the earliest scheduled time."""
        queue = DelayQueue("verisk", tmp_path / "queue.json")

        early = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        late = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)

        queue.push(make_delayed_message(scheduled_time=late))
        queue.push(make_delayed_message(scheduled_time=early))

        assert queue.next_scheduled_time == early


class TestDelayQueuePopReady:
    """Test pop_ready behavior."""

    def test_pop_ready_returns_empty_for_empty_queue(self, tmp_path):
        queue = DelayQueue("verisk", tmp_path / "queue.json")
        result = queue.pop_ready(datetime.now(UTC))
        assert result == []

    def test_pop_ready_returns_due_messages(self, tmp_path):
        """pop_ready returns messages whose scheduled_time has passed."""
        queue = DelayQueue("verisk", tmp_path / "queue.json")

        past = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        queue.push(make_delayed_message(scheduled_time=past))

        ready = queue.pop_ready(now)
        assert len(ready) == 1
        assert ready[0].scheduled_time == past
        assert len(queue) == 0

    def test_pop_ready_returns_message_at_exact_time(self, tmp_path):
        """pop_ready includes messages whose scheduled_time equals now."""
        queue = DelayQueue("verisk", tmp_path / "queue.json")

        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        queue.push(make_delayed_message(scheduled_time=now))

        ready = queue.pop_ready(now)
        assert len(ready) == 1

    def test_pop_ready_leaves_future_messages(self, tmp_path):
        """pop_ready does not return messages scheduled in the future."""
        queue = DelayQueue("verisk", tmp_path / "queue.json")

        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        future = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)

        queue.push(make_delayed_message(scheduled_time=future))

        ready = queue.pop_ready(now)
        assert len(ready) == 0
        assert len(queue) == 1

    def test_pop_ready_returns_only_due_messages(self, tmp_path):
        """pop_ready returns due messages and leaves future ones."""
        queue = DelayQueue("verisk", tmp_path / "queue.json")

        past = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        future = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)

        queue.push(make_delayed_message(scheduled_time=future, target_topic="future"))
        queue.push(make_delayed_message(scheduled_time=past, target_topic="past"))

        ready = queue.pop_ready(now)
        assert len(ready) == 1
        assert ready[0].target_topic == "past"
        assert len(queue) == 1

    def test_pop_ready_returns_multiple_due_messages_in_order(self, tmp_path):
        """pop_ready returns all due messages sorted by scheduled_time."""
        queue = DelayQueue("verisk", tmp_path / "queue.json")

        t1 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 1, 1, 11, 0, 0, tzinfo=UTC)
        now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        queue.push(make_delayed_message(scheduled_time=t2, target_topic="second"))
        queue.push(make_delayed_message(scheduled_time=t1, target_topic="first"))

        ready = queue.pop_ready(now)
        assert len(ready) == 2
        assert ready[0].target_topic == "first"
        assert ready[1].target_topic == "second"


class TestDelayQueueRequeue:
    """Test requeue_with_delay behavior."""

    def test_requeue_adds_message_back_with_new_delay(self, tmp_path):
        """requeue_with_delay puts message back with updated scheduled_time."""
        queue = DelayQueue("verisk", tmp_path / "queue.json")

        msg = make_delayed_message(scheduled_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC))

        queue.requeue_with_delay(msg, delay_seconds=10)

        assert len(queue) == 1
        # The scheduled_time should be updated to approximately now + 10s
        assert msg.scheduled_time > datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)

    def test_requeue_uses_default_5_second_delay(self, tmp_path):
        """requeue_with_delay defaults to 5 second delay."""
        queue = DelayQueue("verisk", tmp_path / "queue.json")

        before = datetime.now(UTC)
        msg = make_delayed_message()
        queue.requeue_with_delay(msg)

        # scheduled_time should be approximately now + 5s
        expected_min = before + timedelta(seconds=4)
        expected_max = before + timedelta(seconds=6)
        assert expected_min <= msg.scheduled_time <= expected_max


class TestDelayQueuePersistence:
    """Test disk persistence."""

    def test_persist_writes_file(self, tmp_path):
        """persist_to_disk creates a JSON file."""
        persistence_file = tmp_path / "queue.json"
        queue = DelayQueue("verisk", persistence_file)

        queue.push(
            make_delayed_message(
                scheduled_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
                message_key=b"test-key",
                message_value=b"test-value",
            )
        )

        queue.persist_to_disk()

        assert persistence_file.exists()
        data = json.loads(persistence_file.read_text())
        assert data["version"] == 1
        assert data["domain"] == "verisk"
        assert len(data["messages"]) == 1
        assert data["messages"][0]["target_topic"] == "test.topic"
        assert data["messages"][0]["message_key"] == base64.b64encode(b"test-key").decode("utf-8")
        assert data["messages"][0]["message_value"] == base64.b64encode(b"test-value").decode(
            "utf-8"
        )

    def test_persist_skips_empty_queue(self, tmp_path):
        """persist_to_disk does nothing for empty queue."""
        persistence_file = tmp_path / "queue.json"
        queue = DelayQueue("verisk", persistence_file)

        queue.persist_to_disk()

        assert not persistence_file.exists()

    def test_persist_with_none_message_key(self, tmp_path):
        """persist_to_disk handles None message_key."""
        persistence_file = tmp_path / "queue.json"
        queue = DelayQueue("verisk", persistence_file)

        queue.push(make_delayed_message(message_key=None))
        queue.persist_to_disk()

        data = json.loads(persistence_file.read_text())
        assert data["messages"][0]["message_key"] is None

    def test_persist_handles_write_error(self, tmp_path):
        """persist_to_disk logs error and does not raise on write failure."""
        persistence_file = tmp_path / "nonexistent_dir" / "queue.json"
        queue = DelayQueue("verisk", persistence_file)
        queue.push(make_delayed_message())

        # Should not raise even though directory does not exist
        queue.persist_to_disk()

    def test_restore_from_disk_with_valid_file(self, tmp_path):
        """restore_from_disk restores messages from a valid file."""
        persistence_file = tmp_path / "queue.json"
        queue = DelayQueue("verisk", persistence_file)

        scheduled_time = datetime.now(UTC) + timedelta(seconds=60)
        queue.push(
            make_delayed_message(
                scheduled_time=scheduled_time,
                target_topic="restored.topic",
                message_key=b"key-1",
                message_value=b"value-1",
            )
        )
        queue.persist_to_disk()

        # Create a new queue and restore
        new_queue = DelayQueue("verisk", persistence_file)
        count = new_queue.restore_from_disk()

        assert count == 1
        assert len(new_queue) == 1
        assert new_queue.next_scheduled_time is not None

        # File should be deleted after restore
        assert not persistence_file.exists()

    def test_restore_returns_zero_when_no_file(self, tmp_path):
        """restore_from_disk returns 0 when file does not exist."""
        queue = DelayQueue("verisk", tmp_path / "nonexistent.json")
        count = queue.restore_from_disk()
        assert count == 0

    def test_restore_skips_expired_messages(self, tmp_path):
        """restore_from_disk skips messages older than grace period."""
        persistence_file = tmp_path / "queue.json"

        expired_time = datetime.now(UTC) - timedelta(seconds=EXPIRED_MESSAGE_GRACE_SECONDS + 60)
        valid_time = datetime.now(UTC) + timedelta(seconds=30)

        data = {
            "version": 1,
            "domain": "verisk",
            "last_persisted": datetime.now(UTC).isoformat(),
            "messages": [
                {
                    "scheduled_time": expired_time.isoformat(),
                    "target_topic": "expired.topic",
                    "retry_count": 1,
                    "worker_type": "download",
                    "message_key": base64.b64encode(b"key").decode("utf-8"),
                    "message_value": base64.b64encode(b"val").decode("utf-8"),
                    "headers": {},
                },
                {
                    "scheduled_time": valid_time.isoformat(),
                    "target_topic": "valid.topic",
                    "retry_count": 1,
                    "worker_type": "download",
                    "message_key": base64.b64encode(b"key").decode("utf-8"),
                    "message_value": base64.b64encode(b"val").decode("utf-8"),
                    "headers": {},
                },
            ],
        }

        persistence_file.write_text(json.dumps(data))

        queue = DelayQueue("verisk", persistence_file)
        count = queue.restore_from_disk()

        assert count == 1
        assert len(queue) == 1

    def test_restore_rejects_wrong_version(self, tmp_path):
        """restore_from_disk ignores file with unknown version."""
        persistence_file = tmp_path / "queue.json"
        data = {
            "version": 99,
            "domain": "verisk",
            "messages": [],
        }
        persistence_file.write_text(json.dumps(data))

        queue = DelayQueue("verisk", persistence_file)
        count = queue.restore_from_disk()
        assert count == 0

    def test_restore_rejects_wrong_domain(self, tmp_path):
        """restore_from_disk ignores file with mismatched domain."""
        persistence_file = tmp_path / "queue.json"
        data = {
            "version": 1,
            "domain": "claimx",
            "messages": [],
        }
        persistence_file.write_text(json.dumps(data))

        queue = DelayQueue("verisk", persistence_file)
        count = queue.restore_from_disk()
        assert count == 0

    def test_restore_handles_corrupt_json(self, tmp_path):
        """restore_from_disk returns 0 on corrupt JSON."""
        persistence_file = tmp_path / "queue.json"
        persistence_file.write_text("not valid json {{{")

        queue = DelayQueue("verisk", persistence_file)
        count = queue.restore_from_disk()
        assert count == 0

    def test_restore_with_none_message_key(self, tmp_path):
        """restore_from_disk handles None message_key."""
        persistence_file = tmp_path / "queue.json"

        data = {
            "version": 1,
            "domain": "verisk",
            "last_persisted": datetime.now(UTC).isoformat(),
            "messages": [
                {
                    "scheduled_time": (datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
                    "target_topic": "test.topic",
                    "retry_count": 1,
                    "worker_type": "download",
                    "message_key": None,
                    "message_value": base64.b64encode(b"val").decode("utf-8"),
                    "headers": {},
                },
            ],
        }
        persistence_file.write_text(json.dumps(data))

        queue = DelayQueue("verisk", persistence_file)
        count = queue.restore_from_disk()

        assert count == 1
        ready = queue.pop_ready(datetime.now(UTC) + timedelta(seconds=60))
        assert ready[0].message_key is None

    def test_roundtrip_persist_and_restore(self, tmp_path):
        """Messages survive a persist/restore cycle."""
        persistence_file = tmp_path / "queue.json"
        queue = DelayQueue("verisk", persistence_file)

        scheduled = datetime.now(UTC) + timedelta(seconds=120)
        queue.push(
            make_delayed_message(
                scheduled_time=scheduled,
                target_topic="roundtrip.topic",
                retry_count=3,
                worker_type="enrichment",
                message_key=b"rkey",
                message_value=b'{"round": "trip"}',
                headers={"retry_count": "3", "domain": "verisk"},
            )
        )

        queue.persist_to_disk()

        restored_queue = DelayQueue("verisk", persistence_file)
        count = restored_queue.restore_from_disk()

        assert count == 1
        ready = restored_queue.pop_ready(scheduled + timedelta(seconds=1))
        assert len(ready) == 1
        msg = ready[0]
        assert msg.target_topic == "roundtrip.topic"
        assert msg.retry_count == 3
        assert msg.worker_type == "enrichment"
        assert msg.message_key == b"rkey"
        assert msg.message_value == b'{"round": "trip"}'
        assert msg.headers == {"retry_count": "3", "domain": "verisk"}
