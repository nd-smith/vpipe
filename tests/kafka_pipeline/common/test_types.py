"""
Tests for transport-agnostic message types.

Tests PipelineMessage, ProduceResult, PartitionInfo, and conversion helpers.
These are pure unit tests with no external dependencies.
"""

import pytest
from aiokafka.structs import ConsumerRecord

from kafka_pipeline.common.types import (
    PipelineMessage,
    ProduceResult,
    PartitionInfo,
    from_consumer_record,
)


class TestPipelineMessage:
    """Tests for PipelineMessage dataclass."""

    def test_creation_minimal(self):
        """Test creating PipelineMessage with minimal required fields."""
        msg = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
        )

        assert msg.topic == "test-topic"
        assert msg.partition == 0
        assert msg.offset == 100
        assert msg.timestamp == 1234567890
        assert msg.key is None
        assert msg.value is None
        assert msg.headers is None

    def test_creation_full(self):
        """Test creating PipelineMessage with all fields."""
        headers = [("header1", b"value1"), ("header2", b"value2")]

        msg = PipelineMessage(
            topic="test-topic",
            partition=3,
            offset=12345,
            timestamp=9876543210,
            key=b"test-key",
            value=b'{"test": "data"}',
            headers=headers,
        )

        assert msg.topic == "test-topic"
        assert msg.partition == 3
        assert msg.offset == 12345
        assert msg.timestamp == 9876543210
        assert msg.key == b"test-key"
        assert msg.value == b'{"test": "data"}'
        assert msg.headers == headers

    def test_immutability(self):
        """Test that PipelineMessage is immutable (frozen dataclass)."""
        msg = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
        )

        # Attempt to modify fields should raise FrozenInstanceError
        with pytest.raises(Exception):  # dataclasses.FrozenInstanceError
            msg.topic = "modified-topic"

        with pytest.raises(Exception):
            msg.partition = 999

        with pytest.raises(Exception):
            msg.offset = 999

    def test_equality(self):
        """Test PipelineMessage equality comparison."""
        msg1 = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            key=b"key1",
            value=b"value1",
        )

        msg2 = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            key=b"key1",
            value=b"value1",
        )

        msg3 = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=101,  # Different offset
            timestamp=1234567890,
            key=b"key1",
            value=b"value1",
        )

        assert msg1 == msg2
        assert msg1 != msg3

    def test_headers_format(self):
        """Test headers are stored as List[Tuple[str, bytes]]."""
        headers = [
            ("trace-id", b"abc123"),
            ("correlation-id", b"xyz789"),
            ("content-type", b"application/json"),
        ]

        msg = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            headers=headers,
        )

        assert len(msg.headers) == 3
        assert msg.headers[0] == ("trace-id", b"abc123")
        assert msg.headers[1] == ("correlation-id", b"xyz789")
        assert msg.headers[2] == ("content-type", b"application/json")

    def test_bytes_fields(self):
        """Test key and value are bytes type."""
        msg = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            key=b"binary-key",
            value=b"\x00\x01\x02\x03",  # Binary data
        )

        assert isinstance(msg.key, bytes)
        assert isinstance(msg.value, bytes)
        assert msg.key == b"binary-key"
        assert msg.value == b"\x00\x01\x02\x03"

    def test_empty_headers(self):
        """Test empty headers list."""
        msg = PipelineMessage(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            headers=[],
        )

        assert msg.headers == []


class TestProduceResult:
    """Tests for ProduceResult dataclass."""

    def test_creation(self):
        """Test creating ProduceResult."""
        result = ProduceResult(
            topic="test-topic",
            partition=2,
            offset=54321,
        )

        assert result.topic == "test-topic"
        assert result.partition == 2
        assert result.offset == 54321

    def test_immutability(self):
        """Test that ProduceResult is immutable (frozen dataclass)."""
        result = ProduceResult(
            topic="test-topic",
            partition=1,
            offset=100,
        )

        # Attempt to modify fields should raise FrozenInstanceError
        with pytest.raises(Exception):  # dataclasses.FrozenInstanceError
            result.topic = "modified-topic"

        with pytest.raises(Exception):
            result.partition = 999

        with pytest.raises(Exception):
            result.offset = 999

    def test_equality(self):
        """Test ProduceResult equality comparison."""
        result1 = ProduceResult(topic="test-topic", partition=0, offset=100)
        result2 = ProduceResult(topic="test-topic", partition=0, offset=100)
        result3 = ProduceResult(topic="test-topic", partition=1, offset=100)

        assert result1 == result2
        assert result1 != result3

    def test_use_as_confirmation(self):
        """Test ProduceResult can be used to verify message was persisted."""
        result = ProduceResult(topic="events", partition=3, offset=12345)

        # Verify we can extract confirmation details
        assert result.topic == "events"
        assert result.partition == 3
        assert result.offset == 12345

        # Can be used to construct unique message identifier
        message_id = f"{result.topic}-{result.partition}-{result.offset}"
        assert message_id == "events-3-12345"


class TestPartitionInfo:
    """Tests for PartitionInfo dataclass."""

    def test_creation(self):
        """Test creating PartitionInfo."""
        partition = PartitionInfo(topic="test-topic", partition=5)

        assert partition.topic == "test-topic"
        assert partition.partition == 5

    def test_immutability(self):
        """Test that PartitionInfo is immutable (frozen dataclass)."""
        partition = PartitionInfo(topic="test-topic", partition=0)

        # Attempt to modify fields should raise FrozenInstanceError
        with pytest.raises(Exception):  # dataclasses.FrozenInstanceError
            partition.topic = "modified-topic"

        with pytest.raises(Exception):
            partition.partition = 999

    def test_equality(self):
        """Test PartitionInfo equality comparison."""
        p1 = PartitionInfo(topic="test-topic", partition=0)
        p2 = PartitionInfo(topic="test-topic", partition=0)
        p3 = PartitionInfo(topic="test-topic", partition=1)
        p4 = PartitionInfo(topic="other-topic", partition=0)

        assert p1 == p2
        assert p1 != p3
        assert p1 != p4

    def test_hashable(self):
        """Test PartitionInfo is hashable (can be used as dict key)."""
        p1 = PartitionInfo(topic="test-topic", partition=0)
        p2 = PartitionInfo(topic="test-topic", partition=1)
        p3 = PartitionInfo(topic="other-topic", partition=0)

        # Should be able to use as dict key
        partition_offsets = {
            p1: 100,
            p2: 200,
            p3: 300,
        }

        assert partition_offsets[p1] == 100
        assert partition_offsets[p2] == 200
        assert partition_offsets[p3] == 300

    def test_use_in_set(self):
        """Test PartitionInfo can be used in sets."""
        p1 = PartitionInfo(topic="test-topic", partition=0)
        p2 = PartitionInfo(topic="test-topic", partition=0)  # Duplicate
        p3 = PartitionInfo(topic="test-topic", partition=1)

        partition_set = {p1, p2, p3}

        # p1 and p2 are equal, so set should only contain 2 items
        assert len(partition_set) == 2
        assert p1 in partition_set
        assert p3 in partition_set

    def test_hash_consistency(self):
        """Test PartitionInfo hash is consistent."""
        p1 = PartitionInfo(topic="test-topic", partition=0)
        p2 = PartitionInfo(topic="test-topic", partition=0)

        # Equal objects should have equal hashes
        assert hash(p1) == hash(p2)


class TestFromConsumerRecord:
    """Tests for from_consumer_record() conversion helper."""

    def create_consumer_record(
        self,
        topic: str = "test-topic",
        partition: int = 0,
        offset: int = 100,
        timestamp: int = 1234567890,
        key: bytes = b"test-key",
        value: bytes = b'{"test": "value"}',
        headers: list = None,
    ) -> ConsumerRecord:
        """Helper to create ConsumerRecord for testing."""
        return ConsumerRecord(
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp=timestamp,
            timestamp_type=0,
            key=key,
            value=value,
            headers=headers or [],
            checksum=None,
            serialized_key_size=len(key) if key else 0,
            serialized_value_size=len(value) if value else 0,
        )

    def test_basic_conversion(self):
        """Test basic ConsumerRecord to PipelineMessage conversion."""
        record = self.create_consumer_record(
            topic="test-topic",
            partition=2,
            offset=12345,
            timestamp=9876543210,
            key=b"key1",
            value=b"value1",
        )

        msg = from_consumer_record(record)

        assert isinstance(msg, PipelineMessage)
        assert msg.topic == "test-topic"
        assert msg.partition == 2
        assert msg.offset == 12345
        assert msg.timestamp == 9876543210
        assert msg.key == b"key1"
        assert msg.value == b"value1"

    def test_conversion_with_headers(self):
        """Test conversion preserves headers."""
        headers = [("header1", b"value1"), ("header2", b"value2")]
        record = self.create_consumer_record(headers=headers)

        msg = from_consumer_record(record)

        assert msg.headers == headers
        assert len(msg.headers) == 2

    def test_conversion_without_headers(self):
        """Test conversion when record has no headers."""
        record = self.create_consumer_record(headers=[])

        msg = from_consumer_record(record)

        assert msg.headers is None

    def test_conversion_with_none_headers(self):
        """Test conversion when record.headers is None."""
        record = self.create_consumer_record()
        record.headers = None  # Explicitly set to None

        msg = from_consumer_record(record)

        assert msg.headers is None

    def test_conversion_with_none_key(self):
        """Test conversion when record has None key."""
        record = self.create_consumer_record(key=None)

        msg = from_consumer_record(record)

        assert msg.key is None

    def test_conversion_with_none_value(self):
        """Test conversion when record has None value."""
        record = self.create_consumer_record(value=None)

        msg = from_consumer_record(record)

        assert msg.value is None

    def test_conversion_preserves_field_values(self):
        """Test all field values are preserved exactly."""
        headers = [
            ("trace-id", b"abc123"),
            ("correlation-id", b"xyz789"),
        ]

        record = self.create_consumer_record(
            topic="events.raw",
            partition=7,
            offset=999999,
            timestamp=1609459200000,  # 2021-01-01 00:00:00 UTC
            key=b"entity-id-12345",
            value=b'{"event": "test", "data": {"field": "value"}}',
            headers=headers,
        )

        msg = from_consumer_record(record)

        # Verify exact field preservation
        assert msg.topic == "events.raw"
        assert msg.partition == 7
        assert msg.offset == 999999
        assert msg.timestamp == 1609459200000
        assert msg.key == b"entity-id-12345"
        assert msg.value == b'{"event": "test", "data": {"field": "value"}}'
        assert msg.headers == headers

    def test_conversion_multiple_records(self):
        """Test converting multiple records in sequence."""
        records = [
            self.create_consumer_record(offset=100, key=b"key1"),
            self.create_consumer_record(offset=101, key=b"key2"),
            self.create_consumer_record(offset=102, key=b"key3"),
        ]

        messages = [from_consumer_record(r) for r in records]

        assert len(messages) == 3
        assert messages[0].offset == 100
        assert messages[0].key == b"key1"
        assert messages[1].offset == 101
        assert messages[1].key == b"key2"
        assert messages[2].offset == 102
        assert messages[2].key == b"key3"


class TestFromConsumerRecordEdgeCases:
    """Additional tests for from_consumer_record() edge cases."""

    def test_conversion_with_binary_data(self):
        """Test conversion preserves binary data correctly."""
        binary_value = b"\x00\x01\x02\x03\xff\xfe\xfd"
        binary_key = b"\x80\x81\x82"

        record = ConsumerRecord(
            topic="binary-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            timestamp_type=0,
            key=binary_key,
            value=binary_value,
            headers=[],
            checksum=None,
            serialized_key_size=len(binary_key),
            serialized_value_size=len(binary_value),
        )

        msg = from_consumer_record(record)

        assert msg.key == binary_key
        assert msg.value == binary_value

    def test_conversion_with_large_headers(self):
        """Test conversion with many headers."""
        headers = [(f"header-{i}", f"value-{i}".encode("utf-8")) for i in range(20)]

        record = ConsumerRecord(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=headers,
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        msg = from_consumer_record(record)

        assert len(msg.headers) == 20
        assert msg.headers == headers

    def test_conversion_with_unicode_headers(self):
        """Test conversion with unicode header values."""
        headers = [
            ("header1", "Hello 世界".encode("utf-8")),
            ("header2", "Привет мир".encode("utf-8")),
        ]

        record = ConsumerRecord(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=1234567890,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=headers,
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        msg = from_consumer_record(record)

        assert msg.headers == headers
        # Verify we can decode the unicode
        assert headers[0][1].decode("utf-8") == "Hello 世界"

    def test_conversion_timestamp_milliseconds(self):
        """Test conversion preserves millisecond timestamp precision."""
        # Kafka timestamps are in milliseconds
        timestamp_ms = 1609459200123  # 2021-01-01 00:00:00.123 UTC

        record = ConsumerRecord(
            topic="test-topic",
            partition=0,
            offset=100,
            timestamp=timestamp_ms,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        msg = from_consumer_record(record)

        # Timestamp should be preserved exactly
        assert msg.timestamp == timestamp_ms

    def test_conversion_zero_offset(self):
        """Test conversion with offset=0 (first message in partition)."""
        record = ConsumerRecord(
            topic="test-topic",
            partition=0,
            offset=0,
            timestamp=1234567890,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        msg = from_consumer_record(record)

        assert msg.offset == 0

    def test_conversion_high_partition_number(self):
        """Test conversion with high partition number."""
        record = ConsumerRecord(
            topic="test-topic",
            partition=999,
            offset=100,
            timestamp=1234567890,
            timestamp_type=0,
            key=b"key",
            value=b"value",
            headers=[],
            checksum=None,
            serialized_key_size=3,
            serialized_value_size=5,
        )

        msg = from_consumer_record(record)

        assert msg.partition == 999


class TestTypeCompatibility:
    """Tests for type compatibility and usage patterns."""

    def test_pipeline_message_in_handler(self):
        """Test PipelineMessage can be used in message handler pattern."""
        import json

        msg = PipelineMessage(
            topic="events",
            partition=0,
            offset=100,
            timestamp=1234567890,
            key=b"entity-123",
            value=json.dumps({"event": "test", "data": "value"}).encode("utf-8"),
        )

        # Simulate message handler usage
        payload = json.loads(msg.value)
        assert payload["event"] == "test"
        assert payload["data"] == "value"

        # Simulate key extraction
        entity_id = msg.key.decode("utf-8")
        assert entity_id == "entity-123"

    def test_produce_result_in_producer_pattern(self):
        """Test ProduceResult can be used in producer pattern."""
        result = ProduceResult(topic="events", partition=3, offset=12345)

        # Simulate producer confirmation logging
        confirmation_msg = (
            f"Published to {result.topic} "
            f"partition {result.partition} "
            f"at offset {result.offset}"
        )
        assert confirmation_msg == "Published to events partition 3 at offset 12345"

    def test_partition_info_for_offset_management(self):
        """Test PartitionInfo can be used for offset management."""
        # Simulate offset tracking per partition
        offsets = {}

        p1 = PartitionInfo(topic="events", partition=0)
        p2 = PartitionInfo(topic="events", partition=1)
        p3 = PartitionInfo(topic="events", partition=2)

        offsets[p1] = 100
        offsets[p2] = 200
        offsets[p3] = 300

        # Simulate offset lookup
        assert offsets[p1] == 100
        assert offsets[PartitionInfo(topic="events", partition=1)] == 200
