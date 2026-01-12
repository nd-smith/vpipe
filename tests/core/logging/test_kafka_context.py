"""Tests for Kafka-specific logging context."""

import pytest

from core.logging.kafka_context import (
    KafkaLogContext,
    clear_kafka_context,
    get_kafka_context,
    set_kafka_context,
)


@pytest.fixture(autouse=True)
def reset_kafka_context():
    """Reset Kafka context before each test."""
    clear_kafka_context()
    yield
    clear_kafka_context()


class TestSetKafkaContext:
    """Tests for set_kafka_context function."""

    def test_sets_all_fields(self):
        """Test all fields can be set."""
        set_kafka_context(
            topic="events",
            partition=0,
            offset=12345,
            key="user-123",
            consumer_group="processor-group",
        )

        ctx = get_kafka_context()
        assert ctx["kafka_topic"] == "events"
        assert ctx["kafka_partition"] == 0
        assert ctx["kafka_offset"] == 12345
        assert ctx["kafka_key"] == "user-123"
        assert ctx["kafka_consumer_group"] == "processor-group"

    def test_sets_subset_of_fields(self):
        """Test only specified fields are set."""
        set_kafka_context(topic="events", partition=0)

        ctx = get_kafka_context()
        assert ctx["kafka_topic"] == "events"
        assert ctx["kafka_partition"] == 0
        # Offset should have default value
        assert ctx["kafka_offset"] == -1

    def test_none_values_dont_override(self):
        """Test None values don't override existing context."""
        set_kafka_context(topic="events", partition=0, offset=100)
        set_kafka_context(partition=1)  # Only update partition

        ctx = get_kafka_context()
        assert ctx["kafka_topic"] == "events"  # Should remain
        assert ctx["kafka_partition"] == 1  # Updated
        assert ctx["kafka_offset"] == 100  # Should remain


class TestGetKafkaContext:
    """Tests for get_kafka_context function."""

    def test_returns_empty_context_initially(self):
        """Test returns default values when nothing is set."""
        ctx = get_kafka_context()
        assert ctx["kafka_topic"] == ""
        assert ctx["kafka_partition"] == -1
        assert ctx["kafka_offset"] == -1
        # Optional fields should not be present
        assert "kafka_key" not in ctx
        assert "kafka_consumer_group" not in ctx

    def test_includes_optional_fields_when_set(self):
        """Test optional fields are included when set."""
        set_kafka_context(key="msg-key", consumer_group="group-1")

        ctx = get_kafka_context()
        assert ctx["kafka_key"] == "msg-key"
        assert ctx["kafka_consumer_group"] == "group-1"

    def test_excludes_empty_optional_fields(self):
        """Test optional fields are excluded if empty."""
        set_kafka_context(topic="events", key="", consumer_group="")

        ctx = get_kafka_context()
        assert "kafka_key" not in ctx
        assert "kafka_consumer_group" not in ctx


class TestClearKafkaContext:
    """Tests for clear_kafka_context function."""

    def test_clears_all_fields(self):
        """Test all context fields are cleared."""
        set_kafka_context(
            topic="events",
            partition=0,
            offset=12345,
            key="key",
            consumer_group="group",
        )

        clear_kafka_context()

        ctx = get_kafka_context()
        assert ctx["kafka_topic"] == ""
        assert ctx["kafka_partition"] == -1
        assert ctx["kafka_offset"] == -1
        assert "kafka_key" not in ctx
        assert "kafka_consumer_group" not in ctx


class TestKafkaLogContext:
    """Tests for KafkaLogContext context manager."""

    def test_sets_context_on_enter(self):
        """Test context is set when entering the context manager."""
        with KafkaLogContext(topic="events", partition=0, offset=12345):
            ctx = get_kafka_context()
            assert ctx["kafka_topic"] == "events"
            assert ctx["kafka_partition"] == 0
            assert ctx["kafka_offset"] == 12345

    def test_restores_context_on_exit(self):
        """Test context is restored when exiting the context manager."""
        # Set initial context
        set_kafka_context(topic="initial", partition=5, offset=999)

        with KafkaLogContext(topic="events", partition=0, offset=12345):
            pass

        # Should restore to initial
        ctx = get_kafka_context()
        assert ctx["kafka_topic"] == "initial"
        assert ctx["kafka_partition"] == 5
        assert ctx["kafka_offset"] == 999

    def test_handles_none_values(self):
        """Test None values don't override existing context."""
        set_kafka_context(topic="existing", partition=3)

        with KafkaLogContext(offset=100):
            ctx = get_kafka_context()
            assert ctx["kafka_topic"] == "existing"  # Not overridden
            assert ctx["kafka_partition"] == 3  # Not overridden
            assert ctx["kafka_offset"] == 100  # Set

    def test_nested_contexts(self):
        """Test nested context managers work correctly."""
        with KafkaLogContext(topic="outer", partition=0, offset=100):
            ctx = get_kafka_context()
            assert ctx["kafka_topic"] == "outer"
            assert ctx["kafka_offset"] == 100

            with KafkaLogContext(topic="inner", partition=1, offset=200):
                ctx = get_kafka_context()
                assert ctx["kafka_topic"] == "inner"
                assert ctx["kafka_partition"] == 1
                assert ctx["kafka_offset"] == 200

            # Should restore to outer context
            ctx = get_kafka_context()
            assert ctx["kafka_topic"] == "outer"
            assert ctx["kafka_partition"] == 0
            assert ctx["kafka_offset"] == 100

    def test_sets_optional_fields(self):
        """Test optional fields can be set."""
        with KafkaLogContext(
            topic="events",
            partition=0,
            offset=12345,
            key="msg-key",
            consumer_group="group-1",
        ):
            ctx = get_kafka_context()
            assert ctx["kafka_key"] == "msg-key"
            assert ctx["kafka_consumer_group"] == "group-1"

    def test_restores_empty_optional_fields(self):
        """Test optional fields are properly restored even if empty."""
        set_kafka_context(key="initial-key")

        with KafkaLogContext(topic="events", key="new-key"):
            ctx = get_kafka_context()
            assert ctx["kafka_key"] == "new-key"

        # Should restore initial key
        ctx = get_kafka_context()
        assert ctx["kafka_key"] == "initial-key"

    def test_context_survives_exceptions(self):
        """Test context is restored even when exception occurs."""
        set_kafka_context(topic="initial", partition=0)

        with pytest.raises(ValueError):
            with KafkaLogContext(topic="temp", partition=1):
                raise ValueError("test error")

        # Context should be restored
        ctx = get_kafka_context()
        assert ctx["kafka_topic"] == "initial"
        assert ctx["kafka_partition"] == 0
