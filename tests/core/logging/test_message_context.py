"""Tests for message-specific logging context."""

import pytest

from core.logging.message_context import (
    MessageLogContext,
    clear_message_context,
    get_message_context,
    set_message_context,
)


@pytest.fixture(autouse=True)
def reset_message_context():
    """Reset message context before each test."""
    clear_message_context()
    yield
    clear_message_context()


class TestSetMessageContext:
    """Tests for set_message_context function."""

    def test_sets_all_fields(self):
        """Test all fields can be set."""
        set_message_context(
            topic="events",
            partition=0,
            offset=12345,
            key="user-123",
            consumer_group="processor-group",
        )

        ctx = get_message_context()
        assert ctx["kafka_topic"] == "events"
        assert ctx["kafka_partition"] == 0
        assert ctx["kafka_offset"] == 12345
        assert ctx["kafka_key"] == "user-123"
        assert ctx["kafka_consumer_group"] == "processor-group"

    def test_sets_subset_of_fields(self):
        """Test only specified fields are set."""
        set_message_context(topic="events", partition=0)

        ctx = get_message_context()
        assert ctx["kafka_topic"] == "events"
        assert ctx["kafka_partition"] == 0
        # Offset should have default value
        assert ctx["kafka_offset"] == -1

    def test_none_values_dont_override(self):
        """Test None values don't override existing context."""
        set_message_context(topic="events", partition=0, offset=100)
        set_message_context(partition=1)  # Only update partition

        ctx = get_message_context()
        assert ctx["kafka_topic"] == "events"  # Should remain
        assert ctx["kafka_partition"] == 1  # Updated
        assert ctx["kafka_offset"] == 100  # Should remain


class TestGetMessageContext:
    """Tests for get_message_context function."""

    def test_returns_empty_context_initially(self):
        """Test returns default values when nothing is set."""
        ctx = get_message_context()
        assert ctx["kafka_topic"] == ""
        assert ctx["kafka_partition"] == -1
        assert ctx["kafka_offset"] == -1
        # Optional fields should not be present
        assert "kafka_key" not in ctx
        assert "kafka_consumer_group" not in ctx

    def test_includes_optional_fields_when_set(self):
        """Test optional fields are included when set."""
        set_message_context(key="msg-key", consumer_group="group-1")

        ctx = get_message_context()
        assert ctx["kafka_key"] == "msg-key"
        assert ctx["kafka_consumer_group"] == "group-1"

    def test_excludes_empty_optional_fields(self):
        """Test optional fields are excluded if empty."""
        set_message_context(topic="events", key="", consumer_group="")

        ctx = get_message_context()
        assert "kafka_key" not in ctx
        assert "kafka_consumer_group" not in ctx


class TestClearMessageContext:
    """Tests for clear_message_context function."""

    def test_clears_all_fields(self):
        """Test all context fields are cleared."""
        set_message_context(
            topic="events",
            partition=0,
            offset=12345,
            key="key",
            consumer_group="group",
        )

        clear_message_context()

        ctx = get_message_context()
        assert ctx["kafka_topic"] == ""
        assert ctx["kafka_partition"] == -1
        assert ctx["kafka_offset"] == -1
        assert "kafka_key" not in ctx
        assert "kafka_consumer_group" not in ctx


class TestMessageLogContext:
    """Tests for MessageLogContext context manager."""

    def test_sets_context_on_enter(self):
        """Test context is set when entering the context manager."""
        with MessageLogContext(topic="events", partition=0, offset=12345):
            ctx = get_message_context()
            assert ctx["kafka_topic"] == "events"
            assert ctx["kafka_partition"] == 0
            assert ctx["kafka_offset"] == 12345

    def test_restores_context_on_exit(self):
        """Test context is restored when exiting the context manager."""
        # Set initial context
        set_message_context(topic="initial", partition=5, offset=999)

        with MessageLogContext(topic="events", partition=0, offset=12345):
            pass

        # Should restore to initial
        ctx = get_message_context()
        assert ctx["kafka_topic"] == "initial"
        assert ctx["kafka_partition"] == 5
        assert ctx["kafka_offset"] == 999

    def test_handles_none_values(self):
        """Test None values don't override existing context."""
        set_message_context(topic="existing", partition=3)

        with MessageLogContext(offset=100):
            ctx = get_message_context()
            assert ctx["kafka_topic"] == "existing"  # Not overridden
            assert ctx["kafka_partition"] == 3  # Not overridden
            assert ctx["kafka_offset"] == 100  # Set

    def test_nested_contexts(self):
        """Test nested context managers work correctly."""
        with MessageLogContext(topic="outer", partition=0, offset=100):
            ctx = get_message_context()
            assert ctx["kafka_topic"] == "outer"
            assert ctx["kafka_offset"] == 100

            with MessageLogContext(topic="inner", partition=1, offset=200):
                ctx = get_message_context()
                assert ctx["kafka_topic"] == "inner"
                assert ctx["kafka_partition"] == 1
                assert ctx["kafka_offset"] == 200

            # Should restore to outer context
            ctx = get_message_context()
            assert ctx["kafka_topic"] == "outer"
            assert ctx["kafka_partition"] == 0
            assert ctx["kafka_offset"] == 100

    def test_sets_optional_fields(self):
        """Test optional fields can be set."""
        with MessageLogContext(
            topic="events",
            partition=0,
            offset=12345,
            key="msg-key",
            consumer_group="group-1",
        ):
            ctx = get_message_context()
            assert ctx["kafka_key"] == "msg-key"
            assert ctx["kafka_consumer_group"] == "group-1"

    def test_restores_empty_optional_fields(self):
        """Test optional fields are properly restored even if empty."""
        set_message_context(key="initial-key")

        with MessageLogContext(topic="events", key="new-key"):
            ctx = get_message_context()
            assert ctx["kafka_key"] == "new-key"

        # Should restore initial key
        ctx = get_message_context()
        assert ctx["kafka_key"] == "initial-key"

    def test_context_survives_exceptions(self):
        """Test context is restored even when exception occurs."""
        set_message_context(topic="initial", partition=0)

        with pytest.raises(ValueError):
            with MessageLogContext(topic="temp", partition=1):
                raise ValueError("test error")

        # Context should be restored
        ctx = get_message_context()
        assert ctx["kafka_topic"] == "initial"
        assert ctx["kafka_partition"] == 0
