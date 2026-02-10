"""Tests for event sink implementations (MessageSink, JsonFileSink)."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from pipeline.common.eventhouse.sinks import (
    EventSink,
    JsonFileSink,
    JsonFileSinkConfig,
    MessageSink,
    MessageSinkConfig,
    create_json_sink,
    create_message_sink,
)

# =============================================================================
# Test helpers
# =============================================================================


class FakeEvent(BaseModel):
    event_id: str = "e1"
    data: str = "hello"


# =============================================================================
# EventSink protocol tests
# =============================================================================


class TestEventSinkProtocol:
    def test_message_sink_is_event_sink(self):
        config = MessageSinkConfig(message_config=MagicMock(), domain="test")
        sink = MessageSink(config)
        assert isinstance(sink, EventSink)

    def test_json_file_sink_is_event_sink(self):
        config = JsonFileSinkConfig(output_path=Path("/tmp/out.jsonl"))
        sink = JsonFileSink(config)
        assert isinstance(sink, EventSink)


# =============================================================================
# MessageSink tests
# =============================================================================


class TestMessageSinkStart:
    @patch("pipeline.common.transport.create_producer")
    async def test_start_creates_producer_and_starts(self, mock_create):
        mock_producer = AsyncMock(spec=["start", "stop", "send", "flush"])
        mock_producer.start = AsyncMock()
        mock_create.return_value = mock_producer

        msg_config = MagicMock()
        msg_config.get_topic.return_value = "test-events"

        config = MessageSinkConfig(message_config=msg_config, domain="verisk", worker_name="poller")
        sink = MessageSink(config)
        await sink.start()

        mock_create.assert_called_once()
        mock_producer.start.assert_awaited_once()
        assert sink._topic == "test-events"

    @patch("pipeline.common.transport.create_producer")
    async def test_start_syncs_eventhub_name_when_present(self, mock_create):
        mock_producer = AsyncMock()
        mock_producer.start = AsyncMock()
        mock_producer.eventhub_name = "real-hub-entity"
        mock_create.return_value = mock_producer

        msg_config = MagicMock()
        msg_config.get_topic.return_value = "kafka-topic"

        config = MessageSinkConfig(message_config=msg_config, domain="test")
        sink = MessageSink(config)
        await sink.start()

        assert sink._topic == "real-hub-entity"


class TestMessageSinkStop:
    async def test_stop_stops_producer(self):
        config = MessageSinkConfig(message_config=MagicMock(), domain="test")
        sink = MessageSink(config)
        sink._producer = AsyncMock()
        await sink.stop()
        sink._producer.stop.assert_awaited_once()

    async def test_stop_noop_when_no_producer(self):
        config = MessageSinkConfig(message_config=MagicMock(), domain="test")
        sink = MessageSink(config)
        # No producer set; should not raise
        await sink.stop()


class TestMessageSinkWrite:
    async def test_write_sends_event(self):
        config = MessageSinkConfig(message_config=MagicMock(), domain="test")
        sink = MessageSink(config)
        sink._producer = AsyncMock()
        sink._topic = "events-topic"

        event = FakeEvent()
        await sink.write(key="k1", event=event, headers={"h": "v"})

        sink._producer.send.assert_awaited_once_with(value=event, key="k1", headers={"h": "v"})

    async def test_write_raises_when_not_started(self):
        config = MessageSinkConfig(message_config=MagicMock(), domain="test")
        sink = MessageSink(config)
        with pytest.raises(RuntimeError, match="not started"):
            await sink.write(key="k", event=FakeEvent())

    async def test_write_raises_when_topic_is_none(self):
        config = MessageSinkConfig(message_config=MagicMock(), domain="test")
        sink = MessageSink(config)
        sink._producer = AsyncMock()
        sink._topic = None
        with pytest.raises(RuntimeError, match="not started"):
            await sink.write(key="k", event=FakeEvent())


class TestMessageSinkFlush:
    async def test_flush_calls_producer_flush(self):
        config = MessageSinkConfig(message_config=MagicMock(), domain="test")
        sink = MessageSink(config)
        sink._producer = AsyncMock()
        await sink.flush()
        sink._producer.flush.assert_awaited_once()

    async def test_flush_noop_when_no_producer(self):
        config = MessageSinkConfig(message_config=MagicMock(), domain="test")
        sink = MessageSink(config)
        await sink.flush()  # Should not raise


# =============================================================================
# JsonFileSink tests
# =============================================================================


class TestJsonFileSinkStart:
    async def test_start_creates_file_and_flush_task(self, tmp_path):
        output = tmp_path / "events.jsonl"
        config = JsonFileSinkConfig(output_path=output, flush_interval_seconds=100)
        sink = JsonFileSink(config)
        await sink.start()

        assert sink._running is True
        assert sink._file is not None
        assert sink._flush_task is not None
        assert sink._current_path == output

        await sink.stop()

    async def test_start_creates_parent_directories(self, tmp_path):
        output = tmp_path / "deep" / "nested" / "events.jsonl"
        config = JsonFileSinkConfig(output_path=output, flush_interval_seconds=100)
        sink = JsonFileSink(config)
        await sink.start()

        assert output.parent.exists()
        await sink.stop()

    async def test_start_appends_to_existing_file(self, tmp_path):
        output = tmp_path / "events.jsonl"
        output.write_text("existing data\n")
        config = JsonFileSinkConfig(output_path=output, flush_interval_seconds=100)
        sink = JsonFileSink(config)
        await sink.start()

        assert sink._current_size == len("existing data\n")
        await sink.stop()


class TestJsonFileSinkStop:
    async def test_stop_flushes_and_closes(self, tmp_path):
        output = tmp_path / "events.jsonl"
        config = JsonFileSinkConfig(
            output_path=output,
            buffer_size=1000,
            flush_interval_seconds=100,
        )
        sink = JsonFileSink(config)
        await sink.start()

        event = FakeEvent()
        await sink.write(key="k1", event=event)
        assert len(sink._buffer) == 1

        await sink.stop()

        assert sink._file is None
        assert sink._running is False
        # Buffer should be flushed to file
        content = output.read_text()
        assert "e1" in content


class TestJsonFileSinkWrite:
    async def test_write_buffers_event(self, tmp_path):
        output = tmp_path / "events.jsonl"
        config = JsonFileSinkConfig(
            output_path=output,
            buffer_size=100,
            flush_interval_seconds=100,
        )
        sink = JsonFileSink(config)
        await sink.start()

        event = FakeEvent(event_id="abc", data="test")
        await sink.write(key="k1", event=event)

        assert len(sink._buffer) == 1
        await sink.stop()

    async def test_write_auto_flushes_when_buffer_full(self, tmp_path):
        output = tmp_path / "events.jsonl"
        config = JsonFileSinkConfig(
            output_path=output,
            buffer_size=2,
            flush_interval_seconds=100,
        )
        sink = JsonFileSink(config)
        await sink.start()

        await sink.write(key="k1", event=FakeEvent(event_id="e1"))
        await sink.write(key="k2", event=FakeEvent(event_id="e2"))

        # Buffer should have been flushed
        assert len(sink._buffer) == 0
        assert sink._events_written == 2
        await sink.stop()


class TestJsonFileSinkFlush:
    async def test_flush_writes_to_file(self, tmp_path):
        output = tmp_path / "events.jsonl"
        config = JsonFileSinkConfig(
            output_path=output,
            buffer_size=1000,
            flush_interval_seconds=100,
        )
        sink = JsonFileSink(config)
        await sink.start()

        await sink.write(key="k1", event=FakeEvent(event_id="flush_test"))
        await sink.flush()

        content = output.read_text()
        assert "flush_test" in content
        assert sink._events_written == 1
        await sink.stop()

    async def test_flush_noop_when_buffer_empty(self, tmp_path):
        output = tmp_path / "events.jsonl"
        config = JsonFileSinkConfig(
            output_path=output,
            flush_interval_seconds=100,
        )
        sink = JsonFileSink(config)
        await sink.start()

        await sink.flush()  # No events buffered
        assert output.read_text() == ""
        await sink.stop()


class TestJsonFileSinkFormatRecord:
    def test_includes_metadata_by_default(self):
        config = JsonFileSinkConfig(
            output_path=Path("/tmp/test.jsonl"),
            include_metadata=True,
        )
        sink = JsonFileSink(config)
        event = FakeEvent(event_id="ev1", data="hello")
        record = sink._format_record("mykey", event, {"h": "v"})

        assert record["_key"] == "mykey"
        assert "_timestamp" in record
        assert record["_headers"] == {"h": "v"}
        assert record["event_id"] == "ev1"
        assert record["data"] == "hello"

    def test_excludes_metadata_when_disabled(self):
        config = JsonFileSinkConfig(
            output_path=Path("/tmp/test.jsonl"),
            include_metadata=False,
        )
        sink = JsonFileSink(config)
        event = FakeEvent(event_id="ev1", data="hello")
        record = sink._format_record("mykey", event, None)

        assert "_key" not in record
        assert "_timestamp" not in record
        assert record == {"event_id": "ev1", "data": "hello"}

    def test_metadata_headers_default_to_empty_dict(self):
        config = JsonFileSinkConfig(
            output_path=Path("/tmp/test.jsonl"),
            include_metadata=True,
        )
        sink = JsonFileSink(config)
        record = sink._format_record("k", FakeEvent(), None)
        assert record["_headers"] == {}


class TestJsonFileSinkRotation:
    async def test_rotates_file_when_size_exceeded(self, tmp_path):
        output = tmp_path / "events.jsonl"
        config = JsonFileSinkConfig(
            output_path=output,
            rotate_size_bytes=10,  # Tiny limit to trigger rotation
            buffer_size=1,  # Flush every event
            flush_interval_seconds=100,
        )
        sink = JsonFileSink(config)
        await sink.start()

        # Write enough to exceed 10 bytes
        for i in range(5):
            await sink.write(key=f"k{i}", event=FakeEvent(event_id=f"e{i}"))

        await sink.stop()

        # Check that rotated file was created
        rotated = tmp_path / "events.001.jsonl"
        assert rotated.exists()

    def test_get_output_path_no_rotation(self):
        config = JsonFileSinkConfig(output_path=Path("/tmp/events.jsonl"))
        sink = JsonFileSink(config)
        sink._file_index = 0
        assert sink._get_output_path() == Path("/tmp/events.jsonl")

    def test_get_output_path_with_rotation_index(self):
        config = JsonFileSinkConfig(output_path=Path("/tmp/events.jsonl"))
        sink = JsonFileSink(config)
        sink._file_index = 3
        assert sink._get_output_path() == Path("/tmp/events.003.jsonl")


class TestJsonFileSinkPrettyPrint:
    async def test_pretty_print_uses_indentation(self, tmp_path):
        output = tmp_path / "events.jsonl"
        config = JsonFileSinkConfig(
            output_path=output,
            pretty_print=True,
            buffer_size=1,
            flush_interval_seconds=100,
        )
        sink = JsonFileSink(config)
        await sink.start()

        await sink.write(key="k1", event=FakeEvent(event_id="pretty"))
        # Buffer auto-flushed because buffer_size=1
        content = output.read_text()
        # Pretty-printed JSON has newlines and indentation
        assert "  " in content
        await sink.stop()


class TestJsonFileSinkStats:
    async def test_stats_returns_correct_values(self, tmp_path):
        output = tmp_path / "events.jsonl"
        config = JsonFileSinkConfig(
            output_path=output,
            buffer_size=1,
            flush_interval_seconds=100,
        )
        sink = JsonFileSink(config)
        await sink.start()

        await sink.write(key="k1", event=FakeEvent())

        stats = sink.stats
        assert stats["events_written"] == 1
        assert stats["current_file"] == str(output)
        assert stats["buffer_size"] == 0  # Auto-flushed
        assert stats["file_index"] == 0
        await sink.stop()


# =============================================================================
# Factory function tests
# =============================================================================


class TestCreateMessageSink:
    def test_creates_message_sink_with_config(self):
        msg_config = MagicMock()
        sink = create_message_sink(
            message_config=msg_config, domain="claimx", worker_name="my_worker"
        )
        assert isinstance(sink, MessageSink)
        assert sink.config.domain == "claimx"
        assert sink.config.worker_name == "my_worker"
        assert sink.config.message_config is msg_config


class TestCreateJsonSink:
    def test_creates_json_sink_with_defaults(self):
        sink = create_json_sink(output_path="/tmp/out.jsonl")
        assert isinstance(sink, JsonFileSink)
        assert sink.config.output_path == Path("/tmp/out.jsonl")
        assert sink.config.rotate_size_bytes == 100 * 1024 * 1024
        assert sink.config.pretty_print is False
        assert sink.config.include_metadata is True

    def test_creates_json_sink_with_custom_options(self):
        sink = create_json_sink(
            output_path="/tmp/out.jsonl",
            rotate_size_mb=50.0,
            pretty_print=True,
            include_metadata=False,
        )
        assert sink.config.rotate_size_bytes == 50 * 1024 * 1024
        assert sink.config.pretty_print is True
        assert sink.config.include_metadata is False

    def test_accepts_path_object(self):
        sink = create_json_sink(output_path=Path("/tmp/out.jsonl"))
        assert sink.config.output_path == Path("/tmp/out.jsonl")
