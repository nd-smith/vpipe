"""Tests for core.logging.context module."""

from core.logging.context import (
    clear_log_context,
    clear_message_context,
    get_log_context,
    get_message_context,
    set_log_context,
    set_message_context,
)


class TestLogContext:
    def setup_method(self):
        clear_log_context()

    def teardown_method(self):
        clear_log_context()

    def test_defaults_are_empty(self):
        ctx = get_log_context()
        assert ctx["cycle_id"] == ""
        assert ctx["stage"] == ""
        assert ctx["worker_id"] == ""
        assert ctx["domain"] == ""
        assert ctx["trace_id"] == ""
        assert ctx["media_id"] == ""
        assert ctx["instance_id"] == ""

    def test_set_all_fields(self):
        set_log_context(
            cycle_id="c1",
            stage="download",
            worker_id="w1",
            domain="verisk",
            trace_id="t1",
            media_id="m1",
            instance_id=42,
        )
        ctx = get_log_context()
        assert ctx["cycle_id"] == "c1"
        assert ctx["stage"] == "download"
        assert ctx["worker_id"] == "w1"
        assert ctx["domain"] == "verisk"
        assert ctx["trace_id"] == "t1"
        assert ctx["media_id"] == "m1"
        assert ctx["instance_id"] == "42"  # converted to str

    def test_partial_set_preserves_others(self):
        set_log_context(cycle_id="c1", stage="download")
        set_log_context(stage="upload")
        ctx = get_log_context()
        assert ctx["cycle_id"] == "c1"
        assert ctx["stage"] == "upload"

    def test_clear_resets_all(self):
        set_log_context(cycle_id="c1", stage="download", worker_id="w1")
        clear_log_context()
        ctx = get_log_context()
        assert all(v == "" for v in ctx.values())

    def test_instance_id_string(self):
        set_log_context(instance_id="str-id")
        assert get_log_context()["instance_id"] == "str-id"


class TestMessageContext:
    def setup_method(self):
        clear_message_context()

    def teardown_method(self):
        clear_message_context()

    def test_defaults(self):
        ctx = get_message_context()
        assert ctx["message_topic"] == ""
        assert ctx["message_partition"] == -1
        assert ctx["message_offset"] == -1
        assert "message_key" not in ctx
        assert "message_consumer_group" not in ctx

    def test_set_all_fields(self):
        set_message_context(
            topic="my-topic",
            partition=3,
            offset=100,
            key="key1",
            consumer_group="group1",
        )
        ctx = get_message_context()
        assert ctx["message_topic"] == "my-topic"
        assert ctx["message_partition"] == 3
        assert ctx["message_offset"] == 100
        assert ctx["message_key"] == "key1"
        assert ctx["message_consumer_group"] == "group1"

    def test_key_and_consumer_group_omitted_when_empty(self):
        set_message_context(topic="t", partition=0, offset=0)
        ctx = get_message_context()
        assert "message_key" not in ctx
        assert "message_consumer_group" not in ctx

    def test_clear_resets_all(self):
        set_message_context(topic="t", partition=1, offset=2, key="k", consumer_group="g")
        clear_message_context()
        ctx = get_message_context()
        assert ctx["message_topic"] == ""
        assert ctx["message_partition"] == -1
        assert ctx["message_offset"] == -1
