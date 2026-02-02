"""
Tests for transport layer factory functions.

These are unit tests that use mocks - no Azure EventHub or Kafka required.
"""

import asyncio
import logging
import os
import pytest
import sys
from unittest.mock import AsyncMock, MagicMock, Mock, patch, call

from aiokafka.structs import ConsumerRecord

# Mock azure.eventhub before importing transport module
sys.modules['azure'] = MagicMock()
sys.modules['azure.eventhub'] = MagicMock()
sys.modules['azure.eventhub.aio'] = MagicMock()
sys.modules['azure.eventhub.extensions'] = MagicMock()
sys.modules['azure.eventhub.extensions.checkpointstoreblobaio'] = MagicMock()


class TestCreateConsumerCheckpointStore:
    """Test create_consumer checkpoint store integration."""

    @pytest.fixture(autouse=True)
    def clear_env_vars(self):
        """Clear transport-related environment variables before each test."""
        env_vars = [
            "PIPELINE_TRANSPORT",
            "EVENTHUB_NAMESPACE_CONNECTION_STRING",
            "EVENTHUB_CONNECTION_STRING",
        ]
        original_values = {}
        for var in env_vars:
            original_values[var] = os.environ.get(var)
            if var in os.environ:
                del os.environ[var]

        yield

        # Restore original values
        for var, value in original_values.items():
            if value is not None:
                os.environ[var] = value
            elif var in os.environ:
                del os.environ[var]

    @pytest.fixture(autouse=True)
    def reset_checkpoint_store(self):
        """Reset checkpoint store singleton before each test."""
        from pipeline.common.eventhub.checkpoint_store import reset_checkpoint_store
        reset_checkpoint_store()
        yield
        reset_checkpoint_store()

    @pytest.fixture(autouse=True)
    def reset_eventhub_config(self):
        """Reset eventhub config cache before each test."""
        from pipeline.common.transport import reset_eventhub_config
        reset_eventhub_config()
        yield
        reset_eventhub_config()

    @pytest.mark.asyncio
    async def test_create_consumer_eventhub_gets_checkpoint_store(self):
        """Test create_consumer gets checkpoint store when using EventHub transport."""
        from pipeline.common.transport import create_consumer, TransportType
        from config.config import KafkaConfig

        # Set EventHub transport
        os.environ["PIPELINE_TRANSPORT"] = "eventhub"
        os.environ["EVENTHUB_NAMESPACE_CONNECTION_STRING"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123"

        mock_kafka_config = MagicMock(spec=KafkaConfig)
        mock_kafka_config.get_consumer_group.return_value = "$Default"

        mock_handler = AsyncMock()
        mock_checkpoint_store = MagicMock()
        mock_eventhub_consumer = MagicMock()

        # Mock get_checkpoint_store to return a mock store
        with patch("pipeline.common.transport.get_checkpoint_store", new_callable=AsyncMock) as mock_get_checkpoint:
            mock_get_checkpoint.return_value = mock_checkpoint_store

            # Mock EventHubConsumer constructor
            with patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_consumer_class:
                mock_consumer_class.return_value = mock_eventhub_consumer

                consumer = await create_consumer(
                    config=mock_kafka_config,
                    domain="verisk",
                    worker_name="test_worker",
                    topics=["verisk_events"],
                    message_handler=mock_handler,
                    transport_type=TransportType.EVENTHUB,
                )

        # Verify get_checkpoint_store was called
        mock_get_checkpoint.assert_called_once()

        # Verify checkpoint_store was passed to EventHubConsumer
        mock_consumer_class.assert_called_once()
        call_kwargs = mock_consumer_class.call_args.kwargs
        assert call_kwargs["checkpoint_store"] is mock_checkpoint_store

    @pytest.mark.asyncio
    async def test_create_consumer_eventhub_passes_none_when_checkpoint_store_not_configured(self):
        """Test create_consumer passes None when checkpoint store is not configured."""
        from pipeline.common.transport import create_consumer, TransportType
        from config.config import KafkaConfig

        os.environ["PIPELINE_TRANSPORT"] = "eventhub"
        os.environ["EVENTHUB_NAMESPACE_CONNECTION_STRING"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123"

        mock_kafka_config = MagicMock(spec=KafkaConfig)
        mock_kafka_config.get_consumer_group.return_value = "$Default"

        mock_handler = AsyncMock()
        mock_eventhub_consumer = MagicMock()

        # Mock get_checkpoint_store to return None (not configured)
        with patch("pipeline.common.transport.get_checkpoint_store", new_callable=AsyncMock) as mock_get_checkpoint:
            mock_get_checkpoint.return_value = None

            with patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_consumer_class:
                mock_consumer_class.return_value = mock_eventhub_consumer

                consumer = await create_consumer(
                    config=mock_kafka_config,
                    domain="verisk",
                    worker_name="test_worker",
                    topics=["verisk_events"],
                    message_handler=mock_handler,
                    transport_type=TransportType.EVENTHUB,
                )

        # Verify get_checkpoint_store was called
        mock_get_checkpoint.assert_called_once()

        # Verify checkpoint_store=None was passed to EventHubConsumer
        mock_consumer_class.assert_called_once()
        call_kwargs = mock_consumer_class.call_args.kwargs
        assert call_kwargs["checkpoint_store"] is None

    @pytest.mark.asyncio
    async def test_create_consumer_eventhub_handles_checkpoint_store_error(self, caplog):
        """Test create_consumer handles checkpoint store initialization error gracefully."""
        from pipeline.common.transport import create_consumer, TransportType
        from config.config import KafkaConfig

        os.environ["PIPELINE_TRANSPORT"] = "eventhub"
        os.environ["EVENTHUB_NAMESPACE_CONNECTION_STRING"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123"

        mock_kafka_config = MagicMock(spec=KafkaConfig)
        mock_kafka_config.get_consumer_group.return_value = "$Default"

        mock_handler = AsyncMock()
        mock_eventhub_consumer = MagicMock()

        # Mock get_checkpoint_store to raise an error
        with patch("pipeline.common.transport.get_checkpoint_store", new_callable=AsyncMock) as mock_get_checkpoint:
            mock_get_checkpoint.side_effect = RuntimeError("Azure connection failed")

            with patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_consumer_class:
                mock_consumer_class.return_value = mock_eventhub_consumer

                with caplog.at_level(logging.ERROR):
                    consumer = await create_consumer(
                        config=mock_kafka_config,
                        domain="verisk",
                        worker_name="test_worker",
                        topics=["verisk_events"],
                        message_handler=mock_handler,
                        transport_type=TransportType.EVENTHUB,
                    )

        # Consumer should still be created (fallback to in-memory)
        assert consumer is mock_eventhub_consumer

        # Verify checkpoint_store=None was passed (fallback to in-memory)
        mock_consumer_class.assert_called_once()
        call_kwargs = mock_consumer_class.call_args.kwargs
        assert call_kwargs["checkpoint_store"] is None

        # Verify error was logged
        log_messages = [record.message for record in caplog.records if record.levelname == "ERROR"]
        assert any("checkpoint store" in msg.lower() for msg in log_messages)

    @pytest.mark.asyncio
    async def test_create_consumer_eventhub_logs_checkpoint_mode(self, caplog):
        """Test create_consumer logs appropriate message based on checkpoint store availability."""
        from pipeline.common.transport import create_consumer, TransportType
        from config.config import KafkaConfig

        os.environ["PIPELINE_TRANSPORT"] = "eventhub"
        os.environ["EVENTHUB_NAMESPACE_CONNECTION_STRING"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123"

        mock_kafka_config = MagicMock(spec=KafkaConfig)
        mock_kafka_config.get_consumer_group.return_value = "$Default"

        mock_handler = AsyncMock()
        mock_checkpoint_store = MagicMock()
        mock_eventhub_consumer = MagicMock()

        with patch("pipeline.common.transport.get_checkpoint_store", new_callable=AsyncMock) as mock_get_checkpoint:
            mock_get_checkpoint.return_value = mock_checkpoint_store

            with patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_consumer_class:
                mock_consumer_class.return_value = mock_eventhub_consumer

                with caplog.at_level(logging.INFO):
                    await create_consumer(
                        config=mock_kafka_config,
                        domain="verisk",
                        worker_name="test_worker",
                        topics=["verisk_events"],
                        message_handler=mock_handler,
                        transport_type=TransportType.EVENTHUB,
                    )

        # Check for log message about blob checkpoint store
        log_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        assert any("blob checkpoint store" in msg.lower() for msg in log_messages)

    @pytest.mark.asyncio
    async def test_create_consumer_eventhub_logs_in_memory_mode(self, caplog):
        """Test create_consumer logs when using in-memory checkpointing."""
        from pipeline.common.transport import create_consumer, TransportType
        from config.config import KafkaConfig

        os.environ["PIPELINE_TRANSPORT"] = "eventhub"
        os.environ["EVENTHUB_NAMESPACE_CONNECTION_STRING"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123"

        mock_kafka_config = MagicMock(spec=KafkaConfig)
        mock_kafka_config.get_consumer_group.return_value = "$Default"

        mock_handler = AsyncMock()
        mock_eventhub_consumer = MagicMock()

        with patch("pipeline.common.transport.get_checkpoint_store", new_callable=AsyncMock) as mock_get_checkpoint:
            mock_get_checkpoint.return_value = None

            with patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_consumer_class:
                mock_consumer_class.return_value = mock_eventhub_consumer

                with caplog.at_level(logging.INFO):
                    await create_consumer(
                        config=mock_kafka_config,
                        domain="verisk",
                        worker_name="test_worker",
                        topics=["verisk_events"],
                        message_handler=mock_handler,
                        transport_type=TransportType.EVENTHUB,
                    )

        # Check for log message about in-memory checkpointing
        log_messages = [record.message for record in caplog.records if record.levelname == "INFO"]
        assert any("in-memory checkpointing" in msg.lower() for msg in log_messages)

    @pytest.mark.asyncio
    async def test_create_consumer_kafka_does_not_use_checkpoint_store(self):
        """Test create_consumer does not attempt to get checkpoint store for Kafka transport."""
        from pipeline.common.transport import create_consumer, TransportType
        from config.config import KafkaConfig

        os.environ["PIPELINE_TRANSPORT"] = "kafka"

        mock_kafka_config = MagicMock(spec=KafkaConfig)
        mock_kafka_config.bootstrap_servers = "localhost:9092"
        mock_kafka_config.get_consumer_group.return_value = "test-group"

        mock_handler = AsyncMock()
        mock_kafka_consumer = MagicMock()

        with patch("pipeline.common.transport.get_checkpoint_store", new_callable=AsyncMock) as mock_get_checkpoint:
            with patch("pipeline.common.consumer.BaseKafkaConsumer") as mock_consumer_class:
                mock_consumer_class.return_value = mock_kafka_consumer

                consumer = await create_consumer(
                    config=mock_kafka_config,
                    domain="verisk",
                    worker_name="test_worker",
                    topics=["verisk_events"],
                    message_handler=mock_handler,
                    transport_type=TransportType.KAFKA,
                )

        # Verify get_checkpoint_store was NOT called for Kafka
        mock_get_checkpoint.assert_not_called()

        # Verify BaseKafkaConsumer was created
        mock_consumer_class.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_consumer_with_topic_key_resolves_eventhub_name(self):
        """Test create_consumer uses topic_key to resolve EventHub name from config."""
        from pipeline.common.transport import create_consumer, TransportType
        from config.config import KafkaConfig

        os.environ["PIPELINE_TRANSPORT"] = "eventhub"
        os.environ["EVENTHUB_NAMESPACE_CONNECTION_STRING"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test123"

        mock_kafka_config = MagicMock(spec=KafkaConfig)
        mock_kafka_config.get_consumer_group.return_value = "$Default"

        mock_handler = AsyncMock()
        mock_checkpoint_store = MagicMock()
        mock_eventhub_consumer = MagicMock()

        # Mock config loading to return EventHub name for topic_key
        mock_eventhub_config = {
            "xact": {
                "events": {
                    "eventhub_name": "com.allstate.pcesdopodappv1.verisk.events.raw",
                    "consumer_group": "xact-workers"
                }
            }
        }

        with patch("pipeline.common.transport.get_checkpoint_store", new_callable=AsyncMock) as mock_get_checkpoint:
            mock_get_checkpoint.return_value = mock_checkpoint_store

            with patch("pipeline.common.transport._load_eventhub_config") as mock_load_config:
                mock_load_config.return_value = mock_eventhub_config

                with patch("pipeline.common.eventhub.consumer.EventHubConsumer") as mock_consumer_class:
                    mock_consumer_class.return_value = mock_eventhub_consumer

                    consumer = await create_consumer(
                        config=mock_kafka_config,
                        domain="verisk",
                        worker_name="test_worker",
                        topics=["verisk_events"],  # This is ignored when topic_key is provided
                        message_handler=mock_handler,
                        transport_type=TransportType.EVENTHUB,
                        topic_key="events",
                    )

        # Verify EventHubConsumer was created with correct eventhub_name from config
        mock_consumer_class.assert_called_once()
        call_kwargs = mock_consumer_class.call_args.kwargs
        assert call_kwargs["eventhub_name"] == "com.allstate.pcesdopodappv1.verisk.events.raw"
        assert call_kwargs["consumer_group"] == "xact-workers"
        assert call_kwargs["checkpoint_store"] is mock_checkpoint_store
