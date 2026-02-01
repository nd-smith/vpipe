"""Azure Event Hub transport adapters.

Provides Event Hub producer and consumer implementations that are compatible
with the BaseKafkaProducer and BaseKafkaConsumer interfaces.

Uses azure-eventhub SDK with AMQP over WebSocket transport for compatibility
with Azure Private Link endpoints.

Imports are lazy to avoid pulling in consumer dependencies (e.g. EventPosition)
when only the producer is needed.
"""

__all__ = [
    "EventHubProducer",
    "EventHubRecordMetadata",
    "EventHubConsumer",
    "EventHubConsumerRecord",
    "get_checkpoint_store",
    "close_checkpoint_store",
    "reset_checkpoint_store",
]


def __getattr__(name):
    if name in ("EventHubProducer", "EventHubRecordMetadata"):
        from kafka_pipeline.common.eventhub.producer import (
            EventHubProducer,
            EventHubRecordMetadata,
        )

        return (
            EventHubProducer if name == "EventHubProducer" else EventHubRecordMetadata
        )
    if name in ("EventHubConsumer", "EventHubConsumerRecord"):
        from kafka_pipeline.common.eventhub.consumer import (
            EventHubConsumer,
            EventHubConsumerRecord,
        )

        return (
            EventHubConsumer if name == "EventHubConsumer" else EventHubConsumerRecord
        )
    if name in (
        "get_checkpoint_store",
        "close_checkpoint_store",
        "reset_checkpoint_store",
    ):
        from kafka_pipeline.common.eventhub.checkpoint_store import (
            close_checkpoint_store,
            get_checkpoint_store,
            reset_checkpoint_store,
        )

        return {
            "get_checkpoint_store": get_checkpoint_store,
            "close_checkpoint_store": close_checkpoint_store,
            "reset_checkpoint_store": reset_checkpoint_store,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
