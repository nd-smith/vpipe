"""Azure Event Hub transport adapters.

Provides Event Hub producer and consumer implementations that are compatible
with the BaseKafkaProducer and BaseKafkaConsumer interfaces.

Uses azure-eventhub SDK with AMQP over WebSocket transport for compatibility
with Azure Private Link endpoints.
"""

from kafka_pipeline.common.eventhub.producer import EventHubProducer, EventHubRecordMetadata
from kafka_pipeline.common.eventhub.consumer import EventHubConsumer, EventHubConsumerRecord

__all__ = [
    "EventHubProducer",
    "EventHubRecordMetadata",
    "EventHubConsumer",
    "EventHubConsumerRecord",
]
