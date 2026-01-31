# Azure Event Hub Transport Integration

## Overview

This module provides Azure Event Hub transport adapters that replace aiokafka for internal pipeline communication when Azure Private Link endpoints are used. The corporate Event Hub infrastructure exposes **AMQP protocol on port 443** (via WebSocket) but does **NOT** expose the Kafka protocol on port 9093.

## Architecture

### Problem Statement

- **Corporate Event Hub**: Behind Azure Private Link
- **Available Protocol**: AMQP (port 5671) and AMQP over WebSocket (port 443)
- **Not Available**: Kafka protocol (port 9093)
- **Impact**: aiokafka client cannot connect

### Solution

Replace aiokafka with `azure-eventhub` SDK using `TransportType.AmqpOverWebsocket` for all internal pipeline communication.

### Components

```
src/kafka_pipeline/common/eventhub/
├── __init__.py           # Module exports
├── producer.py           # EventHubProducer (replaces BaseKafkaProducer)
├── consumer.py           # EventHubConsumer (replaces BaseKafkaConsumer)
└── README.md            # This file

src/kafka_pipeline/common/
└── transport.py          # Factory functions for creating producers/consumers
```

## Configuration

### Environment Variables

#### Required

```bash
# Transport selection (default: eventhub)
PIPELINE_TRANSPORT=eventhub

# Namespace-level connection string (NO EntityPath)
# One secret manages access to ALL Event Hubs in the namespace
EVENTHUB_NAMESPACE_CONNECTION_STRING="Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>"
```

#### Optional

```bash
# Default consumer group (when not specified per-topic in config.yaml)
EVENTHUB_DEFAULT_CONSUMER_GROUP=$Default

# Legacy connection string (backward compat — EntityPath stripped automatically)
# EVENTHUB_CONNECTION_STRING="Endpoint=sb://...;EntityPath=..."

# Worker-specific entity name overrides (config.yaml per-topic mapping is preferred)
EVENTHUB_ENTITY_DELTA_EVENTS_WRITER=xact-events-raw
EVENTHUB_ENTITY_RESULT_PROCESSOR=xact-downloads-results

# SSL verification bypass for local dev (corporate proxy with self-signed CA)
DISABLE_SSL_VERIFY=true  # NEVER use in production!
```

### Connection String Format

Namespace-level (no EntityPath):

```
Endpoint=sb://<namespace>.servicebus.windows.net/;
SharedAccessKeyName=<policy-name>;
SharedAccessKey=<access-key>
```

**Example**:
```
Endpoint=sb://eh-0418b0006320-eus2-pcesdopodappv1.servicebus.windows.net/;
SharedAccessKeyName=eventhub-auth-rule-pcesdopodappv1;
SharedAccessKey=<your-key>
```

Entity names and consumer groups are defined per-topic in `config.yaml`:

```yaml
eventhub:
  namespace_connection_string: ${EVENTHUB_NAMESPACE_CONNECTION_STRING:-}
  xact:
    events:
      eventhub_name: ${EVENTHUB_XACT_EVENTS_NAME:-verisk_events}
      consumer_group: ${EVENTHUB_XACT_EVENTS_CONSUMER_GROUP:-xact-pipeline}
```

## Key Differences: Event Hub vs Kafka

| Feature | Kafka | Event Hub |
|---------|-------|-----------|
| **Protocol** | Kafka (port 9093) | AMQP (port 5671 / 443 WebSocket) |
| **Topics** | Multiple per connection | One entity per connection |
| **Partitions** | Manual assignment | Automatic load balancing |
| **Offsets** | Consumer group based | Checkpoint store based |
| **Keys** | Native support | Stored in event properties |
| **Headers** | Native support | Stored in event properties |
| **Metadata** | RecordMetadata (partition, offset) | Limited (no sync offset return) |

### Multi-Topic Architecture

**Problem**: Kafka allows consuming from multiple topics with one consumer. Event Hub requires one connection per entity.

**Solution**: One namespace connection string + per-topic entity mapping in `config.yaml`:

1. **Namespace connection string** (one secret in Key Vault / Jenkins):
   ```bash
   EVENTHUB_NAMESPACE_CONNECTION_STRING="Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=key"
   ```

2. **Per-domain Event Hub mapping** in `config.yaml`:
   ```yaml
   eventhub:
     xact:
       events:
         eventhub_name: ${EVENTHUB_XACT_EVENTS_NAME:-verisk_events}
         consumer_group: ${EVENTHUB_XACT_EVENTS_CONSUMER_GROUP:-xact-pipeline}
     claimx:
       events:
         eventhub_name: ${EVENTHUB_CLAIMX_EVENTS_NAME:-claimx_events}
         consumer_group: ${EVENTHUB_CLAIMX_EVENTS_CONSUMER_GROUP:-claimx-pipeline}
   ```

3. **Transport factory** resolves entity name via `topic_key` parameter:
   ```python
   producer = create_producer(config, domain="xact", worker_name="writer", topic_key="events")
   # → Uses eventhub_name="verisk_events" from config.yaml
   ```

**Alternative**: Hybrid approach — Event Hub for external input, local Kafka for internal pipeline.

## SSL Certificate Handling

### Corporate Proxy Issue

Corporate proxies often intercept TLS traffic with a self-signed CA that Python doesn't trust.

### Solution

The `core.security.ssl_dev_bypass` module patches `ssl.create_default_context()` when:
- `DISABLE_SSL_VERIFY=true` is set (only in `.env`, which is gitignored)
- Environment is NOT production

### Verification

```python
# Applied automatically in producer.start() and consumer.start()
from core.security.ssl_dev_bypass import apply_ssl_dev_bypass
apply_ssl_dev_bypass()
```

**WARNING**: This bypass is **only for local development**. Production deployments must use proper certificate chains.

## Testing

### Quick Connection Test

```python
"""Test Event Hub connection with AMQP over WebSocket."""
from azure.eventhub import EventHubProducerClient, EventData, TransportType

# Set connection string
CONNECTION_STR = "Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=..."

# Create producer with WebSocket transport
producer = EventHubProducerClient.from_connection_string(
    CONNECTION_STR,
    transport_type=TransportType.AmqpOverWebsocket,
)

# Send test message
with producer:
    batch = producer.create_batch()
    batch.add(EventData("test message"))
    producer.send_batch(batch)
    print("✓ Message sent successfully")
```

### Integration Test

Create `test_eventhub_transport.py`:

```python
"""Test Event Hub transport integration."""
import asyncio
import os
from kafka_pipeline.common.transport import create_producer, TransportType
from config.config import KafkaConfig

async def test_eventhub_producer():
    # Set required env vars
    os.environ["PIPELINE_TRANSPORT"] = "eventhub"
    os.environ["EVENTHUB_CONNECTION_STRING"] = "your-connection-string-here"
    os.environ["DISABLE_SSL_VERIFY"] = "true"  # For local dev only

    # Create minimal config
    config = KafkaConfig(
        bootstrap_servers="unused",  # Not used for Event Hub
        xact={"topics": {"events": "xact-events-raw"}},
    )

    # Create producer using factory
    producer = create_producer(
        config=config,
        domain="xact",
        worker_name="test-producer",
        transport_type=TransportType.EVENTHUB,
    )

    # Start and test
    await producer.start()

    # Send test message
    from pydantic import BaseModel

    class TestEvent(BaseModel):
        message: str

    metadata = await producer.send(
        topic="xact-events-raw",
        key="test-key",
        value=TestEvent(message="Hello Event Hub!"),
        headers={"source": "test"},
    )

    print(f"✓ Sent to {metadata.topic}")

    await producer.stop()

if __name__ == "__main__":
    asyncio.run(test_eventhub_producer())
```

## Migration Checklist

### For Developers

- [ ] Install `azure-eventhub>=5.11.0` dependency
- [ ] Set `PIPELINE_TRANSPORT=eventhub` in `.env`
- [ ] Configure `EVENTHUB_NAMESPACE_CONNECTION_STRING` (namespace-level, no EntityPath)
- [ ] Verify entity names in `config.yaml` match your Event Hub entities
- [ ] Enable `DISABLE_SSL_VERIFY=true` for local dev (if behind corporate proxy)
- [ ] Test producer connection with quick test script
- [ ] Verify consumer can read messages
- [ ] Check logs for transport type confirmation

### For DevOps/Production

- [ ] Provision Event Hub namespace in Azure
- [ ] Create Event Hub entities for each pipeline topic (see `config.yaml` entity mapping)
- [ ] Configure Private Link endpoint if needed
- [ ] Create Shared Access Policy with Send/Listen permissions at namespace level
- [ ] Store namespace connection string in Key Vault
- [ ] Inject `EVENTHUB_NAMESPACE_CONNECTION_STRING` via Jenkins/environment
- [ ] Set `PIPELINE_TRANSPORT=eventhub` in deployment config
- [ ] **DO NOT** set `DISABLE_SSL_VERIFY` in production

## Troubleshooting

### Connection Refused

**Symptom**: `ConnectionRefusedError` or timeout

**Causes**:
1. Incorrect namespace in connection string
2. Private Link not configured
3. Firewall blocking port 443
4. Wrong transport type (use `AmqpOverWebsocket`)

**Solution**:
```bash
# Verify connection string
echo $EVENTHUB_CONNECTION_STRING | grep "Endpoint=sb://"

# Test connectivity
curl -v https://<namespace>.servicebus.windows.net
```

### SSL Certificate Errors

**Symptom**: `SSLError: [SSL: CERTIFICATE_VERIFY_FAILED]`

**Cause**: Corporate proxy with self-signed CA

**Solution**:
```bash
# Local dev only!
echo "DISABLE_SSL_VERIFY=true" >> .env
```

### Entity Not Found

**Symptom**: `EventHubError: The messaging entity 'X' could not be found`

**Cause**: Entity name mismatch between `config.yaml` and Azure

**Solution**:
```bash
# Check eventhub_name in config.yaml under eventhub.{domain}.{topic_key}.eventhub_name
# Verify it matches the Event Hub name in Azure Portal
# Or set env var override:
export EVENTHUB_ENTITY_NAME=your-entity-name
```

### Multiple Topics Not Working

**Symptom**: Consumer fails when configured with multiple topics

**Cause**: Event Hub only supports one entity per consumer

**Solution**:
- Use separate consumers for each topic
- OR configure hybrid transport (Event Hub + Kafka)
- OR create multiple Event Hub entities

## Performance Considerations

### Batch Size

Event Hub batches are size-limited:
```python
# Default max batch size: ~256 KB
# Adjust based on your message sizes
producer.create_batch(max_size_in_bytes=256000)
```

### Throughput Units

Event Hub throughput is limited by:
- **Basic/Standard**: Throughput Units (TUs) - 1 MB/s ingress, 2 MB/s egress per TU
- **Premium**: Processing Units (PUs) - Higher throughput

**Recommendation**: Start with 2 TUs, monitor with Azure Monitor, scale as needed.

### Consumer Checkpointing

Unlike Kafka's automatic offset management, Event Hub requires explicit checkpointing:

```python
# Automatic checkpointing (default)
await partition_context.update_checkpoint(event)

# Manual batching (better performance)
# Only checkpoint every N messages
if message_count % 100 == 0:
    await partition_context.update_checkpoint(event)
```

## References

- [Azure Event Hub Python SDK](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/eventhub/azure-eventhub)
- [Event Hub AMQP Protocol](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-amqp-protocol-guide)
- [Private Link for Event Hubs](https://learn.microsoft.com/en-us/azure/event-hubs/private-link-service)
- [Migration from Kafka to Event Hubs](https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-migration-guide)
