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

# Event Hub connection string with SharedAccessKey authentication
EVENTHUB_CONNECTION_STRING="Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>;EntityPath=<entity>"
```

#### Optional

```bash
# Default entity name (overrides EntityPath in connection string)
EVENTHUB_ENTITY_NAME=pcesdopodappv1

# Consumer group for offset tracking
EVENTHUB_CONSUMER_GROUP=xact-pipeline

# Worker-specific entity names (for multi-topic scenarios)
EVENTHUB_ENTITY_DELTA_EVENTS_WRITER=xact-events-raw
EVENTHUB_ENTITY_RESULT_PROCESSOR=xact-downloads-results

# SSL verification bypass for local dev (corporate proxy with self-signed CA)
DISABLE_SSL_VERIFY=true  # NEVER use in production!
```

### Connection String Format

```
Endpoint=sb://<namespace>.servicebus.windows.net/;
SharedAccessKeyName=<policy-name>;
SharedAccessKey=<access-key>;
EntityPath=<event-hub-name>
```

**Example** (from task description):
```
Endpoint=sb://eh-0418b0006320-eus2-pcesdopodappv1.servicebus.windows.net/;
SharedAccessKeyName=eventhub-auth-rule-pcesdopodappv1;
SharedAccessKey=<your-key>;
EntityPath=pcesdopodappv1
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

**Solutions**:

1. **Multiple Event Hubs**: Create separate Event Hub entities for each pipeline topic
   - `xact-events-raw` → Event Hub entity
   - `xact-downloads-pending` → Event Hub entity
   - `xact-downloads-results` → Event Hub entity

2. **Dynamic EntityPath**: Use the same namespace but different entities
   ```bash
   # Base connection string (without EntityPath)
   EVENTHUB_CONNECTION_STRING="Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=key"

   # Worker-specific entity names
   EVENTHUB_ENTITY_DELTA_EVENTS_WRITER=xact-events-raw
   EVENTHUB_ENTITY_RESULT_PROCESSOR=xact-downloads-results
   ```

3. **Hybrid Approach**: Use Event Hub for external input, keep local Kafka for internal pipeline
   - External → Event Hub (Private Link)
   - Internal pipeline → Local Kafka (Docker Compose for dev, corporate Kafka for prod)

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
- [ ] Configure `EVENTHUB_CONNECTION_STRING` with correct EntityPath
- [ ] Enable `DISABLE_SSL_VERIFY=true` for local dev (if behind corporate proxy)
- [ ] Test producer connection with quick test script
- [ ] Verify consumer can read messages
- [ ] Check logs for transport type confirmation

### For DevOps/Production

- [ ] Provision Event Hub namespace in Azure
- [ ] Create Event Hub entities for each pipeline topic:
  - `xact-events-raw`
  - `xact-downloads-pending`
  - `xact-downloads-cached`
  - `xact-downloads-results`
- [ ] Configure Private Link endpoint if needed
- [ ] Create Shared Access Policy with Send/Listen permissions
- [ ] Store connection string in Key Vault
- [ ] Inject `EVENTHUB_CONNECTION_STRING` via Jenkins/environment
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

**Cause**: Entity name mismatch

**Solution**:
```bash
# Check EntityPath in connection string
# OR set explicit entity name
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
