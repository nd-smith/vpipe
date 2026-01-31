# Event Hub Transport Migration Guide

## Executive Summary

The pipeline has been updated to support **Azure Event Hub** as the primary internal transport, replacing aiokafka. This change enables the pipeline to work with Azure Private Link endpoints, which expose AMQP (port 443) but NOT the Kafka protocol (port 9093).

**Status**: Event Hub is now the **default** transport. Kafka transport remains available as a fallback option.

## What Changed

### 1. New Dependencies

Added `azure-eventhub>=5.11.0` to `requirements.txt` for AMQP transport support.

### 2. New Transport Layer

Created transport abstraction layer with Event Hub adapters:

```
src/kafka_pipeline/common/
├── eventhub/
│   ├── __init__.py
│   ├── producer.py         # EventHubProducer (AMQP)
│   ├── consumer.py         # EventHubConsumer (AMQP)
│   └── README.md           # Detailed documentation
├── transport.py            # Factory for creating producers/consumers
├── producer.py             # BaseKafkaProducer (Kafka protocol) - still exists
└── consumer.py             # BaseKafkaConsumer (Kafka protocol) - still exists
```

### 3. Configuration Changes

#### `config.yaml`

Added transport configuration:

```yaml
# Transport selection (default: eventhub)
transport:
  type: ${PIPELINE_TRANSPORT:-eventhub}

# Event Hub configuration
eventhub:
  connection_string: ${EVENTHUB_CONNECTION_STRING:-}
  entity_name: ${EVENTHUB_ENTITY_NAME:-pcesdopodappv1}
  consumer_group: ${EVENTHUB_CONSUMER_GROUP:-xact-pipeline}
  transport_type: AmqpOverWebsocket
```

#### `.env.example`

Created example configuration showing Event Hub setup:

```bash
# Transport selection
PIPELINE_TRANSPORT=eventhub

# Event Hub connection
EVENTHUB_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=..."

# SSL bypass for local dev (corporate proxy)
DISABLE_SSL_VERIFY=true
```

### 4. Updated Runner Functions

Modified `kafka_pipeline/runners/common.py`:

- `execute_worker_with_producer()` now uses transport factory instead of direct class instantiation
- Automatically selects Event Hub or Kafka based on `PIPELINE_TRANSPORT` env var

### 5. Tools and Documentation

- **Test Script**: `scripts/test_eventhub_connection.py` - Validates Event Hub connectivity
- **Documentation**: `kafka_pipeline/common/eventhub/README.md` - Comprehensive guide
- **Example Config**: `.env.example` - Shows proper configuration

## Migration Steps

### For Local Development

1. **Install dependencies**:
   ```bash
   cd src
   pip install -r requirements.txt
   ```

2. **Configure Event Hub connection**:
   ```bash
   cp .env.example .env
   # Edit .env and set your Event Hub connection string:
   # EVENTHUB_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=..."
   ```

3. **Enable SSL bypass for corporate proxy** (local dev only):
   ```bash
   echo "DISABLE_SSL_VERIFY=true" >> .env
   ```

4. **Test connection**:
   ```bash
   python scripts/test_eventhub_connection.py
   ```

5. **Run pipeline**:
   ```bash
   # Event Hub is now the default, no changes needed
   python -m kafka_pipeline.runners.xact_runners
   ```

### For Production/Deployment

1. **Provision Event Hub resources** in Azure:
   - Create Event Hub namespace
   - Create Event Hub entities for each topic:
     - `xact-events-raw`
     - `xact-downloads-pending`
     - `xact-downloads-cached`
     - `xact-downloads-results`
   - Configure Private Link if needed
   - Create Shared Access Policy with Send/Listen permissions

2. **Store connection string** in Azure Key Vault

3. **Configure deployment** (Jenkins/Azure DevOps):
   ```bash
   PIPELINE_TRANSPORT=eventhub
   EVENTHUB_CONNECTION_STRING=<from-key-vault>
   # DO NOT set DISABLE_SSL_VERIFY in production!
   ```

4. **Deploy and verify**:
   - Check logs for "Event Hub producer started successfully"
   - Monitor Azure Event Hub metrics (ingress/egress)

## Configuration Reference

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PIPELINE_TRANSPORT` | No | `eventhub` | Transport type: `eventhub` or `kafka` |
| `EVENTHUB_CONNECTION_STRING` | Yes* | - | Event Hub connection string with EntityPath |
| `EVENTHUB_ENTITY_NAME` | No | `pcesdopodappv1` | Default entity name (overrides EntityPath) |
| `EVENTHUB_CONSUMER_GROUP` | No | `xact-pipeline` | Consumer group for offset management |
| `DISABLE_SSL_VERIFY` | No | `false` | **Local dev only!** Bypass SSL verification |

*Required when `PIPELINE_TRANSPORT=eventhub`

### Worker-Specific Entity Names

For multi-topic scenarios, you can configure entity names per worker:

```bash
# Base connection string (without EntityPath)
EVENTHUB_CONNECTION_STRING="Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=key"

# Worker-specific entities (optional)
EVENTHUB_ENTITY_DELTA_EVENTS_WRITER=xact-events-raw
EVENTHUB_ENTITY_RESULT_PROCESSOR=xact-downloads-results
EVENTHUB_ENTITY_UPLOAD_WORKER=xact-downloads-cached
```

The transport factory will automatically build the correct connection string with EntityPath for each worker.

## Architecture Notes

### Event Hub vs Kafka: Key Differences

| Aspect | Kafka | Event Hub |
|--------|-------|-----------|
| **Protocol** | Kafka (TCP 9093) | AMQP (WebSocket 443) |
| **Topics** | Multiple per connection | One entity per connection |
| **Partitions** | Manual assignment | Automatic load balancing |
| **Offset Management** | Consumer groups | Checkpoint store |
| **Private Link** | ❌ Not exposed | ✅ Fully supported |

### Multi-Topic Support

**Challenge**: Kafka consumers can read from multiple topics. Event Hub requires one connection per entity.

**Solution**: The transport factory dynamically builds connection strings with the correct `EntityPath` for each worker:

```python
# Producer for "xact-events-raw" topic
producer1 = create_producer(config, domain="xact", worker_name="delta_events_writer")
# → Uses EntityPath=xact-events-raw

# Producer for "xact-downloads-results" topic
producer2 = create_producer(config, domain="xact", worker_name="result_processor")
# → Uses EntityPath=xact-downloads-results
```

### SSL Verification Bypass

**Problem**: Corporate proxies intercept TLS with self-signed CAs not in Python's trust store.

**Solution**: The existing `core.security.ssl_dev_bypass` module patches `ssl.create_default_context()` when `DISABLE_SSL_VERIFY=true`.

**Safety**:
- ✅ Only applies if `DISABLE_SSL_VERIFY=true` is set
- ✅ Refuses to apply if `ENVIRONMENT=production`
- ✅ Should only exist in `.env` (gitignored)
- ❌ **NEVER** set this in production!

## Testing

### Quick Test (from task description)

```python
from azure.eventhub import EventHubProducerClient, EventData, TransportType

CONNECTION_STR = "Endpoint=sb://eh-0418b0006320-eus2-pcesdopodappv1.servicebus.windows.net/;SharedAccessKeyName=eventhub-auth-rule-pcesdopodappv1;SharedAccessKey=<key>;EntityPath=pcesdopodappv1"

producer = EventHubProducerClient.from_connection_string(
    CONNECTION_STR,
    transport_type=TransportType.AmqpOverWebsocket,
)

with producer:
    batch = producer.create_batch()
    batch.add(EventData("test message"))
    producer.send_batch(batch)
    print("Message sent successfully")
```

### Comprehensive Test

```bash
# Run the provided test script
python scripts/test_eventhub_connection.py
```

Expected output:
```
============================================================
Event Hub Connection Test
============================================================

Connection Info:
  Namespace: sb://eh-0418b0006320-eus2-pcesdopodappv1.servicebus.windows.net/
  Policy: eventhub-auth-rule-pcesdopodappv1
  Entity: pcesdopodappv1

=== Testing Event Hub Producer (Sync) ===
✓ Connected to Event Hub: pcesdopodappv1
  Partitions: 4
Sending test message...
✓ Message sent successfully

✅ Producer test PASSED

=== Testing Event Hub Consumer (Async) ===
✓ Consumer created
Waiting for up to 3 messages (timeout: 10s)...

✓ Received message #1:
  Partition: 0
  Offset: 12345
  Body: {'type': 'test', 'message': 'Hello from Event Hub test script', 'timestamp': 1234567890.123}

✅ Consumer test PASSED

============================================================
Test Summary
============================================================
Producer: ✅ PASS
Consumer: ✅ PASS

🎉 All tests passed! Event Hub connection is working.
```

## Rollback Plan

If issues arise, you can switch back to Kafka transport:

```bash
# In .env or deployment config
PIPELINE_TRANSPORT=kafka

# Ensure Kafka connection details are configured
LOCAL_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
LOCAL_KAFKA_SECURITY_PROTOCOL=PLAINTEXT
```

The original `BaseKafkaProducer` and `BaseKafkaConsumer` classes remain unchanged and fully functional.

## Performance Considerations

### Throughput

Event Hub throughput is governed by:
- **Throughput Units (TUs)**: 1 MB/s ingress, 2 MB/s egress per TU
- **Partitions**: Similar to Kafka, more partitions = more parallelism

**Recommendation**: Start with 2-4 TUs, monitor with Azure Monitor, scale as needed.

### Latency

- Event Hub typically has slightly higher latency than Kafka (AMQP overhead)
- WebSocket transport adds minimal overhead (~5-10ms)
- Batch processing helps amortize connection overhead

### Checkpointing

For best performance, checkpoint periodically (not every message):

```python
# In consumer message handler
if message_count % 100 == 0:
    await partition_context.update_checkpoint(event)
```

## Troubleshooting

### Connection Refused

**Error**: `ConnectionRefusedError` or timeout

**Solutions**:
1. Verify namespace in connection string
2. Ensure Private Link is configured
3. Check firewall (port 443 must be open)
4. Confirm `TransportType.AmqpOverWebsocket` is used

### SSL Certificate Errors

**Error**: `SSLError: [SSL: CERTIFICATE_VERIFY_FAILED]`

**Solutions**:
1. Set `DISABLE_SSL_VERIFY=true` in `.env` (local dev only)
2. Verify corporate proxy CA is not interfering
3. Test with `curl -v https://<namespace>.servicebus.windows.net`

### Entity Not Found

**Error**: `EventHubError: The messaging entity 'X' could not be found`

**Solutions**:
1. Verify entity name matches Event Hub name in Azure Portal
2. Check `EntityPath` in connection string
3. Set explicit entity name: `EVENTHUB_ENTITY_NAME=your-entity`

### Messages Not Appearing

**Possible causes**:
1. Wrong consumer group (check `EVENTHUB_CONSUMER_GROUP`)
2. Starting position (Event Hub starts from latest by default)
3. Partition assignment delay

**Solutions**:
```bash
# Use a unique consumer group
export EVENTHUB_CONSUMER_GROUP=my-unique-group

# Check Azure Portal for incoming messages (Monitor tab)
# Verify producer is sending (check metrics)
```

## Support

### Documentation

- **Event Hub README**: `src/kafka_pipeline/common/eventhub/README.md`
- **Azure Docs**: https://learn.microsoft.com/en-us/azure/event-hubs/
- **Python SDK**: https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/eventhub/azure-eventhub

### Logs

Event Hub connections log at INFO level:

```
INFO: Creating Event Hub producer: domain=xact, worker=delta_events_writer, entity=xact-events-raw
INFO: Event Hub producer started successfully
INFO: Creating Event Hub consumer: domain=xact, worker=delta_events_writer, entity=xact-events-raw, group=xact-delta-writer
INFO: Event Hub consumer started successfully
```

Look for these log messages to confirm Event Hub is being used.

## Summary

✅ **Event Hub transport is now the default**
✅ **Kafka transport remains available as fallback**
✅ **SSL bypass working for corporate proxy**
✅ **Comprehensive testing and documentation provided**
✅ **Zero changes required to worker code (transparent migration)**

The pipeline is now compatible with Azure Private Link endpoints while maintaining backward compatibility with Kafka-based deployments.
