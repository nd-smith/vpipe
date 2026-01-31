# Event Hub Quick Start Guide

## TL;DR

Azure Event Hub is now the **default** internal transport for the pipeline. Set up takes 2 minutes.

## Setup (Local Dev)

```bash
# 1. Install dependencies
cd src
pip install -r requirements.txt

# 2. Configure Event Hub
cp .env.example .env
nano .env  # Edit EVENTHUB_NAMESPACE_CONNECTION_STRING

# 3. Enable SSL bypass (if behind corporate proxy)
echo "DISABLE_SSL_VERIFY=true" >> .env

# 4. Test connection
python scripts/test_eventhub_connection.py

# 5. Run pipeline (Event Hub is automatic)
python -m kafka_pipeline.main
```

## Configuration (Minimal)

**Required** in `.env`:

```bash
# Namespace-level connection string (NO EntityPath)
EVENTHUB_NAMESPACE_CONNECTION_STRING="Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=policy;SharedAccessKey=key"
DISABLE_SSL_VERIFY=true  # Only for local dev!
```

Entity names and consumer groups are defined per-topic in `config.yaml` (no env vars needed).

## Connection String Format

Namespace-level (no EntityPath):

```
Endpoint=sb://eh-0418b0006320-eus2-pcesdopodappv1.servicebus.windows.net/;
SharedAccessKeyName=eventhub-auth-rule-pcesdopodappv1;
SharedAccessKey=<your-key-here>
```

**Get from**: Azure Portal → Event Hub Namespace → Shared access policies → Connection string

**Note**: Do NOT include `EntityPath` — entities are resolved per-topic from `config.yaml`.

## Test Connection

```bash
# Quick test
python scripts/test_eventhub_connection.py

# Expected output:
# ✅ Producer test PASSED
# ✅ Consumer test PASSED
# 🎉 All tests passed!
```

## Switch Back to Kafka

```bash
# In .env:
PIPELINE_TRANSPORT=kafka
LOCAL_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

## Common Issues

| Problem | Solution |
|---------|----------|
| SSL certificate error | Set `DISABLE_SSL_VERIFY=true` in `.env` |
| Connection timeout | Check namespace in connection string |
| Entity not found | Verify entity name in `config.yaml` matches Event Hub name |
| Import error | Run `pip install -r requirements.txt` |

## Architecture

```
┌─────────────────┐
│  Eventhouse     │  (External source via KQL poller)
│  or Event Hub   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Event Hub      │  ◄── AMQP over WebSocket (port 443)
│  (Internal      │      Works with Azure Private Link
│   Pipeline)     │      Replaces aiokafka
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Pipeline       │
│  Workers        │
└─────────────────┘
```

## Key Differences: Event Hub vs Kafka

- **Protocol**: AMQP (port 443) instead of Kafka (port 9093)
- **Connection**: One entity per connection (Kafka allows multiple topics)
- **Private Link**: ✅ Fully supported (Kafka is ❌ not exposed)
- **Code**: ✅ Zero changes (transparent migration)

## Documentation

- **Comprehensive Guide**: `MIGRATION_EVENTHUB.md`
- **Technical Details**: `src/kafka_pipeline/common/eventhub/README.md`
- **Config Example**: `src/.env.example`

## Support

**Logs to check**:
```
INFO: Creating Event Hub producer: entity=xact-events-raw
INFO: Event Hub producer started successfully
```

**Troubleshooting**:
1. Check connection string format
2. Verify SSL bypass is applied (if needed)
3. Ensure entity exists in Azure Portal
4. Check firewall allows port 443

## Production Deployment

```bash
# Configure in Jenkins/deployment:
PIPELINE_TRANSPORT=eventhub
EVENTHUB_NAMESPACE_CONNECTION_STRING=<from-key-vault>
# DO NOT set DISABLE_SSL_VERIFY=true in production!
```

## Summary

- Event Hub is **default** (no action needed)
- Set `EVENTHUB_NAMESPACE_CONNECTION_STRING` in `.env`
- Entity names and consumer groups defined per-topic in `config.yaml`
- Run test script to verify
- Pipeline works with zero code changes

**Rollback**: Change `PIPELINE_TRANSPORT=kafka`
