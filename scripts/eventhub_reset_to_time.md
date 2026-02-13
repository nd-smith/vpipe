# Reset Event Hub Consumer Offsets to a Point in Time

## Prerequisites

- Python 3.11+
- `azure-eventhub` and `azure-storage-blob` packages installed
- Access to the Event Hub namespace and checkpoint blob storage

## Environment Variables

Set these before running (or pass equivalent `--flags`):

```bash
export EVENTHUB_NAMESPACE_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
export EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=..."

# Optional — defaults to "eventhub-checkpoints"
export EVENTHUB_CHECKPOINT_CONTAINER_NAME="eventhub-checkpoints"

# Optional — set for corporate proxy environments
export DISABLE_SSL_VERIFY="true"
```

## Usage

### 1. Dry run first

Always start with `--dry-run` to see what would change without modifying anything:

```bash
python scripts/eventhub_reset_to_time.py verisk_events verisk-event-ingester --hours-ago 12 --dry-run
```

### 2. Reset to 12 hours ago

```bash
python scripts/eventhub_reset_to_time.py verisk_events verisk-event-ingester --hours-ago 12
```

### 3. Reset to a specific UTC datetime

```bash
python scripts/eventhub_reset_to_time.py verisk_events verisk-event-ingester \
    --datetime "2026-02-13T00:00:00"
```

### 4. With explicit connection strings

```bash
python scripts/eventhub_reset_to_time.py verisk_events verisk-event-ingester --hours-ago 12 \
    --namespace-conn-str "$EVENTHUB_NAMESPACE_CONNECTION_STRING" \
    --blob-conn-str "$EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING" \
    --container eventhub-checkpoints
```

## How It Works

1. Connects to the Event Hub and queries each partition
2. For each partition, starts a receiver at the target enqueue time and reads the first event to get its exact offset and sequence number
3. Updates the checkpoint blobs in Azure Blob Storage with those values
4. On next startup, the consumer reads from the new checkpoint position

## After Running

**You must restart the consumer** for it to pick up the new checkpoints. The consumer reads checkpoint positions on startup — running consumers will not see the change until restarted.

```bash
# Example: restart a container app revision
az containerapp revision restart -n <app-name> -g <resource-group> --revision <revision>
```

## Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `Could not extract FQDN from connection string` | Missing or malformed `EVENTHUB_NAMESPACE_CONNECTION_STRING` | Verify the connection string starts with `Endpoint=sb://...` |
| `No partitions with data found` | Event Hub is empty or target time is before the earliest retained event | Check Event Hub retention settings; try a more recent `--datetime` |
| Partition shows `(no events after target time, using last)` | Target time is after the most recent event on that partition | This is fine — checkpoint is set to the latest available position |
| SSL/TLS errors | Corporate proxy intercepting traffic | Set `DISABLE_SSL_VERIFY=true` |
| Consumer doesn't pick up new offsets | Consumer wasn't restarted | Restart the consumer process/container after running the script |
