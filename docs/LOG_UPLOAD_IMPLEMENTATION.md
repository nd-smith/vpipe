# Log Upload to OneLake Implementation

## Overview

Implemented automatic log rotation and upload to OneLake for Cloud Foundry deployments. Logs rotate based on **whichever limit is reached first** (time OR size), then upload to OneLake and cleanup locally.

## How It Works

### Rotation Triggers (Either/Or):
1. **Time Limit**: Every N minutes (default: 15 min for "live" logs)
2. **Size Limit**: Max MB per file (default: 50 MB)

### On Rotation:
1. Log file rotates (e.g., `xact_poller_0127_1430_happy-tiger.log` → `xact_poller_0127_1430_happy-tiger.log.2026-01-27`)
2. Rotated file moved to `logs/archive/` subdirectory
3. **If upload enabled**: Upload to OneLake at `logs/{domain}/{date}/{filename}`
4. **After upload**: Delete local copy to free disk space
5. Keep only last N hours of logs locally (default: 2 hours)

### On Startup:
- Clean up any orphaned log files older than retention period
- Prevents disk filling from previous crashed containers

## Configuration

### Environment Variables:

```bash
# Enable OneLake upload (required for upload to work)
LOG_UPLOAD_ENABLED=true

# Maximum log file size before rotation (MB)
LOG_MAX_SIZE_MB=50

# Time-based rotation interval (minutes)
LOG_ROTATION_MINUTES=15

# Local retention period (hours) - older logs deleted
LOG_RETENTION_HOURS=2

# OneLake path for logs (optional, defaults to ONELAKE_BASE_PATH/logs)
ONELAKE_LOG_PATH=abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Files/logs
```

### Recommended Settings for CF:

**For "live" logs with frequent uploads:**
```bash
LOG_UPLOAD_ENABLED=true
LOG_MAX_SIZE_MB=25          # Small files for frequent rotation
LOG_ROTATION_MINUTES=10     # Rotate every 10 minutes
LOG_RETENTION_HOURS=1       # Keep only 1 hour locally
```

**For less frequent uploads (reduce OneLake API calls):**
```bash
LOG_UPLOAD_ENABLED=true
LOG_MAX_SIZE_MB=100         # Larger files
LOG_ROTATION_MINUTES=60     # Rotate hourly
LOG_RETENTION_HOURS=2       # Keep 2 hours locally
```

**For local development (no upload):**
```bash
LOG_UPLOAD_ENABLED=false    # Disable upload
# Other settings ignored when disabled
```

## OneLake Structure

Logs are uploaded to OneLake with the following structure:

```
{ONELAKE_BASE_PATH}/logs/
├── xact/
│   └── 2026-01-27/
│       ├── xact_poller_0127_1430_happy-tiger.log.2026-01-27-14-30
│       ├── xact_poller_0127_1445_happy-tiger.log.2026-01-27-14-45
│       └── xact_enricher_0127_1430_calm-ocean.log.2026-01-27-14-30
└── claimx/
    └── 2026-01-27/
        └── claimx_poller_0127_1430_brave-wolf.log.2026-01-27-14-30
```

## Jenkinsfile Configuration

Add to `envVars` in your Jenkinsfile:

```groovy
envVars: [
    ["name": "APP_ENV", "value": "${profileLowerCase}"],
    ["name": "WORKER_NAME", "value": "${workerName}"],

    // Enable log upload for CF deployments
    ["name": "LOG_UPLOAD_ENABLED", "value": "true"],
    ["name": "LOG_MAX_SIZE_MB", "value": "25"],
    ["name": "LOG_ROTATION_MINUTES", "value": "10"],
    ["name": "LOG_RETENTION_HOURS", "value": "1"],

    // ... other env vars
]
```

## Benefits

✅ **Persistent logs** - Survive container restarts/crashes
✅ **Frequent uploads** - Get "live" logs in OneLake quickly
✅ **Disk management** - Automatic cleanup prevents filling CF container disk
✅ **Centralized** - All worker logs in one OneLake location
✅ **Structured** - Organized by domain/date for easy querying
✅ **No separate worker** - Built into existing log rotation

## Disk Usage

With recommended settings (25 MB max, 10 min rotation, 1 hour retention):
- **Max per worker**: ~150 MB (6 rotations × 25 MB)
- **5 XACT workers**: ~750 MB total
- **Well within CF limits** (typically 1-2 GB disk quota)

## Failure Handling

- **Upload fails**: Log file kept locally, retry on next rotation
- **OneLake unavailable**: Falls back to local-only logging
- **Container restart**: Startup cleanup removes orphaned logs

## Code Changes

### Modified Files:
1. `src/core/logging/setup.py` - Added `OneLakeRotatingFileHandler` class
2. `.env.example` - Added new environment variable documentation

### New Handler: `OneLakeRotatingFileHandler`
- Extends `ArchivingTimedRotatingFileHandler`
- Adds size-based rotation check
- Integrates OneLake upload on rotation
- Manages local file cleanup

## Testing

### Local Testing (without upload):
```bash
LOG_UPLOAD_ENABLED=false python -m pipeline --worker xact-poller
```

### Test Upload to OneLake:
```bash
LOG_UPLOAD_ENABLED=true \
LOG_MAX_SIZE_MB=1 \
LOG_ROTATION_MINUTES=1 \
python -m pipeline --worker xact-poller
```

Check OneLake after 1 minute or 1 MB - should see uploaded log files.

## Notes

- **Async-safe**: Upload happens synchronously during rotation (brief pause)
- **Thread-safe**: File rotation uses standard Python logging locks
- **CF-optimized**: Designed for ephemeral container filesystems
- **Backward compatible**: Disabled by default, no impact on existing deployments
