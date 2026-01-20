# Kafka Pipeline RHEL8 Deployment Guide

Complete guide for deploying kafka-pipeline workers as systemd services on RHEL8.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Worker Management](#worker-management)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Architecture Overview

The pipeline deploys each worker type as separate systemd services, allowing:

- **Independent scaling**: Run multiple instances of heavy workers (download, upload)
- **Fault isolation**: Worker crashes don't affect other workers
- **Resource management**: Per-worker CPU and memory limits
- **Targeted operations**: Start, stop, restart specific workers

### Worker Types

**XACT Domain:**
- `xact-poller` - Polls Eventhouse for events (single instance)
- `xact-event-ingester` - Ingests and parses events (single instance)
- `xact-enricher` - Executes plugins and enrichment (multi-instance)
- `xact-download` - Downloads files from S3 (multi-instance, 3-4 recommended)
- `xact-upload` - Uploads to OneLake (multi-instance, 2-3 recommended)
- `xact-delta-writer` - Writes to Delta tables (multi-instance, 2-3 recommended)
- `xact-result-processor` - Processes results (single instance)
- `xact-retry-scheduler` - Handles retry logic (single instance)

**ClaimX Domain:**
- `claimx-poller` - Polls Eventhouse for events (single instance)
- `claimx-ingester` - Ingests and parses events (single instance)
- `claimx-enricher` - API enrichment (multi-instance, 2-3 recommended)
- `claimx-downloader` - Downloads from ClaimX S3 (multi-instance, 3-4 recommended)
- `claimx-uploader` - Uploads to OneLake (multi-instance, 2-3 recommended)
- `claimx-delta-writer` - Writes events to Delta (single instance)
- `claimx-entity-writer` - Writes entities to Delta (single instance)
- `claimx-result-processor` - Processes results (single instance)
- `claimx-retry-scheduler` - Handles retry logic (single instance)

### Resource Requirements

**Estimated total for full deployment: 20-30GB RAM**

| Worker Type | Memory | CPU | Recommended Instances |
|-------------|--------|-----|----------------------|
| Pollers | 512 MB | 100% | 1 per domain |
| Ingesters | 512 MB | 100% | 1 per domain |
| Enrichers | 1 GB | 100% | 2-3 per domain |
| Download/Upload | 2 GB | 150% | 3-4 per domain |
| Delta Writers | 1 GB | 100% | 2-3 per domain |
| Result Processors | 512 MB | 100% | 1 per domain |
| Retry Schedulers | 512 MB | 100% | 1 per domain |

## Prerequisites

### System Requirements

- RHEL 8.x
- Python 3.9 or higher
- systemd
- Minimum 32GB RAM (for full deployment)
- 50GB disk space

### Required Packages

```bash
sudo dnf install -y python39 python39-devel python39-pip git rsync podman podman-compose
```

### Network Requirements

- Connectivity to Kafka brokers (or run Kafka locally in Podman)
- Connectivity to Azure Event Hub / Eventhouse (if used)
- Connectivity to S3 (for downloads)
- Connectivity to OneLake (for uploads)

## Installation

### Step 0: Set Up Kafka (Optional - if not using external Kafka)

If you need to run Kafka locally in a container:

```bash
# On the RHEL8 server
cd /tmp/kafka-pipeline/scripts/deployment
sudo ./setup-kafka.sh install
```

This will:
1. Install Podman and podman-compose
2. Start Kafka container (Apache Kafka 3.7.0)
3. Create all required topics
4. Set up systemd service for auto-restart
5. Expose Kafka on localhost:9094 for workers

**Verify Kafka is running:**
```bash
sudo ./setup-kafka.sh status
sudo ./setup-kafka.sh topics
```

**Skip this step if:** You're connecting to an external Kafka cluster.

### Step 1: Transfer Files to RHEL8 Server

From your development machine:

```bash
# Option 1: Using rsync (recommended)
rsync -avz --exclude='.git' --exclude='venv' --exclude='__pycache__' \
    /home/nick/projects/vpipe/ user@rhel-server:/tmp/kafka-pipeline/

# Option 2: Using git clone on the server
ssh user@rhel-server
cd /tmp
git clone <your-repo-url> kafka-pipeline
```

### Step 2: Run Installation Script

On the RHEL8 server:

```bash
cd /tmp/kafka-pipeline/scripts/deployment
sudo ./install.sh
```

This script will:
1. Create `kafka-pipeline` user and group
2. Create `/opt/kafka-pipeline` directory structure
3. Copy application files
4. Set up Python virtual environment
5. Install dependencies
6. Copy environment configuration
7. Install systemd service files
8. Reload systemd daemon

**Custom installation directory:**
```bash
sudo ./install.sh --install-dir /custom/path
```

**Custom service user:**
```bash
sudo ./install.sh --user myuser
```

### Step 3: Configure Environment

Edit the environment configuration:

```bash
sudo nano /opt/kafka-pipeline/config/.env
```

See [Configuration](#configuration) section for details.

## Configuration

### Environment Variables

The main configuration file is `/opt/kafka-pipeline/config/.env`.

**Required settings:**

```bash
# Kafka connection
# If using local Podman Kafka (from setup-kafka.sh):
KAFKA_BOOTSTRAP_SERVERS=localhost:9094

# If using external Kafka cluster:
# KAFKA_BOOTSTRAP_SERVERS=your-kafka-broker:9092

# Event source
EVENT_SOURCE=eventhouse  # or eventhub

# Eventhouse (if using)
XACT_EVENTHOUSE_CLUSTER_URL=https://your-cluster.kusto.windows.net
XACT_EVENTHOUSE_DATABASE=your-database
XACT_EVENTHOUSE_SOURCE_TABLE=your_table

# Delta Lake paths
DELTA_EVENTS_TABLE_PATH=abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/xact_events

# Azure authentication
AZURE_AUTH_METHOD=managed-identity  # recommended for RHEL8 VM
```

**Template available at:**
```bash
/tmp/kafka-pipeline/scripts/deployment/.env.template
```

### Service Configuration

Systemd service files are located at:
```
/etc/systemd/system/kafka-pipeline-*.service
```

**To modify resource limits**, edit the service file:

```bash
sudo nano /etc/systemd/system/kafka-pipeline-xact-download@.service
```

Then reload:
```bash
sudo systemctl daemon-reload
```

## Worker Management

### Using the Management Script (Recommended)

The `manage-workers.sh` script provides a simplified interface:

**Show status of all workers:**
```bash
sudo /tmp/kafka-pipeline/scripts/deployment/manage-workers.sh status
```

**Start workers:**
```bash
# Start single-instance worker
sudo ./manage-workers.sh start xact-poller

# Start 4 instances of download worker
sudo ./manage-workers.sh start xact-download 4

# Start 3 instances of ClaimX enricher
sudo ./manage-workers.sh start claimx-enricher 3
```

**Stop workers:**
```bash
# Stop all instances of a worker
sudo ./manage-workers.sh stop xact-download

# Stop specific instance
sudo ./manage-workers.sh stop xact-download 2
```

**Restart workers:**
```bash
sudo ./manage-workers.sh restart xact-download 4
```

**Enable on boot:**
```bash
sudo ./manage-workers.sh enable xact-poller
sudo ./manage-workers.sh enable xact-download 4
```

**View logs:**
```bash
# Tail logs for specific instance
sudo ./manage-workers.sh logs xact-download 1

# Tail logs for all instances
sudo ./manage-workers.sh logs xact-download
```

**Get PIDs:**
```bash
# Get PID for specific instance
sudo ./manage-workers.sh pid xact-download 1

# Get PIDs for all instances
sudo ./manage-workers.sh pid xact-download
```

### Using systemctl Directly

**Start workers:**
```bash
# Single instance worker
sudo systemctl start kafka-pipeline-xact-poller

# Multiple instances (template service)
sudo systemctl start kafka-pipeline-xact-download@{1..4}
```

**Check status:**
```bash
sudo systemctl status kafka-pipeline-xact-poller
sudo systemctl status kafka-pipeline-xact-download@1
```

**View logs:**
```bash
# Follow logs for single worker
sudo journalctl -u kafka-pipeline-xact-poller -f

# Follow logs for specific instance
sudo journalctl -u kafka-pipeline-xact-download@1 -f

# Follow logs for all download instances
sudo journalctl -u kafka-pipeline-xact-download@* -f
```

**Get PID:**
```bash
sudo systemctl show -p MainPID kafka-pipeline-xact-download@1 --value
```

**Enable on boot:**
```bash
sudo systemctl enable kafka-pipeline-xact-poller
sudo systemctl enable kafka-pipeline-xact-download@{1..4}
```

### Recommended Startup Configuration

For a typical production deployment:

```bash
# XACT Domain
sudo systemctl start kafka-pipeline-xact-poller
sudo systemctl start kafka-pipeline-xact-event-ingester
sudo systemctl start kafka-pipeline-xact-enricher@{1..2}
sudo systemctl start kafka-pipeline-xact-download@{1..4}
sudo systemctl start kafka-pipeline-xact-upload@{1..3}
sudo systemctl start kafka-pipeline-xact-delta-writer@{1..2}
sudo systemctl start kafka-pipeline-xact-result-processor
sudo systemctl start kafka-pipeline-xact-retry-scheduler

# ClaimX Domain
sudo systemctl start kafka-pipeline-claimx-poller
sudo systemctl start kafka-pipeline-claimx-ingester
sudo systemctl start kafka-pipeline-claimx-enricher@{1..2}
sudo systemctl start kafka-pipeline-claimx-downloader@{1..4}
sudo systemctl start kafka-pipeline-claimx-uploader@{1..3}
sudo systemctl start kafka-pipeline-claimx-delta-writer
sudo systemctl start kafka-pipeline-claimx-entity-writer
sudo systemctl start kafka-pipeline-claimx-result-processor
sudo systemctl start kafka-pipeline-claimx-retry-scheduler

# Enable on boot
sudo systemctl enable kafka-pipeline-xact-poller
sudo systemctl enable kafka-pipeline-xact-download@{1..4}
# ... enable other workers as needed
```

## Monitoring

### Prometheus Metrics

Each worker exposes Prometheus metrics on a unique port:

| Worker | Port Range |
|--------|-----------|
| xact-download | 8000-8009 (instance 1-10) |
| xact-upload | 8010-8019 |
| xact-enricher | 8020-8029 |
| xact-delta-writer | 8030-8039 |
| xact-poller | 8100 |
| xact-event-ingester | 8101 |
| xact-result-processor | 8102 |
| xact-retry-scheduler | 8103 |
| claimx-enricher | 8100-8109 |
| claimx-downloader | 8110-8119 |
| claimx-uploader | 8120-8129 |
| claimx-poller | 8200 |
| claimx-ingester | 8201 |
| claimx-delta-writer | 8202 |
| claimx-entity-writer | 8203 |
| claimx-result-processor | 8204 |
| claimx-retry-scheduler | 8205 |

**Scrape metrics:**
```bash
curl http://localhost:8001/metrics  # xact-download instance 1
```

### Log Files

Logs are written to:
- **systemd journal**: `journalctl -u kafka-pipeline-*`
- **Application logs**: `/opt/kafka-pipeline/logs/{domain}/{date}/{worker}_{timestamp}_{coolname}.log`

**Log structure:**
```
/opt/kafka-pipeline/logs/
├── xact/
│   └── 2026-01-20/
│       ├── xact_download_0120_1430_happy-tiger.log
│       ├── xact_download_0120_1430_calm-ocean.log
│       └── archive/
│           ├── xact_download_0120_1430_happy-tiger.log.1
│           └── xact_download_0120_1430_happy-tiger.log.2
└── claimx/
    └── 2026-01-20/
        └── claimx_enricher_0120_0930_brave-wolf.log
```

**Viewing logs:**
```bash
# Real-time journal logs
sudo journalctl -u kafka-pipeline-xact-download@1 -f

# Application log files
sudo tail -f /opt/kafka-pipeline/logs/xact/2026-01-20/xact_download_*.log

# Search JSON logs
sudo grep '"error_category"' /opt/kafka-pipeline/logs/xact/2026-01-20/*.log | jq .
```

### Worker Instance Identification

Each worker instance logs with a unique `worker_id`:

```json
{"ts": "2026-01-20T14:30:00.123Z", "worker_id": "xact-download-1", "msg": "Processing batch..."}
{"ts": "2026-01-20T14:30:01.456Z", "worker_id": "xact-download-2", "msg": "Processing batch..."}
```

## Troubleshooting

### Worker Won't Start

**Check service status:**
```bash
sudo systemctl status kafka-pipeline-xact-download@1
```

**Check logs:**
```bash
sudo journalctl -u kafka-pipeline-xact-download@1 -n 50
```

**Common issues:**
- Missing environment variables in `/opt/kafka-pipeline/config/.env`
- Python dependencies not installed
- Permission issues on log directory
- Port already in use (metrics port conflict)

### Worker Crashes or Restarts

**Check resource limits:**
```bash
sudo systemctl show kafka-pipeline-xact-download@1 | grep Memory
```

**Check for OOM kills:**
```bash
sudo journalctl -k | grep -i oom
```

**Increase memory limit if needed:**
```bash
sudo nano /etc/systemd/system/kafka-pipeline-xact-download@.service
# Change MemoryMax=2G to higher value
sudo systemctl daemon-reload
sudo systemctl restart kafka-pipeline-xact-download@1
```

### High Memory Usage

**Check actual usage:**
```bash
ps aux | grep kafka_pipeline
```

**Reduce concurrency in environment:**
```bash
sudo nano /opt/kafka-pipeline/config/.env
# Add or modify:
XACT_DOWNLOAD_CONCURRENCY=10  # Reduce from 20
```

### Kafka Connection Issues

**Verify Kafka connectivity:**
```bash
# For local Podman Kafka
telnet localhost 9094

# For external Kafka
telnet your-kafka-broker 9092
```

**Check bootstrap servers:**
```bash
sudo grep KAFKA_BOOTSTRAP_SERVERS /opt/kafka-pipeline/config/.env
```

**Test with Podman Kafka:**
```bash
# List topics
sudo podman exec kafka-pipeline-local /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Test consumer (from inside container)
sudo podman exec kafka-pipeline-local /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic xact.events.raw \
    --from-beginning \
    --max-messages 1

# Test from host (workers' perspective)
sudo podman exec kafka-pipeline-local /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9094 \
    --topic xact.events.raw \
    --from-beginning \
    --max-messages 1
```

**Manage Kafka container:**
```bash
# Check Kafka status
sudo ./setup-kafka.sh status

# View Kafka logs
sudo ./setup-kafka.sh logs

# Restart Kafka
sudo ./setup-kafka.sh restart
```

### Authentication Issues

**For managed identity:**
```bash
# Verify VM has managed identity enabled
az vm identity show --resource-group <rg> --name <vm-name>

# Test token acquisition
curl 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://storage.azure.com/' -H Metadata:true
```

**For service principal:**
```bash
# Verify environment variables
sudo grep AZURE_CLIENT /opt/kafka-pipeline/config/.env
```

### Permission Denied Errors

**Fix ownership:**
```bash
sudo chown -R kafka-pipeline:kafka-pipeline /opt/kafka-pipeline
```

**Fix .env permissions:**
```bash
sudo chmod 600 /opt/kafka-pipeline/config/.env
sudo chown kafka-pipeline:kafka-pipeline /opt/kafka-pipeline/config/.env
```

### Viewing All Running Workers

```bash
sudo systemctl list-units 'kafka-pipeline-*' --type=service --state=running
```

### Stopping All Workers

```bash
sudo systemctl stop 'kafka-pipeline-*'
```

## Updating the Application

To deploy a new version:

```bash
# 1. Stop all workers
sudo systemctl stop 'kafka-pipeline-*'

# 2. Update code
cd /tmp
rsync -avz user@dev-machine:/path/to/kafka-pipeline/ kafka-pipeline/

# 3. Run install script (will update files)
cd kafka-pipeline/scripts/deployment
sudo ./install.sh

# 4. Restart workers
sudo systemctl start kafka-pipeline-xact-poller
sudo systemctl start kafka-pipeline-xact-download@{1..4}
# ... restart other workers
```

## Support

For issues or questions:
- Check logs: `sudo journalctl -u kafka-pipeline-* -f`
- Review environment: `sudo cat /opt/kafka-pipeline/config/.env`
- Check service status: `sudo ./manage-workers.sh status`
