# Kafka Pipeline - Quick Reference Guide

## Installation

```bash
# On RHEL8 server
cd /tmp/kafka-pipeline/scripts/deployment
sudo ./install.sh
sudo nano /opt/kafka-pipeline/config/.env  # Configure environment
```

## Common Operations

### Using Management Script

```bash
# Show status of all workers
sudo ./manage-workers.sh status

# Start workers
sudo ./manage-workers.sh start xact-poller
sudo ./manage-workers.sh start xact-download 4      # 4 instances

# Stop workers
sudo ./manage-workers.sh stop xact-download         # All instances
sudo ./manage-workers.sh stop xact-download 2       # Instance 2 only

# Restart workers
sudo ./manage-workers.sh restart xact-download 4

# View logs
sudo ./manage-workers.sh logs xact-download 1       # Instance 1
sudo ./manage-workers.sh logs xact-download         # All instances

# Get PIDs
sudo ./manage-workers.sh pid xact-download 1
sudo ./manage-workers.sh pid xact-download          # All instances

# Enable on boot
sudo ./manage-workers.sh enable xact-poller
sudo ./manage-workers.sh enable xact-download 4
```

### Using systemctl Directly

```bash
# Start
sudo systemctl start kafka-pipeline-xact-poller
sudo systemctl start kafka-pipeline-xact-download@{1..4}

# Stop
sudo systemctl stop kafka-pipeline-xact-poller
sudo systemctl stop kafka-pipeline-xact-download@2

# Restart
sudo systemctl restart kafka-pipeline-xact-download@1

# Status
sudo systemctl status kafka-pipeline-xact-download@1

# Logs
sudo journalctl -u kafka-pipeline-xact-download@1 -f

# PID
sudo systemctl show -p MainPID kafka-pipeline-xact-download@1 --value

# Enable on boot
sudo systemctl enable kafka-pipeline-xact-download@{1..4}
```

## Typical Production Setup

```bash
# Start XACT pipeline (recommended configuration)
sudo systemctl start kafka-pipeline-xact-poller
sudo systemctl start kafka-pipeline-xact-event-ingester
sudo systemctl start kafka-pipeline-xact-enricher@{1..2}
sudo systemctl start kafka-pipeline-xact-download@{1..4}
sudo systemctl start kafka-pipeline-xact-upload@{1..3}
sudo systemctl start kafka-pipeline-xact-delta-writer@{1..2}
sudo systemctl start kafka-pipeline-xact-result-processor
sudo systemctl start kafka-pipeline-xact-retry-scheduler

# Start ClaimX pipeline
sudo systemctl start kafka-pipeline-claimx-poller
sudo systemctl start kafka-pipeline-claimx-ingester
sudo systemctl start kafka-pipeline-claimx-enricher@{1..2}
sudo systemctl start kafka-pipeline-claimx-downloader@{1..4}
sudo systemctl start kafka-pipeline-claimx-uploader@{1..3}
sudo systemctl start kafka-pipeline-claimx-delta-writer
sudo systemctl start kafka-pipeline-claimx-entity-writer
sudo systemctl start kafka-pipeline-claimx-result-processor
sudo systemctl start kafka-pipeline-claimx-retry-scheduler
```

## Monitoring

```bash
# Check all running workers
sudo systemctl list-units 'kafka-pipeline-*' --type=service --state=running

# View metrics (Prometheus)
curl http://localhost:8001/metrics  # xact-download instance 1

# View application logs
sudo tail -f /opt/kafka-pipeline/logs/xact/$(date +%Y-%m-%d)/*.log

# Search logs for errors
sudo grep -r '"level":"ERROR"' /opt/kafka-pipeline/logs/ | jq .

# Check worker_id in logs
sudo grep '"worker_id"' /opt/kafka-pipeline/logs/xact/$(date +%Y-%m-%d)/*.log | jq .worker_id
```

## Troubleshooting

```bash
# Check service status
sudo systemctl status kafka-pipeline-xact-download@1

# View recent logs
sudo journalctl -u kafka-pipeline-xact-download@1 -n 50

# Check for errors
sudo journalctl -u kafka-pipeline-xact-download@1 -p err -n 20

# Check resource usage
ps aux | grep kafka_pipeline
sudo systemctl show kafka-pipeline-xact-download@1 | grep Memory

# Test Kafka connection
telnet your-kafka-broker 9092

# Verify environment
sudo cat /opt/kafka-pipeline/config/.env | grep KAFKA_BOOTSTRAP_SERVERS

# Check permissions
ls -la /opt/kafka-pipeline/config/.env
sudo systemctl show kafka-pipeline-xact-download@1 | grep User
```

## Emergency Operations

```bash
# Stop all workers immediately
sudo systemctl stop 'kafka-pipeline-*'

# Force kill a worker (last resort)
sudo systemctl kill -s SIGKILL kafka-pipeline-xact-download@1

# Restart all XACT workers
sudo systemctl restart kafka-pipeline-xact-*

# View all systemd errors
sudo journalctl -p err -n 100
```

## File Locations

| Path | Description |
|------|-------------|
| `/opt/kafka-pipeline/src` | Application source code |
| `/opt/kafka-pipeline/config/.env` | Environment configuration |
| `/opt/kafka-pipeline/logs/` | Application logs |
| `/opt/kafka-pipeline/venv` | Python virtual environment |
| `/etc/systemd/system/kafka-pipeline-*.service` | Systemd service files |
| `/tmp/kafka-pipeline/scripts/deployment/manage-workers.sh` | Management script |

## Metrics Ports

| Worker | Port Range |
|--------|-----------|
| xact-download | 8000-8009 |
| xact-upload | 8010-8019 |
| xact-enricher | 8020-8029 |
| xact-delta-writer | 8030-8039 |
| xact-poller | 8100 |
| claimx-enricher | 8100-8109 |
| claimx-downloader | 8110-8119 |
| claimx-uploader | 8120-8129 |
| claimx-poller | 8200 |

## Resource Recommendations

| Worker | Memory | CPU | Instances |
|--------|--------|-----|-----------|
| xact-download | 2GB | 150% | 3-4 |
| xact-upload | 2GB | 150% | 2-3 |
| xact-enricher | 1GB | 100% | 2 |
| xact-delta-writer | 1GB | 100% | 2-3 |
| xact-poller | 512MB | 100% | 1 |
| claimx-downloader | 2GB | 150% | 3-4 |
| claimx-uploader | 2GB | 150% | 2-3 |
| claimx-enricher | 1GB | 100% | 2-3 |

**Total estimated: 20-30GB RAM**
