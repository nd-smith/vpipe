# VPipe Observability Stack

This document describes the observability stack integrated into the VPipe simulation environment.

## Components

### Prometheus
**URL:** http://localhost:9090

Metrics collection and storage system that scrapes metrics from all VPipe workers every 15 seconds.

**Key Metrics:**
- `messages_consumed_total` - Total messages consumed by workers
- `messages_produced_total` - Total messages produced by workers
- `processing_errors_total` - Processing errors by worker
- `retry_messages_queued` - Messages waiting in retry queue
- `retry_messages_routed_total` - Messages routed from retry queue
- `retry_messages_delayed_total` - Messages added to retry queue
- `retry_messages_exhausted_total` - Messages that exhausted retries (sent to DLQ)
- `dlq_messages_total` - Messages sent to Dead Letter Queue
- `message_processing_duration_seconds` - Processing duration histogram
- `kafka_consumer_lag` - Consumer lag per topic/partition
- `connection_status` - Connection status (1=connected, 0=disconnected)

### Grafana
**URL:** http://localhost:3000
**Credentials:** admin / admin

Visualization and dashboarding system with pre-configured VPipe dashboards.

**Default Dashboard:** VPipe Overview
- Message consumption and production rates
- Retry queue sizes (ClaimX and XACT)
- Retry message routing rates
- Processing error rates
- DLQ message rates

**Creating Custom Dashboards:**
1. Navigate to http://localhost:3000
2. Login with admin/admin
3. Click "+" → "Dashboard" → "Add new panel"
4. Select Prometheus as datasource
5. Enter PromQL queries (see metrics above)

### Jaeger (Optional)
**URL:** http://localhost:16686

Distributed tracing system for request flow visualization.

Requires `observability` profile:
```bash
docker-compose -f docker-compose.simulation.yml --profile simulation --profile observability up -d
```

## Usage

### Starting with Observability

**Basic (Prometheus + Grafana):**
```bash
docker-compose -f docker-compose.simulation.yml --profile simulation up -d
```

**Full Stack (includes Jaeger):**
```bash
docker-compose -f docker-compose.simulation.yml --profile simulation --profile observability up -d
```

### Accessing Dashboards

1. **Grafana:**
   - Open http://localhost:3000
   - Login: admin / admin
   - Navigate to "Dashboards" → "VPipe Overview"

2. **Prometheus:**
   - Open http://localhost:9090
   - Try queries:
     - `rate(messages_consumed_total[1m])` - Message rate
     - `retry_messages_queued` - Current retry queue size
     - `sum by (domain) (processing_errors_total)` - Errors by domain

3. **Jaeger (if enabled):**
   - Open http://localhost:16686
   - Select service and search for traces

### Monitoring Retry System

**PromQL Queries:**

```promql
# Messages currently queued for retry
retry_messages_queued{domain="claimx"}

# Retry routing rate (messages/sec)
rate(retry_messages_routed_total{domain="claimx"}[1m])

# Messages sent to retry (5min rate)
rate(retry_messages_delayed_total{domain="claimx"}[5m])

# Retry exhaustion rate (messages that hit max retries)
rate(retry_messages_exhausted_total{domain="claimx"}[5m])

# Success rate (messages consumed - errors) / messages consumed
1 - (rate(processing_errors_total[1m]) / rate(messages_consumed_total[1m]))
```

### Monitoring Performance

**PromQL Queries:**

```promql
# P95 processing latency
histogram_quantile(0.95, rate(message_processing_duration_seconds_bucket[5m]))

# Consumer lag
kafka_consumer_lag

# Messages per second by worker
sum by (worker_type) (rate(messages_consumed_total[1m]))

# Error rate by domain
sum by (domain) (rate(processing_errors_total[1m]))
```

## Configuration

### Prometheus Scrape Targets

Edit `prometheus.yml` to add/modify scrape targets:

```yaml
scrape_configs:
  - job_name: 'my-worker'
    static_configs:
      - targets: ['my-worker-container:8000']
        labels:
          domain: 'claimx'
          worker_type: 'my-worker'
```

### Grafana Datasources

Prometheus is auto-configured as the default datasource via provisioning.

To add additional datasources, edit:
`grafana/provisioning/datasources/prometheus.yml`

### Adding Dashboards

Place dashboard JSON files in:
`grafana/provisioning/dashboards/`

They will be automatically loaded on Grafana startup.

## Troubleshooting

### Metrics Not Showing

1. **Check worker is exposing metrics:**
   ```bash
   curl http://localhost:8000/metrics
   ```

2. **Check Prometheus targets:**
   - Open http://localhost:9090/targets
   - Verify all workers show as "UP"

3. **Check container names match prometheus.yml:**
   ```bash
   docker ps --format "{{.Names}}"
   ```

### Grafana Dashboard Not Loading

1. **Verify Prometheus datasource:**
   - Grafana → Configuration → Data Sources
   - Test the Prometheus connection

2. **Check dashboard provisioning:**
   ```bash
   docker exec grafana-simulation ls /etc/grafana/provisioning/dashboards/
   ```

### High Memory Usage

If observability stack uses too much memory:

1. **Reduce Prometheus retention:**
   ```yaml
   command:
     - '--storage.tsdb.retention.time=6h'  # Default is 15d
   ```

2. **Disable Jaeger:**
   - Don't use `--profile observability`

## Best Practices

1. **Use Grafana for visualization** - Much easier than raw PromQL
2. **Create alerts** - Set up Grafana alerts for error rates
3. **Monitor retry queues** - Watch for infinite retry loops
4. **Track success rates** - Monitor processing success percentage
5. **Use histograms** - Better than averages for latency
6. **Label wisely** - Use domain/worker_type labels for filtering
7. **Time windows** - Use appropriate time windows (1m for rates, 5m for trends)

## Example Queries

### Finding Problematic Workers

```promql
# Workers with highest error rates
topk(5, rate(processing_errors_total[5m]))

# Workers with highest consumer lag
topk(5, kafka_consumer_lag)

# Slowest workers (P99 latency)
topk(5, histogram_quantile(0.99, rate(message_processing_duration_seconds_bucket[5m])))
```

### Capacity Planning

```promql
# Total throughput
sum(rate(messages_consumed_total[5m]))

# Throughput by domain
sum by (domain) (rate(messages_consumed_total[5m]))

# Worker utilization (messages per worker instance)
rate(messages_consumed_total[5m]) / count by (worker_type) (up)
```

### Retry System Health

```promql
# Retry success rate (routed / delayed)
rate(retry_messages_routed_total[5m]) / rate(retry_messages_delayed_total[5m])

# Retry exhaustion percentage
rate(retry_messages_exhausted_total[5m]) / rate(retry_messages_delayed_total[5m]) * 100

# Average retry queue depth
avg_over_time(retry_messages_queued[5m])
```
