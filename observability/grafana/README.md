# Kafka Pipeline Grafana Dashboards

This directory contains production-ready Grafana dashboards for monitoring the Kafka-based data pipeline.

## Dashboard Overview

### 1. Pipeline Overview (`kafka-pipeline-overview.json`)

**Purpose**: High-level system health and performance monitoring

**Key Metrics**:
- **Throughput**:
  - Events consumed per second
  - Download tasks created per second
- **Latency**:
  - Message processing time (p50, p95, p99)
  - Target: <5s p95 end-to-end latency
- **Health**:
  - Total consumer lag across all topics
  - Circuit breaker states (CLOSED=0, OPEN=1, HALF-OPEN=2)
- **Errors**:
  - Error rate by category (transient, permanent, auth)

**Use Cases**:
- Daily operational monitoring
- Performance trend analysis
- Quick health checks
- Executive dashboards

**Alerts**:
- Consumer lag > 10,000 messages
- Error rate > 1% for 5 minutes
- Circuit breaker open > 2 minutes

---

### 2. Consumer Health (`consumer-health.json`)

**Purpose**: Deep dive into Kafka consumer behavior and partition management

**Key Metrics**:
- **Connection Status**: Consumer connected/disconnected state
- **Lag Monitoring**:
  - Total consumer lag across all partitions
  - Per-partition lag breakdown
  - Lag trends over time
- **Partition Management**:
  - Number of assigned partitions per consumer group
  - Partition assignment changes (indicates rebalancing)
- **Consumption Rates**:
  - Messages consumed per second by consumer group and topic
  - Offset progress per partition

**Use Cases**:
- Diagnosing consumer lag issues
- Detecting rebalancing events
- Capacity planning for horizontal scaling
- Troubleshooting partition distribution

**Alerts**:
- Consumer disconnected
- Lag > 10,000 on any partition
- Rapid lag growth (>1000 messages/minute)

---

### 3. Download Performance (`download-performance.json`)

**Purpose**: Monitor download worker performance and OneLake upload operations

**Key Metrics**:
- **Success Rates**:
  - Download failure rate percentage
  - Error distribution by category
- **Latency**:
  - Download processing time (p50, p95, p99)
  - OneLake/Delta write latency
- **Throughput**:
  - Downloads per second
  - Bytes processed per second
  - Delta write rate by table
- **Concurrency**:
  - Approximate concurrent downloads (1-minute rate)
  - Target: 50 parallel downloads (NFR-1.3)

**Use Cases**:
- Performance optimization
- Identifying slow downloads
- Detecting OneLake/storage issues
- Capacity planning for download workers

**Alerts**:
- Download failure rate > 5%
- p95 latency > 10 seconds
- OneLake write failures

---

### 4. DLQ Monitoring (`dlq-monitoring.json`)

**Purpose**: Track and manage failed messages requiring manual intervention

**Key Metrics**:
- **DLQ Size**:
  - Current message count in DLQ
  - DLQ growth trends
  - Oldest message age
- **Rates**:
  - Messages entering DLQ per second
  - Messages being replayed per second
- **Error Analysis**:
  - Error distribution by category
  - Ingestion rate by error type

**Use Cases**:
- Manual review workflow
- Identifying recurring failure patterns
- Tracking replay operations
- Prioritizing DLQ cleanup

**Alerts**:
- DLQ size > 100 messages
- DLQ growth rate > 10 messages/minute
- Oldest message age > 24 hours

---

## Installation

### Prerequisites

- Grafana instance (version 10.0.0 or later)
- Prometheus datasource configured in Grafana
- Kafka pipeline metrics being exported to Prometheus

### Import Dashboards

1. **Via Grafana UI**:
   ```
   1. Navigate to Dashboards → Import
   2. Upload JSON file or paste JSON content
   3. Select Prometheus datasource
   4. Click Import
   ```

2. **Via API** (automated deployment):
   ```bash
   for dashboard in observability/grafana/dashboards/*.json; do
     curl -X POST \
       -H "Authorization: Bearer ${GRAFANA_API_KEY}" \
       -H "Content-Type: application/json" \
       -d @${dashboard} \
       ${GRAFANA_URL}/api/dashboards/db
   done
   ```

3. **Via Terraform** (infrastructure as code):
   ```hcl
   resource "grafana_dashboard" "kafka_pipeline_overview" {
     config_json = file("${path.module}/observability/grafana/dashboards/kafka-pipeline-overview.json")
   }
   ```

---

## Configuration

### Dashboard Variables

All dashboards support the following template variables:

- **`$datasource`**: Prometheus datasource (auto-populated)
- **`$topic`**: Filter by Kafka topic (multi-select, includes all by default)
- **`$consumer_group`**: Filter by consumer group (multi-select, includes all by default)

### Time Range

- **Default**: Last 1 hour
- **Refresh**: 10 seconds (configurable)
- **Recommended ranges**:
  - Real-time monitoring: Last 5-15 minutes
  - Troubleshooting: Last 1-6 hours
  - Trend analysis: Last 24 hours to 7 days

### Panel Thresholds

Thresholds are pre-configured based on NFR requirements:

| Metric | Warning | Critical | Source |
|--------|---------|----------|--------|
| Consumer Lag | 5,000 msgs | 10,000 msgs | NFR-1.4 |
| Download Failure Rate | 1% | 5% | NFR-1.2 |
| Processing Latency (p95) | 5s | 10s | NFR-1.1 |
| DLQ Size | 10 msgs | 100 msgs | Operational |
| OneLake Write Failures | 1% | 5% | NFR-2.10 |

---

## Metrics Reference

### Kafka Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kafka_messages_consumed_total` | Counter | topic, consumer_group, status | Total messages consumed |
| `kafka_messages_produced_total` | Counter | topic, status | Total messages produced |
| `kafka_consumer_lag` | Gauge | topic, partition, consumer_group | Current lag in messages |
| `kafka_consumer_offset` | Gauge | topic, partition, consumer_group | Current offset position |
| `kafka_processing_errors_total` | Counter | topic, consumer_group, error_category | Processing errors by category |
| `kafka_message_processing_duration_seconds` | Histogram | topic, consumer_group | Message processing time |
| `kafka_circuit_breaker_state` | Gauge | component | Circuit breaker state (0/1/2) |
| `kafka_connection_status` | Gauge | component | Connection status (0/1) |
| `kafka_consumer_assigned_partitions` | Gauge | consumer_group | Assigned partitions count |

### Delta Lake Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `delta_writes_total` | Counter | table, status | Total Delta write operations |
| `delta_events_written_total` | Counter | table | Events written to Delta tables |
| `delta_write_duration_seconds` | Histogram | table | Delta write operation time |

---

## Troubleshooting

### Common Issues

**1. No Data Displayed**
- Verify Prometheus datasource is configured correctly
- Check that metrics are being exported from the pipeline
- Verify time range includes recent data
- Check Prometheus query syntax in panel edit mode

**2. Missing Metrics**
- Ensure all pipeline workers are running
- Verify Prometheus scrape configuration includes pipeline endpoints
- Check metric names match the dashboard queries

**3. Dashboard Import Fails**
- Verify Grafana version compatibility (10.0.0+)
- Check JSON syntax validity
- Ensure Prometheus plugin is installed

**4. Variable Not Populating**
- Verify metrics with expected labels exist in Prometheus
- Check variable query syntax in dashboard settings
- Ensure datasource is correctly selected

### Query Debugging

To debug a panel query:

1. Edit panel → Query tab
2. Click "Query Inspector"
3. Review Prometheus query and response
4. Test query directly in Prometheus UI

### Performance Optimization

For large-scale deployments:

1. **Reduce refresh rate**: 30s-1m instead of 10s
2. **Limit time range**: Default to last 1h instead of 24h
3. **Use recording rules**: Pre-compute expensive aggregations
4. **Enable query caching**: Configure Grafana query cache

---

## Best Practices

### Operational Workflows

1. **Daily Monitoring**:
   - Start with Pipeline Overview dashboard
   - Check for any red/yellow threshold violations
   - Review lag trends on Consumer Health dashboard
   - Spot-check DLQ size

2. **Performance Investigation**:
   - Use Download Performance dashboard for latency analysis
   - Correlate errors with consumer lag spikes
   - Check circuit breaker states for connectivity issues

3. **Incident Response**:
   - DLQ Monitoring dashboard for failure analysis
   - Consumer Health for partition rebalancing issues
   - Download Performance for throughput problems

### Alert Configuration

Recommended alert rules (see `../prometheus/alerts/kafka-pipeline.yml`):

- Consumer lag > 10,000 for 5 minutes → Critical
- DLQ growth > 10 msgs/min for 5 minutes → Warning
- Error rate > 1% for 5 minutes → Warning
- Error rate > 5% for 5 minutes → Critical
- Circuit breaker open > 2 minutes → Critical

### Dashboard Maintenance

- **Version Control**: All dashboards are JSON files in git
- **Updates**: Import updated JSON to preserve UIDs
- **Backups**: Export dashboards before major Grafana upgrades
- **Documentation**: Update this README when adding/modifying panels

---

## Architecture Integration

These dashboards integrate with the broader observability stack:

```
┌─────────────────┐
│  Kafka Pipeline │
│    (Workers)    │
└────────┬────────┘
         │ Metrics Export
         ▼
┌─────────────────┐
│   Prometheus    │  ← Scrapes metrics from /metrics endpoint
│   (TSDB)        │
└────────┬────────┘
         │ Query
         ▼
┌─────────────────┐
│    Grafana      │  ← Dashboards query Prometheus
│  (Dashboards)   │
└────────┬────────┘
         │ Alerts
         ▼
┌─────────────────┐
│ Microsoft Teams │  ← Alert notifications (future)
└─────────────────┘
```

---

## Support

For issues or questions:

1. **Documentation**: See `docs/kafka-greenfield-implementation-plan.md`
2. **Runbooks**: See `docs/runbooks/` for operational procedures
3. **Metrics**: See `src/kafka_pipeline/metrics.py` for metric definitions
4. **Alerts**: See `../prometheus/alerts/kafka-pipeline.yml` for alert rules

---

## Dashboard UIDs

For programmatic access and linking:

- **Pipeline Overview**: `kafka-pipeline-overview`
- **Consumer Health**: `kafka-consumer-health`
- **Download Performance**: `kafka-download-performance`
- **DLQ Monitoring**: `kafka-dlq-monitoring`

Example dashboard URL:
```
https://grafana.example.com/d/kafka-pipeline-overview/kafka-pipeline-overview
```

---

## Future Enhancements

Planned improvements (Phase 5):

- [ ] Cost tracking metrics (Azure consumption)
- [ ] Retention policy visualization
- [ ] Topic partition utilization heatmaps
- [ ] Worker instance health tracking
- [ ] Custom annotations for deployments/incidents
- [ ] SLO/SLA tracking panels
- [ ] Predictive lag forecasting
