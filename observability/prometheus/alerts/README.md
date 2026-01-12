# Kafka Pipeline - Prometheus Alert Rules

This directory contains Prometheus alerting rules for monitoring the Kafka pipeline's health, performance, and operational status.

## Overview

The alert rules are designed to detect critical operational issues and provide early warning for degrading performance. Alerts are organized into logical groups covering different aspects of the pipeline:

- **Consumer health**: Lag, connectivity, partition assignment
- **DLQ monitoring**: Dead-letter queue growth and size
- **Error tracking**: Processing errors, download failures, producer errors
- **Circuit breaker**: Service health and failure protection
- **Performance**: Latency, throughput, resource utilization
- **Delta Lake**: Write failures and performance
- **Producer health**: Connectivity and error rates
- **Overall health**: Worker liveness and system health

## Alert Files

| File | Description |
|------|-------------|
| `kafka-pipeline.yml` | Main alert rules for pipeline monitoring |

## Alert Severity Levels

Alerts are classified into three severity levels:

### Critical
- **Impact**: Production-impacting issues requiring immediate attention
- **Response Time**: Within 15 minutes
- **Examples**: Consumer disconnected, circuit breaker open, high error rates
- **Routing**: Should page on-call engineer

### Warning
- **Impact**: Degraded performance or early indicators of problems
- **Response Time**: Within 1-2 hours during business hours
- **Examples**: Elevated consumer lag, elevated DLQ rate, high latency
- **Routing**: Notification to team channel (Microsoft Teams)

### Info
- **Impact**: Informational, system state changes
- **Response Time**: Best effort, investigate when convenient
- **Examples**: Circuit breaker testing recovery
- **Routing**: Low-priority notification or logging only

## Alert Groups

### kafka_pipeline_consumer

Monitors consumer health and message consumption.

| Alert | Severity | Threshold | Description |
|-------|----------|-----------|-------------|
| `KafkaConsumerLagHigh` | Critical | lag > 10,000 for 5m | Consumer cannot keep up with message production |
| `KafkaConsumerLagWarning` | Warning | lag > 5,000 for 10m | Early detection of consumer lag buildup |
| `KafkaConsumerDisconnected` | Critical | connection down for 1m | Consumer lost connection to Kafka broker |
| `KafkaConsumerNoPartitions` | Warning | 0 partitions for 3m | Consumer has no assigned partitions |

### kafka_pipeline_dlq

Monitors dead-letter queue for systematic failures.

| Alert | Severity | Threshold | Description |
|-------|----------|-----------|-------------|
| `KafkaDLQGrowthRapid` | Critical | rate > 10/min for 5m | Rapid increase in DLQ messages indicates systematic failures |
| `KafkaDLQGrowthWarning` | Warning | rate > 5/min for 10m | Elevated DLQ message rate |
| `KafkaDLQSizeHigh` | Warning | > 1,000 messages for 30m | Large number of messages requiring manual review |

### kafka_pipeline_errors

Monitors error rates and processing failures.

| Alert | Severity | Threshold | Description |
|-------|----------|-----------|-------------|
| `KafkaErrorRateHigh` | Critical | > 1% for 5m | High overall message processing error rate |
| `KafkaDownloadFailureRateHigh` | Critical | > 5% for 5m | High download failure rate |
| `KafkaProducerErrorsHigh` | Warning | rate > 1/sec for 5m | Elevated producer error rate |

### kafka_pipeline_circuit_breaker

Monitors circuit breaker state and failures.

| Alert | Severity | Threshold | Description |
|-------|----------|-----------|-------------|
| `KafkaCircuitBreakerOpen` | Critical | open for 2m | Circuit breaker protecting against cascading failures |
| `KafkaCircuitBreakerHalfOpen` | Info | half-open for 1m | Circuit breaker testing service recovery |
| `KafkaCircuitBreakerFailuresHigh` | Warning | rate > 5 failures/10m | Frequent circuit breaker failures |

### kafka_pipeline_performance

Monitors processing latency and throughput.

| Alert | Severity | Threshold | Description |
|-------|----------|-----------|-------------|
| `KafkaProcessingLatencyHigh` | Critical | p95 > 10s for 5m | High message processing latency |
| `KafkaProcessingLatencyWarning` | Warning | p95 > 5s for 10m | Elevated processing latency |
| `KafkaThroughputLow` | Warning | < 100 msg/s for 10m | Low overall pipeline throughput |

### kafka_pipeline_delta

Monitors Delta Lake write operations.

| Alert | Severity | Threshold | Description |
|-------|----------|-----------|-------------|
| `KafkaDeltaWriteFailures` | Critical | any failures for 5m | Delta Lake writes failing |
| `KafkaDeltaWriteLatencyHigh` | Warning | p95 > 30s for 10m | High Delta write latency |
| `KafkaDeltaWriteSuccessRateLow` | Warning | < 95% for 10m | Low Delta write success rate |

### kafka_pipeline_producer

Monitors producer health and connectivity.

| Alert | Severity | Threshold | Description |
|-------|----------|-----------|-------------|
| `KafkaProducerDisconnected` | Critical | connection down for 1m | Producer lost connection to Kafka broker |

### kafka_pipeline_health

Monitors overall worker and system health.

| Alert | Severity | Threshold | Description |
|-------|----------|-----------|-------------|
| `KafkaWorkerDead` | Critical | 0 consumption for 10m | Worker instances appear dead or stuck |

## Configuration

### Loading Alert Rules

Add the alert rules to your Prometheus configuration:

```yaml
# prometheus.yml
rule_files:
  - /etc/prometheus/alerts/kafka-pipeline.yml
```

### Alert Manager Integration

Configure AlertManager to route alerts based on severity:

```yaml
# alertmanager.yml
route:
  receiver: 'default'
  group_by: ['alertname', 'component']
  group_wait: 10s
  group_interval: 5m
  repeat_interval: 12h
  routes:
    # Critical alerts - page on-call
    - match:
        severity: critical
      receiver: 'pagerduty'
      continue: true

    # Warning alerts - notify team channel
    - match:
        severity: warning
      receiver: 'teams-channel'
      continue: true

    # Info alerts - log only
    - match:
        severity: info
      receiver: 'logs'

receivers:
  - name: 'default'
    # Default fallback receiver

  - name: 'pagerduty'
    # Configure PagerDuty integration here
    # (Placeholder - future implementation)

  - name: 'teams-channel'
    # Configure Microsoft Teams webhook here
    webhook_configs:
      - url: 'https://outlook.office.com/webhook/...'
        send_resolved: true

  - name: 'logs'
    # Log-only receiver for informational alerts
```

## Customizing Alerts

### Adjusting Thresholds

Alert thresholds are tuned for typical production workloads. You may need to adjust based on your specific requirements:

1. **Consumer Lag**: Default is 10,000 messages for critical, 5,000 for warning
   - Adjust based on your typical message production rate and acceptable lag
   - Example: High-volume topics may need higher thresholds

2. **Error Rates**: Default is 1% overall error rate, 5% download failure rate
   - Adjust based on your quality targets and historical error patterns
   - Consider different thresholds for different environments (dev/staging/prod)

3. **Latency**: Default is 10s p95 for critical, 5s for warning
   - Adjust based on your SLA requirements and typical processing times
   - Large file downloads may have higher acceptable latency

### Adding Custom Alerts

To add new alerts, follow this template:

```yaml
- alert: YourAlertName
  expr: |
    # PromQL expression
    metric_name{labels} > threshold
  for: duration
  labels:
    severity: critical|warning|info
    component: component_name
  annotations:
    summary: "Brief description with context: {{ $labels.label_name }}"
    description: |
      Detailed description with current value: {{ $value }}

      Current state: {{ $value }}
      Threshold: X
      Duration: Y
    runbook_url: "https://github.com/verisk/pipeline/docs/runbooks/runbook-name.md"
    remediation: |
      Step-by-step remediation actions
```

## Alert Testing

### Verify Alert Rules

Test that alert rules are valid:

```bash
# Validate alert rule syntax
promtool check rules observability/prometheus/alerts/kafka-pipeline.yml

# Test alert evaluation
promtool test rules observability/prometheus/alerts/test_alerts.yml
```

### Trigger Test Alerts

To verify alert routing and notification channels, you can trigger test alerts:

```bash
# Using amtool (AlertManager CLI)
amtool alert add --annotation=summary="Test alert" alertname="TestAlert"

# Or send test alert directly to AlertManager API
curl -XPOST http://alertmanager:9093/api/v1/alerts -d '[
  {
    "labels": {
      "alertname": "TestAlert",
      "severity": "warning"
    },
    "annotations": {
      "summary": "This is a test alert"
    }
  }
]'
```

## Runbook Integration

Each alert includes a `runbook_url` annotation linking to operational procedures. Ensure runbooks are created and maintained for:

- `consumer-lag.md`: Consumer lag troubleshooting and scaling
- `dlq-management.md`: DLQ inspection, replay, and remediation
- `circuit-breaker-open.md`: Circuit breaker diagnostics and recovery
- `incident-response.md`: General incident response procedures
- `scaling-operations.md`: Horizontal scaling procedures

See `docs/runbooks/` directory for runbook templates and examples.

## Monitoring Alert System Health

Monitor the alerting system itself:

```promql
# Alerts currently firing
ALERTS{alertstate="firing"}

# Alert evaluation failures
prometheus_rule_evaluation_failures_total

# AlertManager notifications
alertmanager_notifications_total
alertmanager_notifications_failed_total
```

## Best Practices

### 1. Alert Tuning
- Start with conservative thresholds and adjust based on actual system behavior
- Monitor alert frequency - too many alerts lead to alert fatigue
- Use warning alerts for early detection, critical for immediate action required
- Review and update thresholds quarterly based on system evolution

### 2. Alert Grouping
- Group related alerts to avoid notification storms
- Use `group_by` in AlertManager to consolidate similar alerts
- Set appropriate `group_wait` and `group_interval` to batch notifications

### 3. Runbook Discipline
- Every critical alert must have a runbook
- Runbooks should be tested and kept up-to-date
- Include specific CLI commands and troubleshooting steps
- Document common resolution patterns from past incidents

### 4. Alert Resolution
- Always set `send_resolved: true` to notify when alerts clear
- Include resolution time in alert metadata for trend analysis
- Review long-running alerts weekly - may indicate systemic issues

### 5. On-Call Integration
- Critical alerts should page on-call engineer immediately
- Warning alerts should notify during business hours only
- Use escalation policies for alerts that remain unacknowledged
- Include alert context in pages to speed up triage

## Metrics Reference

For complete metrics documentation, see:
- `src/kafka_pipeline/metrics.py`: Metric definitions and instrumentation
- `observability/grafana/dashboards/`: Grafana dashboards using these metrics

## Support and Feedback

For questions or issues with alerting:
1. Check runbooks in `docs/runbooks/`
2. Review Grafana dashboards for metric visualization
3. Consult with platform engineering team
4. Update this documentation with lessons learned

## Changelog

| Date | Change | Author |
|------|--------|--------|
| 2024-12-27 | Initial alert rules creation (WP-409) | System |

## Future Enhancements

Planned improvements for the alerting system:

- [ ] Container/VM resource monitoring integration (CPU, memory, disk)
- [ ] Network latency and connectivity monitoring
- [ ] Kubernetes-specific alerts (pod restarts, OOMKills)
- [ ] Advanced anomaly detection using Prometheus recording rules
- [ ] Integration with log aggregation for alert context enrichment
- [ ] Alert dashboards showing alert history and trends
- [ ] Automated alert threshold tuning based on historical data
