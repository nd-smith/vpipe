# Kafka Pipeline Performance Benchmarks

Performance benchmark results and analysis for the Kafka-based attachment processing pipeline.

## Overview

This document contains performance benchmark results validating the pipeline against Non-Functional Requirements (NFRs) defined in the implementation plan.

## Performance Targets

| Metric | Target | Current Baseline | NFR ID |
|--------|--------|------------------|--------|
| End-to-end latency (p95) | < 5 seconds | 60-120 seconds | NFR-1.1 |
| Throughput | 1,000 events/second | 100 events/cycle | NFR-1.2 |
| Download concurrency | 50 parallel downloads | 10 parallel | NFR-1.3 |
| Consumer lag recovery | < 10 minutes for 100k backlog | N/A | NFR-1.4 |
| Memory per worker | < 512MB | TBD | - |
| CPU usage (mean) | < 80% | TBD | - |

## Running Benchmarks

### Quick Test (Fast Benchmarks Only)
```bash
pytest tests/kafka_pipeline/performance/ -m performance -v
```

### Full Test Suite (Including Slow Benchmarks)
```bash
pytest tests/kafka_pipeline/performance/ -m "performance or slow" -v
```

### Individual Benchmark Categories
```bash
# Throughput benchmarks
pytest tests/kafka_pipeline/performance/test_throughput.py -v

# Latency benchmarks
pytest tests/kafka_pipeline/performance/test_latency.py -v

# Resource usage benchmarks
pytest tests/kafka_pipeline/performance/test_resource_usage.py -v
```

## Benchmark Results

### Throughput Benchmarks

#### Event Ingestion Throughput
**Test**: `test_event_ingestion_throughput`
**Description**: Measures event ingester's ability to consume events and produce download tasks

**Results**: _(To be filled after running tests)_
```
Messages Processed: [TBD]
Processing Time: [TBD]
Throughput: [TBD] events/sec
Peak CPU: [TBD]%
Peak Memory: [TBD] MB

Status: [PASS/FAIL]
```

#### Download Worker Throughput
**Test**: `test_download_worker_throughput`
**Description**: Measures download worker's processing rate with mocked downloads

**Results**: _(To be filled after running tests)_
```
Downloads Processed: [TBD]
Processing Time: [TBD]
Throughput: [TBD] downloads/sec
Peak CPU: [TBD]%
Peak Memory: [TBD] MB

Status: [PASS/FAIL]
```

#### Result Processor Throughput
**Test**: `test_result_processor_throughput`
**Description**: Measures result processor's batching and write performance

**Results**: _(To be filled after running tests)_
```
Results Processed: [TBD]
Processing Time: [TBD]
Throughput: [TBD] results/sec
Peak CPU: [TBD]%
Peak Memory: [TBD] MB

Status: [PASS/FAIL]
```

#### Sustained Throughput
**Test**: `test_sustained_throughput`
**Description**: Full pipeline throughput over extended duration (60,000 events)

**Results**: _(To be filled after running tests)_
```
Events Processed: [TBD]
Processing Time: [TBD]
Sustained Throughput: [TBD] events/sec
Peak CPU: [TBD]%
Peak Memory: [TBD] MB

Status: [PASS/FAIL]
```

---

### Latency Benchmarks

#### End-to-End Latency
**Test**: `test_end_to_end_latency`
**Description**: Complete pipeline latency from event ingestion to inventory write

**Results**: _(To be filled after running tests)_
```
Events Processed: [TBD]
Latency p50: [TBD]s
Latency p95: [TBD]s (Target: < 5.0s)
Latency p99: [TBD]s
Latency max: [TBD]s

Status: [PASS/FAIL]
```

#### Latency Under Load
**Test**: `test_latency_under_load`
**Description**: Latency degradation under high load (10,000 events)

**Results**: _(To be filled after running tests)_
```
Events Processed: [TBD]
Latency p50: [TBD]s
Latency p95: [TBD]s (Target: < 5.0s)
Latency p99: [TBD]s
Latency max: [TBD]s

Status: [PASS/FAIL]
```

#### Latency Percentiles
**Test**: `test_latency_percentiles`
**Description**: Comprehensive latency distribution (5,000 events)

**Results**: _(To be filled after running tests)_
```
Events Processed: [TBD]
Latency p50: [TBD]s (Target: < 0.5s)
Latency p95: [TBD]s (Target: < 5.0s)
Latency p99: [TBD]s (Target: < 10.0s)
Latency mean: [TBD]s
Latency max: [TBD]s

Status: [PASS/FAIL]
```

---

### Resource Usage Benchmarks

#### Memory Usage Under Load
**Test**: `test_memory_usage_under_load`
**Description**: Memory consumption during sustained processing (20,000 events)

**Results**: _(To be filled after running tests)_
```
Events Processed: [TBD]
Peak Memory: [TBD] MB (Target: < 1536 MB for 3 workers)
Mean Memory: [TBD] MB
Peak CPU: [TBD]%
Mean CPU: [TBD]%

Status: [PASS/FAIL]
```

#### Download Concurrency
**Test**: `test_download_concurrency`
**Description**: Maximum concurrent downloads (NFR-1.3 target: 50 parallel)

**Results**: _(To be filled after running tests)_
```
Downloads Processed: [TBD]
Max Concurrent Downloads: [TBD] (Target: >= 50)
Peak Memory: [TBD] MB
Peak CPU: [TBD]%

Status: [PASS/FAIL]
```

#### Consumer Lag Recovery
**Test**: `test_consumer_lag_recovery`
**Description**: Time to clear 100k message backlog (NFR-1.4 target: < 10 minutes)

**Results**: _(To be filled after running tests)_
```
Backlog Size: [TBD] messages
Recovery Time: [TBD]s ([TBD]min) (Target: < 600s)
Recovery Rate: [TBD] msg/sec
Peak Memory: [TBD] MB
Peak CPU: [TBD]%

Status: [PASS/FAIL]
```

#### CPU Usage Under Load
**Test**: `test_cpu_usage_under_load`
**Description**: CPU utilization during high-throughput processing (15,000 events)

**Results**: _(To be filled after running tests)_
```
Events Processed: [TBD]
Peak CPU: [TBD]%
Mean CPU: [TBD]% (Target: < 80%)
Peak Memory: [TBD] MB

Status: [PASS/FAIL]
```

---

## Performance Analysis

### Bottlenecks Identified

_(To be filled after running tests and analysis)_

1. **[Component Name]**: [Description of bottleneck]
   - Impact: [Effect on throughput/latency]
   - Mitigation: [Proposed solution]

### Optimization Opportunities

_(To be filled after running tests and analysis)_

1. **[Optimization Area]**: [Description]
   - Expected Improvement: [Estimated gain]
   - Priority: [High/Medium/Low]

### Scalability Assessment

_(To be filled after running tests and analysis)_

- **Horizontal Scaling**: [Assessment of ability to add more workers]
- **Vertical Scaling**: [Assessment of benefit from more CPU/memory]
- **Resource Bottlenecks**: [Identified resource constraints]

## Recommendations

### Immediate Actions

_(To be filled after running tests and analysis)_

1. [Recommendation based on results]
2. [Recommendation based on results]

### Future Improvements

_(To be filled after running tests and analysis)_

1. [Long-term optimization suggestion]
2. [Long-term optimization suggestion]

## Test Environment

### Hardware Specifications

```
Processor: [TBD]
Memory: [TBD]
Storage: [TBD]
Network: [TBD]
```

### Software Versions

```
Python: [TBD]
Kafka: [TBD] (Testcontainers)
aiokafka: [TBD]
OS: [TBD]
```

### Test Configuration

```
Kafka Partitions: [TBD]
Consumer Group Size: [TBD]
Batch Sizes: [TBD]
Compression: lz4
```

## Regression Testing

Performance benchmarks should be run regularly to detect regressions:

- **On PR merge**: Run fast benchmarks (< 2 minutes)
- **Weekly**: Run full benchmark suite
- **Before release**: Complete benchmark analysis with report

### Performance Regression Thresholds

- **Throughput**: > 10% decrease = warning, > 20% decrease = failure
- **Latency p95**: > 15% increase = warning, > 30% increase = failure
- **Memory**: > 20% increase = warning, > 40% increase = failure

## Appendix

### Benchmark Methodology

#### Throughput Measurement
- Events are produced in batches to Kafka
- Workers process messages concurrently
- Throughput = (total messages processed) / (processing duration)
- Metrics collected via ResourceMonitor

#### Latency Measurement
- Timestamp recorded when event is produced
- Completion timestamp determined when inventory record exists
- Latency = completion_time - produce_time
- Percentiles calculated from latency distribution

#### Resource Monitoring
- CPU and memory sampled every 1 second via psutil
- Peak values represent maximum observed during test
- Mean values represent average across all samples
- Process-level metrics (not system-wide)

### Interpreting Results

#### Pass/Fail Criteria
- **PASS**: Metric meets or exceeds target (with margin for variability)
- **FAIL**: Metric below target by > 20%

#### Margin for Test Environment
- Targets allow 20% margin for test environment variability
- Production performance expected to match or exceed test results
- CI/CD environments may show higher variability

---

*Last Updated*: [TBD]
*Test Version*: [TBD]
*Document Version*: 1.0
