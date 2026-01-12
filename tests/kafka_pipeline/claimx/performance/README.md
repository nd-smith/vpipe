# ClaimX Performance Tests

Performance and throughput benchmarks for ClaimX Kafka pipeline workers.

## Test Suite Overview

### Enrichment Worker Performance (`TestEnrichmentWorkerPerformance`)
- **test_enrichment_throughput_single_worker**: Measures event processing rate
- **test_enrichment_api_call_latency**: Measures API call overhead

### Download Worker Performance (`TestDownloadWorkerPerformance`)
- **test_download_throughput_single_worker**: Measures file download rate
- **test_download_concurrent_processing**: Tests concurrency impact

### End-to-End Pipeline Performance (`TestPipelineE2EPerformance`)
- **test_single_file_e2e_latency**: Measures complete pipeline latency

## Running Performance Tests

```bash
# Run all performance tests
pytest tests/kafka_pipeline/claimx/performance/ -v -s -m performance

# Run specific test class
pytest tests/kafka_pipeline/claimx/performance/test_throughput.py::TestEnrichmentWorkerPerformance -v -s

# Run without slow E2E tests
pytest tests/kafka_pipeline/claimx/performance/ -v -s -m "performance and not slow"
```

## Performance Baseline Targets

These are **conservative targets** for single-worker instances with mocked external services (S3, OneLake, ClaimX API).

### Enrichment Worker
- **Target**: ≥ 50 events/sec
- **Measured**: Varies based on event type and API call frequency
- **Notes**:
  - PROJECT_CREATED events require 1 API call
  - PROJECT_FILE_ADDED events require 2 API calls (project + media)
  - Batching improves throughput for media-heavy events

### Download Worker
- **Target**: ≥ 5 files/sec
- **Measured**: Depends on file size and S3 latency
- **Notes**:
  - Performance with mocked S3 (10-20ms latency simulation)
  - Real S3 downloads will be slower (network-bound)
  - Concurrency setting: 10 concurrent downloads (default)

### API Call Latency
- **Target**: < 200ms per call (mocked API)
- **Measured**: Measures overhead of async HTTP + circuit breaker
- **Notes**:
  - Real ClaimX API will have higher latency (50-500ms typical)
  - Circuit breaker adds minimal overhead (< 1ms)

### End-to-End Pipeline
- **Target**: < 5 seconds (download → upload → result)
- **Measured**: Complete single-file pipeline latency
- **Notes**:
  - Includes Kafka messaging overhead
  - Mocked S3 (50ms) and OneLake (instant)
  - Real-world latency will be higher

## Test Configuration

Performance test parameters can be adjusted in `test_throughput.py`:

```python
ENRICHMENT_EVENT_COUNT = 100  # Events for throughput test
DOWNLOAD_FILE_COUNT = 50      # Files for download test
ENRICHMENT_BATCH_SIZE = 10    # Events per batch
DOWNLOAD_CONCURRENCY = 10     # Concurrent downloads

# Performance thresholds
MIN_ENRICHMENT_THROUGHPUT = 50.0  # events/sec
MIN_DOWNLOAD_THROUGHPUT = 5.0     # files/sec
MAX_API_LATENCY_MS = 200.0        # milliseconds
```

## Performance Metrics Collected

### Throughput Metrics
- **Total items processed**: Count of events/files processed
- **Elapsed time**: Total test duration in seconds
- **Throughput**: Items per second

### Latency Metrics (when available)
- **Average latency**: Mean processing time per item
- **Median (p50)**: 50th percentile latency
- **p95**: 95th percentile latency
- **p99**: 99th percentile latency

## Interpreting Results

### Good Performance Indicators
- ✅ Throughput meets or exceeds targets
- ✅ p95/p99 latencies within 2x of average
- ✅ No timeout warnings in logs
- ✅ Concurrent processing faster than sequential

### Performance Issues to Investigate
- ❌ Throughput significantly below target
- ❌ High p99 latencies (> 10x average)
- ❌ Timeout warnings or test failures
- ❌ Concurrent processing not faster than sequential

## Comparison with Xact Pipeline

While direct comparison is difficult due to different data models, ClaimX targets should be similar to xact:

| Metric | Xact | ClaimX | Notes |
|--------|------|--------|-------|
| Enrichment | ~100 events/sec | ≥ 50 events/sec | ClaimX has more complex entities |
| Download | ~10 files/sec | ≥ 5 files/sec | Conservative target |
| API latency | < 100ms | < 200ms | ClaimX API may be slower |

## Troubleshooting Performance Issues

### Low Enrichment Throughput
1. Check Kafka consumer lag (`wait_for_condition` timeouts)
2. Verify API client isn't rate-limited
3. Check Delta Lake write performance
4. Increase batch size for media events

### Low Download Throughput
1. Verify concurrent processing is working (check logs)
2. Increase `DOWNLOAD_CONCURRENCY` setting
3. Check cache directory I/O performance
4. Monitor HTTP connection pool utilization

### High Latencies
1. Check for blocking I/O operations
2. Verify async/await usage throughout
3. Profile with `py-spy` or `yappi`
4. Check for database/storage bottlenecks

## Future Enhancements

- [ ] Add memory usage profiling
- [ ] Add CPU usage monitoring
- [ ] Test with various batch sizes
- [ ] Benchmark Delta Lake write performance
- [ ] Load test with multiple workers
- [ ] Sustained throughput tests (hours/days)
