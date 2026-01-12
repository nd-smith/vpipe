"""
Performance and benchmark tests for Kafka pipeline.

This package contains tests for measuring and validating:
- Throughput (messages/second)
- Latency (end-to-end processing time)
- Resource usage (CPU, memory)
- Consumer lag recovery
- Download concurrency

Performance targets from NFR requirements:
- NFR-1.1: End-to-end latency < 5 seconds (p95)
- NFR-1.2: Throughput 1,000 events/second sustained
- NFR-1.3: Download concurrency 50 parallel downloads
- NFR-1.4: Consumer lag recovery < 10 minutes for 100k backlog
"""
