"""
Pytest fixtures for performance testing.

Provides fixtures for:
- Resource monitoring (CPU, memory)
- Load generation
- Performance metrics collection
- Benchmark reporting
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional

import psutil
import pytest

# Ensure test mode environment variables are set
if "TEST_MODE" not in os.environ:
    os.environ["TEST_MODE"] = "true"
if "AUDIT_LOG_PATH" not in os.environ:
    os.environ["AUDIT_LOG_PATH"] = "/tmp/test_audit.log"
if "ALLOWED_ATTACHMENT_DOMAINS" not in os.environ:
    os.environ["ALLOWED_ATTACHMENT_DOMAINS"] = "example.com,claimxperience.com"
if "ONELAKE_BASE_PATH" not in os.environ:
    os.environ["ONELAKE_BASE_PATH"] = "abfss://test@test.dfs.core.windows.net/Files"

from kafka_pipeline.config import KafkaConfig

logger = logging.getLogger(__name__)


@dataclass
class ResourceSnapshot:
    """Snapshot of resource usage at a point in time."""

    timestamp: datetime
    cpu_percent: float
    memory_mb: float
    memory_percent: float

    def to_dict(self) -> Dict:
        """Convert to dictionary for reporting."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "cpu_percent": round(self.cpu_percent, 2),
            "memory_mb": round(self.memory_mb, 2),
            "memory_percent": round(self.memory_percent, 2),
        }


@dataclass
class PerformanceMetrics:
    """Collection of performance metrics from a test run."""

    test_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    # Throughput metrics
    messages_processed: int = 0
    messages_per_second: Optional[float] = None

    # Latency metrics (in seconds)
    latencies: List[float] = field(default_factory=list)
    latency_p50: Optional[float] = None
    latency_p95: Optional[float] = None
    latency_p99: Optional[float] = None
    latency_mean: Optional[float] = None
    latency_max: Optional[float] = None

    # Resource usage
    resource_snapshots: List[ResourceSnapshot] = field(default_factory=list)
    peak_cpu_percent: Optional[float] = None
    peak_memory_mb: Optional[float] = None
    mean_cpu_percent: Optional[float] = None
    mean_memory_mb: Optional[float] = None

    # Error tracking
    errors: int = 0
    error_rate: Optional[float] = None

    def finalize(self) -> None:
        """Calculate final metrics after test completion."""
        self.end_time = datetime.now(timezone.utc)
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()

        # Calculate throughput
        if self.duration_seconds > 0:
            self.messages_per_second = self.messages_processed / self.duration_seconds

        # Calculate latency percentiles
        if self.latencies:
            sorted_latencies = sorted(self.latencies)
            n = len(sorted_latencies)
            self.latency_p50 = sorted_latencies[int(n * 0.50)]
            self.latency_p95 = sorted_latencies[int(n * 0.95)]
            self.latency_p99 = sorted_latencies[int(n * 0.99)]
            self.latency_mean = sum(sorted_latencies) / n
            self.latency_max = sorted_latencies[-1]

        # Calculate resource usage statistics
        if self.resource_snapshots:
            cpu_values = [s.cpu_percent for s in self.resource_snapshots]
            memory_values = [s.memory_mb for s in self.resource_snapshots]

            self.peak_cpu_percent = max(cpu_values)
            self.peak_memory_mb = max(memory_values)
            self.mean_cpu_percent = sum(cpu_values) / len(cpu_values)
            self.mean_memory_mb = sum(memory_values) / len(memory_values)

        # Calculate error rate
        total_operations = self.messages_processed + self.errors
        if total_operations > 0:
            self.error_rate = self.errors / total_operations

    def to_dict(self) -> Dict:
        """Convert metrics to dictionary for reporting."""
        return {
            "test_name": self.test_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": round(self.duration_seconds, 2) if self.duration_seconds else None,
            "throughput": {
                "messages_processed": self.messages_processed,
                "messages_per_second": round(self.messages_per_second, 2) if self.messages_per_second else None,
            },
            "latency": {
                "p50_seconds": round(self.latency_p50, 3) if self.latency_p50 else None,
                "p95_seconds": round(self.latency_p95, 3) if self.latency_p95 else None,
                "p99_seconds": round(self.latency_p99, 3) if self.latency_p99 else None,
                "mean_seconds": round(self.latency_mean, 3) if self.latency_mean else None,
                "max_seconds": round(self.latency_max, 3) if self.latency_max else None,
                "sample_count": len(self.latencies),
            },
            "resources": {
                "peak_cpu_percent": round(self.peak_cpu_percent, 2) if self.peak_cpu_percent else None,
                "peak_memory_mb": round(self.peak_memory_mb, 2) if self.peak_memory_mb else None,
                "mean_cpu_percent": round(self.mean_cpu_percent, 2) if self.mean_cpu_percent else None,
                "mean_memory_mb": round(self.mean_memory_mb, 2) if self.mean_memory_mb else None,
                "snapshot_count": len(self.resource_snapshots),
            },
            "errors": {
                "error_count": self.errors,
                "error_rate": round(self.error_rate, 4) if self.error_rate else None,
            },
        }


class ResourceMonitor:
    """Monitor CPU and memory usage during test execution."""

    def __init__(self, sampling_interval: float = 1.0):
        """
        Initialize resource monitor.

        Args:
            sampling_interval: Seconds between resource snapshots (default: 1.0)
        """
        self.sampling_interval = sampling_interval
        self.snapshots: List[ResourceSnapshot] = []
        self._process = psutil.Process()
        self._monitor_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start monitoring resources."""
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def stop(self) -> None:
        """Stop monitoring resources."""
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

    async def _monitor_loop(self) -> None:
        """Background loop that samples resource usage."""
        while self._running:
            snapshot = ResourceSnapshot(
                timestamp=datetime.now(timezone.utc),
                cpu_percent=self._process.cpu_percent(interval=0.1),
                memory_mb=self._process.memory_info().rss / 1024 / 1024,
                memory_percent=self._process.memory_percent(),
            )
            self.snapshots.append(snapshot)
            await asyncio.sleep(self.sampling_interval)

    def get_snapshots(self) -> List[ResourceSnapshot]:
        """Get all collected resource snapshots."""
        return self.snapshots.copy()


@pytest.fixture
def resource_monitor() -> ResourceMonitor:
    """
    Provide resource monitor for tracking CPU and memory usage.

    Returns:
        ResourceMonitor: Monitor instance
    """
    return ResourceMonitor(sampling_interval=1.0)


@pytest.fixture
def performance_metrics(request) -> PerformanceMetrics:
    """
    Provide performance metrics collector for a test.

    Automatically initializes metrics with test name and start time.

    Args:
        request: Pytest request fixture

    Returns:
        PerformanceMetrics: Metrics collector
    """
    metrics = PerformanceMetrics(
        test_name=request.node.name,
        start_time=datetime.now(timezone.utc),
    )
    return metrics


@asynccontextmanager
async def monitor_performance(
    metrics: PerformanceMetrics,
    resource_monitor: ResourceMonitor,
):
    """
    Context manager for monitoring performance during a test section.

    Usage:
        async with monitor_performance(metrics, resource_monitor):
            # Code to measure
            ...

    Args:
        metrics: Performance metrics collector
        resource_monitor: Resource monitor instance

    Yields:
        None
    """
    # Start resource monitoring
    await resource_monitor.start()

    try:
        yield
    finally:
        # Stop resource monitoring
        await resource_monitor.stop()

        # Collect resource snapshots
        metrics.resource_snapshots = resource_monitor.get_snapshots()

        # Finalize metrics
        metrics.finalize()


@pytest.fixture
def performance_report_dir(tmp_path: Path) -> Path:
    """
    Provide directory for performance reports.

    Args:
        tmp_path: Pytest temp directory

    Returns:
        Path: Directory for performance reports
    """
    report_dir = tmp_path / "performance_reports"
    report_dir.mkdir(exist_ok=True)
    return report_dir


def save_performance_report(
    metrics: PerformanceMetrics,
    report_dir: Path,
    format: str = "json",
) -> Path:
    """
    Save performance metrics to a report file.

    Args:
        metrics: Performance metrics to save
        report_dir: Directory for reports
        format: Report format (default: "json")

    Returns:
        Path: Path to saved report file
    """
    import json

    # Create filename with timestamp
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{metrics.test_name}_{timestamp}.{format}"
    report_path = report_dir / filename

    # Save report
    if format == "json":
        with open(report_path, "w") as f:
            json.dump(metrics.to_dict(), f, indent=2)
    else:
        raise ValueError(f"Unsupported format: {format}")

    logger.info(f"Performance report saved: {report_path}")
    return report_path


@pytest.fixture
async def load_generator(test_kafka_config: KafkaConfig):
    """
    Provide utilities for generating high-volume test load.

    Returns:
        dict: Load generation utilities
    """
    from kafka_pipeline.xact.schemas.events import EventMessage
    from tests.kafka_pipeline.integration.fixtures.generators import create_event_message

    async def generate_events(
        count: int,
        prefix: str = "load-test",
        attachments_per_event: int = 1,
    ) -> List[EventMessage]:
        """
        Generate multiple test events.

        Args:
            count: Number of events to generate
            prefix: Prefix for trace IDs
            attachments_per_event: Number of attachments per event

        Returns:
            List[EventMessage]: Generated events
        """
        events = []
        for i in range(count):
            attachments = [
                f"https://example.com/file{j}.pdf"
                for j in range(attachments_per_event)
            ]
            event = create_event_message(
                trace_id=f"{prefix}-{i:06d}",
                event_type="claim",
                event_subtype="created",
                attachments=attachments,
                payload={
                    "claim_id": f"C-{i:06d}",
                    "assignment_id": f"A-{i:06d}",
                    "amount": 10000 + i,
                },
            )
            events.append(event)
        return events

    return {
        "generate_events": generate_events,
    }
