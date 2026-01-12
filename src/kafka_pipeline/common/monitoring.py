"""
Monitoring web UI for Kafka pipeline.

Provides a simple dashboard showing:
- Topic stats (messages, lag, partitions)
- Worker status (connection, in-flight, errors)
- Recent activity stream

Usage:
    python -m kafka_pipeline.monitoring --port 8080
    python -m kafka_pipeline.monitoring --port 8080 --metrics-url http://localhost:9000/metrics
"""

import argparse
import asyncio
import logging
import re
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_PORT = 8080
DEFAULT_METRICS_URL = "http://localhost:8000/metrics"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"


@dataclass
class TopicStats:
    """Statistics for a single Kafka topic."""
    name: str
    partitions: int = 0
    total_lag: int = 0
    messages_consumed: int = 0
    messages_produced: int = 0


@dataclass
class WorkerStats:
    """Statistics for a worker type."""
    name: str
    connected: bool = False
    in_flight: int = 0
    errors: int = 0
    circuit_breaker: str = "closed"


@dataclass
class DashboardData:
    """Aggregated dashboard data."""
    topics: Dict[str, TopicStats] = field(default_factory=dict)
    workers: Dict[str, WorkerStats] = field(default_factory=dict)
    timestamp: str = ""
    metrics_error: Optional[str] = None
    kafka_error: Optional[str] = None


class MetricsParser:
    """Parse Prometheus metrics text format."""

    # Patterns for metric lines
    METRIC_PATTERN = re.compile(
        r'^(\w+)(?:\{([^}]*)\})?\s+([\d.eE+-]+|NaN|Inf|-Inf)$'
    )
    LABEL_PATTERN = re.compile(r'(\w+)="([^"]*)"')

    def parse(self, text: str) -> Dict[str, List[Dict[str, Any]]]:
        """Parse Prometheus metrics text into structured data.

        Returns:
            Dict mapping metric names to list of {labels: {}, value: float}
        """
        metrics: Dict[str, List[Dict[str, Any]]] = {}

        for line in text.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            match = self.METRIC_PATTERN.match(line)
            if not match:
                continue

            name = match.group(1)
            labels_str = match.group(2) or ""
            value_str = match.group(3)

            # Parse labels
            labels = {}
            for label_match in self.LABEL_PATTERN.finditer(labels_str):
                labels[label_match.group(1)] = label_match.group(2)

            # Parse value
            try:
                if value_str in ('NaN', 'Inf', '-Inf'):
                    value = float(value_str)
                else:
                    value = float(value_str)
            except ValueError:
                continue

            if name not in metrics:
                metrics[name] = []
            metrics[name].append({'labels': labels, 'value': value})

        return metrics


class MonitoringServer:
    """HTTP server for monitoring dashboard."""

    def __init__(
        self,
        port: int = DEFAULT_PORT,
        metrics_url: str = DEFAULT_METRICS_URL,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    ):
        self.port = port
        self.metrics_url = metrics_url
        self.bootstrap_servers = bootstrap_servers
        self.parser = MetricsParser()
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None

    async def fetch_metrics(self) -> Optional[str]:
        """Fetch raw metrics from Prometheus endpoint."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.metrics_url, timeout=5) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    logger.warning(f"Metrics endpoint returned {resp.status}")
                    return None
        except Exception as e:
            logger.warning(f"Failed to fetch metrics: {e}")
            return None

    async def get_dashboard_data(self) -> DashboardData:
        """Collect all data for dashboard display."""
        data = DashboardData(timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # Fetch and parse Prometheus metrics
        metrics_text = await self.fetch_metrics()
        if metrics_text is None:
            data.metrics_error = f"Could not connect to {self.metrics_url}"
        else:
            metrics = self.parser.parse(metrics_text)
            self._extract_topic_stats(metrics, data)
            self._extract_worker_stats(metrics, data)

        return data

    def _extract_topic_stats(
        self, metrics: Dict[str, List[Dict[str, Any]]], data: DashboardData
    ) -> None:
        """Extract topic statistics from parsed metrics."""
        # Known topics to track
        topic_names = [
            "xact.downloads.pending",
            "xact.downloads.cached",
            "xact.downloads.results",
            "xact.downloads.dlq",
            "xact.events.raw",
        ]

        # Initialize topics
        for name in topic_names:
            short_name = name.replace("xact.", "")
            data.topics[name] = TopicStats(name=short_name)

        # Consumer lag per topic
        for entry in metrics.get("kafka_consumer_lag", []):
            topic = entry['labels'].get('topic', '')
            if topic in data.topics:
                data.topics[topic].total_lag += int(entry['value'])

        # Partition count from lag metrics (count unique partitions)
        partition_counts: Dict[str, set] = {t: set() for t in topic_names}
        for entry in metrics.get("kafka_consumer_lag", []):
            topic = entry['labels'].get('topic', '')
            partition = entry['labels'].get('partition', '')
            if topic in partition_counts:
                partition_counts[topic].add(partition)

        for topic, partitions in partition_counts.items():
            if topic in data.topics:
                data.topics[topic].partitions = len(partitions)

        # Messages consumed (total counter)
        for entry in metrics.get("kafka_messages_consumed_total", []):
            topic = entry['labels'].get('topic', '')
            if topic in data.topics:
                data.topics[topic].messages_consumed += int(entry['value'])

        # Messages produced (total counter)
        for entry in metrics.get("kafka_messages_produced_total", []):
            topic = entry['labels'].get('topic', '')
            if topic in data.topics:
                data.topics[topic].messages_produced += int(entry['value'])

    def _extract_worker_stats(
        self, metrics: Dict[str, List[Dict[str, Any]]], data: DashboardData
    ) -> None:
        """Extract worker statistics from parsed metrics."""
        # Initialize workers
        worker_names = ["download_worker", "upload_worker", "result_processor", "event_ingester"]
        for name in worker_names:
            display_name = name.replace("_", " ").title()
            data.workers[name] = WorkerStats(name=display_name)

        # Connection status
        for entry in metrics.get("kafka_connection_status", []):
            component = entry['labels'].get('component', '')
            connected = entry['value'] == 1.0
            # Map component to worker
            if component == "consumer":
                for w in ["download_worker", "upload_worker", "result_processor", "event_ingester"]:
                    if w in data.workers:
                        data.workers[w].connected = connected
            elif component == "producer":
                for w in data.workers.values():
                    w.connected = w.connected or connected

        # Concurrent downloads
        for entry in metrics.get("kafka_downloads_concurrent", []):
            worker = entry['labels'].get('worker', '')
            if worker in data.workers:
                data.workers[worker].in_flight = int(entry['value'])

        # Processing errors
        for entry in metrics.get("kafka_processing_errors_total", []):
            # Sum all errors - metrics don't have worker label, use topic to infer
            topic = entry['labels'].get('topic', '')
            if 'pending' in topic or 'retry' in topic:
                data.workers.get("download_worker", WorkerStats("")).errors += int(entry['value'])
            elif 'cached' in topic:
                data.workers.get("upload_worker", WorkerStats("")).errors += int(entry['value'])
            elif 'results' in topic:
                data.workers.get("result_processor", WorkerStats("")).errors += int(entry['value'])

        # Circuit breaker state
        for entry in metrics.get("kafka_circuit_breaker_state", []):
            component = entry['labels'].get('component', '')
            state_val = int(entry['value'])
            state = {0: "closed", 1: "open", 2: "half-open"}.get(state_val, "unknown")
            if component in data.workers:
                data.workers[component].circuit_breaker = state

    def render_dashboard(self, data: DashboardData) -> str:
        """Render HTML dashboard."""
        # Build topic rows
        topic_rows = ""
        for topic in data.topics.values():
            lag_class = "text-success" if topic.total_lag == 0 else (
                "text-warning" if topic.total_lag < 100 else "text-danger"
            )
            topic_rows += f"""
                <tr>
                    <td><code>{topic.name}</code></td>
                    <td class="text-end">{topic.messages_consumed:,}</td>
                    <td class="text-end">{topic.messages_produced:,}</td>
                    <td class="text-end {lag_class}">{topic.total_lag:,}</td>
                    <td class="text-end">{topic.partitions}</td>
                </tr>
            """

        # Build worker rows
        worker_rows = ""
        for worker in data.workers.values():
            status_badge = (
                '<span class="badge bg-success">Connected</span>' if worker.connected
                else '<span class="badge bg-danger">Disconnected</span>'
            )
            cb_badge = {
                "closed": '<span class="badge bg-success">Closed</span>',
                "open": '<span class="badge bg-danger">Open</span>',
                "half-open": '<span class="badge bg-warning">Half-Open</span>',
            }.get(worker.circuit_breaker, '<span class="badge bg-secondary">Unknown</span>')

            error_class = "text-danger" if worker.errors > 0 else ""

            worker_rows += f"""
                <tr>
                    <td>{worker.name}</td>
                    <td>{status_badge}</td>
                    <td>{cb_badge}</td>
                    <td class="text-end">{worker.in_flight}</td>
                    <td class="text-end {error_class}">{worker.errors:,}</td>
                </tr>
            """

        # Error alerts
        alerts = ""
        if data.metrics_error:
            alerts += f"""
                <div class="alert alert-warning" role="alert">
                    <strong>Metrics unavailable:</strong> {data.metrics_error}
                </div>
            """
        if data.kafka_error:
            alerts += f"""
                <div class="alert alert-warning" role="alert">
                    <strong>Kafka unavailable:</strong> {data.kafka_error}
                </div>
            """

        return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kafka Pipeline Monitor</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body {{ background-color: #1a1a2e; color: #eee; }}
        .card {{ background-color: #16213e; border: 1px solid #0f3460; }}
        .card-header {{ background-color: #0f3460; border-bottom: 1px solid #0f3460; }}
        .table {{ color: #eee; }}
        .table-dark {{ --bs-table-bg: #16213e; }}
        code {{ color: #e94560; }}
        .navbar {{ background-color: #0f3460 !important; }}
    </style>
</head>
<body>
    <nav class="navbar navbar-dark mb-4">
        <div class="container-fluid">
            <span class="navbar-brand mb-0 h1">Kafka Pipeline Monitor</span>
            <div class="d-flex align-items-center">
                <span class="text-muted me-3">{data.timestamp}</span>
                <a href="/" class="btn btn-outline-light btn-sm">Refresh</a>
            </div>
        </div>
    </nav>

    <div class="container-fluid">
        {alerts}

        <div class="row">
            <div class="col-lg-7 mb-4">
                <div class="card">
                    <div class="card-header">
                        <h5 class="mb-0">Topic Stats</h5>
                    </div>
                    <div class="card-body p-0">
                        <table class="table table-dark table-striped table-hover mb-0">
                            <thead>
                                <tr>
                                    <th>Topic</th>
                                    <th class="text-end">Consumed</th>
                                    <th class="text-end">Produced</th>
                                    <th class="text-end">Lag</th>
                                    <th class="text-end">Partitions</th>
                                </tr>
                            </thead>
                            <tbody>
                                {topic_rows}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <div class="col-lg-5 mb-4">
                <div class="card">
                    <div class="card-header">
                        <h5 class="mb-0">Worker Status</h5>
                    </div>
                    <div class="card-body p-0">
                        <table class="table table-dark table-striped table-hover mb-0">
                            <thead>
                                <tr>
                                    <th>Worker</th>
                                    <th>Status</th>
                                    <th>Circuit</th>
                                    <th class="text-end">In-Flight</th>
                                    <th class="text-end">Errors</th>
                                </tr>
                            </thead>
                            <tbody>
                                {worker_rows}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5 class="mb-0">Configuration</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-6">
                                <p><strong>Metrics URL:</strong> <code>{self.metrics_url}</code></p>
                            </div>
                            <div class="col-md-6">
                                <p><strong>Kafka:</strong> <code>{self.bootstrap_servers}</code></p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
        """

    async def handle_index(self, request: web.Request) -> web.Response:
        """Handle GET / - render dashboard."""
        data = await self.get_dashboard_data()
        html = self.render_dashboard(data)
        return web.Response(text=html, content_type="text/html")

    async def handle_api_stats(self, request: web.Request) -> web.Response:
        """Handle GET /api/stats - return JSON stats."""
        data = await self.get_dashboard_data()
        return web.json_response({
            "timestamp": data.timestamp,
            "topics": {k: vars(v) for k, v in data.topics.items()},
            "workers": {k: vars(v) for k, v in data.workers.items()},
            "errors": {
                "metrics": data.metrics_error,
                "kafka": data.kafka_error,
            },
        })

    def create_app(self) -> web.Application:
        """Create aiohttp application."""
        app = web.Application()
        app.router.add_get("/", self.handle_index)
        app.router.add_get("/api/stats", self.handle_api_stats)
        return app

    async def start(self) -> None:
        """Start the monitoring server."""
        self._app = self.create_app()
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await site.start()
        logger.info(f"Monitoring server started on http://localhost:{self.port}")

    async def stop(self) -> None:
        """Stop the monitoring server."""
        if self._runner:
            await self._runner.cleanup()
            logger.info("Monitoring server stopped")


async def main() -> None:
    """Main entry point for standalone monitoring server."""
    parser = argparse.ArgumentParser(
        description="Kafka Pipeline Monitoring Dashboard"
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT,
        help=f"HTTP server port (default: {DEFAULT_PORT})"
    )
    parser.add_argument(
        "--metrics-url", type=str, default=DEFAULT_METRICS_URL,
        help=f"Prometheus metrics URL (default: {DEFAULT_METRICS_URL})"
    )
    parser.add_argument(
        "--bootstrap-servers", type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Enable debug logging"
    )
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    server = MonitoringServer(
        port=args.port,
        metrics_url=args.metrics_url,
        bootstrap_servers=args.bootstrap_servers,
    )

    print(f"Starting monitoring dashboard at http://localhost:{args.port}")
    print(f"Fetching metrics from {args.metrics_url}")
    print("Press Ctrl+C to stop\n")

    await server.start()

    # Keep running until interrupted
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested")
