"""
Monitoring aggregator service for external health checks.

This service queries Prometheus to check the health of all pipeline workers
and exposes HTTP endpoints that external monitoring systems (like Iris) can call.

Endpoints:
    GET /health - Overall system health (200 if all workers up, 503 otherwise)
    GET /health/workers - Detailed health status for each worker
    GET /health/prometheus - Check if Prometheus itself is reachable
    GET /status - Worker status with consumer lag information

Usage:
    python -m kafka_pipeline.common.monitoring_service --prometheus-url http://localhost:9090
"""

import argparse
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class PrometheusClient:
    """Client for querying Prometheus metrics."""

    def __init__(self, prometheus_url: str):
        """
        Initialize Prometheus client.

        Args:
            prometheus_url: Base URL of Prometheus (e.g., http://localhost:9090)
        """
        self.prometheus_url = prometheus_url.rstrip("/")
        self.query_url = f"{self.prometheus_url}/api/v1/query"

    async def query(self, query: str) -> Dict[str, Any]:
        """
        Execute a PromQL query.

        Args:
            query: PromQL query string

        Returns:
            Query result as dict

        Raises:
            aiohttp.ClientError: If request fails
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(self.query_url, params={"query": query}) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if data.get("status") != "success":
                    raise Exception(f"Prometheus query failed: {data}")
                return data["data"]

    async def get_worker_status(self) -> List[Dict[str, Any]]:
        """
        Get health status of all workers from Prometheus.

        Returns:
            List of worker status dicts with keys: job, instance, status, domain, worker_type
        """
        # Query the 'up' metric which indicates if Prometheus can scrape the target
        result = await self.query('up{job=~".*(worker|ingester).*"}')

        workers = []
        for metric in result.get("result", []):
            labels = metric["metric"]
            value = int(metric["value"][1])  # [timestamp, value]

            workers.append(
                {
                    "job": labels.get("job", "unknown"),
                    "instance": labels.get("instance", "unknown"),
                    "domain": labels.get("domain", "unknown"),
                    "worker_type": labels.get("worker_type", "unknown"),
                    "status": "up" if value == 1 else "down",
                }
            )

        return workers

    async def get_consumer_lag(self) -> List[Dict[str, Any]]:
        """
        Get consumer lag for all workers from Prometheus.

        Returns:
            List of consumer lag dicts with keys: consumer_group, topic, partition, lag
        """
        # Query the kafka_consumer_lag metric
        result = await self.query("kafka_consumer_lag")

        lags = []
        for metric in result.get("result", []):
            labels = metric["metric"]
            lag_value = float(metric["value"][1])  # [timestamp, value]

            lags.append(
                {
                    "consumer_group": labels.get("consumer_group", "unknown"),
                    "topic": labels.get("topic", "unknown"),
                    "partition": int(labels.get("partition", 0)),
                    "lag": int(lag_value),
                }
            )

        return lags

    async def check_prometheus_health(self) -> bool:
        """
        Check if Prometheus is reachable and healthy.

        Returns:
            True if healthy, False otherwise
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.prometheus_url}/-/healthy") as resp:
                    return resp.status == 200
        except Exception as e:
            logger.error(f"Failed to check Prometheus health: {e}")
            return False


class MonitoringService:
    """HTTP service for external health check monitoring."""

    def __init__(self, prometheus_client: PrometheusClient, port: int = 9091):
        """
        Initialize monitoring service.

        Args:
            prometheus_client: Client for querying Prometheus
            port: HTTP port to listen on
        """
        self.prometheus = prometheus_client
        self.port = port
        self.app = self._create_app()

    def _create_app(self) -> web.Application:
        """Create aiohttp application with health endpoints."""
        app = web.Application()
        app.router.add_get("/health", self.handle_health)
        app.router.add_get("/health/workers", self.handle_workers_health)
        app.router.add_get("/health/prometheus", self.handle_prometheus_health)
        app.router.add_get("/status", self.handle_status)
        return app

    async def handle_health(self, request: web.Request) -> web.Response:
        """
        Handle GET /health - Overall system health.

        Returns:
            200 OK if all workers are up
            503 Service Unavailable if any worker is down
        """
        try:
            workers = await self.prometheus.get_worker_status()

            if not workers:
                return web.json_response(
                    {
                        "status": "unknown",
                        "message": "No workers found in Prometheus",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                    status=503,
                )

            down_workers = [w for w in workers if w["status"] == "down"]
            all_up = len(down_workers) == 0

            response_data = {
                "status": "healthy" if all_up else "unhealthy",
                "workers_total": len(workers),
                "workers_up": len(workers) - len(down_workers),
                "workers_down": len(down_workers),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            if down_workers:
                response_data["down_workers"] = [
                    {"job": w["job"], "instance": w["instance"]} for w in down_workers
                ]

            return web.json_response(
                response_data,
                status=200 if all_up else 503,
            )

        except Exception as e:
            logger.error(f"Error checking system health: {e}", exc_info=True)
            return web.json_response(
                {
                    "status": "error",
                    "message": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                status=503,
            )

    async def handle_workers_health(self, request: web.Request) -> web.Response:
        """
        Handle GET /health/workers - Detailed worker health status.

        Returns:
            200 OK with detailed status for each worker
        """
        try:
            workers = await self.prometheus.get_worker_status()

            return web.json_response(
                {
                    "workers": workers,
                    "total": len(workers),
                    "up": len([w for w in workers if w["status"] == "up"]),
                    "down": len([w for w in workers if w["status"] == "down"]),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                status=200,
            )

        except Exception as e:
            logger.error(f"Error getting worker health: {e}", exc_info=True)
            return web.json_response(
                {
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                status=503,
            )

    async def handle_prometheus_health(self, request: web.Request) -> web.Response:
        """
        Handle GET /health/prometheus - Check Prometheus health.

        Returns:
            200 OK if Prometheus is healthy
            503 Service Unavailable if Prometheus is down
        """
        try:
            is_healthy = await self.prometheus.check_prometheus_health()

            return web.json_response(
                {
                    "status": "healthy" if is_healthy else "unhealthy",
                    "prometheus_url": self.prometheus.prometheus_url,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                status=200 if is_healthy else 503,
            )

        except Exception as e:
            logger.error(f"Error checking Prometheus health: {e}", exc_info=True)
            return web.json_response(
                {
                    "status": "error",
                    "message": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                status=503,
            )

    async def handle_status(self, request: web.Request) -> web.Response:
        """
        Handle GET /status - Worker status with consumer lag.

        Returns:
            200 OK with worker health and consumer lag information
        """
        try:
            # Get worker health and consumer lag in parallel
            workers_task = self.prometheus.get_worker_status()
            lag_task = self.prometheus.get_consumer_lag()

            workers, lag_metrics = await asyncio.gather(workers_task, lag_task)

            # Aggregate lag by consumer group
            lag_by_consumer_group = {}
            for lag in lag_metrics:
                consumer_group = lag["consumer_group"]
                if consumer_group not in lag_by_consumer_group:
                    lag_by_consumer_group[consumer_group] = {
                        "consumer_group": consumer_group,
                        "total_lag": 0,
                        "topics": {},
                    }

                topic = lag["topic"]
                partition = lag["partition"]
                lag_value = lag["lag"]

                # Add to total lag
                lag_by_consumer_group[consumer_group]["total_lag"] += lag_value

                # Track per-topic lag
                if topic not in lag_by_consumer_group[consumer_group]["topics"]:
                    lag_by_consumer_group[consumer_group]["topics"][topic] = {
                        "topic": topic,
                        "total_lag": 0,
                        "partitions": [],
                    }

                lag_by_consumer_group[consumer_group]["topics"][topic]["total_lag"] += lag_value
                lag_by_consumer_group[consumer_group]["topics"][topic]["partitions"].append(
                    {"partition": partition, "lag": lag_value}
                )

            # Convert topics dict to list for easier JSON serialization
            for cg_data in lag_by_consumer_group.values():
                cg_data["topics"] = list(cg_data["topics"].values())

            # Build worker status with lag information
            worker_status = []
            for worker in workers:
                worker_info = {
                    "job": worker["job"],
                    "instance": worker["instance"],
                    "domain": worker["domain"],
                    "worker_type": worker["worker_type"],
                    "status": worker["status"],
                    "consumer_lag": None,
                }

                # Try to match worker to consumer group
                # Consumer groups typically follow pattern: domain-worker_type
                potential_group_name = f"{worker['domain']}-{worker['worker_type']}"
                if potential_group_name in lag_by_consumer_group:
                    worker_info["consumer_lag"] = lag_by_consumer_group[potential_group_name]

                worker_status.append(worker_info)

            return web.json_response(
                {
                    "workers": worker_status,
                    "total_workers": len(workers),
                    "workers_up": len([w for w in workers if w["status"] == "up"]),
                    "workers_down": len([w for w in workers if w["status"] == "down"]),
                    "consumer_groups": list(lag_by_consumer_group.values()),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                status=200,
            )

        except Exception as e:
            logger.error(f"Error getting status: {e}", exc_info=True)
            return web.json_response(
                {
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                status=503,
            )

    async def start(self) -> None:
        """Start the monitoring service."""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.port)
        await site.start()

        logger.info(
            f"Monitoring service started on port {self.port}",
            extra={
                "port": self.port,
                "endpoints": [
                    f"http://localhost:{self.port}/health",
                    f"http://localhost:{self.port}/health/workers",
                    f"http://localhost:{self.port}/health/prometheus",
                    f"http://localhost:{self.port}/status",
                ],
            },
        )


async def main():
    """Run the monitoring service."""
    parser = argparse.ArgumentParser(
        description="Monitoring aggregator service for Kafka pipeline"
    )
    parser.add_argument(
        "--prometheus-url",
        default="http://localhost:9090",
        help="Prometheus base URL (default: http://localhost:9090)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9091,
        help="HTTP port to listen on (default: 9091)",
    )

    args = parser.parse_args()

    # Create Prometheus client
    prometheus = PrometheusClient(args.prometheus_url)

    # Check Prometheus connectivity on startup
    logger.info(f"Connecting to Prometheus at {args.prometheus_url}")
    if not await prometheus.check_prometheus_health():
        logger.error(
            f"Cannot connect to Prometheus at {args.prometheus_url}. "
            "Make sure Prometheus is running."
        )
        return

    # Start monitoring service
    service = MonitoringService(prometheus, port=args.port)
    await service.start()

    # Run forever
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Shutting down monitoring service")


if __name__ == "__main__":
    asyncio.run(main())
