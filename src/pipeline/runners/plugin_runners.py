"""Plugin worker runners.

Runner functions for plugin workers that integrate with the standard
execute_worker_with_shutdown() lifecycle (startup retry, health server, shutdown).
"""

import asyncio
import logging

from pipeline.common.health import HealthCheckServer
from pipeline.runners.common import execute_worker_with_shutdown

logger = logging.getLogger(__name__)


async def run_itel_cabinet_tracking(shutdown_event: asyncio.Event, **kwargs):
    """Run the iTel Cabinet tracking worker with standard lifecycle management."""
    from pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker import (
        build_tracking_worker,
        load_worker_config,
    )

    worker_config = load_worker_config()

    processing_config = worker_config.get("processing", {})
    health_port = processing_config.get("health_port", 8096)
    health_enabled = processing_config.get("health_enabled", True)

    health_server = HealthCheckServer(
        port=health_port,
        worker_name="itel-cabinet-tracking",
        enabled=health_enabled,
    )
    await health_server.start()

    worker, connection_manager, producer = await build_tracking_worker(
        worker_config=worker_config,
        health_server=health_server,
    )
    await connection_manager.start()

    try:
        await execute_worker_with_shutdown(worker, "itel-cabinet-tracking", shutdown_event)
    finally:
        await connection_manager.close()
        await producer.stop()
        await health_server.stop()


async def run_claimx_mitigation_tracking(shutdown_event: asyncio.Event, **kwargs):
    """Run the ClaimX Mitigation Task tracking worker with standard lifecycle management."""
    from pipeline.plugins.claimx_mitigation_task.mitigation_tracking_worker import (
        build_tracking_worker,
        load_worker_config,
    )

    worker_config = load_worker_config()

    processing_config = worker_config.get("processing", {})
    health_port = processing_config.get("health_port", 8098)
    health_enabled = processing_config.get("health_enabled", True)

    health_server = HealthCheckServer(
        port=health_port,
        worker_name="claimx-mitigation-tracking",
        enabled=health_enabled,
    )
    await health_server.start()

    worker, connection_manager, producer = await build_tracking_worker(
        worker_config=worker_config,
        health_server=health_server,
    )
    await connection_manager.start()

    try:
        await execute_worker_with_shutdown(worker, "claimx-mitigation-tracking", shutdown_event)
    finally:
        await connection_manager.close()
        await producer.stop()
        await health_server.stop()


async def run_itel_cabinet_api(shutdown_event: asyncio.Event, **kwargs):
    """Run the iTel Cabinet API worker with standard lifecycle management."""
    from pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker import (
        build_api_worker,
    )

    worker, connection_manager = await build_api_worker()
    await connection_manager.start()

    try:
        await execute_worker_with_shutdown(worker, "itel-cabinet-api", shutdown_event)
    finally:
        await connection_manager.close()


async def run_eventhub_ui(shutdown_event: asyncio.Event, **kwargs):
    """Run the EventHub UI dashboard as a worker.

    Starts the FastAPI app via uvicorn and shuts down when the shutdown event fires.
    Default port: 8550.
    """
    import uvicorn

    config = uvicorn.Config(
        "pipeline.tools.eventhub_ui.app:app",
        host="0.0.0.0",
        port=8550,
        log_level="info",
    )
    server = uvicorn.Server(config)

    # Override uvicorn's default signal handlers — the pipeline manages shutdown
    server.install_signal_handlers = lambda: None

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info("Shutdown signal received, stopping eventhub-ui...")
        server.should_exit = True

    watcher = asyncio.create_task(shutdown_watcher())

    try:
        await server.serve()
    finally:
        watcher.cancel()
        try:
            await watcher
        except asyncio.CancelledError:
            pass
