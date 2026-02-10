"""Plugin worker runners.

Runner functions for plugin workers that integrate with the standard
execute_worker_with_shutdown() lifecycle (startup retry, health server, shutdown).
"""

import asyncio
import logging

from pipeline.runners.common import execute_worker_with_shutdown

logger = logging.getLogger(__name__)


async def run_itel_cabinet_tracking(shutdown_event: asyncio.Event, **kwargs):
    """Run the iTel Cabinet tracking worker with standard lifecycle management."""
    from pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker import (
        build_tracking_worker,
    )

    worker, connection_manager, producer = await build_tracking_worker()
    await connection_manager.start()

    try:
        await execute_worker_with_shutdown(worker, "itel-cabinet-tracking", shutdown_event)
    finally:
        await connection_manager.close()
        await producer.stop()


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
