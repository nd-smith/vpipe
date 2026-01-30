"""Common worker execution patterns and utilities.

Provides reusable templates for running workers with consistent:
- Shutdown handling
- Logging context
- Error handling
- Resource cleanup
"""

import asyncio
import logging
import os
from typing import Any, Callable, Optional

from core.logging.context import set_log_context

logger = logging.getLogger(__name__)

# Startup retry configuration (overridable via env vars)
DEFAULT_STARTUP_RETRIES = 5
DEFAULT_STARTUP_BACKOFF_BASE = 5  # seconds


async def _start_with_retry(
    start_fn: Callable,
    label: str,
    max_retries: Optional[int] = None,
    backoff_base: Optional[int] = None,
) -> None:
    """Retry an async start function with exponential backoff.

    On exhaustion, re-raises the last exception so the caller's fatal error
    handler can log it and enter health-server error mode.

    Args:
        start_fn: Async callable (e.g. worker.start, producer.start)
        label: Human-readable label for log messages
        max_retries: Number of retry attempts (default: 5, env: STARTUP_MAX_RETRIES)
        backoff_base: Base seconds for backoff (default: 5, env: STARTUP_BACKOFF_SECONDS)
    """
    max_retries = max_retries or int(
        os.getenv("STARTUP_MAX_RETRIES", str(DEFAULT_STARTUP_RETRIES))
    )
    backoff_base = backoff_base or int(
        os.getenv("STARTUP_BACKOFF_SECONDS", str(DEFAULT_STARTUP_BACKOFF_BASE))
    )

    for attempt in range(1, max_retries + 1):
        try:
            await start_fn()
            return
        except Exception as e:
            if attempt == max_retries:
                logger.error(
                    f"Failed to start {label} after {max_retries} attempts, giving up",
                    extra={"error": str(e), "attempts": max_retries},
                )
                raise
            delay = backoff_base * attempt
            logger.warning(
                f"Failed to start {label} (attempt {attempt}/{max_retries}), "
                f"retrying in {delay}s",
                extra={"error": str(e), "attempt": attempt, "delay": delay},
            )
            await asyncio.sleep(delay)


async def execute_worker_with_shutdown(
    worker_instance,
    stage_name: str,
    shutdown_event: asyncio.Event,
    stop_method: str = "stop",
    instance_id: Optional[int] = None,
) -> None:
    """Execute a worker with standard shutdown handling.

    Args:
        worker_instance: Worker instance with start() and stop() methods
        stage_name: Name for logging context
        shutdown_event: Event to signal graceful shutdown
        stop_method: Name of the stop method on worker (default: "stop")
        instance_id: Instance identifier for multi-instance deployments (optional)
    """
    # Set log context with instance_id if provided
    context = {"stage": stage_name}
    if instance_id is not None:
        context["instance_id"] = instance_id
        context["worker_id"] = f"{stage_name}-{instance_id}"
        logger_suffix = f" (instance {instance_id})"
    else:
        logger_suffix = ""

    set_log_context(**context)
    logger.info(f"Starting {stage_name}{logger_suffix}...")

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info(f"Shutdown signal received, stopping {stage_name}{logger_suffix}...")
        stop_fn = getattr(worker_instance, stop_method)
        await stop_fn()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await _start_with_retry(worker_instance.start, stage_name)
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        stop_fn = getattr(worker_instance, stop_method)
        await stop_fn()


async def execute_worker_with_producer(
    worker_class,
    producer_class,
    kafka_config,
    domain: str,
    stage_name: str,
    shutdown_event: asyncio.Event,
    worker_kwargs: Optional[dict] = None,
    producer_worker_name: Optional[str] = None,
    instance_id: Optional[int] = None,
) -> None:
    """Execute a worker that requires a producer with shutdown handling.

    Args:
        worker_class: Worker class to instantiate
        producer_class: Producer class to instantiate
        kafka_config: Kafka configuration
        domain: Domain name (xact/claimx)
        stage_name: Name for logging context
        shutdown_event: Event to signal graceful shutdown
        worker_kwargs: Additional kwargs for worker instantiation
        producer_worker_name: Name for producer (defaults to stage_name)
        instance_id: Instance identifier for multi-instance deployments (optional)
    """
    # Set log context with instance_id if provided
    context = {"stage": stage_name}
    if instance_id is not None:
        context["instance_id"] = instance_id
        context["worker_id"] = f"{stage_name}-{instance_id}"
        logger_suffix = f" (instance {instance_id})"
    else:
        logger_suffix = ""

    set_log_context(**context)
    logger.info(f"Starting {stage_name}{logger_suffix}...")

    worker_kwargs = worker_kwargs or {}
    producer_worker_name = producer_worker_name or stage_name.replace("-", "_")

    # Add instance_id to producer and worker if provided
    if instance_id is not None:
        producer_worker_name = f"{producer_worker_name}_{instance_id}"
        worker_kwargs["instance_id"] = instance_id

    producer = producer_class(
        config=kafka_config,
        domain=domain,
        worker_name=producer_worker_name,
    )
    await _start_with_retry(producer.start, f"{stage_name}-producer")

    worker = worker_class(
        config=kafka_config,
        producer=producer,
        domain=domain,
        **worker_kwargs,
    )

    async def shutdown_watcher():
        await shutdown_event.wait()
        logger.info(f"Shutdown signal received, stopping {stage_name}{logger_suffix}...")
        await worker.stop()

    watcher_task = asyncio.create_task(shutdown_watcher())

    try:
        await _start_with_retry(worker.start, stage_name)
    finally:
        try:
            watcher_task.cancel()
            await watcher_task
        except (asyncio.CancelledError, RuntimeError):
            pass
        await worker.stop()
        await producer.stop()


async def execute_poller_with_shutdown(
    poller_class,
    poller_config,
    stage_name: str,
    shutdown_event: asyncio.Event,
) -> None:
    """Execute an Eventhouse poller with shutdown handling.

    Args:
        poller_class: Poller class to instantiate
        poller_config: Poller configuration object
        stage_name: Name for logging context
        shutdown_event: Event to signal graceful shutdown
    """
    set_log_context(stage=stage_name)
    logger.info(f"Starting {stage_name}...")

    async def shutdown_watcher(poller):
        """Wait for shutdown signal and stop poller gracefully."""
        await shutdown_event.wait()
        logger.info(f"Shutdown signal received, stopping {stage_name}...")
        await poller.stop()

    async with poller_class(poller_config) as poller:
        # Start shutdown watcher alongside poller
        watcher_task = asyncio.create_task(shutdown_watcher(poller))

        try:
            await _start_with_retry(poller.run, stage_name)
        finally:
            # Guard against event loop being closed during shutdown
            try:
                watcher_task.cancel()
                await watcher_task
            except (asyncio.CancelledError, RuntimeError):
                # RuntimeError occurs if event loop is closed
                pass
