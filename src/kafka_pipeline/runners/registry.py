"""Worker registry for mapping CLI worker names to runner functions.

This registry defines all available workers and how to execute them.
Each worker entry specifies:
- runner: The async function to execute
- args_builder: Function to build arguments from pipeline config
- requires: Optional list of requirements (e.g., "eventhouse")
"""

import asyncio
import logging
import os
from typing import Any

from kafka_pipeline.runners import claimx_runners, verisk_runners

logger = logging.getLogger(__name__)


# Worker registry mapping worker names to their runner functions and config builders
WORKER_REGISTRY: dict[str, dict[str, Any]] = {
    # XACT workers
    "xact-poller": {
        "runner": verisk_runners.run_eventhouse_poller,
        "requires_eventhouse": True,
    },
    "xact-json-poller": {
        "runner": verisk_runners.run_eventhouse_json_poller,
        "requires_eventhouse": True,
    },
    "xact-event-ingester": {
        "runner": lambda **kwargs: (
            verisk_runners.run_local_event_ingester(**kwargs)
            if "local_kafka_config" in kwargs and "eventhub_config" not in kwargs
            else verisk_runners.run_event_ingester(**kwargs)
        ),
    },
    "xact-delta-writer": {
        "runner": verisk_runners.run_delta_events_worker,
    },
    "xact-retry-scheduler": {
        "runner": verisk_runners.run_xact_retry_scheduler,
    },
    "xact-enricher": {
        "runner": verisk_runners.run_xact_enrichment_worker,
    },
    "xact-download": {
        "runner": verisk_runners.run_download_worker,
    },
    "xact-upload": {
        "runner": verisk_runners.run_upload_worker,
    },
    "xact-result-processor": {
        "runner": verisk_runners.run_result_processor,
    },
    # ClaimX workers
    "claimx-poller": {
        "runner": claimx_runners.run_claimx_eventhouse_poller,
    },
    "claimx-ingester": {
        "runner": claimx_runners.run_claimx_event_ingester,
    },
    "claimx-enricher": {
        "runner": claimx_runners.run_claimx_enrichment_worker,
    },
    "claimx-downloader": {
        "runner": claimx_runners.run_claimx_download_worker,
    },
    "claimx-uploader": {
        "runner": claimx_runners.run_claimx_upload_worker,
    },
    "claimx-result-processor": {
        "runner": claimx_runners.run_claimx_result_processor,
    },
    "claimx-delta-writer": {
        "runner": claimx_runners.run_claimx_delta_events_worker,
    },
    "claimx-retry-scheduler": {
        "runner": claimx_runners.run_claimx_retry_scheduler,
    },
    "claimx-entity-writer": {
        "runner": claimx_runners.run_claimx_entity_delta_worker,
    },
    # NOTE: Dummy data producer has been moved to simulation module.
    # It is now a simulation-only tool and cannot be run in production.
    # Use: SIMULATION_MODE=true python -m kafka_pipeline.simulation.dummy_producer
    # Or: ./scripts/generate_test_data.sh
    # See: kafka_pipeline/simulation/README.md
}


async def run_worker_from_registry(
    worker_name: str,
    pipeline_config,
    shutdown_event: asyncio.Event,
    enable_delta_writes: bool = True,
    eventhub_config=None,
    local_kafka_config=None,
    instance_id: int | None = None,
    simulation_mode: bool = False,
):
    """Run a worker by looking it up in the registry.

    Args:
        worker_name: Name of the worker to run
        pipeline_config: Pipeline configuration
        shutdown_event: Shutdown event for graceful shutdown
        enable_delta_writes: Whether to enable Delta writes
        eventhub_config: Event Hub configuration (optional)
        local_kafka_config: Local Kafka configuration (optional)
        instance_id: Instance identifier for multi-instance deployments (optional)
        simulation_mode: Enable simulation mode with mock dependencies (optional)

    Raises:
        ValueError: If worker not found in registry or requirements not met
    """
    if worker_name not in WORKER_REGISTRY:
        raise ValueError(f"Unknown worker: {worker_name}")

    worker_def = WORKER_REGISTRY[worker_name]

    # Check requirements
    if worker_def.get("requires_eventhouse"):
        from config.pipeline_config import EventSourceType

        if pipeline_config.event_source != EventSourceType.EVENTHOUSE:
            raise ValueError(f"{worker_name} requires EVENT_SOURCE=eventhouse")

    # Build arguments - pass all common parameters directly
    # (args_builder pattern removed - functions above are now unused dead code)
    kwargs = {
        "pipeline_config": pipeline_config,
        "shutdown_event": shutdown_event,
        "enable_delta_writes": enable_delta_writes,
        "eventhub_config": eventhub_config,
        "local_kafka_config": local_kafka_config,
        "kafka_config": local_kafka_config,
        "simulation_mode": simulation_mode,
        "domain": pipeline_config.domain,
    }

    if instance_id is not None:
        kwargs["instance_id"] = instance_id

    # Run the worker
    runner = worker_def["runner"]
    await runner(**kwargs)
