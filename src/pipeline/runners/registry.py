"""Worker registry for mapping CLI worker names to runner functions.

This registry defines all available workers and how to execute them.
Each worker entry specifies:
- runner: The async function to execute
- requires: Optional list of requirements (e.g., "eventhouse")
"""

import asyncio
import inspect
from typing import Any

from pipeline.runners import claimx_runners, verisk_runners


async def _run_event_ingester_router(**kwargs):
    """Route to appropriate event ingester based on configuration.

    Uses local ingester if only local_kafka_config provided (dev mode),
    otherwise uses Event Hub ingester.
    """
    if "local_kafka_config" in kwargs and "eventhub_config" not in kwargs:
        return await verisk_runners.run_local_event_ingester(**kwargs)
    return await verisk_runners.run_event_ingester(**kwargs)


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
        "runner": _run_event_ingester_router,
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
    # Deprecated workers (captured for better error messages)
    "dummy-source": {
        "deprecated": True,
        "message": "Worker 'dummy-source' has been removed along with simulation mode support.",
    },
    "dummy_source": {
        "deprecated": True,
        "message": "Worker 'dummy_source' has been removed along with simulation mode support.",
    },
}


async def run_worker_from_registry(
    worker_name: str,
    pipeline_config,
    shutdown_event: asyncio.Event,
    enable_delta_writes: bool = True,
    eventhub_config=None,
    local_kafka_config=None,
    instance_id: int | None = None,
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

    Raises:
        ValueError: If worker not found in registry or requirements not met
    """
    if worker_name not in WORKER_REGISTRY:
        raise ValueError(f"Unknown worker: {worker_name}")

    worker_def = WORKER_REGISTRY[worker_name]

    # Check if deprecated
    if worker_def.get("deprecated"):
        raise ValueError(
            worker_def.get("message", f"Worker '{worker_name}' is deprecated")
        )

    # Check requirements
    if worker_def.get("requires_eventhouse"):
        from config.pipeline_config import EventSourceType

        if pipeline_config.event_source != EventSourceType.EVENTHOUSE:
            raise ValueError(f"{worker_name} requires EVENT_SOURCE=eventhouse")

    # Build arguments - pass all common parameters directly
    # Note: Previously used args_builder pattern was removed in favor of direct parameter passing
    kwargs = {
        "pipeline_config": pipeline_config,
        "shutdown_event": shutdown_event,
        "enable_delta_writes": enable_delta_writes,
        "eventhub_config": eventhub_config,
        "local_kafka_config": local_kafka_config,
        "kafka_config": local_kafka_config,
        "domain": pipeline_config.domain,
    }

    if instance_id is not None:
        kwargs["instance_id"] = instance_id

    # Add worker-specific table paths
    if worker_name == "xact-delta-writer":
        kwargs["events_table_path"] = pipeline_config.events_table_path
    elif worker_name == "claimx-delta-writer":
        # Use delta config path (CLAIMX_DELTA_EVENTS_TABLE env var)
        # Falls back to eventhouse poller path if delta path not configured
        kwargs["events_table_path"] = (
            pipeline_config.claimx_events_table_path
            or (
                pipeline_config.claimx_eventhouse.claimx_events_table_path
                if pipeline_config.claimx_eventhouse
                else ""
            )
        )
    elif worker_name == "claimx-entity-writer":
        kwargs["projects_table_path"] = pipeline_config.claimx_projects_table_path
        kwargs["contacts_table_path"] = pipeline_config.claimx_contacts_table_path
        kwargs["media_table_path"] = pipeline_config.claimx_media_table_path
        kwargs["tasks_table_path"] = pipeline_config.claimx_tasks_table_path
        kwargs["task_templates_table_path"] = (
            pipeline_config.claimx_task_templates_table_path
        )
        kwargs["external_links_table_path"] = (
            pipeline_config.claimx_external_links_table_path
        )
        kwargs["video_collab_table_path"] = pipeline_config.claimx_video_collab_table_path

    # Run the worker, passing only kwargs that match the runner's signature
    runner = worker_def["runner"]
    sig = inspect.signature(runner)
    filtered_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
    await runner(**filtered_kwargs)
