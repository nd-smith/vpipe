"""Worker registry for mapping CLI worker names to runner functions.

This registry defines all available workers and how to execute them.
Each worker entry specifies:
- runner: The async function to execute
"""

import asyncio
import inspect
from typing import Any

from pipeline.runners import claimx_runners, plugin_runners, verisk_runners

# Worker registry mapping worker names to their runner functions and config builders
WORKER_REGISTRY: dict[str, dict[str, Any]] = {
    # XACT workers
    "xact-event-ingester": {
        "runner": verisk_runners.run_event_ingester,
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
    # Plugin workers
    "claimx-mitigation-tracking": {
        "runner": plugin_runners.run_claimx_mitigation_tracking,
    },
    "itel-cabinet-tracking": {
        "runner": plugin_runners.run_itel_cabinet_tracking,
    },
    "itel-cabinet-api": {
        "runner": plugin_runners.run_itel_cabinet_api,
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

    Raises:
        ValueError: If worker not found in registry or requirements not met
    """
    if worker_name not in WORKER_REGISTRY:
        raise ValueError(f"Unknown worker: {worker_name}")

    worker_def = WORKER_REGISTRY[worker_name]

    # Build arguments - pass all common parameters directly
    # Note: Previously used args_builder pattern was removed in favor of direct parameter passing
    kwargs = {
        "pipeline_config": pipeline_config,
        "shutdown_event": shutdown_event,
        "enable_delta_writes": enable_delta_writes,
        "claimx_projects_table_path": pipeline_config.claimx_projects_table_path,
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
    elif worker_name == "xact-result-processor":
        kwargs["inventory_table_path"] = pipeline_config.inventory_table_path
        kwargs["failed_table_path"] = pipeline_config.failed_table_path
    elif worker_name == "claimx-delta-writer":
        kwargs["events_table_path"] = pipeline_config.claimx_events_table_path
    elif worker_name == "claimx-entity-writer":
        kwargs["projects_table_path"] = pipeline_config.claimx_projects_table_path
        kwargs["contacts_table_path"] = pipeline_config.claimx_contacts_table_path
        kwargs["media_table_path"] = pipeline_config.claimx_media_table_path
        kwargs["tasks_table_path"] = pipeline_config.claimx_tasks_table_path
        kwargs["task_templates_table_path"] = pipeline_config.claimx_task_templates_table_path
        kwargs["external_links_table_path"] = pipeline_config.claimx_external_links_table_path
        kwargs["video_collab_table_path"] = pipeline_config.claimx_video_collab_table_path

    # Run the worker, passing only kwargs that match the runner's signature
    runner = worker_def["runner"]
    sig = inspect.signature(runner)
    has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    if has_var_keyword:
        filtered_kwargs = kwargs
    else:
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
    await runner(**filtered_kwargs)
