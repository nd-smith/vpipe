# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

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
from typing import Any, Callable, Dict, Optional

from kafka_pipeline.runners import claimx_runners, xact_runners

logger = logging.getLogger(__name__)


def build_xact_poller_args(pipeline_config, shutdown_event: asyncio.Event, **kwargs):
    """Build arguments for xact-poller worker."""
    return {
        "pipeline_config": pipeline_config,
        "shutdown_event": shutdown_event,
    }


def build_xact_json_poller_args(pipeline_config, shutdown_event: asyncio.Event, **kwargs):
    """Build arguments for xact-json-poller worker.

    Environment variables:
        JSON_OUTPUT_PATH: Output file path (default: output/xact_events.jsonl)
        JSON_ROTATE_SIZE_MB: File rotation size in MB (default: 100)
        JSON_PRETTY_PRINT: Format with indentation (default: false)
        JSON_INCLUDE_METADATA: Include _key, _timestamp, _headers (default: true)
    """
    return {
        "pipeline_config": pipeline_config,
        "shutdown_event": shutdown_event,
        "output_path": os.getenv("JSON_OUTPUT_PATH", "output/xact_events.jsonl"),
        "rotate_size_mb": float(os.getenv("JSON_ROTATE_SIZE_MB", "100")),
        "pretty_print": os.getenv("JSON_PRETTY_PRINT", "false").lower() == "true",
        "include_metadata": os.getenv("JSON_INCLUDE_METADATA", "true").lower() == "true",
    }


def build_xact_event_ingester_args(
    pipeline_config,
    shutdown_event: asyncio.Event,
    eventhub_config=None,
    local_kafka_config=None,
    **kwargs,
):
    """Build arguments for xact-event-ingester worker."""
    from config.pipeline_config import EventSourceType

    if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
        return {
            "local_kafka_config": local_kafka_config,
            "shutdown_event": shutdown_event,
            "domain": pipeline_config.domain,
        }
    else:
        return {
            "eventhub_config": eventhub_config,
            "local_kafka_config": local_kafka_config,
            "shutdown_event": shutdown_event,
            "domain": pipeline_config.domain,
        }


def build_xact_delta_writer_args(
    pipeline_config,
    shutdown_event: asyncio.Event,
    local_kafka_config=None,
    **kwargs,
):
    """Build arguments for xact-delta-writer worker.

    Environment variables (checked in order):
        XACT_EVENTS_TABLE_PATH: Primary env var (matches ClaimX pattern)
        XACT_DELTA_EVENTS_TABLE: Alternative env var name
    """
    from config.pipeline_config import EventSourceType

    # Check environment variables first (domain-specific like ClaimX)
    events_table_path = os.getenv("XACT_EVENTS_TABLE_PATH", "")
    if not events_table_path:
        events_table_path = os.getenv("XACT_DELTA_EVENTS_TABLE", "")
    if not events_table_path:
        events_table_path = pipeline_config.events_table_path
        if pipeline_config.event_source == EventSourceType.EVENTHOUSE:
            events_table_path = (
                pipeline_config.xact_eventhouse.xact_events_table_path or events_table_path
            )

    if not events_table_path:
        raise ValueError(
            "XACT_EVENTS_TABLE_PATH or XACT_DELTA_EVENTS_TABLE is required for xact-delta-writer"
        )

    return {
        "kafka_config": local_kafka_config,
        "events_table_path": events_table_path,
        "shutdown_event": shutdown_event,
    }


def build_xact_result_processor_args(
    pipeline_config,
    shutdown_event: asyncio.Event,
    enable_delta_writes: bool,
    local_kafka_config=None,
    **kwargs,
):
    """Build arguments for xact-result-processor worker."""
    return {
        "kafka_config": local_kafka_config,
        "shutdown_event": shutdown_event,
        "enable_delta_writes": enable_delta_writes,
        "inventory_table_path": pipeline_config.inventory_table_path,
        "failed_table_path": pipeline_config.failed_table_path,
    }


def build_claimx_delta_writer_args(
    pipeline_config,
    shutdown_event: asyncio.Event,
    local_kafka_config=None,
    **kwargs,
):
    """Build arguments for claimx-delta-writer worker.

    Environment variables (checked in order):
        CLAIMX_EVENTS_TABLE_PATH: Primary env var
        CLAIMX_DELTA_EVENTS_TABLE: Alternative env var name
    """
    claimx_events_table_path = os.getenv("CLAIMX_EVENTS_TABLE_PATH", "")
    if not claimx_events_table_path:
        claimx_events_table_path = os.getenv("CLAIMX_DELTA_EVENTS_TABLE", "")
    if not claimx_events_table_path and pipeline_config.claimx_eventhouse:
        claimx_events_table_path = pipeline_config.claimx_eventhouse.claimx_events_table_path

    if not claimx_events_table_path:
        raise ValueError(
            "CLAIMX_EVENTS_TABLE_PATH or CLAIMX_DELTA_EVENTS_TABLE is required for claimx-delta-writer"
        )

    return {
        "kafka_config": local_kafka_config,
        "events_table_path": claimx_events_table_path,
        "shutdown_event": shutdown_event,
    }


def build_claimx_enricher_args(
    pipeline_config,
    shutdown_event: asyncio.Event,
    local_kafka_config=None,
    simulation_mode: bool = False,
    **kwargs,
):
    """Build arguments for claimx-enricher worker."""
    return {
        "kafka_config": local_kafka_config,
        "pipeline_config": pipeline_config,
        "shutdown_event": shutdown_event,
        "simulation_mode": simulation_mode,
    }


def build_claimx_result_processor_args(
    pipeline_config,
    shutdown_event: asyncio.Event,
    local_kafka_config=None,
    **kwargs,
):
    """Build arguments for claimx-result-processor worker."""
    return {
        "kafka_config": local_kafka_config,
        "pipeline_config": pipeline_config,
        "shutdown_event": shutdown_event,
    }


def build_claimx_entity_writer_args(
    pipeline_config,
    shutdown_event: asyncio.Event,
    local_kafka_config=None,
    **kwargs,
):
    """Build arguments for claimx-entity-writer worker.

    Environment variables (checked in order for each table):
        CLAIMX_{TABLE}_TABLE_PATH: Primary env var (e.g., CLAIMX_PROJECTS_TABLE_PATH)
        CLAIMX_DELTA_{TABLE}_TABLE: Alternative env var (e.g., CLAIMX_DELTA_PROJECTS_TABLE)

    Reads env vars directly and passes explicit paths to the runner (same pattern as
    claimx-delta-writer) to avoid issues with pipeline_config loading order.
    """
    # Read each table path from env vars (primary, then alternative)
    def get_table_path(primary_env: str, alt_env: str) -> str:
        return os.getenv(primary_env, "") or os.getenv(alt_env, "")

    projects_table_path = get_table_path("CLAIMX_PROJECTS_TABLE_PATH", "CLAIMX_DELTA_PROJECTS_TABLE")
    contacts_table_path = get_table_path("CLAIMX_CONTACTS_TABLE_PATH", "CLAIMX_DELTA_CONTACTS_TABLE")
    media_table_path = get_table_path("CLAIMX_MEDIA_TABLE_PATH", "CLAIMX_DELTA_MEDIA_TABLE")
    tasks_table_path = get_table_path("CLAIMX_TASKS_TABLE_PATH", "CLAIMX_DELTA_TASKS_TABLE")
    task_templates_table_path = get_table_path("CLAIMX_TASK_TEMPLATES_TABLE_PATH", "CLAIMX_DELTA_TASK_TEMPLATES_TABLE")
    external_links_table_path = get_table_path("CLAIMX_EXTERNAL_LINKS_TABLE_PATH", "CLAIMX_DELTA_EXTERNAL_LINKS_TABLE")
    video_collab_table_path = get_table_path("CLAIMX_VIDEO_COLLAB_TABLE_PATH", "CLAIMX_DELTA_VIDEO_COLLAB_TABLE")

    # Validate all paths are set
    table_paths = {
        "projects": projects_table_path,
        "contacts": contacts_table_path,
        "media": media_table_path,
        "tasks": tasks_table_path,
        "task_templates": task_templates_table_path,
        "external_links": external_links_table_path,
        "video_collab": video_collab_table_path,
    }

    missing_tables = [name for name, path in table_paths.items() if not path]
    if missing_tables:
        env_hints = {
            "projects": "CLAIMX_DELTA_PROJECTS_TABLE",
            "contacts": "CLAIMX_DELTA_CONTACTS_TABLE",
            "media": "CLAIMX_DELTA_MEDIA_TABLE",
            "tasks": "CLAIMX_DELTA_TASKS_TABLE",
            "task_templates": "CLAIMX_DELTA_TASK_TEMPLATES_TABLE",
            "external_links": "CLAIMX_DELTA_EXTERNAL_LINKS_TABLE",
            "video_collab": "CLAIMX_DELTA_VIDEO_COLLAB_TABLE",
        }
        missing_info = [f"{name} ({env_hints[name]})" for name in missing_tables]
        raise ValueError(
            f"Missing required table paths for claimx-entity-writer: {', '.join(missing_info)}"
        )

    # Pass explicit paths to runner (same pattern as claimx-delta-writer)
    return {
        "kafka_config": local_kafka_config,
        "projects_table_path": projects_table_path,
        "contacts_table_path": contacts_table_path,
        "media_table_path": media_table_path,
        "tasks_table_path": tasks_table_path,
        "task_templates_table_path": task_templates_table_path,
        "external_links_table_path": external_links_table_path,
        "video_collab_table_path": video_collab_table_path,
        "shutdown_event": shutdown_event,
    }


# Worker registry mapping worker names to their runner functions and config builders
WORKER_REGISTRY: Dict[str, Dict[str, Any]] = {
    # XACT workers
    "xact-poller": {
        "runner": xact_runners.run_eventhouse_poller,
        "args_builder": build_xact_poller_args,
        "requires_eventhouse": True,
    },
    "xact-json-poller": {
        "runner": xact_runners.run_eventhouse_json_poller,
        "args_builder": build_xact_json_poller_args,
        "requires_eventhouse": True,
    },
    "xact-event-ingester": {
        "runner": lambda **kwargs: (
            xact_runners.run_local_event_ingester(**kwargs)
            if "local_kafka_config" in kwargs and "eventhub_config" not in kwargs
            else xact_runners.run_event_ingester(**kwargs)
        ),
        "args_builder": build_xact_event_ingester_args,
    },
    "xact-local-ingester": {
        "runner": xact_runners.run_local_event_ingester,
        "args_builder": lambda pc, se, **kw: {
            "local_kafka_config": kw.get("local_kafka_config"),
            "shutdown_event": se,
            "domain": pc.domain,
        },
    },
    "xact-delta-writer": {
        "runner": xact_runners.run_delta_events_worker,
        "args_builder": build_xact_delta_writer_args,
    },
    "xact-retry-scheduler": {
        "runner": xact_runners.run_xact_retry_scheduler,
        "args_builder": lambda pc, se, **kw: {
            "kafka_config": kw.get("local_kafka_config"),
            "shutdown_event": se,
        },
    },
    "xact-enricher": {
        "runner": xact_runners.run_xact_enrichment_worker,
        "args_builder": lambda pc, se, **kw: {
            "kafka_config": kw.get("local_kafka_config"),
            "shutdown_event": se,
            "simulation_mode": kw.get("simulation_mode", False),
        },
    },
    "xact-download": {
        "runner": xact_runners.run_download_worker,
        "args_builder": lambda pc, se, **kw: {
            "kafka_config": kw.get("local_kafka_config"),
            "shutdown_event": se,
        },
    },
    "xact-upload": {
        "runner": xact_runners.run_upload_worker,
        "args_builder": lambda pc, se, **kw: {
            "kafka_config": kw.get("local_kafka_config"),
            "shutdown_event": se,
            "simulation_mode": kw.get("simulation_mode", False),
        },
    },
    "xact-result-processor": {
        "runner": xact_runners.run_result_processor,
        "args_builder": build_xact_result_processor_args,
    },
    # ClaimX workers
    "claimx-poller": {
        "runner": claimx_runners.run_claimx_eventhouse_poller,
        "args_builder": lambda pc, se, **kw: {
            "pipeline_config": pc,
            "shutdown_event": se,
        },
    },
    "claimx-ingester": {
        "runner": claimx_runners.run_claimx_event_ingester,
        "args_builder": lambda pc, se, **kw: {
            "kafka_config": kw.get("local_kafka_config"),
            "shutdown_event": se,
        },
    },
    "claimx-enricher": {
        "runner": claimx_runners.run_claimx_enrichment_worker,
        "args_builder": build_claimx_enricher_args,
    },
    "claimx-downloader": {
        "runner": claimx_runners.run_claimx_download_worker,
        "args_builder": lambda pc, se, **kw: {
            "kafka_config": kw.get("local_kafka_config"),
            "shutdown_event": se,
        },
    },
    "claimx-uploader": {
        "runner": claimx_runners.run_claimx_upload_worker,
        "args_builder": lambda pc, se, **kw: {
            "kafka_config": kw.get("local_kafka_config"),
            "shutdown_event": se,
            "simulation_mode": kw.get("simulation_mode", False),
        },
    },
    "claimx-result-processor": {
        "runner": claimx_runners.run_claimx_result_processor,
        "args_builder": build_claimx_result_processor_args,
    },
    "claimx-delta-writer": {
        "runner": claimx_runners.run_claimx_delta_events_worker,
        "args_builder": build_claimx_delta_writer_args,
    },
    "claimx-retry-scheduler": {
        "runner": claimx_runners.run_claimx_retry_scheduler,
        "args_builder": lambda pc, se, **kw: {
            "kafka_config": kw.get("local_kafka_config"),
            "shutdown_event": se,
        },
    },
    "claimx-entity-writer": {
        "runner": claimx_runners.run_claimx_entity_delta_worker,
        "args_builder": build_claimx_entity_writer_args,
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
    instance_id: Optional[int] = None,
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

    # Build arguments
    args_builder = worker_def["args_builder"]
    kwargs = args_builder(
        pipeline_config,
        shutdown_event,
        enable_delta_writes=enable_delta_writes,
        eventhub_config=eventhub_config,
        local_kafka_config=local_kafka_config,
        simulation_mode=simulation_mode,
    )

    # Add instance_id if provided
    if instance_id is not None:
        kwargs["instance_id"] = instance_id

    # Run the worker
    runner = worker_def["runner"]
    await runner(**kwargs)
