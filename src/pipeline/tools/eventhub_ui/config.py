"""Load EventHub topology from config.yaml for the UI."""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class EventHubInfo:
    """An EventHub and its consumer groups, as defined in config.yaml."""

    domain: str  # "verisk", "claimx", "plugins", or "source.verisk", "source.claimx"
    topic_key: str  # e.g. "enrichment_pending", "events"
    eventhub_name: str  # actual Azure EventHub name
    consumer_groups: dict[str, str]  # worker_name -> consumer_group_name
    is_source: bool  # True if this is a source (external) namespace hub


def load_eventhub_config() -> dict[str, Any]:
    """Load the raw eventhub section from config.yaml with env var expansion."""
    from config.config import DEFAULT_CONFIG_FILE, _expand_env_vars, load_yaml

    if not DEFAULT_CONFIG_FILE.exists():
        return {}

    data = load_yaml(DEFAULT_CONFIG_FILE)
    data = _expand_env_vars(data)
    return data.get("eventhub", {})


def get_namespace_connection_string() -> str:
    """Get the internal pipeline namespace connection string."""
    config = load_eventhub_config()

    conn = os.getenv("EVENTHUB_NAMESPACE_CONNECTION_STRING") or config.get(
        "namespace_connection_string", ""
    )
    if not conn:
        raise ValueError(
            "No EventHub namespace connection string configured. "
            "Set EVENTHUB_NAMESPACE_CONNECTION_STRING."
        )
    return _strip_entity_path(conn)


def get_source_connection_string() -> str:
    """Get the source (external) namespace connection string."""
    config = load_eventhub_config()

    conn = os.getenv("SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING") or (
        config.get("source", {}).get("namespace_connection_string", "")
    )
    if not conn:
        raise ValueError(
            "No source EventHub namespace connection string configured. "
            "Set SOURCE_EVENTHUB_NAMESPACE_CONNECTION_STRING."
        )
    return _strip_entity_path(conn)


def get_blob_connection_string() -> str:
    """Get the checkpoint blob storage connection string."""
    config = load_eventhub_config()

    conn = os.getenv("EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING") or (
        config.get("checkpoint_store", {}).get("blob_storage_connection_string", "")
    )
    if not conn:
        raise ValueError(
            "No checkpoint blob connection string configured. "
            "Set EVENTHUB_CHECKPOINT_BLOB_CONNECTION_STRING."
        )
    return conn


def get_checkpoint_container_name() -> str:
    config = load_eventhub_config()
    return os.getenv("EVENTHUB_CHECKPOINT_CONTAINER_NAME") or (
        config.get("checkpoint_store", {}).get("container_name", "eventhub-checkpoints")
    )


def extract_fqdn(conn_str: str) -> str:
    """Extract the namespace FQDN from a connection string."""
    match = re.search(r"Endpoint=sb://([^/;]+)", conn_str, re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not extract FQDN from connection string")
    return match.group(1)


def get_ssl_kwargs() -> dict:
    """SSL kwargs for Azure SDK clients. Always disables verification for this tool."""
    try:
        from core.security.ssl_dev_bypass import apply_ssl_dev_bypass

        apply_ssl_dev_bypass()
    except ImportError:
        pass
    return {"connection_verify": False}


def list_eventhubs() -> list[EventHubInfo]:
    """Extract all EventHub definitions from config.yaml."""
    config = load_eventhub_config()
    hubs = []

    # Internal domains: verisk, claimx, plugins
    skip_keys = {
        "namespace_connection_string", "transport_type",
        "default_consumer_group", "checkpoint_store", "dedup_store", "source",
    }

    for domain_key, domain_config in config.items():
        if domain_key in skip_keys or not isinstance(domain_config, dict):
            continue

        for topic_key, topic_config in domain_config.items():
            if not isinstance(topic_config, dict) or "eventhub_name" not in topic_config:
                continue

            consumer_groups = topic_config.get("consumer_groups", {})
            hubs.append(EventHubInfo(
                domain=domain_key,
                topic_key=topic_key,
                eventhub_name=topic_config["eventhub_name"],
                consumer_groups=dict(consumer_groups),
                is_source=False,
            ))

    # Source domains: source.verisk, source.claimx
    source_config = config.get("source", {})
    source_skip = {"namespace_connection_string"}
    for domain_key, domain_config in source_config.items():
        if domain_key in source_skip or not isinstance(domain_config, dict):
            continue

        for topic_key, topic_config in domain_config.items():
            if not isinstance(topic_config, dict) or "eventhub_name" not in topic_config:
                continue

            consumer_groups = topic_config.get("consumer_groups", {})
            hubs.append(EventHubInfo(
                domain=f"source.{domain_key}",
                topic_key=topic_key,
                eventhub_name=topic_config["eventhub_name"],
                consumer_groups=dict(consumer_groups),
                is_source=True,
            ))

    return hubs


def _strip_entity_path(conn_str: str) -> str:
    """Remove EntityPath from a connection string if present."""
    parts = [
        part for part in conn_str.split(";")
        if part.strip() and not part.startswith("EntityPath=")
    ]
    return ";".join(parts)
