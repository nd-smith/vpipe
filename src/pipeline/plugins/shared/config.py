"""Shared configuration loading for plugin workers.

Loads YAML config files and connection definitions used by plugin workers.
Extracted from the iTel cabinet workers to avoid duplication.
"""

import logging
import os
from pathlib import Path

import yaml

from pipeline.plugins.shared.connections import AuthType, ConnectionConfig

logger = logging.getLogger(__name__)


def load_yaml_config(path: Path) -> dict:
    """Load YAML configuration file.

    Args:
        path: Path to YAML file

    Returns:
        Parsed YAML contents as dict

    Raises:
        FileNotFoundError: If path does not exist
    """
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    logger.info("Loading configuration from %s", path)
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_connections(config_path: Path) -> list[ConnectionConfig]:
    """Load connection configurations from a YAML file.

    Expands environment variables in base_url and auth_token fields.

    Args:
        config_path: Path to connections YAML file

    Returns:
        List of ConnectionConfig objects

    Raises:
        FileNotFoundError: If config_path does not exist
        ValueError: If environment variables are not expanded
    """
    config_data = load_yaml_config(config_path)

    connections = []
    for conn_name, conn_data in config_data.get("connections", {}).items():
        if not conn_data or not conn_data.get("base_url"):
            continue

        base_url = os.path.expandvars(conn_data["base_url"])
        auth_token = os.path.expandvars(conn_data.get("auth_token", ""))

        if "${" in base_url:
            raise ValueError(
                f"Environment variable not expanded in base_url for connection '{conn_name}': {base_url}. "
                f"Check that all required environment variables are set in .env file."
            )
        if auth_token and "${" in auth_token:
            raise ValueError(
                f"Environment variable not expanded in auth_token for connection '{conn_name}'. "
                f"Check that all required environment variables are set in .env file."
            )

        auth_type = conn_data.get("auth_type", "none")
        if isinstance(auth_type, str):
            auth_type = AuthType(auth_type)

        conn = ConnectionConfig(
            name=conn_data.get("name", conn_name),
            base_url=base_url,
            auth_type=auth_type,
            auth_token=auth_token,
            auth_header=conn_data.get("auth_header"),
            timeout_seconds=conn_data.get("timeout_seconds", 30),
            max_retries=conn_data.get("max_retries", 3),
            retry_backoff_base=conn_data.get("retry_backoff_base", 2),
            retry_backoff_max=conn_data.get("retry_backoff_max", 60),
            headers=conn_data.get("headers", {}),
        )
        connections.append(conn)
        logger.info("Loaded connection: %s -> %s", conn.name, conn.base_url)

    return connections
