"""Shared configuration loading for plugin workers.

Loads YAML config files and connection definitions used by plugin workers.
Extracted from the iTel cabinet workers to avoid duplication.
"""

import logging
from pathlib import Path

import yaml

from config.config import expand_env_var_string
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


def _validate_env_expansion(value: str, field_label: str, conn_name: str) -> None:
    """Raise ValueError if an environment variable reference was not expanded."""
    if value and "${" in value:
        raise ValueError(
            f"Environment variable not expanded in {field_label} for connection '{conn_name}'. "
            f"Check that the required environment variables are set or that defaults are configured in the connection YAML."
        )


def _load_oauth2_config(oauth2_data: dict, conn_name: str) -> dict:
    """Load and validate OAuth2 config fields. Returns dict of oauth2_* kwargs."""
    result = {}
    for field_name, key in [
        ("oauth2_client_id", "client_id"),
        ("oauth2_client_credential", "client_credential"),
        ("oauth2_token_url", "token_url"),
        ("oauth2_scope", "scope"),
    ]:
        value = expand_env_var_string(oauth2_data.get(key, "")) or None
        _validate_env_expansion(value or "", f"oauth2.{key}", conn_name)
        result[field_name] = value
    return result


def load_connections(config_path: Path) -> list[ConnectionConfig]:
    """Load connection configurations from a YAML file.

    Expands environment variables in base_url, auth_token, endpoint, and method fields.

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

        base_url = expand_env_var_string(conn_data["base_url"])
        auth_token = expand_env_var_string(conn_data.get("auth_token", ""))

        _validate_env_expansion(base_url, "base_url", conn_name)
        _validate_env_expansion(auth_token, "auth_token", conn_name)

        auth_type = conn_data.get("auth_type", "none")
        if isinstance(auth_type, str):
            auth_type = AuthType(auth_type)

        oauth2_kwargs = _load_oauth2_config(conn_data.get("oauth2", {}), conn_name) if conn_data.get("oauth2") else {}

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
            endpoint=expand_env_var_string(conn_data.get("endpoint", "")),
            method=expand_env_var_string(conn_data.get("method", "POST")),
            **oauth2_kwargs,
        )
        connections.append(conn)
        logger.info("Loaded connection: %s -> %s", conn.name, conn.base_url)

    return connections
