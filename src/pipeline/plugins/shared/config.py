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

        if "${" in base_url:
            raise ValueError(
                f"Environment variable not expanded in base_url for connection '{conn_name}': {base_url}. "
                f"Check that the required environment variables are set or that defaults are configured in the connection YAML."
            )
        if auth_token and "${" in auth_token:
            raise ValueError(
                f"Environment variable not expanded in auth_token for connection '{conn_name}'. "
                f"Check that the required environment variables are set or that defaults are configured in the connection YAML."
            )

        auth_type = conn_data.get("auth_type", "none")
        if isinstance(auth_type, str):
            auth_type = AuthType(auth_type)

        # Load OAuth2 config if present
        oauth2_data = conn_data.get("oauth2", {})
        oauth2_client_id = None
        oauth2_client_credential = None
        oauth2_token_url = None
        oauth2_scope = None

        if oauth2_data:
            oauth2_client_id = expand_env_var_string(oauth2_data.get("client_id", "")) or None
            oauth2_client_credential = expand_env_var_string(oauth2_data.get("client_credential", "")) or None
            oauth2_token_url = expand_env_var_string(oauth2_data.get("token_url", "")) or None
            oauth2_scope = expand_env_var_string(oauth2_data.get("scope", "")) or None

            # Validate env var expansion for OAuth2 fields
            for field_name, field_value in [
                ("client_id", oauth2_client_id),
                ("client_credential", oauth2_client_credential),
                ("token_url", oauth2_token_url),
                ("scope", oauth2_scope),
            ]:
                if field_value and "${" in field_value:
                    raise ValueError(
                        f"Environment variable not expanded in oauth2.{field_name} "
                        f"for connection '{conn_name}'. "
                        f"Check that the required environment variables are set or that defaults are configured in the connection YAML."
                    )

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
            oauth2_client_id=oauth2_client_id,
            oauth2_client_credential=oauth2_client_credential,
            oauth2_token_url=oauth2_token_url,
            oauth2_scope=oauth2_scope,
            endpoint=expand_env_var_string(conn_data.get("endpoint", "")),
            method=expand_env_var_string(conn_data.get("method", "POST")),
        )
        connections.append(conn)
        logger.info("Loaded connection: %s -> %s", conn.name, conn.base_url)

    return connections
