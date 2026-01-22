# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
# 
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
KQL Client for querying Microsoft Fabric Eventhouse.

Provides async interface for executing KQL queries against Eventhouse
with connection pooling, retry logic, and proper error classification.
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Optional

import requests
import yaml
from azure.core.credentials import AccessToken
from azure.identity import DefaultAzureCredential
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from core.errors.classifiers import StorageErrorClassifier
from core.errors.exceptions import KustoError, KustoQueryError
from kafka_pipeline.common.storage.onelake import (
    _register_file_credential,
    _refresh_all_credentials,
    FileBackedTokenCredential,
)

logger = logging.getLogger(__name__)

# Default config path: config.yaml in src/ directory
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent.parent / "config.yaml"

# Kusto/Fabric resource for token acquisition
KUSTO_RESOURCE = "https://kusto.kusto.windows.net"

# Global flag to track if we've patched requests
_requests_patched = False


def _patch_requests_for_proxy(proxy_url: str) -> None:
    """Patch requests.Session to always use the specified proxy.

    This ensures that ALL HTTP requests made by any library using requests
    (including MSAL/AAD authentication) will go through the proxy.
    """
    global _requests_patched
    if _requests_patched:
        return

    original_init = requests.Session.__init__

    @wraps(original_init)
    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        # Force proxy configuration on every new session
        self.trust_env = True
        self.proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }

    requests.Session.__init__ = patched_init
    _requests_patched = True
    logger.info(
        "Patched requests.Session to use proxy globally",
        extra={"proxy_url": proxy_url},
    )


@dataclass
class EventhouseConfig:
    """Configuration for connecting to Eventhouse.

    Load configuration using EventhouseConfig.load_config() which reads from
    config.yaml with environment variable overrides.
    """

    # Connection
    cluster_url: str  # e.g., "https://your-cluster.kusto.fabric.microsoft.com"
    database: str  # e.g., "your-database"
    query_timeout_seconds: int = 120  # Default query timeout
    max_retries: int = 3  # Max retry attempts for transient failures
    retry_base_delay_seconds: float = 1.0  # Base delay between retries
    retry_max_delay_seconds: float = 30.0  # Max delay between retries
    proxy_url: Optional[str] = None  # HTTP proxy URL (e.g., "http://proxy:8080")

    @classmethod
    def load_config(
        cls,
        config_path: Optional[Path] = None,
    ) -> "EventhouseConfig":
        """Load configuration from YAML file with environment variable overrides.

        Configuration priority (highest to lowest):
        1. Environment variables
        2. config.yaml file (under 'eventhouse:' key)
        3. Dataclass defaults

        """
        config_path = config_path or DEFAULT_CONFIG_PATH
        data: Dict[str, Any] = {}
        if config_path.exists():
            with open(config_path, "r") as f:
                yaml_data = yaml.safe_load(f) or {}
            data = yaml_data.get("eventhouse", {})

        # Apply environment variable overrides
        env_overrides = {
            "cluster_url": os.getenv("EVENTHOUSE_CLUSTER_URL"),
            "database": os.getenv("EVENTHOUSE_DATABASE"),
            "query_timeout_seconds": os.getenv("EVENTHOUSE_QUERY_TIMEOUT"),
            "max_retries": os.getenv("EVENTHOUSE_MAX_RETRIES"),
            "retry_base_delay_seconds": os.getenv("EVENTHOUSE_RETRY_BASE_DELAY"),
            "retry_max_delay_seconds": os.getenv("EVENTHOUSE_RETRY_MAX_DELAY"),
            # Proxy: check EVENTHOUSE_PROXY_URL first, then HTTPS_PROXY, then HTTP_PROXY
            "proxy_url": os.getenv("EVENTHOUSE_PROXY_URL")
            or os.getenv("HTTPS_PROXY")
            or os.getenv("HTTP_PROXY"),
        }
        for key, value in env_overrides.items():
            if value is not None:
                data[key] = value
        cluster_url = data.get("cluster_url", "")
        database = data.get("database", "")

        if not cluster_url:
            raise ValueError(
                "eventhouse.cluster_url is required. "
                "Set in config.yaml or via EVENTHOUSE_CLUSTER_URL env var."
            )
        if not database:
            raise ValueError(
                "eventhouse.database is required. "
                "Set in config.yaml or via EVENTHOUSE_DATABASE env var."
            )

        return cls(
            cluster_url=cluster_url,
            database=database,
            query_timeout_seconds=int(data.get("query_timeout_seconds", 120)),
            max_retries=int(data.get("max_retries", 3)),
            retry_base_delay_seconds=float(data.get("retry_base_delay_seconds", 1.0)),
            retry_max_delay_seconds=float(data.get("retry_max_delay_seconds", 30.0)),
            proxy_url=data.get("proxy_url"),
        )


@dataclass
class KQLQueryResult:
    """Result of a KQL query execution."""

    # Result data as list of dicts (each dict is a row)
    rows: list[dict[str, Any]] = field(default_factory=list)
    query_duration_ms: float = 0.0
    row_count: int = 0
    is_partial: bool = False

    # For debugging/logging
    query_text: str = ""

    @property
    def is_empty(self):
        """Check if query returned no results."""
        return self.row_count == 0


class KQLClient:
    """
    Async client for querying Microsoft Fabric Eventhouse.

    Uses DefaultAzureCredential for authentication, which supports:
    - Managed Identity (in production)
    - Azure CLI credentials (local development)
    - Service Principal (via environment variables)

    Example:
        import logging
        logger = logging.getLogger(__name__)
        config = EventhouseConfig.load_config()
        async with KQLClient(config) as client:
            result = await client.execute_query(
                "Events | where ingestion_time() > ago(1h) | take 100"
            )
            for row in result.rows:
                logger.debug("Query result row", extra={"row": row})
    """

    def __init__(self, config: EventhouseConfig):
        """Initialize KQL client with configuration."""
        self.config = config
        self._client: Optional[KustoClient] = None
        self._credential: Optional[DefaultAzureCredential] = None

    async def __aenter__(self) -> "KQLClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def connect(self) -> None:
        """Establish connection to Eventhouse.

        Authentication priority:
        1. Token file (if AZURE_TOKEN_FILE is set)
        2. SPN credentials (if AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID are set)
        3. DefaultAzureCredential (managed identity, CLI, etc.)
        """
        if self._client is not None:
            return  # Already connected

        # CRITICAL: Set proxy environment variables FIRST, before any credential or
        # connection string builder operations. The Azure Identity SDK and Kusto SDK
        # make HTTP requests during initialization (auth metadata discovery, token
        # acquisition) that need the proxy to be configured in the environment.
        if self.config.proxy_url:
            os.environ["HTTP_PROXY"] = self.config.proxy_url
            os.environ["HTTPS_PROXY"] = self.config.proxy_url
            # Also set lowercase variants for libraries that check these
            os.environ["http_proxy"] = self.config.proxy_url
            os.environ["https_proxy"] = self.config.proxy_url

            # Patch requests.Session globally to ensure ALL HTTP requests
            # (including MSAL/AAD internal requests) use the proxy
            _patch_requests_for_proxy(self.config.proxy_url)

            logger.info(
                "Set proxy environment variables for Azure/Kusto SDK auth requests",
                extra={"proxy_url": self.config.proxy_url},
            )

        # Log proxy configuration for troubleshooting
        proxy_env_vars = {
            "EVENTHOUSE_PROXY_URL": os.getenv("EVENTHOUSE_PROXY_URL"),
            "HTTPS_PROXY": os.getenv("HTTPS_PROXY"),
            "HTTP_PROXY": os.getenv("HTTP_PROXY"),
        }
        logger.info(
            "Proxy environment: %s -> using: %s",
            {k: (v[:20] + "..." if v and len(v) > 20 else v) for k, v in proxy_env_vars.items()},
            self.config.proxy_url or "(none)",
        )

        logger.info(
            "Connecting to Eventhouse",
            extra={
                "cluster_url": self.config.cluster_url,
                "database": self.config.database,
            },
        )

        try:
            # Check for token file authentication first
            token_file = os.getenv("AZURE_TOKEN_FILE")
            auth_mode = "default"
            kcsb = None

            # Check for SPN credentials
            client_id = os.getenv("AZURE_CLIENT_ID")
            client_secret = os.getenv("AZURE_CLIENT_SECRET")
            tenant_id = os.getenv("AZURE_TENANT_ID")
            has_spn = client_id and client_secret and tenant_id

            if token_file:
                token_path = Path(token_file)
                if token_path.exists():
                    try:
                        self._credential = FileBackedTokenCredential(
                            resource=KUSTO_RESOURCE,
                        )
                        auth_mode = "token_file"
                        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
                            self.config.cluster_url,
                            self._credential,
                        )
                        logger.info(
                            "Using token file for Eventhouse authentication via unified auth",
                            extra={"token_file": token_file},
                        )
                    except Exception as e:
                        logger.warning(
                            "Token file auth failed, trying SPN/default",
                            extra={"error": str(e)[:200]},
                        )

            # Use SPN with direct AAD app key auth (more reliable than token credential)
            if kcsb is None and has_spn:
                kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                    self.config.cluster_url,
                    client_id,
                    client_secret,
                    tenant_id,
                )
                auth_mode = "spn"
                logger.info(
                    "Using SPN credentials for Eventhouse authentication",
                    extra={"client_id": client_id[:8] + "..."},
                )

            # Fall back to DefaultAzureCredential
            if kcsb is None:
                self._credential = DefaultAzureCredential()
                kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
                    self.config.cluster_url,
                    self._credential,
                )
                auth_mode = "default"

            # Create client (sync client, will execute in thread pool)
            # Note: Proxy env vars were set at the start of connect() to ensure
            # they're available for auth metadata requests during client creation
            self._client = KustoClient(kcsb)

            # Configure proxy if specified (belt and suspenders)
            if self.config.proxy_url:
                self._client.set_proxy(self.config.proxy_url)
                # Also ensure the session trusts env vars and has proxy set directly
                if hasattr(self._client, "_session"):
                    self._client._session.trust_env = True
                    self._client._session.proxies = {
                        "http": self.config.proxy_url,
                        "https": self.config.proxy_url,
                    }
                logger.info(
                    "Configured proxy for Eventhouse",
                    extra={"proxy_url": self.config.proxy_url},
                )

            logger.info(
                "Connected to Eventhouse",
                extra={
                    "cluster_url": self.config.cluster_url,
                    "database": self.config.database,
                    "auth_mode": auth_mode,
                    "proxy_configured": bool(self.config.proxy_url),
                },
            )

        except Exception as e:
            logger.error(
                "Failed to connect to Eventhouse: %s",
                str(e)[:200],
                extra={
                    "cluster_url": self.config.cluster_url,
                    "database": self.config.database,
                    "error": str(e)[:200],
                },
            )
            raise StorageErrorClassifier.classify_kusto_error(
                e, {"operation": "connect"}
            ) from e

    async def close(self) -> None:
        """Close connection and cleanup resources."""
        if self._client is not None:
            try:
                # KustoClient.close() is sync
                self._client.close()
            except Exception as e:
                logger.warning(
                    "Error closing Kusto client: %s",
                    str(e)[:100],
                )
            finally:
                self._client = None
                self._credential = None

            logger.debug("Eventhouse connection closed")

    async def execute_query(
        self,
        query: str,
        database: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> KQLQueryResult:
        """Execute a KQL query with retry logic.
        """
        if self._client is None:
            await self.connect()

        db = database or self.config.database
        timeout = timeout_seconds or self.config.query_timeout_seconds
        last_error: Optional[Exception] = None

        for attempt in range(self.config.max_retries):
            try:
                result = await self._execute_query_impl(query, db, timeout)

                # Log success after retries
                if attempt > 0:
                    logger.info(
                        "Query succeeded after %d retries",
                        attempt,
                        extra={
                            "attempt": attempt + 1,
                            "query_length": len(query),
                        },
                    )

                return result

            except KustoQueryError:
                # Query errors are not retryable (syntax/semantic errors)
                raise

            except Exception as e:
                last_error = e
                classified = StorageErrorClassifier.classify_kusto_error(
                    e, {"operation": "execute_query", "attempt": attempt + 1}
                )

                # Clear all credential caches on auth errors to force token re-read
                if classified.should_refresh_auth:
                    logger.info(
                        "Auth error detected, refreshing all credentials",
                        extra={
                            "attempt": attempt + 1,
                            "error": str(e)[:200],
                        },
                    )
                    _refresh_all_credentials()

                # Check if error is retryable
                if not classified.is_retryable:
                    raise classified from e

                # Check if we have more retries
                if attempt + 1 >= self.config.max_retries:
                    logger.error(
                        "Max retries exhausted for query",
                        extra={
                            "attempt": attempt + 1,
                            "max_retries": self.config.max_retries,
                            "error": str(e)[:200],
                        },
                    )
                    raise classified from e

                # Calculate retry delay with exponential backoff
                delay = min(
                    self.config.retry_base_delay_seconds * (2**attempt),
                    self.config.retry_max_delay_seconds,
                )

                logger.warning(
                    "Retrying query after error",
                    extra={
                        "attempt": attempt + 1,
                        "max_retries": self.config.max_retries,
                        "delay_seconds": delay,
                        "error": str(e)[:200],
                    },
                )

                await asyncio.sleep(delay)

        # Should not reach here, but just in case
        if last_error:
            raise StorageErrorClassifier.classify_kusto_error(
                last_error, {"operation": "execute_query"}
            ) from last_error

        raise KustoError("Unknown error during query execution")

    async def _execute_query_impl(
        self,
        query: str,
        database: str,
        timeout_seconds: int,
    ) -> KQLQueryResult:
        """Execute query implementation (runs in thread pool).
        """
        start_time = time.perf_counter()

        try:
            # Execute in thread pool since KustoClient is sync
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.execute(database, query),
            )

            query_duration_ms = (time.perf_counter() - start_time) * 1000

            # Get primary results table
            if not response.primary_results:
                return KQLQueryResult(
                    rows=[],
                    query_duration_ms=query_duration_ms,
                    row_count=0,
                    is_partial=False,
                    query_text=query,
                )

            primary_table = response.primary_results[0]

            # Convert to list of dicts
            rows = []
            column_names = [col.column_name for col in primary_table.columns]

            for row in primary_table:
                row_dict = {}
                for i, col_name in enumerate(column_names):
                    value = row[i]
                    if isinstance(value, datetime):
                        row_dict[col_name] = value.isoformat()
                    else:
                        row_dict[col_name] = value
                rows.append(row_dict)

            result = KQLQueryResult(
                rows=rows,
                query_duration_ms=query_duration_ms,
                row_count=len(rows),
                is_partial=False,
                query_text=query,
            )

            logger.debug(
                "Query executed successfully",
                extra={
                    "database": database,
                    "query_length": len(query),
                    "row_count": result.row_count,
                    "duration_ms": round(query_duration_ms, 2),
                },
            )

            return result

        except KustoServiceError as e:
            query_duration_ms = (time.perf_counter() - start_time) * 1000

            # Extract detailed error info from KustoServiceError
            error_details = {}
            try:
                # Try to get structured error info if available
                if hasattr(e, 'get_api_errors'):
                    api_errors = e.get_api_errors()
                    if api_errors:
                        error_details["api_errors"] = str(api_errors)[:500]
                if hasattr(e, 'http_response') and e.http_response:
                    error_details["http_status"] = getattr(e.http_response, 'status_code', None) or getattr(e.http_response, 'status', None)
            except Exception:
                pass  # Don't fail on error introspection

            logger.error(
                "KQL query failed",
                extra={
                    "database": database,
                    "query_length": len(query),
                    "query": query[:500] if len(query) > 500 else query,
                    "duration_ms": round(query_duration_ms, 2),
                    "error": str(e)[:1000],
                    "error_type": type(e).__name__,
                    **error_details,
                },
            )

            # Check if it's a query error (syntax/semantic)
            error_str = str(e).lower()
            if "semantic error" in error_str or "syntax error" in error_str:
                raise KustoQueryError(
                    f"KQL query error: {e}",
                    cause=e,
                    context={"database": database, "query_length": len(query)},
                ) from e

            raise

        except Exception as e:
            query_duration_ms = (time.perf_counter() - start_time) * 1000

            logger.error(
                "Query execution failed",
                extra={
                    "database": database,
                    "query_length": len(query),
                    "query": query[:500] if len(query) > 500 else query,
                    "duration_ms": round(query_duration_ms, 2),
                    "error": str(e)[:1000],
                    "error_type": type(e).__name__,
                },
            )

            raise
