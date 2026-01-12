"""
KQL Client for querying Microsoft Fabric Eventhouse.

Provides async interface for executing KQL queries against Eventhouse
with connection pooling, retry logic, and proper error classification.
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

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
)

logger = logging.getLogger(__name__)

# Default config path: config.yaml in src/ directory
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent.parent / "config.yaml"

# Kusto/Fabric resource for token acquisition
KUSTO_RESOURCE = "https://kusto.kusto.windows.net"


class FileBackedKustoCredential:
    """
    Credential that reads tokens from a JSON file for Kusto authentication.

    The token file should be a JSON object with resource URLs as keys:
    {
        "https://kusto.kusto.windows.net": "token_value",
        "https://storage.azure.com/": "token_value"
    }

    This credential re-reads from the file when tokens are near expiry,
    allowing token_refresher to keep tokens updated externally.
    """

    def __init__(
        self,
        token_file: str,
        resource: str = KUSTO_RESOURCE,
        refresh_threshold_minutes: int = 10,
    ):
        """
        Initialize file-backed credential.

        Args:
            token_file: Path to JSON token file
            resource: Azure resource to get token for
            refresh_threshold_minutes: Re-read file if token older than this
        """
        self._token_file = Path(token_file)
        self._resource = resource
        self._refresh_threshold = timedelta(minutes=refresh_threshold_minutes)
        self._cached_token: Optional[str] = None
        self._token_acquired_at: Optional[datetime] = None
        # Register for coordinated refresh on auth errors
        _register_file_credential(self)

    def _should_refresh(self) -> bool:
        """Check if token should be refreshed from file."""
        if self._cached_token is None or self._token_acquired_at is None:
            return True
        age = datetime.now(timezone.utc) - self._token_acquired_at
        return age >= self._refresh_threshold

    def _read_token(self) -> str:
        """Read token from file."""
        if not self._token_file.exists():
            raise RuntimeError(f"Token file not found: {self._token_file}")

        content = self._token_file.read_text(encoding="utf-8-sig").strip()
        if not content:
            raise RuntimeError(f"Token file is empty: {self._token_file}")

        try:
            tokens = json.loads(content)
            if isinstance(tokens, dict):
                # Try exact match
                if self._resource in tokens:
                    return tokens[self._resource]
                # Try normalized match (with/without trailing slash)
                resource_normalized = self._resource.rstrip("/")
                for key, value in tokens.items():
                    if key.rstrip("/") == resource_normalized:
                        return value
                # Try cluster URL pattern for Kusto
                for key, value in tokens.items():
                    if "kusto" in key.lower() or "fabric" in key.lower():
                        logger.debug(
                            "Using token for resource %s (requested %s)",
                            key, self._resource
                        )
                        return value
                raise RuntimeError(
                    f"Token file does not contain token for resource: {self._resource}"
                )
            else:
                # Plain text token
                return content
        except json.JSONDecodeError:
            # Not JSON, treat as plain text
            return content

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        """
        Return token in Azure SDK expected format.

        Automatically refreshes from file if token is near expiry.
        """
        if self._should_refresh():
            self._cached_token = self._read_token()
            self._token_acquired_at = datetime.now(timezone.utc)
            logger.debug(
                "Read Kusto token from file",
                extra={"token_file": str(self._token_file)},
            )

        # Token expiry is approximate (assume 1 hour from read)
        expires_on = int(
            (self._token_acquired_at + timedelta(hours=1)).timestamp()
        )
        return AccessToken(self._cached_token, expires_on)

    def close(self) -> None:
        """Close the credential (no-op for file-backed credential)."""
        # No resources to clean up - the file handle is not kept open
        pass


@dataclass
class EventhouseConfig:
    """Configuration for connecting to Eventhouse.

    Load from environment using EventhouseConfig.from_env().
    """

    # Connection
    cluster_url: str  # e.g., "https://your-cluster.kusto.fabric.microsoft.com"
    database: str  # e.g., "your-database"

    # Query defaults
    query_timeout_seconds: int = 120  # Default query timeout
    max_retries: int = 3  # Max retry attempts for transient failures
    retry_base_delay_seconds: float = 1.0  # Base delay between retries
    retry_max_delay_seconds: float = 30.0  # Max delay between retries

    # Connection pooling
    max_connections: int = 10  # Connection pool size

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

        Args:
            config_path: Path to YAML config file. Defaults to src/config.yaml.

        Returns:
            EventhouseConfig instance

        Raises:
            ValueError: If required fields are missing
        """
        config_path = config_path or DEFAULT_CONFIG_PATH

        # Load YAML
        data: Dict[str, Any] = {}
        if config_path.exists():
            with open(config_path, "r") as f:
                yaml_data = yaml.safe_load(f) or {}
            # Extract eventhouse section
            data = yaml_data.get("eventhouse", {})

        # Apply environment variable overrides
        env_overrides = {
            "cluster_url": os.getenv("EVENTHOUSE_CLUSTER_URL"),
            "database": os.getenv("EVENTHOUSE_DATABASE"),
            "query_timeout_seconds": os.getenv("EVENTHOUSE_QUERY_TIMEOUT"),
            "max_retries": os.getenv("EVENTHOUSE_MAX_RETRIES"),
            "retry_base_delay_seconds": os.getenv("EVENTHOUSE_RETRY_BASE_DELAY"),
            "retry_max_delay_seconds": os.getenv("EVENTHOUSE_RETRY_MAX_DELAY"),
            "max_connections": os.getenv("EVENTHOUSE_MAX_CONNECTIONS"),
        }
        for key, value in env_overrides.items():
            if value is not None:
                data[key] = value

        # Validate required fields
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
            max_connections=int(data.get("max_connections", 10)),
        )

    @classmethod
    def from_env(cls) -> "EventhouseConfig":
        """Load configuration from environment variables.

        DEPRECATED: Use load_config() instead.
        This method is kept for backwards compatibility.

        Required environment variables:
            EVENTHOUSE_CLUSTER_URL: Kusto cluster URL
            EVENTHOUSE_DATABASE: Database name

        Optional environment variables (with defaults):
            EVENTHOUSE_QUERY_TIMEOUT: Query timeout in seconds (default: 120)
            EVENTHOUSE_MAX_RETRIES: Max retry attempts (default: 3)
            EVENTHOUSE_RETRY_BASE_DELAY: Base retry delay in seconds (default: 1.0)
            EVENTHOUSE_RETRY_MAX_DELAY: Max retry delay in seconds (default: 30.0)
            EVENTHOUSE_MAX_CONNECTIONS: Connection pool size (default: 10)

        Raises:
            ValueError: If required environment variables are missing
        """
        cluster_url = os.getenv("EVENTHOUSE_CLUSTER_URL")
        if not cluster_url:
            raise ValueError(
                "EVENTHOUSE_CLUSTER_URL environment variable is required"
            )

        database = os.getenv("EVENTHOUSE_DATABASE")
        if not database:
            raise ValueError("EVENTHOUSE_DATABASE environment variable is required")

        return cls(
            cluster_url=cluster_url,
            database=database,
            query_timeout_seconds=int(os.getenv("EVENTHOUSE_QUERY_TIMEOUT", "120")),
            max_retries=int(os.getenv("EVENTHOUSE_MAX_RETRIES", "3")),
            retry_base_delay_seconds=float(
                os.getenv("EVENTHOUSE_RETRY_BASE_DELAY", "1.0")
            ),
            retry_max_delay_seconds=float(
                os.getenv("EVENTHOUSE_RETRY_MAX_DELAY", "30.0")
            ),
            max_connections=int(os.getenv("EVENTHOUSE_MAX_CONNECTIONS", "10")),
        )


@dataclass
class KQLQueryResult:
    """Result of a KQL query execution."""

    # Result data as list of dicts (each dict is a row)
    rows: list[dict[str, Any]] = field(default_factory=list)

    # Query metadata
    query_duration_ms: float = 0.0
    row_count: int = 0
    is_partial: bool = False

    # For debugging/logging
    query_text: str = ""

    @property
    def is_empty(self) -> bool:
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
        config = EventhouseConfig.from_env()
        async with KQLClient(config) as client:
            result = await client.execute_query(
                "Events | where ingestion_time() > ago(1h) | take 100"
            )
            for row in result.rows:
                print(row)
    """

    def __init__(self, config: EventhouseConfig):
        """Initialize KQL client with configuration.

        Args:
            config: Eventhouse configuration
        """
        self.config = config
        self._client: Optional[KustoClient] = None
        self._credential: Optional[DefaultAzureCredential] = None
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "KQLClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def connect(self) -> None:
        """Establish connection to Eventhouse.

        Uses token file authentication if AZURE_TOKEN_FILE is set,
        otherwise falls back to DefaultAzureCredential.
        """
        async with self._lock:
            if self._client is not None:
                return  # Already connected

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

                if token_file:
                    token_path = Path(token_file)
                    if token_path.exists():
                        try:
                            self._credential = FileBackedKustoCredential(
                                token_file=token_file,
                                resource=KUSTO_RESOURCE,
                            )
                            auth_mode = "token_file"
                            logger.info(
                                "Using token file for Eventhouse authentication",
                                extra={"token_file": token_file},
                            )
                        except Exception as e:
                            logger.warning(
                                "Token file auth failed, falling back to DefaultAzureCredential",
                                extra={"error": str(e)[:200]},
                            )
                            self._credential = DefaultAzureCredential()
                    else:
                        logger.warning(
                            "AZURE_TOKEN_FILE set but file not found, using DefaultAzureCredential",
                            extra={"token_file": token_file},
                        )
                        self._credential = DefaultAzureCredential()
                else:
                    # Use DefaultAzureCredential for authentication
                    self._credential = DefaultAzureCredential()

                # Build connection string with token credential
                kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
                    self.config.cluster_url,
                    self._credential,
                )

                # Create client (sync client, will execute in thread pool)
                self._client = KustoClient(kcsb)

                logger.info(
                    "Connected to Eventhouse",
                    extra={
                        "cluster_url": self.config.cluster_url,
                        "database": self.config.database,
                        "auth_mode": auth_mode,
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
        async with self._lock:
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

        Args:
            query: KQL query string
            database: Database to query (defaults to config.database)
            timeout_seconds: Query timeout (defaults to config.query_timeout_seconds)

        Returns:
            KQLQueryResult with rows and metadata

        Raises:
            KustoQueryError: For query syntax/semantic errors
            KustoError: For other Kusto errors
        """
        if self._client is None:
            await self.connect()

        db = database or self.config.database
        timeout = timeout_seconds or self.config.query_timeout_seconds

        # Execute with retry
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

        Args:
            query: KQL query string
            database: Database name
            timeout_seconds: Query timeout

        Returns:
            KQLQueryResult
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
                    # Handle datetime serialization
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

    async def health_check(self) -> bool:
        """Check if connection is healthy.

        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            # Simple query to verify connection
            result = await self.execute_query(
                "print 1",
                timeout_seconds=10,
            )
            return result.row_count == 1

        except Exception as e:
            logger.warning(
                "Eventhouse health check failed: %s",
                str(e)[:100],
            )
            return False

    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._client is not None
