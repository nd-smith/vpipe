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
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from azure.identity import DefaultAzureCredential
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError

from core.errors.classifiers import StorageErrorClassifier
from core.errors.exceptions import KustoError, KustoQueryError
from pipeline.common.storage.onelake import (
    FileBackedTokenCredential,
    _refresh_all_credentials,
)

logger = logging.getLogger(__name__)

# Default config path: config.yaml in src/ directory
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent.parent / "config.yaml"

# Kusto/Fabric resource for token acquisition
KUSTO_RESOURCE = "https://kusto.kusto.windows.net"


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
    proxy_url: str | None = None  # HTTP proxy URL (e.g., "http://proxy:8080")

    @classmethod
    def load_config(
        cls,
        config_path: Path | None = None,
    ) -> "EventhouseConfig":
        """Load configuration from YAML file with environment variable overrides.

        Configuration priority (highest to lowest):
        1. Environment variables
        2. config.yaml file (under 'eventhouse:' key)
        3. Dataclass defaults

        """
        config_path = config_path or DEFAULT_CONFIG_PATH
        data: dict[str, Any] = {}
        if config_path.exists():
            with open(config_path) as f:
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
        self._client: KustoClient | None = None
        self._credential: DefaultAzureCredential | None = None

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

        print("\n" + "="*80)
        print("[EVENTHOUSE CONNECTION] Starting Eventhouse connection process")
        print("="*80)
        print(f"[EVENTHOUSE CONNECTION] Target cluster: {self.config.cluster_url}")
        print(f"[EVENTHOUSE CONNECTION] Target database: {self.config.database}")
        print(f"[EVENTHOUSE CONNECTION] Query timeout: {self.config.query_timeout_seconds}s")
        print(f"[EVENTHOUSE CONNECTION] Max retries: {self.config.max_retries}")
        print("="*80 + "\n")

        logger.info(
            "Connecting to Eventhouse",
            extra={
                "cluster_url": self.config.cluster_url,
                "database": self.config.database,
            },
        )

        try:
            print("[EVENTHOUSE CONNECTION] Detecting authentication method...")

            # Check for token file and SPN credentials
            token_file = os.getenv("AZURE_TOKEN_FILE")
            client_id = os.getenv("AZURE_CLIENT_ID")
            client_secret = os.getenv("AZURE_CLIENT_SECRET")
            tenant_id = os.getenv("AZURE_TENANT_ID")
            has_spn = client_id and client_secret and tenant_id

            print(f"[EVENTHOUSE CONNECTION] Authentication environment:")
            print(f"[EVENTHOUSE CONNECTION]   - AZURE_TOKEN_FILE: {token_file if token_file else 'Not set'}")
            print(f"[EVENTHOUSE CONNECTION]   - SPN credentials available: {has_spn}")
            if has_spn:
                print(f"[EVENTHOUSE CONNECTION]   - AZURE_CLIENT_ID: {client_id[:8]}...")
                print(f"[EVENTHOUSE CONNECTION]   - AZURE_TENANT_ID: {tenant_id}")
                print(f"[EVENTHOUSE CONNECTION]   - AZURE_CLIENT_SECRET: {'Set (' + str(len(client_secret)) + ' chars)' if client_secret else 'Not set'}")

            auth_mode = "default"
            kcsb = None

            # Prioritize SPN with direct AAD app key auth (avoids token refresh warnings)
            if has_spn:
                print("\n[EVENTHOUSE CONNECTION] Using Service Principal (SPN) authentication")
                print(f"[EVENTHOUSE CONNECTION] Building connection string for: {self.config.cluster_url}")
                print(f"[EVENTHOUSE CONNECTION]   - Method: AAD Application Key Authentication")
                print(f"[EVENTHOUSE CONNECTION]   - Client ID: {client_id[:8]}...")
                print(f"[EVENTHOUSE CONNECTION]   - Tenant ID: {tenant_id}")
                kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                    self.config.cluster_url,
                    client_id,
                    client_secret,
                    tenant_id,
                )
                auth_mode = "spn"
                print("[EVENTHOUSE CONNECTION] SPN connection string builder created successfully")
                logger.info(
                    "Using SPN credentials for Eventhouse authentication",
                    extra={"client_id": client_id[:8] + "..."},
                )
            # Use token file only if SPN is not available
            elif token_file:
                print(f"\n[EVENTHOUSE CONNECTION] Using token file authentication")
                print(f"[EVENTHOUSE CONNECTION] Token file path: {token_file}")
                token_path = Path(token_file)
                if token_path.exists():
                    print("[EVENTHOUSE CONNECTION] Token file exists, reading credentials...")
                    try:
                        self._credential = FileBackedTokenCredential(
                            resource=KUSTO_RESOURCE,
                        )
                        auth_mode = "token_file"
                        print(f"[EVENTHOUSE CONNECTION] Building connection string with token credential")
                        print(f"[EVENTHOUSE CONNECTION]   - Target resource: {KUSTO_RESOURCE}")
                        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
                            self.config.cluster_url,
                            self._credential,
                        )
                        print("[EVENTHOUSE CONNECTION] Token file connection string builder created successfully")
                        logger.info(
                            "Using token file for Eventhouse authentication via unified auth",
                            extra={"token_file": token_file},
                        )
                    except Exception as e:
                        print(f"[EVENTHOUSE CONNECTION] ERROR: Token file auth failed - {e}")
                        print(f"[EVENTHOUSE CONNECTION] Falling back to DefaultAzureCredential...")
                        logger.warning(
                            "Token file auth failed, falling back to default",
                            extra={"error": str(e)[:200]},
                        )
                else:
                    print(f"[EVENTHOUSE CONNECTION] WARNING: Token file does not exist: {token_file}")
                    print(f"[EVENTHOUSE CONNECTION] Falling back to DefaultAzureCredential...")

            # Fall back to DefaultAzureCredential
            if kcsb is None:
                print("\n[EVENTHOUSE CONNECTION] Using DefaultAzureCredential (managed identity/CLI/etc.)")
                print("[EVENTHOUSE CONNECTION] Attempting credential chain:")
                print("[EVENTHOUSE CONNECTION]   1. Environment variables")
                print("[EVENTHOUSE CONNECTION]   2. Managed Identity")
                print("[EVENTHOUSE CONNECTION]   3. Azure CLI")
                print("[EVENTHOUSE CONNECTION]   4. Azure PowerShell")
                self._credential = DefaultAzureCredential()
                kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
                    self.config.cluster_url,
                    self._credential,
                )
                auth_mode = "default"
                print("[EVENTHOUSE CONNECTION] DefaultAzureCredential connection string builder created")

            # Check for proxy configuration
            proxy_from_env = (
                os.getenv("EVENTHOUSE_PROXY_URL")
                or os.getenv("HTTPS_PROXY")
                or os.getenv("HTTP_PROXY")
            )
            if proxy_from_env or self.config.proxy_url:
                print(f"\n[EVENTHOUSE CONNECTION] Proxy configuration detected:")
                print(f"[EVENTHOUSE CONNECTION]   - Config proxy: {self.config.proxy_url}")
                print(f"[EVENTHOUSE CONNECTION]   - Environment proxy: {proxy_from_env}")

            # Create client (sync client, will execute in thread pool)
            print(f"\n[EVENTHOUSE CONNECTION] Creating KustoClient instance (auth_mode={auth_mode})")
            print(f"[EVENTHOUSE CONNECTION]   - Cluster: {self.config.cluster_url}")
            print(f"[EVENTHOUSE CONNECTION]   - Database: {self.config.database}")
            self._client = KustoClient(kcsb)
            print("[EVENTHOUSE CONNECTION] KustoClient instance created successfully")
            print("[EVENTHOUSE CONNECTION] NOTE: Actual network connection happens on first query")

            # Configure proxy if specified
            if self.config.proxy_url:
                print(f"[EVENTHOUSE CONNECTION] Configuring proxy: {self.config.proxy_url}")
                self._client.set_proxy(self.config.proxy_url)
                logger.info(
                    "Configured proxy for Eventhouse",
                    extra={"proxy_url": self.config.proxy_url},
                )

            print(f"\n[EVENTHOUSE CONNECTION] Client initialization complete!")
            print("="*80 + "\n")
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
            print("\n" + "="*80)
            print("[EVENTHOUSE CONNECTION] FAILED TO CONNECT")
            print("="*80)
            print(f"[EVENTHOUSE CONNECTION] Error type: {type(e).__name__}")
            print(f"[EVENTHOUSE CONNECTION] Error message: {str(e)[:500]}")
            print(f"[EVENTHOUSE CONNECTION] Cluster URL: {self.config.cluster_url}")
            print(f"[EVENTHOUSE CONNECTION] Database: {self.config.database}")
            if hasattr(e, "__cause__") and e.__cause__:
                print(f"[EVENTHOUSE CONNECTION] Caused by: {type(e.__cause__).__name__}")
                print(f"[EVENTHOUSE CONNECTION] Cause message: {str(e.__cause__)[:500]}")
            print("="*80 + "\n")

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
        database: str | None = None,
        timeout_seconds: int | None = None,
    ) -> KQLQueryResult:
        """Execute a KQL query with retry logic."""
        if self._client is None:
            await self.connect()

        db = database or self.config.database
        timeout = timeout_seconds or self.config.query_timeout_seconds
        last_error: Exception | None = None

        print(f"[DEBUG] Executing KQL query on database: {db}")
        print(f"[DEBUG] Query: {query[:200]}{'...' if len(query) > 200 else ''}")
        print(f"[DEBUG] Max retries: {self.config.max_retries}, timeout: {timeout}s")

        for attempt in range(self.config.max_retries):
            print(f"[DEBUG] Query attempt {attempt + 1}/{self.config.max_retries}")
            try:
                print("[DEBUG] Calling _execute_query_impl...")
                result = await self._execute_query_impl(query, db, timeout)
                print(f"[DEBUG] Query succeeded, returned {result.row_count} rows")

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
                print("[DEBUG] Query error (not retryable) - re-raising")
                raise

            except Exception as e:
                last_error = e
                print(f"[DEBUG] Query attempt {attempt + 1} failed: {type(e).__name__}")
                print(f"[DEBUG] Error message: {str(e)[:500]}")

                classified = StorageErrorClassifier.classify_kusto_error(
                    e, {"operation": "execute_query", "attempt": attempt + 1}
                )
                print(f"[DEBUG] Error classified as: {type(classified).__name__}")
                print(f"[DEBUG] Is retryable: {classified.is_retryable}")
                print(f"[DEBUG] Should refresh auth: {classified.should_refresh_auth}")

                # Clear all credential caches on auth errors to force token re-read
                if classified.should_refresh_auth:
                    print("[DEBUG] Refreshing credentials due to auth error")
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
                    print("[DEBUG] Error is not retryable - raising classified error")
                    raise classified from e

                # Check if we have more retries
                if attempt + 1 >= self.config.max_retries:
                    print(f"[DEBUG] Max retries ({self.config.max_retries}) exhausted")
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

                print(f"[DEBUG] Will retry after {delay}s delay")
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
        """Execute query implementation (runs in thread pool)."""
        start_time = time.perf_counter()

        print("\n" + "-"*80)
        print("[QUERY EXECUTION] Starting KQL query execution")
        print("-"*80)
        print(f"[QUERY EXECUTION] Cluster: {self.config.cluster_url}")
        print(f"[QUERY EXECUTION] Database: {database}")
        print(f"[QUERY EXECUTION] Query: {query[:200]}{'...' if len(query) > 200 else ''}")
        print(f"[QUERY EXECUTION] Timeout: {timeout_seconds}s")
        print("-"*80)

        try:
            # Execute in thread pool since KustoClient is sync
            print("[QUERY EXECUTION] Making network request to Kusto endpoint...")
            print("[QUERY EXECUTION] This will:")
            print("[QUERY EXECUTION]   1. Authenticate using configured credentials")
            print("[QUERY EXECUTION]   2. Establish TCP connection to cluster")
            print("[QUERY EXECUTION]   3. Execute the query")
            print("[QUERY EXECUTION] Waiting for response...")
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.execute(database, query),
            )
            print("[QUERY EXECUTION] Network request completed successfully!")

            query_duration_ms = (time.perf_counter() - start_time) * 1000
            print(f"[DEBUG] Query executed in {query_duration_ms:.0f}ms")

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

            print("\n" + "!"*80)
            print(f"[QUERY EXECUTION] KustoServiceError after {query_duration_ms:.0f}ms")
            print("!"*80)
            print(f"[QUERY EXECUTION] Error type: {type(e).__name__}")
            print(f"[QUERY EXECUTION] Error message: {str(e)[:1000]}")
            print(f"[QUERY EXECUTION] Cluster: {self.config.cluster_url}")
            print(f"[QUERY EXECUTION] Database: {database}")

            # Extract detailed error info from KustoServiceError
            error_details = {}
            try:
                # Try to get structured error info if available
                if hasattr(e, "get_api_errors"):
                    api_errors = e.get_api_errors()
                    if api_errors:
                        error_details["api_errors"] = str(api_errors)[:500]
                        print(f"[DEBUG] API errors: {str(api_errors)[:500]}")
                if hasattr(e, "http_response") and e.http_response:
                    error_details["http_status"] = getattr(
                        e.http_response, "status_code", None
                    ) or getattr(e.http_response, "status", None)
                    print(f"[DEBUG] HTTP status: {error_details.get('http_status')}")

                # Try to extract more details from the exception
                if hasattr(e, "args") and e.args:
                    print(f"[DEBUG] Exception args: {e.args}")
                if hasattr(e, "__cause__") and e.__cause__:
                    print(f"[DEBUG] Caused by: {type(e.__cause__).__name__}: {str(e.__cause__)[:500]}")

            except Exception as introspection_error:
                print(f"[DEBUG] Error during exception introspection: {introspection_error}")

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
                print("[DEBUG] Classified as query syntax/semantic error")
                raise KustoQueryError(
                    f"KQL query error: {e}",
                    cause=e,
                    context={"database": database, "query_length": len(query)},
                ) from e

            print("[DEBUG] Re-raising KustoServiceError")
            raise

        except Exception as e:
            query_duration_ms = (time.perf_counter() - start_time) * 1000

            print(f"[DEBUG] Exception occurred after {query_duration_ms:.0f}ms")
            print(f"[DEBUG] Exception type: {type(e).__name__}")
            print(f"[DEBUG] Exception string: {str(e)[:1000]}")

            # Try to get more details
            if hasattr(e, "__cause__") and e.__cause__:
                print(f"[DEBUG] Caused by: {type(e.__cause__).__name__}: {str(e.__cause__)[:500]}")
            if hasattr(e, "args") and e.args:
                print(f"[DEBUG] Exception args: {e.args}")

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

            print("[DEBUG] Re-raising exception")
            raise
