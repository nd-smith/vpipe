"""
Plugin Connection Management

Provides infrastructure for managing outgoing HTTP connections used by plugins
and plugin workers. Supports named connection definitions with authentication,
retry policies, and lifecycle management.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import aiohttp

from core.errors.exceptions import TransientError
from core.resilience.retry import RetryConfig, with_retry_async

logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Authentication types for HTTP connections."""

    NONE = "none"
    BEARER = "bearer"
    API_KEY = "api_key"
    BASIC = "basic"


def is_http_error(status_code: int) -> bool:
    """Check if HTTP status code represents an error.

    Args:
        status_code: HTTP status code to check

    Returns:
        True if status code is outside the 2xx success range (< 200 or >= 300)
    """
    return status_code < 200 or status_code >= 300


@dataclass
class ConnectionConfig:
    """Configuration for a named HTTP connection.

    Attributes:
        name: Unique identifier for this connection
        base_url: Base URL for API endpoint (e.g., https://api.example.com)
        auth_type: Authentication method to use
        auth_token: Token/key for authentication (can use env var reference)
        auth_header: Header name for auth (default: Authorization for bearer, X-API-Key for api_key)
        timeout_seconds: Request timeout in seconds
        max_retries: Maximum number of retry attempts for failed requests
        retry_backoff_base: Base for exponential backoff (seconds)
        retry_backoff_max: Maximum backoff between retries (seconds)
        headers: Additional headers to include in all requests
    """

    name: str
    base_url: str
    auth_type: AuthType = AuthType.NONE
    auth_token: str | None = None
    auth_header: str | None = None
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_base: int = 2
    retry_backoff_max: int = 60
    headers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        """Validate and normalize configuration."""
        # Ensure base_url doesn't have trailing slash
        self.base_url = self.base_url.rstrip("/")

        # Set default auth header if not provided
        if self.auth_header is None:
            if self.auth_type in (AuthType.BEARER, AuthType.BASIC):
                self.auth_header = "Authorization"
            elif self.auth_type == AuthType.API_KEY:
                self.auth_header = "X-API-Key"

        # Convert string enum to AuthType if needed
        if isinstance(self.auth_type, str):
            self.auth_type = AuthType(self.auth_type)


class ConnectionManager:
    """Manages HTTP client connections for plugins.

    Provides a shared aiohttp ClientSession with connection pooling,
    authentication, and retry logic. Supports multiple named connections
    to different API endpoints.

    Usage:
        manager = ConnectionManager()
        manager.add_connection(ConnectionConfig(
            name="external_api",
            base_url="https://api.example.com",
            auth_type=AuthType.BEARER,
            auth_token="secret-token"
        ))

        await manager.start()

        response = await manager.request(
            connection_name="external_api",
            method="POST",
            path="/v1/events",
            json={"event": "data"}
        )

        await manager.close()
    """

    def __init__(self, connector_limit: int = 100, connector_limit_per_host: int = 30):
        """Initialize connection manager.

        Args:
            connector_limit: Total connection pool size
            connector_limit_per_host: Max connections per host
        """
        self._connections: dict[str, ConnectionConfig] = {}
        self._session: aiohttp.ClientSession | None = None
        self._connector_limit = connector_limit
        self._connector_limit_per_host = connector_limit_per_host
        self._started = False

    def add_connection(self, config: ConnectionConfig) -> None:
        """Register a named connection configuration.

        Args:
            config: Connection configuration

        Raises:
            ValueError: If connection with same name already exists
        """
        if config.name in self._connections:
            raise ValueError(f"Connection '{config.name}' already exists")

        logger.info(
            f"Registered connection '{config.name}' -> {config.base_url} "
            f"(auth: {config.auth_type.value})"
        )
        self._connections[config.name] = config

    def get_connection(self, name: str) -> ConnectionConfig:
        """Get connection configuration by name.

        Args:
            name: Connection name

        Returns:
            Connection configuration

        Raises:
            KeyError: If connection not found
        """
        if name not in self._connections:
            raise KeyError(
                f"Connection '{name}' not found. "
                f"Available: {list(self._connections.keys())}"
            )
        return self._connections[name]

    async def _execute_request(
        self,
        method: str,
        url: str,
        request_headers: dict[str, str],
        timeout: int,
        json: dict[str, Any] | None,
        data: Any | None,
        params: dict[str, Any] | None,
    ) -> aiohttp.ClientResponse:
        """
        Execute single HTTP request with error classification.

        This method is decorated with retry logic. On 5xx error, raises
        TransientError to trigger retry.
        """
        async with self._session.request(
            method=method,
            url=url,
            json=json,
            data=data,
            params=params,
            headers=request_headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as response:
            logger.debug(f"Response {response.status} from {method} {url}")

            # Classify 5xx errors as transient for retry
            if response.status >= 500:
                body = await response.text()
                logger.warning(
                    f"Server error {response.status} from {url}, will retry. "
                    f"Body: {body[:200]}"
                )
                raise TransientError(
                    f"HTTP {response.status}: {body[:200]}",
                    context={"status_code": response.status, "url": url},
                )

            # Read body before context manager closes
            await response.read()
            return response

    async def start(self) -> None:
        """Initialize HTTP client session.

        Must be called before making requests.
        """
        if self._started:
            logger.warning("ConnectionManager already started")
            return

        connector = aiohttp.TCPConnector(
            limit=self._connector_limit,
            limit_per_host=self._connector_limit_per_host,
            enable_cleanup_closed=True,
        )

        self._session = aiohttp.ClientSession(
            connector=connector,
            raise_for_status=False,  # We handle status codes ourselves
            timeout=aiohttp.ClientTimeout(total=None),  # Per-request timeout
        )

        self._started = True
        logger.info(
            f"ConnectionManager started with {len(self._connections)} connections"
        )

    async def close(self) -> None:
        """Close HTTP client session and cleanup resources."""
        if not self._started:
            return

        if self._session:
            await self._session.close()
            # Give time for connections to close
            await asyncio.sleep(0.250)

        self._started = False
        logger.info("ConnectionManager closed")

    async def request(
        self,
        connection_name: str,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
        data: Any | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout_override: int | None = None,
        retry_override: int | None = None,
    ) -> aiohttp.ClientResponse:
        """Make HTTP request using named connection.

        Args:
            connection_name: Name of connection to use
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            path: API path (e.g., /v1/events)
            json: JSON body to send
            data: Raw body to send (alternative to json)
            params: Query parameters
            headers: Additional headers (merged with connection headers)
            timeout_override: Override connection timeout for this request
            retry_override: Override connection max_retries for this request

        Returns:
            aiohttp response object

        Raises:
            RuntimeError: If manager not started
            KeyError: If connection not found
            aiohttp.ClientError: If request fails after retries
        """
        if not self._started:
            raise RuntimeError("ConnectionManager not started. Call start() first.")

        config = self.get_connection(connection_name)

        # Build full URL
        url = f"{config.base_url}{path}"

        # Merge headers
        request_headers = {**config.headers}
        if headers:
            request_headers.update(headers)

        # Add authentication
        if config.auth_type == AuthType.BEARER and config.auth_token:
            request_headers[config.auth_header] = f"Bearer {config.auth_token}"
        elif config.auth_type == AuthType.API_KEY and config.auth_token:
            request_headers[config.auth_header] = config.auth_token
        elif config.auth_type == AuthType.BASIC and config.auth_token:
            # Expect token to be base64 encoded "username:password"
            request_headers[config.auth_header] = f"Basic {config.auth_token}"

        # Determine timeout and retries
        timeout = (
            timeout_override if timeout_override is not None else config.timeout_seconds
        )
        max_retries = (
            retry_override if retry_override is not None else config.max_retries
        )

        # Create retry configuration
        retry_config = RetryConfig(
            max_attempts=max_retries,
            base_delay=config.retry_backoff_base,
            max_delay=config.retry_backoff_max,
            exponential_base=2.0,
            respect_permanent=True,
            respect_retry_after=True,
        )

        # Apply retry decorator to helper
        retryable_request = with_retry_async(
            config=retry_config,
            wrap_errors=True,
        )(self._execute_request)

        # Execute request with retry
        logger.debug(f"Request {method} {url}")
        return await retryable_request(
            method=method,
            url=url,
            request_headers=request_headers,
            timeout=timeout,
            json=json,
            data=data,
            params=params,
        )

    async def request_json(
        self,
        connection_name: str,
        method: str,
        path: str,
        **kwargs,
    ) -> tuple[int, dict[str, Any]]:
        """Convenience method to make request and parse JSON response.

        Args:
            connection_name: Name of connection to use
            method: HTTP method
            path: API path
            **kwargs: Additional arguments to pass to request()

        Returns:
            Tuple of (status_code, response_json)

        Raises:
            aiohttp.ContentTypeError: If response is not JSON
        """
        response = await self.request(
            connection_name=connection_name,
            method=method,
            path=path,
            **kwargs,
        )

        status = response.status
        body = await response.json()

        return status, body

    def list_connections(self) -> list[str]:
        """Get list of registered connection names."""
        return list(self._connections.keys())

    @property
    def is_started(self) -> bool:
        """Check if manager is started."""
        return self._started
