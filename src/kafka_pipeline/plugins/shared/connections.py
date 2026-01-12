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
from typing import Any, Optional

import aiohttp
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Supported authentication types for HTTP connections."""

    NONE = "none"
    BEARER = "bearer"
    API_KEY = "api_key"
    BASIC = "basic"


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
    auth_token: Optional[str] = None
    auth_header: Optional[str] = None
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
            if self.auth_type == AuthType.BEARER:
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
        self._session: Optional[aiohttp.ClientSession] = None
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
        json: Optional[dict[str, Any]] = None,
        data: Optional[Any] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        timeout_override: Optional[int] = None,
        retry_override: Optional[int] = None,
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
        timeout = timeout_override if timeout_override is not None else config.timeout_seconds
        max_retries = retry_override if retry_override is not None else config.max_retries

        # Execute with retry logic
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(
                multiplier=config.retry_backoff_base,
                max=config.retry_backoff_max,
            ),
            retry=retry_if_exception_type(
                (
                    aiohttp.ClientConnectionError,
                    aiohttp.ServerTimeoutError,
                    asyncio.TimeoutError,
                )
            ),
            reraise=True,
        ):
            with attempt:
                logger.debug(
                    f"Request {method} {url} (attempt {attempt.retry_state.attempt_number})"
                )

                async with self._session.request(
                    method=method,
                    url=url,
                    json=json,
                    data=data,
                    params=params,
                    headers=request_headers,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as response:
                    # Log response
                    logger.debug(
                        f"Response {response.status} from {method} {url} "
                        f"in {response.request_info.headers.get('X-Request-Duration', 'N/A')}"
                    )

                    # For 5xx errors, retry if we have attempts left
                    if response.status >= 500 and attempt.retry_state.attempt_number < max_retries:
                        body = await response.text()
                        logger.warning(
                            f"Server error {response.status} from {url}, will retry. "
                            f"Body: {body[:200]}"
                        )
                        raise aiohttp.ServerTimeoutError()

                    # For other errors or final attempt, return as-is
                    # Caller decides how to handle status codes
                    return response

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


# Singleton instance for global access
_global_connection_manager: Optional[ConnectionManager] = None


def get_connection_manager() -> ConnectionManager:
    """Get global connection manager instance.

    Returns:
        Global ConnectionManager instance
    """
    global _global_connection_manager
    if _global_connection_manager is None:
        _global_connection_manager = ConnectionManager()
    return _global_connection_manager


def set_connection_manager(manager: ConnectionManager) -> None:
    """Set global connection manager instance.

    Args:
        manager: ConnectionManager instance to use globally
    """
    global _global_connection_manager
    _global_connection_manager = manager
