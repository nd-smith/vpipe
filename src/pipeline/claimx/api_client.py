"""ClaimX REST API client with circuit breaker protection and rate limiting."""

import asyncio
import logging
from datetime import datetime
from typing import Any

import aiohttp

from core.resilience.circuit_breaker import (
    CLAIMX_API_CIRCUIT_CONFIG,
    get_circuit_breaker,
)
from core.resilience.rate_limiter import (
    CLAIMX_API_RATE_CONFIG,
    get_rate_limiter,
)
from core.types import ErrorCategory

logger = logging.getLogger(__name__)


class ClaimXApiError(Exception):
    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        category: ErrorCategory = ErrorCategory.TRANSIENT,
        is_retryable: bool = True,
        should_refresh_auth: bool = False,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.category = category
        self.is_retryable = is_retryable
        self.should_refresh_auth = should_refresh_auth


def classify_api_error(status: int, url: str) -> ClaimXApiError:
    """Classify HTTP status codes into error categories with appropriate retry/category flags."""
    if status == 401:
        return ClaimXApiError(
            f"Unauthorized (401): {url}",
            status_code=status,
            category=ErrorCategory.AUTH,
            is_retryable=False,
            should_refresh_auth=False,
        )

    if status == 403:
        return ClaimXApiError(
            f"Forbidden (403): {url}",
            status_code=status,
            category=ErrorCategory.PERMANENT,
            is_retryable=False,
        )

    if status == 404:
        return ClaimXApiError(
            f"Not found (404): {url}",
            status_code=status,
            category=ErrorCategory.PERMANENT,
            is_retryable=False,
        )

    if status == 429:
        return ClaimXApiError(
            f"Rate limited (429): {url}",
            status_code=status,
            category=ErrorCategory.TRANSIENT,
            is_retryable=True,
        )

    if status in (500, 502, 503, 504):
        return ClaimXApiError(
            f"Server error ({status}): {url}",
            status_code=status,
            category=ErrorCategory.TRANSIENT,
            is_retryable=True,
        )

    if 400 <= status < 500:
        return ClaimXApiError(
            f"Client error ({status}): {url}",
            status_code=status,
            category=ErrorCategory.PERMANENT,
            is_retryable=False,
        )

    return ClaimXApiError(
        f"HTTP error ({status}): {url}",
        status_code=status,
        category=ErrorCategory.TRANSIENT,
        is_retryable=True,
    )


class ClaimXApiClient:
    """Async client for ClaimX REST API with circuit breaker and rate limiting."""

    def __init__(
        self,
        base_url: str,
        token: str,
        timeout_seconds: int = 30,
        max_concurrent: int = 20,
        sender_username: str = "user@example.com",
    ):
        self.base_url = base_url.rstrip("/") if base_url else ""

        if not self.base_url:
            raise ValueError(
                "ClaimXApiClient requires 'base_url'. "
                "Set CLAIMX_API_BASE_PATH environment variable or configure claimx_api.base_url in config."
            )

        # Validate base_url has a scheme (http/https)
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError(
                f"ClaimXApiClient base_url must start with http:// or https://, got: {self.base_url!r}. "
                "Set CLAIMX_API_BASE_PATH environment variable or configure claimx_api.base_url in config."
            )

        if not token:
            raise ValueError("ClaimXApiClient requires 'token'")

        self._auth_header = f"Basic {token}"
        self.timeout_seconds = timeout_seconds
        self.max_concurrent = max_concurrent
        self.sender_username = sender_username

        self._session: aiohttp.ClientSession | None = None
        self._semaphore: asyncio.Semaphore | None = None
        self._circuit = get_circuit_breaker("claimx_api", CLAIMX_API_CIRCUIT_CONFIG)
        self._rate_limiter = get_rate_limiter("claimx_api", CLAIMX_API_RATE_CONFIG)

        # Log configuration at startup for debugging
        logger.info(
            "ClaimXApiClient initialized",
            extra={
                "base_url": self.base_url,
                "timeout_seconds": self.timeout_seconds,
                "max_concurrent": self.max_concurrent,
            },
        )

    async def __aenter__(self) -> "ClaimXApiClient":
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def _ensure_session(self) -> None:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent,
                limit_per_host=self.max_concurrent,
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            self._semaphore = asyncio.Semaphore(self.max_concurrent)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        _auth_retry: bool = False,
    ) -> dict[str, Any]:
        await self._ensure_session()

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        logger.debug(
            "API request starting",
            extra={
                "api_endpoint": endpoint,
                "api_method": method,
                "api_url": url,
                "has_params": params is not None,
                "has_body": json_body is not None,
            },
        )

        if self._circuit.is_open:
            retry_after = self._circuit._get_retry_after()
            error = ClaimXApiError(
                f"Circuit open, retry after {retry_after:.0f}s",
                category=ErrorCategory.CIRCUIT_OPEN,
                is_retryable=True,
            )
            logger.warning(
                "Circuit breaker open - request rejected",
                extra={
                    "api_endpoint": endpoint,
                    "api_method": method,
                    "api_url": url,
                    "circuit_state": "open",
                    "retry_after_seconds": round(retry_after, 1),
                },
            )
            raise error

        request_headers = {"Authorization": self._auth_header}

        # Rate limiting - controls throughput to prevent hitting API rate limits
        await self._rate_limiter.acquire()

        async with self._semaphore:
            start_time = asyncio.get_event_loop().time()
            try:
                if self._session is None:
                    raise RuntimeError(
                        "HTTP session not initialized - call _ensure_session() first"
                    )
                async with self._session.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=request_headers,
                    timeout=aiohttp.ClientTimeout(total=self.timeout_seconds),
                ) as response:
                    duration = asyncio.get_event_loop().time() - start_time

                    if response.status != 200:
                        try:
                            response_body = await response.text()
                            response_body_log = (
                                response_body[:500] + "..."
                                if len(response_body) > 500
                                else response_body
                            )
                        except Exception:
                            response_body_log = "<unable to read response body>"

                        error = classify_api_error(response.status, url)

                        if error.is_retryable:
                            self._circuit.record_failure(error)
                        logger.warning(
                            "API request failed",
                            extra={
                                "api_endpoint": endpoint,
                                "api_method": method,
                                "api_url": url,
                                "http_status": response.status,
                                "error_category": error.category.value,
                                "is_retryable": error.is_retryable,
                                "response_body": response_body_log,
                                "duration_seconds": round(duration, 3),
                            },
                        )
                        raise error

                    self._circuit.record_success()

                    data = await response.json()

                    logger.debug(
                        "API request succeeded",
                        extra={
                            "api_endpoint": endpoint,
                            "api_method": method,
                            "http_status": response.status,
                            "duration_seconds": round(duration, 3),
                        },
                    )

                    return data

            except TimeoutError as e:
                duration = asyncio.get_event_loop().time() - start_time
                error = ClaimXApiError(
                    f"Timeout after {self.timeout_seconds}s: {url}",
                    category=ErrorCategory.TRANSIENT,
                    is_retryable=True,
                )
                self._circuit.record_failure(error)
                logger.warning(
                    "API request timeout",
                    extra={
                        "api_endpoint": endpoint,
                        "api_method": method,
                        "api_url": url,
                        "timeout_seconds": self.timeout_seconds,
                        "duration_seconds": round(duration, 3),
                        "error_category": "transient",
                        "is_retryable": True,
                    },
                )
                raise error from e

            except aiohttp.ClientError as e:
                duration = asyncio.get_event_loop().time() - start_time
                error = ClaimXApiError(
                    f"Connection error: {e}",
                    category=ErrorCategory.TRANSIENT,
                    is_retryable=True,
                )
                self._circuit.record_failure(error)
                logger.error(
                    "API connection error",
                    exc_info=True,
                    extra={
                        "api_endpoint": endpoint,
                        "api_method": method,
                        "api_url": url,
                        "duration_seconds": round(duration, 3),
                        "error_category": "transient",
                        "is_retryable": True,
                    },
                )
                raise error from e

    async def get_project(self, project_id: int) -> dict[str, Any]:
        """Get full project details. Used for PROJECT_CREATED, PROJECT_MFN_ADDED events."""
        return await self._request("GET", f"/export/project/{project_id}")

    async def get_project_id_by_claim_number(self, claim_number: str) -> int | None:
        """
        Get ClaimX project ID from claim number.

        Useful for Verisk domain events that have claim number but need ClaimX project ID.

        Args:
            claim_number: The claim number (e.g., "ABC123456")

        Returns:
            ClaimX project ID if found, None otherwise
        """
        response = await self._request(
            "GET",
            "/export/project/projectId",
            params={"projectNumber": claim_number},  # API uses projectNumber param
        )

        # API may return just the ID as a number, or in a dict
        if isinstance(response, int):
            return response
        elif isinstance(response, dict):
            return response.get("projectId") or response.get("id")

        return None

    async def get_project_media(
        self,
        project_id: int,
        media_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        """Get media metadata for a project. Used for PROJECT_FILE_ADDED events."""
        params = {}
        if media_ids:
            params["mediaIds"] = ",".join(str(m) for m in media_ids)

        response = await self._request(
            "GET",
            f"/export/project/{project_id}/media",
            params=params if params else None,
        )

        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            if "data" in response:
                return response["data"]
            if "media" in response:
                return response["media"]
            return [response]
        return []

    async def get_project_contacts(self, project_id: int) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            f"/export/project/{project_id}/contacts",
        )

        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            if "data" in response:
                return response["data"]
            if "contacts" in response:
                return response["contacts"]
            return [response]
        return []

    async def get_custom_task(self, assignment_id: int) -> dict[str, Any]:
        """Get custom task assignment. Used for CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED events."""
        return await self._request(
            "GET",
            f"/customTasks/assignment/{assignment_id}",
            params={"full": "true"},
        )

    async def get_project_tasks(self, project_id: int) -> list[dict[str, Any]]:
        body = {
            "reportType": "CUSTOM_TASK_HIGH_LEVEL",
            "projectId": project_id,
            "senderUsername": self.sender_username,
        }

        response = await self._request("POST", "/data", json_body=body)

        if isinstance(response, dict) and "data" in response:
            return response["data"]
        return []

    async def get_video_collaboration(
        self,
        project_id: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        sender_username: str | None = None,
    ) -> dict[str, Any]:
        """Get video collaboration report. Used for VIDEO_COLLABORATION_INVITE_SENT, VIDEO_COLLABORATION_COMPLETED events."""
        body: dict[str, Any] = {
            "reportType": "VIDEO_COLLABORATION",
            "projectId": int(project_id),
            "senderUsername": sender_username or self.sender_username,
        }

        if start_date:
            body["startDate"] = start_date.isoformat()
        if end_date:
            body["endDate"] = end_date.isoformat()

        return await self._request("POST", "/data", json_body=body)

    async def get_project_conversations(self, project_id: int) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            f"/export/project/{project_id}/conversations",
        )

        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            if "data" in response:
                return response["data"]
            if "conversations" in response:
                return response["conversations"]
            return [response]
        return []

    def get_circuit_status(self) -> dict[str, Any]:
        return self._circuit.get_diagnostics()

    @property
    def is_circuit_open(self) -> bool:
        return self._circuit.is_open
