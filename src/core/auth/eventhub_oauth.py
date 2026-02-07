"""
Azure EventHub SASL/OAUTHBEARER authentication.

This module provides OAuth token callback functions for aiokafka clients
connecting to Azure EventHub with SASL/OAUTHBEARER authentication.

The callback uses Azure AD authentication (via Service Principal or other
credential methods) to obtain OAuth tokens for EventHub access.

Example:
    >>> from core.auth import AzureCredentialProvider
    >>> from core.auth.eventhub_oauth import create_eventhub_oauth_callback
    >>>
    >>> provider = AzureCredentialProvider(
    ...     client_id="...",
    ...     client_secret="...",
    ...     tenant_id="..."
    ... )
    >>> callback = create_eventhub_oauth_callback(provider)
    >>>
    >>> # Use with aiokafka
    >>> from aiokafka import AIOKafkaProducer
    >>> producer = AIOKafkaProducer(
    ...     bootstrap_servers=["..."],
    ...     security_protocol="SASL_SSL",
    ...     sasl_mechanism="OAUTHBEARER",
    ...     sasl_oauth_token_provider=callback,
    ... )

Security Notes:
    - Tokens are cached by the underlying AzureCredentialProvider
    - The callback does not log tokens or credentials
    - Uses Azure AD authentication with configurable credential sources
"""

import logging
from collections.abc import Callable

from core.auth.credentials import AzureAuthError, AzureCredentialProvider

logger = logging.getLogger(__name__)


# Azure EventHub resource URL for OAuth token requests
EVENTHUB_RESOURCE = "https://eventhubs.azure.net/"
EVENTHUB_SCOPE = "https://eventhubs.azure.net/.default"


class EventHubAuthError(Exception):
    """
    Raised when EventHub OAuth token acquisition fails.

    This exception indicates authentication issues specific to EventHub
    that may require reconfiguration or credential refresh.
    """

    pass


def create_eventhub_oauth_callback(
    provider: AzureCredentialProvider | None = None,
) -> Callable[[], str]:
    """
    Create EventHub OAUTHBEARER token callback for aiokafka.

    The returned callback function can be passed to aiokafka's
    `sasl_oauth_token_provider` parameter for OAUTHBEARER authentication.

    Args:
        provider: AzureCredentialProvider instance. If None, creates a new
                 provider that loads configuration from environment variables.

    Returns:
        Callable that returns OAuth token string when invoked

    Raises:
        EventHubAuthError: If token acquisition fails

    Example:
        >>> # With explicit provider
        >>> provider = AzureCredentialProvider(
        ...     client_id="...",
        ...     client_secret="...",
        ...     tenant_id="..."
        ... )
        >>> callback = create_eventhub_oauth_callback(provider)

        >>> # With environment variables
        >>> # (requires AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
        >>> callback = create_eventhub_oauth_callback()

    Note:
        The callback captures the provider instance, so the same provider
        and its token cache are reused across multiple token requests.
    """
    if provider is None:
        # Create provider from environment variables
        provider = AzureCredentialProvider()
        logger.info(
            "Created EventHub OAuth callback with environment-based auth",
            extra={"auth_mode": provider.auth_mode},
        )
    else:
        logger.info(
            "Created EventHub OAuth callback with provided credential provider",
            extra={"auth_mode": provider.auth_mode},
        )

    def oauth_callback() -> str:
        """
        Get OAuth token for EventHub authentication.

        This function is called by aiokafka when authentication is needed.
        It obtains a token from Azure AD for the EventHub resource.

        Returns:
            OAuth access token string

        Raises:
            EventHubAuthError: If token acquisition fails
        """
        try:
            token = provider.get_token_for_resource(EVENTHUB_RESOURCE)

            logger.debug(
                "Successfully acquired EventHub OAuth token",
                extra={"resource": EVENTHUB_RESOURCE, "auth_mode": provider.auth_mode},
            )

            return token

        except AzureAuthError as e:
            logger.error(
                "Failed to acquire EventHub OAuth token",
                extra={
                    "resource": EVENTHUB_RESOURCE,
                    "auth_mode": provider.auth_mode,
                    "error": str(e),
                },
            )
            raise EventHubAuthError(
                f"Failed to acquire OAuth token for EventHub\n"
                f"Auth mode: {provider.auth_mode}\n"
                f"Error: {str(e)}"
            ) from e
        except Exception as e:
            logger.error(
                "Unexpected error in EventHub OAuth callback",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise EventHubAuthError(
                f"Unexpected error acquiring EventHub OAuth token: {str(e)}"
            ) from e

    return oauth_callback


def get_eventhub_oauth_token(
    provider: AzureCredentialProvider | None = None, force_refresh: bool = False
) -> str:
    """
    Get EventHub OAuth token directly (non-callback usage).

    This is a convenience function for cases where you need the token
    directly rather than as a callback. For aiokafka, prefer using
    create_eventhub_oauth_callback().

    Args:
        provider: AzureCredentialProvider instance. If None, creates new provider.
        force_refresh: Skip cache and fetch fresh token

    Returns:
        OAuth access token string

    Raises:
        EventHubAuthError: If token acquisition fails

    Example:
        >>> token = get_eventhub_oauth_token()
        >>> # Use token in custom authentication flow
    """
    if provider is None:
        provider = AzureCredentialProvider()

    try:
        return provider.get_token_for_resource(EVENTHUB_RESOURCE, force_refresh)
    except AzureAuthError as e:
        raise EventHubAuthError(f"Failed to get EventHub OAuth token: {str(e)}") from e


__all__ = [
    "create_eventhub_oauth_callback",
    "get_eventhub_oauth_token",
    "EventHubAuthError",
    "EVENTHUB_RESOURCE",
    "EVENTHUB_SCOPE",
]
