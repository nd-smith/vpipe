"""
Authentication module.

Provides unified authentication for Azure services and Kafka.

Components:
    - TokenCache: Thread-safe token caching with expiry checks (WP-102 ✓)
    - Azure credential abstraction (CLI, SPN, managed identity) (WP-103 ✓)
    - Token refresh logic (WP-103 ✓)
    - Kafka OAUTHBEARER callback for SASL authentication (WP-104)

Review checklist for WP-102:
    [x] Token refresh timing (50min buffer for 60min tokens - CORRECT)
    [x] Thread safety of token cache (threading.Lock added)
    [x] Proper timezone handling (UTC throughout)

Review checklist for WP-103:
    [x] All auth methods work correctly (CLI, SPN secret/cert, DefaultAzureCredential)
    [x] Error messages are actionable
    [x] No secrets logged
"""

from .token_cache import TokenCache, CachedToken, TOKEN_REFRESH_MINS, TOKEN_EXPIRY_MINS
from .credentials import (
    AzureAuthError,
    AzureCredentialProvider,
    get_default_provider,
    get_storage_options,
    clear_token_cache,
    STORAGE_RESOURCE,
    STORAGE_SCOPE,
)
from .kafka_oauth import (
    create_kafka_oauth_callback,
    get_kafka_oauth_token,
    KafkaOAuthError,
    EVENTHUB_RESOURCE,
    EVENTHUB_SCOPE,
)

__all__ = [
    # Token cache (WP-102)
    "TokenCache",
    "CachedToken",
    "TOKEN_REFRESH_MINS",
    "TOKEN_EXPIRY_MINS",
    # Azure credentials (WP-103)
    "AzureAuthError",
    "AzureCredentialProvider",
    "get_default_provider",
    "get_storage_options",
    "clear_token_cache",
    "STORAGE_RESOURCE",
    "STORAGE_SCOPE",
    # Kafka OAuth (WP-104)
    "create_kafka_oauth_callback",
    "get_kafka_oauth_token",
    "KafkaOAuthError",
    "EVENTHUB_RESOURCE",
    "EVENTHUB_SCOPE",
]
