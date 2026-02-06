# OAuth2 Token Manager

Intelligent OAuth2 token management with automatic caching and refresh for plugins.

## Features

- **Multiple Providers**: Support for Azure AD and generic OAuth2 servers
- **Intelligent Caching**: Tokens cached with automatic expiration tracking
- **Automatic Refresh**: Tokens refreshed before expiration (5-minute buffer)
- **Thread-Safe**: Safe for concurrent access from multiple plugins/workers
- **Simple API**: Easy integration with existing code

## Quick Start

### Azure AD Provider

```python
import os
from core.oauth2 import OAuth2TokenManager, AzureADProvider

# Create manager
manager = OAuth2TokenManager()

# Add Azure AD provider
azure_provider = AzureADProvider(
    provider_name="azure_storage",
    client_id=os.getenv("AZURE_CLIENT_ID"),
    client_secret=os.getenv("AZURE_CLIENT_SECRET"),
    tenant_id=os.getenv("AZURE_TENANT_ID"),
    scopes=["https://storage.azure.com/.default"]
)
manager.add_provider(azure_provider)

# Get token (automatically cached and refreshed)
token = await manager.get_token("azure_storage")

# Use in HTTP request
headers = {"Authorization": f"Bearer {token}"}
```

### Generic OAuth2 Provider

```python
from core.oauth2 import OAuth2TokenManager, GenericOAuth2Provider, OAuth2Config

# Create configuration
config = OAuth2Config(
    provider_name="external_api",
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    token_url="https://auth.example.com/oauth/token",
    scope="read write"
)

# Create provider
provider = GenericOAuth2Provider(config)

# Add to manager
manager = OAuth2TokenManager()
manager.add_provider(provider)

# Get token
token = await manager.get_token("external_api")
```

### Using Default Singleton

```python
from core.oauth2 import get_default_manager, AzureADProvider

# Get singleton instance
manager = get_default_manager()

# Configure once, use everywhere
manager.add_provider(azure_provider)

# In any module
from core.oauth2 import get_default_manager

token = await get_default_manager().get_token("azure_storage")
```

## Plugin Integration

### Example: Plugin Worker

```python
from core.oauth2 import OAuth2TokenManager, AzureADProvider
from pipeline.plugins.shared.base import Plugin, PluginContext

class MyAPIPlugin(Plugin):
    def __init__(self, config):
        super().__init__(config)

        # Create OAuth2 manager
        self.oauth_manager = OAuth2TokenManager()

        # Add provider
        azure_provider = AzureADProvider(
            provider_name="my_api",
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            tenant_id=config["tenant_id"],
            scopes=config.get("scopes", ["https://management.azure.com/.default"])
        )
        self.oauth_manager.add_provider(azure_provider)

    async def process(self, context: PluginContext):
        # Get token automatically
        token = await self.oauth_manager.get_token("my_api")

        # Make API request
        headers = {"Authorization": f"Bearer {token}"}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.example.com/data",
                headers=headers
            ) as response:
                data = await response.json()

        # Process data...
```

### Example: With ConnectionManager

```python
from core.oauth2 import get_default_manager
from pipeline.plugins.shared.connections import ConnectionManager, ConnectionConfig, AuthType

# Setup OAuth2 manager
oauth_manager = get_default_manager()
oauth_manager.add_provider(azure_provider)

# Create connection manager
conn_manager = ConnectionManager()

# For OAuth2, get token dynamically in your request
async def make_request():
    token = await oauth_manager.get_token("my_api")

    # Use with ConnectionManager by passing as header
    response = await conn_manager.request(
        connection_name="my_connection",
        method="GET",
        path="/api/data",
        headers={"Authorization": f"Bearer {token}"}
    )
    return response
```

## Configuration

### Environment Variables

```bash
# Azure AD
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
export AZURE_TENANT_ID="your-tenant-id"

# Generic OAuth2
export OAUTH2_CLIENT_ID="your-client-id"
export OAUTH2_CLIENT_SECRET="your-client-secret"
export OAUTH2_TOKEN_URL="https://auth.example.com/oauth/token"
export OAUTH2_SCOPE="read write"
```

### YAML Configuration (for plugins)

```yaml
# config/plugins/my_plugin/config.yaml
oauth2:
  provider_name: "my_api"
  provider_type: "azure"  # or "generic"
  client_id: ${AZURE_CLIENT_ID}
  client_secret: ${AZURE_CLIENT_SECRET}
  tenant_id: ${AZURE_TENANT_ID}
  scopes:
    - "https://management.azure.com/.default"
```

## API Reference

### OAuth2TokenManager

Main token management class.

**Methods:**

- `add_provider(provider)` - Register an OAuth2 provider
- `get_token(provider_name, force_refresh=False)` - Get access token (cached)
- `clear_token(provider_name=None)` - Clear cached token(s)
- `refresh_all()` - Force refresh all cached tokens
- `get_cached_token_info(provider_name)` - Get token diagnostics
- `list_providers()` - List registered provider names
- `close()` - Clean up resources

### Providers

#### AzureADProvider

Azure Active Directory OAuth2 provider.

**Constructor:**
```python
AzureADProvider(
    provider_name: str,
    client_id: str,
    client_secret: str,
    tenant_id: str,
    scopes: list[str] | None = None
)
```

#### GenericOAuth2Provider

Standard OAuth2 client credentials flow provider.

**Constructor:**
```python
GenericOAuth2Provider(config: OAuth2Config)
```

### Models

#### OAuth2Token

Token model with expiration tracking.

**Attributes:**
- `access_token` - The access token string
- `token_type` - Token type (typically "Bearer")
- `expires_at` - UTC datetime when token expires
- `scope` - Space-separated scopes
- `refresh_token` - Optional refresh token

**Methods:**
- `is_expired(buffer_seconds=300)` - Check if token needs refresh
- `remaining_lifetime` - Get timedelta until expiry

#### OAuth2Config

Configuration for GenericOAuth2Provider.

**Attributes:**
- `provider_name` - Unique identifier
- `client_id` - OAuth2 client ID
- `client_secret` - OAuth2 client secret
- `token_url` - Token endpoint URL
- `scope` - Scopes to request (string or list)
- `additional_params` - Extra parameters for token request

## Token Lifecycle

1. **First Request**: Token acquired from provider
2. **Caching**: Token cached with expiration timestamp
3. **Subsequent Requests**: Cached token returned (fast path)
4. **Near Expiry**: Token automatically refreshed (5-minute buffer)
5. **Refresh**: New token acquired and cached
6. **Concurrent Requests**: Deduplicated to prevent multiple refreshes

## Thread Safety

All operations are thread-safe:

- Token cache protected by `threading.Lock`
- Refresh operations protected by `asyncio.Lock` per provider
- Concurrent requests deduplicated automatically
- Safe for use in multi-threaded workers

## Error Handling

```python
from core.oauth2.exceptions import (
    OAuth2Error,
    TokenAcquisitionError,
    TokenRefreshError,
    InvalidConfigurationError
)

try:
    token = await manager.get_token("my_provider")
except TokenAcquisitionError as e:
    logger.error(f"Failed to get token: {e}")
    # Handle error (retry, fallback, etc.)
except InvalidConfigurationError as e:
    logger.error(f"Invalid OAuth2 configuration: {e}")
    # Fix configuration
```

## Best Practices

1. **Use Singleton for Shared State**
   ```python
   from core.oauth2 import get_default_manager

   manager = get_default_manager()
   ```

2. **Configure Once, Use Everywhere**
   - Add providers during application startup
   - Reuse manager instance across plugins

3. **Let It Handle Refresh**
   - Don't manually refresh unless needed
   - Trust automatic expiration handling

4. **Handle Errors Gracefully**
   - Catch `TokenAcquisitionError` for network issues
   - Retry with backoff for transient failures

5. **Use Diagnostics for Debugging**
   ```python
   info = manager.get_cached_token_info("my_provider")
   logger.debug(f"Token expires in {info['remaining_seconds']}s")
   ```

6. **Clean Up Resources**
   ```python
   await manager.close()  # Call during shutdown
   ```

## Testing

```python
from core.oauth2 import OAuth2TokenManager
from core.oauth2.providers.base import BaseOAuth2Provider

class MockProvider(BaseOAuth2Provider):
    async def acquire_token(self):
        return OAuth2Token(
            access_token="mock_token",
            token_type="Bearer",
            expires_at=datetime.now(UTC) + timedelta(hours=1)
        )

    async def refresh_token(self, token):
        return await self.acquire_token()

# Use in tests
manager = OAuth2TokenManager()
manager.add_provider(MockProvider("test"))
token = await manager.get_token("test")
```

## Performance

- **First Request**: ~100-500ms (network call to OAuth2 server)
- **Cached Requests**: <1ms (in-memory lookup)
- **Refresh**: ~100-500ms (network call, transparent to caller)
- **Memory**: ~1KB per cached token

## Limitations

- Azure SDK's `get_token()` is synchronous (called in async context)
- No persistent cache (tokens lost on restart)
- No support for authorization code flow (only client credentials)
- Refresh buffer fixed at 5 minutes

## Future Enhancements

- [ ] Persistent token cache (file or database)
- [ ] Authorization code flow support
- [ ] Configurable refresh buffer per provider
- [ ] Run Azure SDK in thread pool executor
- [ ] Token revocation support
- [ ] Metrics and observability
