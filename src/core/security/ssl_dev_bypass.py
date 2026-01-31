"""SSL verification bypass for local development behind corporate proxies.

When DISABLE_SSL_VERIFY=true is set (typically in .env, which is gitignored),
this module patches SSL verification at multiple layers:

0. ssl.SSLContext - replaced with subclass that disables verification on every
   new context. Covers ALL libraries (AMQP, WebSocket, etc.) that create SSL
   contexts directly. ssl.SSLContext is a C extension type whose methods cannot
   be monkey-patched, so subclassing is the only way to intercept construction.
1. ssl.create_default_context - covers libraries that use the stdlib default context
2. urllib3 SSL context creation - covers libraries using urllib3 directly
3. requests.Session - covers the Azure SDKs (Kusto, Identity, etc.) which use requests

This is required when a corporate proxy intercepts TLS with a self-signed CA
that is not in Python's trust store.

WARNING: This must NEVER be enabled in production. The env var should only
exist in .env (gitignored) or be set manually for local testing.
"""

import logging
import os
import ssl
import _ssl
import sys

logger = logging.getLogger(__name__)

_patched = False


def apply_ssl_dev_bypass() -> None:
    """Conditionally disable SSL verification if DISABLE_SSL_VERIFY=true.

    Must be called early in startup, after load_dotenv() but before any
    SSL connections are made (Kafka, Event Hub, Azure, etc.).

    Only applies the patch once, even if called multiple times.
    """
    global _patched

    if _patched:
        return

    enabled = os.getenv("DISABLE_SSL_VERIFY", "false").lower() in ("true", "1", "yes")
    if not enabled:
        return

    env = os.getenv("ENVIRONMENT", "").lower()
    app_env = os.getenv("APP_ENV", "").lower()
    if env == "production" or app_env == "production":
        logger.error(
            "DISABLE_SSL_VERIFY is set but ENVIRONMENT or APP_ENV is 'production'. "
            "Refusing to disable SSL verification in production."
        )
        return

    # Layer 0: Replace ssl.SSLContext with a subclass that disables verification.
    # ssl.SSLContext is a C extension type — its methods (like __init__) are
    # immutable and cannot be monkey-patched. Subclassing is the only way to
    # intercept context creation. This covers libraries that call
    # ssl.SSLContext(PROTOCOL_TLS_CLIENT) directly (e.g. azure-eventhub's
    # pyamqp WebSocket transport, websocket-client).
    _OriginalSSLContext = ssl.SSLContext

    class _UnverifiedSSLContext(_OriginalSSLContext):
        # In Python 3.13, SSLContext is a C extension type where the protocol
        # parameter is handled in __new__, not __init__. The __init__ is
        # effectively object.__init__() and accepts no extra arguments.
        #
        # IMPORTANT: We must use the C-level descriptors from _ssl._SSLContext
        # to set check_hostname and verify_mode. The Python-level setters in
        # ssl.SSLContext use super(SSLContext, SSLContext) which resolves the
        # name "SSLContext" from the ssl module's globals. After we replace
        # ssl.SSLContext with this subclass, that super() call resolves back
        # to _UnverifiedSSLContext, causing infinite recursion.
        def __init__(self, *args, **kwargs):
            _ssl._SSLContext.check_hostname.__set__(self, False)
            _ssl._SSLContext.verify_mode.__set__(self, ssl.CERT_NONE)

    ssl.SSLContext = _UnverifiedSSLContext

    # Patch modules that already imported SSLContext before this ran.
    # Skip the ssl module itself — its verify_mode/check_hostname setters
    # use super(SSLContext, SSLContext) which must keep referring to the
    # original class to avoid recursion.
    for mod in list(sys.modules.values()):
        try:
            if mod is not ssl and getattr(mod, "SSLContext", None) is _OriginalSSLContext:
                mod.SSLContext = _UnverifiedSSLContext
        except Exception:
            pass

    # Helper to disable verification using C-level descriptors (recursion-safe).
    def _disable_ctx_verification(ctx):
        _ssl._SSLContext.check_hostname.__set__(ctx, False)
        _ssl._SSLContext.verify_mode.__set__(ctx, ssl.CERT_NONE)

    # Layer 1: Patch ssl.create_default_context (covers aiohttp, aiokafka, etc.)
    _original_create_default_context = ssl.create_default_context

    def _patched_create_default_context(*args, **kwargs):
        ctx = _original_create_default_context(*args, **kwargs)
        _disable_ctx_verification(ctx)
        return ctx

    ssl.create_default_context = _patched_create_default_context

    # Layer 2: Patch urllib3 SSL context creation
    # urllib3 uses its own create_urllib3_context() which calls
    # SSLContext(PROTOCOL_TLS_CLIENT) directly, bypassing ssl.create_default_context.
    # This is the path used by requests -> urllib3 -> Azure Kusto SDK.
    try:
        import urllib3.util.ssl_ as urllib3_ssl

        _original_create_urllib3_context = urllib3_ssl.create_urllib3_context

        def _patched_create_urllib3_context(*args, **kwargs):
            ctx = _original_create_urllib3_context(*args, **kwargs)
            _disable_ctx_verification(ctx)
            return ctx

        urllib3_ssl.create_urllib3_context = _patched_create_urllib3_context
    except (ImportError, AttributeError):
        pass

    # Layer 3: Patch requests.Session.request to force verify=False
    # Azure SDKs (Kusto, Identity/MSAL) use requests.Session internally
    # and don't expose a way to pass verify=False through their APIs.
    # Patching Session.__init__ to set self.verify is NOT sufficient because
    # merge_environment_settings() can override it depending on the requests
    # version. Patching .request() directly ensures verify=False is always
    # passed regardless of how the SDK calls it.
    try:
        import requests

        _original_session_request = requests.Session.request

        def _patched_session_request(self, *args, **kwargs):
            kwargs["verify"] = False
            return _original_session_request(self, *args, **kwargs)

        requests.Session.request = _patched_session_request

        # Suppress InsecureRequestWarning noise when verify=False
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except (ImportError, AttributeError):
        pass

    _patched = True

    logger.warning(
        "SSL verification DISABLED (DISABLE_SSL_VERIFY=true). "
        "Patched: ssl.SSLContext, ssl.create_default_context, urllib3 context, "
        "requests.Session. Do NOT use this in production."
    )
