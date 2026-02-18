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

import _ssl
import logging
import os
import ssl
import sys

logger = logging.getLogger(__name__)

_patched = False


def _patch_imported_modules(original_cls, new_cls) -> None:
    """Replace SSLContext in all modules that imported the original before patching.

    Skips the ssl module itself — its verify_mode/check_hostname setters
    use super(SSLContext, SSLContext) which must keep referring to the
    original class to avoid recursion.
    """
    for mod in list(sys.modules.values()):
        try:
            if mod is not ssl and getattr(mod, "SSLContext", None) is original_cls:
                mod.SSLContext = new_cls
        except Exception:
            pass


def _patch_urllib3(disable_ctx_verification) -> None:
    """Layer 2: Patch urllib3 SSL context creation.

    urllib3 uses its own create_urllib3_context() which calls
    SSLContext(PROTOCOL_TLS_CLIENT) directly, bypassing ssl.create_default_context.
    This is the path used by requests -> urllib3 -> Azure Kusto SDK.
    """
    try:
        import urllib3.util.ssl_ as urllib3_ssl

        _original_create_urllib3_context = urllib3_ssl.create_urllib3_context

        def _patched_create_urllib3_context(*args, **kwargs):
            ctx = _original_create_urllib3_context(*args, **kwargs)
            disable_ctx_verification(ctx)
            return ctx

        urllib3_ssl.create_urllib3_context = _patched_create_urllib3_context
    except (ImportError, AttributeError):
        pass


def _patch_requests() -> None:
    """Layer 3: Patch requests.Session.request to force verify=False.

    Azure SDKs (Kusto, Identity/MSAL) use requests.Session internally
    and don't expose a way to pass verify=False through their APIs.
    Patching .request() directly ensures verify=False is always passed.
    """
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
        #
        # We must ALSO override every property whose setter in the parent
        # class uses super(SSLContext, SSLContext), because any code that
        # later assigns ctx.verify_mode = ..., ctx.options |= ..., etc.
        # will trigger the parent's setter and recurse. Overriding them
        # here short-circuits the chain by going straight to the C-level
        # descriptors on _ssl._SSLContext.

        def __init__(self, *args, **kwargs):
            _ssl._SSLContext.check_hostname.__set__(self, False)
            _ssl._SSLContext.verify_mode.__set__(self, ssl.CERT_NONE)

        # -- verify_mode / check_hostname: always force bypass values ------

        @property
        def verify_mode(self):
            return _ssl._SSLContext.verify_mode.__get__(self, type(self))

        @verify_mode.setter
        def verify_mode(self, value):
            _ssl._SSLContext.check_hostname.__set__(self, False)
            _ssl._SSLContext.verify_mode.__set__(self, ssl.CERT_NONE)

        @property
        def check_hostname(self):
            return _ssl._SSLContext.check_hostname.__get__(self, type(self))

        @check_hostname.setter
        def check_hostname(self, value):
            _ssl._SSLContext.check_hostname.__set__(self, False)

        # -- remaining properties: pass through via C descriptors ----------

        @property
        def options(self):
            return _ssl._SSLContext.options.__get__(self, type(self))

        @options.setter
        def options(self, value):
            _ssl._SSLContext.options.__set__(self, value)

        @property
        def verify_flags(self):
            return _ssl._SSLContext.verify_flags.__get__(self, type(self))

        @verify_flags.setter
        def verify_flags(self, value):
            _ssl._SSLContext.verify_flags.__set__(self, value)

        @property
        def minimum_version(self):
            return _ssl._SSLContext.minimum_version.__get__(self, type(self))

        @minimum_version.setter
        def minimum_version(self, value):
            _ssl._SSLContext.minimum_version.__set__(self, value)

        @property
        def maximum_version(self):
            return _ssl._SSLContext.maximum_version.__get__(self, type(self))

        @maximum_version.setter
        def maximum_version(self, value):
            _ssl._SSLContext.maximum_version.__set__(self, value)

    ssl.SSLContext = _UnverifiedSSLContext

    # Patch modules that already imported SSLContext before this ran.
    _patch_imported_modules(_OriginalSSLContext, _UnverifiedSSLContext)

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

    # Layers 2-3: Patch urllib3 and requests
    _patch_urllib3(_disable_ctx_verification)
    _patch_requests()

    _patched = True

    logger.warning(
        "SSL verification DISABLED (DISABLE_SSL_VERIFY=true). "
        "Patched: ssl.SSLContext, ssl.create_default_context, urllib3 context, "
        "requests.Session. Do NOT use this in production."
    )


def get_eventhub_ssl_kwargs() -> dict:
    """Get SSL-related kwargs for azure-eventhub SDK constructors.

    When the SSL dev bypass is active, returns ``{'connection_verify': False}``
    so the azure-eventhub SDK's pyamqp AMQP transport explicitly disables
    certificate verification on its internal WebSocket / TLS connection.

    This supplements the SSLContext monkey-patch (Layer 0) by using the SDK's
    own ``connection_verify`` parameter.  The monkey-patch replaces
    ``ssl.SSLContext`` globally, but the pyamqp transport may build its TLS
    context through an internal path that is not fully covered — in particular
    during the CBS (Claims-Based Security) token authentication handshake over
    AMQP.  Passing ``connection_verify=False`` ensures the SDK propagates the
    "no-verify" setting all the way through to CBS authentication, which
    resolves CBS token failures seen behind corporate TLS-intercepting proxies.

    Returns:
        Dict with ``connection_verify`` key when bypass is active, empty dict
        otherwise.  Intended to be unpacked into ``from_connection_string()``
        or the ``EventHubProducerClient`` / ``EventHubConsumerClient``
        constructors::

            client = EventHubProducerClient.from_connection_string(
                conn_str=conn,
                eventhub_name=name,
                transport_type=TransportType.AmqpOverWebsocket,
                **get_eventhub_ssl_kwargs(),
            )
    """
    if _patched:
        return {"connection_verify": False}
    return {}
