"""SSL verification bypass for local development behind corporate proxies.

When DISABLE_SSL_VERIFY=true is set (typically in .env, which is gitignored),
this module patches ssl.create_default_context to skip certificate verification.
This is required when a corporate proxy intercepts TLS with a self-signed CA
that is not in Python's trust store.

WARNING: This must NEVER be enabled in production. The env var should only
exist in .env (gitignored) or be set manually for local testing.
"""

import logging
import os
import ssl

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

    _original_create_default_context = ssl.create_default_context

    def _patched_create_default_context(*args, **kwargs):
        ctx = _original_create_default_context(*args, **kwargs)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    ssl.create_default_context = _patched_create_default_context
    _patched = True

    logger.warning(
        "SSL verification DISABLED (DISABLE_SSL_VERIFY=true). "
        "Do NOT use this in production."
    )
