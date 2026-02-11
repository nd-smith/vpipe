"""SSL/TLS utilities for corporate proxy environments."""

import os


def get_ca_bundle_kwargs() -> dict:
    """Return ``{"connection_verify": path}`` if a custom CA bundle is set, else ``{}``."""
    ca_bundle = (
        os.getenv("SSL_CERT_FILE")
        or os.getenv("REQUESTS_CA_BUNDLE")
        or os.getenv("CURL_CA_BUNDLE")
    )
    if ca_bundle:
        return {"connection_verify": ca_bundle}
    return {}
