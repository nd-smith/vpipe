"""Diagnostic utilities for Event Hub connection troubleshooting.

Provides functions for safely logging connection information and
performing network diagnostics to help debug connection failures.
"""

import logging
import os
import re
import socket
from typing import Any

logger = logging.getLogger(__name__)


def mask_connection_string(conn_str: str) -> str:
    if not conn_str:
        return ""

    # Mask the SharedAccessKey value
    masked = re.sub(
        r"(SharedAccessKey=)[^;]+",
        r"\1***MASKED***",
        conn_str,
        flags=re.IGNORECASE,
    )
    return masked


def parse_connection_string(conn_str: str) -> dict[str, str]:
    """Parse Event Hub connection string into components.

    Args:
        conn_str: Event Hub connection string

    Returns:
        Dict with connection string components (Endpoint, SharedAccessKeyName, etc.)
    """
    if not conn_str:
        return {}

    parts = {}
    for part in conn_str.split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            parts[key.strip()] = value.strip()

    return parts


def extract_namespace_host(conn_str: str) -> str | None:
    """Extract the Service Bus namespace hostname from connection string.

    Args:
        conn_str: Event Hub connection string

    Returns:
        Hostname (e.g., "myhub.servicebus.windows.net") or None if not found
    """
    parts = parse_connection_string(conn_str)
    endpoint = parts.get("Endpoint", "")

    # Extract hostname from sb://hostname/ format
    match = re.search(r"sb://([^/]+)", endpoint)
    if match:
        return match.group(1)

    return None


def test_dns_resolution(hostname: str) -> dict[str, Any]:
    """Test DNS resolution for Event Hub namespace.

    Args:
        hostname: Service Bus namespace hostname

    Returns:
        Dict with resolution results
    """
    result = {
        "hostname": hostname,
        "resolved": False,
        "ip_addresses": [],
        "error": None,
    }

    try:
        # Resolve hostname to IP addresses
        addr_info = socket.getaddrinfo(hostname, 443, socket.AF_UNSPEC, socket.SOCK_STREAM)
        ip_addresses = list({addr[4][0] for addr in addr_info})

        result["resolved"] = True
        result["ip_addresses"] = ip_addresses
        logger.info(f"[DEBUG] DNS resolution successful: {hostname} -> {ip_addresses}")

    except socket.gaierror as e:
        result["error"] = f"DNS resolution failed: {e}"
        logger.error(f"[DEBUG] DNS resolution failed for {hostname}: {e}")
    except Exception as e:
        result["error"] = f"DNS resolution error: {e}"
        logger.error(f"[DEBUG] DNS resolution error for {hostname}: {e}")

    return result


def log_connection_diagnostics(conn_str: str, eventhub_name: str) -> None:
    """Log comprehensive connection diagnostics for troubleshooting.

    Args:
        conn_str: Event Hub connection string
        eventhub_name: Event Hub entity name
    """
    logger.info("=" * 60)
    logger.info("[DEBUG] Event Hub Connection Diagnostics")
    logger.info("=" * 60)

    # 1. Masked connection string
    masked_conn_str = mask_connection_string(conn_str)
    logger.info(f"[DEBUG] Connection string (masked): {masked_conn_str}")

    # 2. Connection string components
    parts = parse_connection_string(conn_str)
    logger.info(f"[DEBUG] Endpoint: {parts.get('Endpoint', 'NOT SET')}")
    logger.info(f"[DEBUG] SharedAccessKeyName: {parts.get('SharedAccessKeyName', 'NOT SET')}")
    logger.info(
        f"[DEBUG] SharedAccessKey: {'***SET***' if parts.get('SharedAccessKey') else 'NOT SET'}"
    )
    logger.info(
        f"[DEBUG] EntityPath in connection string: {parts.get('EntityPath', 'NOT SET (correct for namespace-level)')}"
    )
    logger.info(f"[DEBUG] Event Hub name parameter: {eventhub_name}")

    # 3. Connection string source
    conn_str_source = "unknown"
    if os.getenv("EVENTHUB_NAMESPACE_CONNECTION_STRING"):
        conn_str_source = "EVENTHUB_NAMESPACE_CONNECTION_STRING env var"
    elif os.getenv("EVENTHUB_CONNECTION_STRING"):
        conn_str_source = "EVENTHUB_CONNECTION_STRING env var (legacy)"
    else:
        conn_str_source = "config.yaml"
    logger.info(f"[DEBUG] Connection string source: {conn_str_source}")

    # 4. SSL/TLS configuration
    ca_bundle = (
        os.getenv("SSL_CERT_FILE") or os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("CURL_CA_BUNDLE")
    )
    if ca_bundle:
        logger.info(f"[DEBUG] SSL CA bundle: {ca_bundle}")
        # Check if CA bundle file exists
        if os.path.exists(ca_bundle):
            logger.info(f"[DEBUG] CA bundle file exists: {ca_bundle}")
        else:
            logger.error(f"[DEBUG] CA bundle file NOT FOUND: {ca_bundle}")
    else:
        logger.info("[DEBUG] SSL CA bundle: system default")

    # 5. DNS resolution test
    hostname = extract_namespace_host(conn_str)
    if hostname:
        logger.info(f"[DEBUG] Service Bus namespace hostname: {hostname}")
        dns_result = test_dns_resolution(hostname)
        if dns_result["resolved"]:
            logger.info(f"[DEBUG] DNS resolved to: {dns_result['ip_addresses']}")
        else:
            logger.error(f"[DEBUG] DNS resolution failed: {dns_result['error']}")
    else:
        logger.error("[DEBUG] Could not extract hostname from connection string")

    # 6. Environment info
    logger.info(f"[DEBUG] Python version: {os.sys.version}")
    logger.info(f"[DEBUG] Platform: {os.sys.platform}")

    # 7. Azure SDK versions
    try:
        import azure.eventhub

        logger.info(f"[DEBUG] azure-eventhub version: {azure.eventhub.__version__}")
    except Exception as e:
        logger.warning(f"[DEBUG] Could not get azure-eventhub version: {e}")

    logger.info("=" * 60)


def log_connection_attempt_details(
    eventhub_name: str,
    transport_type: str,
    ssl_kwargs: dict[str, Any],
) -> None:
    """Log details about the connection attempt.

    Args:
        eventhub_name: Event Hub entity name
        transport_type: Transport type (e.g., "AmqpOverWebsocket")
        ssl_kwargs: SSL configuration kwargs
    """
    logger.info("[DEBUG] Connection attempt details:")
    logger.info(f"[DEBUG]   Event Hub name: {eventhub_name}")
    logger.info(f"[DEBUG]   Transport type: {transport_type}")
    logger.info("[DEBUG]   Protocol: AMQP over WebSocket (port 443)")
    logger.info(f"[DEBUG]   SSL kwargs: {ssl_kwargs}")

    if ssl_kwargs.get("connection_verify"):
        ca_path = ssl_kwargs["connection_verify"]
        logger.info(f"[DEBUG]   Using custom CA bundle: {ca_path}")
        if os.path.exists(ca_path):
            try:
                size = os.path.getsize(ca_path)
                logger.info(f"[DEBUG]   CA bundle size: {size} bytes")
            except Exception as e:
                logger.warning(f"[DEBUG]   Could not get CA bundle size: {e}")
        else:
            logger.error(f"[DEBUG]   CA bundle file NOT FOUND: {ca_path}")
    else:
        logger.info("[DEBUG]   Using system default certificates")


__all__ = [
    "mask_connection_string",
    "parse_connection_string",
    "extract_namespace_host",
    "test_dns_resolution",
    "log_connection_diagnostics",
    "log_connection_attempt_details",
]
