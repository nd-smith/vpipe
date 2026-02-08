"""Shared Kafka security configuration builder."""

from config.config import MessageConfig
from core.auth.eventhub_oauth import create_eventhub_oauth_callback


def build_kafka_security_config(config: MessageConfig) -> dict:
    """Build Kafka security config dict from MessageConfig.

    Handles OAUTHBEARER, PLAIN, GSSAPI SASL mechanisms and SSL context creation.
    Returns an empty dict for PLAINTEXT connections.
    """
    if config.security_protocol == "PLAINTEXT":
        return {}

    security_config = {
        "security_protocol": config.security_protocol,
        "sasl_mechanism": config.sasl_mechanism,
    }

    if "SSL" in config.security_protocol:
        import ssl

        security_config["ssl_context"] = ssl.create_default_context()

    if config.sasl_mechanism == "OAUTHBEARER":
        security_config["sasl_oauth_token_provider"] = create_eventhub_oauth_callback()
    elif config.sasl_mechanism == "PLAIN":
        security_config["sasl_plain_username"] = config.sasl_plain_username
        security_config["sasl_plain_password"] = config.sasl_plain_password
    elif config.sasl_mechanism == "GSSAPI":
        security_config["sasl_kerberos_service_name"] = config.sasl_kerberos_service_name

    return security_config
