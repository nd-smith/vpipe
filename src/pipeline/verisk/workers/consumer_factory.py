"""Consumer factory for creating AIOKafkaConsumer with standard configuration."""

from typing import Any

from aiokafka import AIOKafkaConsumer

from config.config import KafkaConfig
from core.auth.kafka_oauth import create_kafka_oauth_callback


def create_consumer(
    config: KafkaConfig,
    domain: str,
    worker_name: str,
    topics: list[str] | str,
    instance_id: str | None = None,
    max_poll_records: int | None = None,
) -> AIOKafkaConsumer:
    """
    Create and return an AIOKafkaConsumer with standard configuration.

    Args:
        config: Kafka configuration
        domain: Pipeline domain (e.g., "verisk", "claimx")
        worker_name: Worker name for config lookup
        topics: Topic(s) to consume from
        instance_id: Optional instance ID for client_id
        max_poll_records: Optional max records per poll
    """
    consumer_config_dict = config.get_worker_config(domain, worker_name, "consumer")

    # Build base consumer config
    consumer_config: dict[str, Any] = {
        "bootstrap_servers": config.bootstrap_servers,
        "group_id": config.get_consumer_group(domain, worker_name),
        "client_id": (
            f"{domain}-{worker_name}-{instance_id}"
            if instance_id
            else f"{domain}-{worker_name}"
        ),
        "enable_auto_commit": False,
        "auto_offset_reset": consumer_config_dict.get("auto_offset_reset", "earliest"),
        "session_timeout_ms": consumer_config_dict.get("session_timeout_ms", 60000),
        "max_poll_interval_ms": consumer_config_dict.get(
            "max_poll_interval_ms", 300000
        ),
        "request_timeout_ms": config.request_timeout_ms,
        "metadata_max_age_ms": config.metadata_max_age_ms,
        "connections_max_idle_ms": config.connections_max_idle_ms,
    }

    if max_poll_records is not None:
        consumer_config["max_poll_records"] = max_poll_records

    # Add optional consumer settings
    for key in ["heartbeat_interval_ms", "fetch_min_bytes", "fetch_max_wait_ms"]:
        if key in consumer_config_dict:
            consumer_config[key] = consumer_config_dict[key]

    # Add security configuration
    if config.security_protocol != "PLAINTEXT":
        consumer_config["security_protocol"] = config.security_protocol
        consumer_config["sasl_mechanism"] = config.sasl_mechanism

        if config.sasl_mechanism == "OAUTHBEARER":
            consumer_config["sasl_oauth_token_provider"] = create_kafka_oauth_callback()
        elif config.sasl_mechanism == "PLAIN":
            consumer_config["sasl_plain_username"] = config.sasl_plain_username
            consumer_config["sasl_plain_password"] = config.sasl_plain_password

    # Handle both single topic (str) and multiple topics (list)
    if isinstance(topics, str):
        return AIOKafkaConsumer(topics, **consumer_config)
    return AIOKafkaConsumer(*topics, **consumer_config)
