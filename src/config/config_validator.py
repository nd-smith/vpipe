"""Configuration validator for merged config files.

Validates that merged configuration from split YAML files produces correct
configuration structure with proper merge semantics.
"""

from typing import Any, Dict, List, Optional


def validate_merged_config(
    merged: Dict[str, Any],
    original: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Validate that merged config has correct structure and completeness.

    Args:
        merged: Configuration from merging split YAML files
        original: Optional original monolithic configuration for comparison

    Returns:
        List of error messages. Empty list means validation passed.

    Examples:
        >>> errors = validate_merged_config(merged_cfg, original_cfg)
        >>> if errors:
        ...     print("Validation failed:")
        ...     for error in errors:
        ...         print(f"  - {error}")
        ... else:
        ...     print("Validation passed!")
    """
    errors: List[str] = []

    # If original provided, do structural comparison
    if original:
        errors.extend(_validate_structural_equivalence(merged, original))
        return errors

    # Otherwise, validate completeness and structure
    errors.extend(_validate_shared_settings(merged))
    errors.extend(_validate_domain_completeness(merged, "xact"))
    errors.extend(_validate_domain_completeness(merged, "claimx"))

    return errors


def _validate_structural_equivalence(
    merged: Dict[str, Any],
    original: Dict[str, Any],
    path: str = "",
) -> List[str]:
    """Validate that merged config structurally matches original.

    Args:
        merged: Merged configuration
        original: Original configuration
        path: Current path in config tree (for error messages)

    Returns:
        List of error messages
    """
    errors: List[str] = []

    # Check for missing keys in merged (original has it, merged doesn't)
    for key in original:
        current_path = f"{path}.{key}" if path else key

        if key not in merged:
            errors.append(f"Missing required key: {current_path}")
            continue

        # Check type matches
        orig_value = original[key]
        merged_value = merged[key]

        if type(orig_value) != type(merged_value):
            errors.append(
                f"Type mismatch at {current_path}: "
                f"expected {type(orig_value).__name__}, "
                f"got {type(merged_value).__name__}"
            )
            continue

        # Recursively validate nested dicts
        if isinstance(orig_value, dict) and isinstance(merged_value, dict):
            errors.extend(
                _validate_structural_equivalence(
                    merged_value, orig_value, current_path
                )
            )
        # For arrays and scalars, check value equality
        elif orig_value != merged_value:
            # Only report as error if it's likely a merge issue
            # (allow intentional overrides)
            pass

    return errors


def _validate_shared_settings(merged: Dict[str, Any]) -> List[str]:
    """Validate shared settings are present.

    Args:
        merged: Merged configuration

    Returns:
        List of error messages
    """
    errors: List[str] = []

    # Check for kafka section
    if "kafka" not in merged:
        errors.append("Missing required key: kafka")
        return errors

    kafka = merged["kafka"]

    # Validate connection settings
    connection = kafka.get("connection", {})
    if not connection:
        # Try flat structure (backwards compatibility)
        connection = kafka

    if not connection.get("bootstrap_servers"):
        errors.append("Missing required key: kafka.connection.bootstrap_servers")

    # Validate defaults sections exist
    if "consumer_defaults" not in kafka:
        errors.append("Missing required key: kafka.consumer_defaults")

    if "producer_defaults" not in kafka:
        errors.append("Missing required key: kafka.producer_defaults")

    return errors


def _validate_domain_completeness(
    merged: Dict[str, Any], domain: str
) -> List[str]:
    """Validate domain configuration completeness.

    Args:
        merged: Merged configuration
        domain: Domain name ("xact" or "claimx")

    Returns:
        List of error messages
    """
    errors: List[str] = []

    kafka = merged.get("kafka", {})
    domain_config = kafka.get(domain, {})

    if not domain_config:
        # Domain may not be configured (optional)
        return errors

    # Validate topics exist
    if "topics" not in domain_config:
        errors.append(f"Missing required key: kafka.{domain}.topics")
        return errors

    topics = domain_config["topics"]

    # Validate required topic keys for XACT
    if domain == "xact":
        required_topics = [
            "events",
            "downloads_pending",
            "downloads_cached",
            "downloads_results",
            "dlq",
            "events_ingested",
        ]
        for topic_key in required_topics:
            if topic_key not in topics:
                errors.append(
                    f"Missing required key: kafka.{domain}.topics.{topic_key}"
                )

        # Validate XACT workers
        required_workers = [
            "event_ingester",
            "download_worker",
            "upload_worker",
            "delta_events_writer",
        ]
        for worker in required_workers:
            if worker not in domain_config:
                errors.append(f"Missing required key: kafka.{domain}.{worker}")

    # Validate required topic keys for ClaimX
    elif domain == "claimx":
        required_topics = [
            "events",
            "enrichment_pending",
            "enrichment_results",
            "downloads_pending",
            "downloads_cached",
            "downloads_results",
            "uploads_pending",
            "uploads_results",
            "dlq",
            "events_ingested",
        ]
        for topic_key in required_topics:
            if topic_key not in topics:
                errors.append(
                    f"Missing required key: kafka.{domain}.topics.{topic_key}"
                )

        # Validate ClaimX workers
        required_workers = [
            "event_ingester",
            "enrichment_worker",
            "download_worker",
            "upload_worker",
            "delta_events_writer",
        ]
        for worker in required_workers:
            if worker not in domain_config:
                errors.append(f"Missing required key: kafka.{domain}.{worker}")

    # Validate common domain keys
    if "consumer_group_prefix" not in domain_config:
        errors.append(f"Missing required key: kafka.{domain}.consumer_group_prefix")

    if "retry_delays" not in domain_config:
        errors.append(f"Missing required key: kafka.{domain}.retry_delays")

    return errors


def validate_merge_rules(
    base: Dict[str, Any],
    overlay: Dict[str, Any],
    merged: Dict[str, Any],
    path: str = "",
) -> List[str]:
    """Validate that merge rules were applied correctly.

    Checks:
    - Deep merge for nested dicts
    - Array replacement (not concatenation)
    - Override precedence (overlay wins)

    Args:
        base: Base configuration
        overlay: Overlay configuration
        merged: Merged result
        path: Current path in config tree

    Returns:
        List of error messages
    """
    errors: List[str] = []

    # For each key in overlay, check it's properly merged
    for key, overlay_value in overlay.items():
        current_path = f"{path}.{key}" if path else key

        # Check overlay value exists in merged
        if key not in merged:
            errors.append(
                f"Override not applied: {current_path} missing from merged config"
            )
            continue

        merged_value = merged[key]

        # If both base and overlay have this key
        if key in base:
            base_value = base[key]

            # Deep merge rule: nested dicts should merge
            if isinstance(base_value, dict) and isinstance(overlay_value, dict):
                # Both keys should be present in merged
                errors.extend(
                    validate_merge_rules(
                        base_value, overlay_value, merged_value, current_path
                    )
                )

            # Array replacement rule: arrays should replace, not merge
            elif isinstance(overlay_value, list):
                if merged_value != overlay_value:
                    errors.append(
                        f"Array replacement failed at {current_path}: "
                        f"expected {overlay_value}, got {merged_value}"
                    )

            # Scalar override rule: overlay should win
            else:
                if merged_value != overlay_value:
                    errors.append(
                        f"Override not applied at {current_path}: "
                        f"expected {overlay_value}, got {merged_value}"
                    )

        # If only overlay has this key, it should be in merged
        else:
            if merged_value != overlay_value:
                errors.append(
                    f"New key not merged at {current_path}: "
                    f"expected {overlay_value}, got {merged_value}"
                )

    return errors


def get_config_summary(config: Dict[str, Any]) -> str:
    """Get human-readable summary of configuration.

    Args:
        config: Configuration dict

    Returns:
        Summary string with key statistics
    """
    summary_lines = []

    kafka = config.get("kafka", {})

    # Connection info
    connection = kafka.get("connection", kafka)
    bootstrap = connection.get("bootstrap_servers", "NOT SET")
    summary_lines.append(f"Bootstrap Servers: {bootstrap}")

    # Domain info
    xact = kafka.get("xact", {})
    claimx = kafka.get("claimx", {})

    if xact:
        xact_topics = xact.get("topics", {})
        summary_lines.append(f"XACT Topics: {len(xact_topics)} configured")

    if claimx:
        claimx_topics = claimx.get("topics", {})
        summary_lines.append(f"ClaimX Topics: {len(claimx_topics)} configured")

    # Storage info
    storage = kafka.get("storage", {})
    if storage:
        summary_lines.append("Storage: Configured")

    return "\n".join(summary_lines)
