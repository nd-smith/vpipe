"""Configuration validator for merged config files.

Validates that merged configuration from split YAML files produces correct
configuration structure with proper merge semantics. Also includes security
validation to prevent secrets from being stored in YAML files.
"""

import os
import re
from typing import Any, Dict, List, Optional, Set


# SECURITY: All secrets must be loaded from environment variables, never stored in YAML
REQUIRED_ENV_SECRETS: List[str] = [
    "CLAIMX_API_TOKEN",
    "ONELAKE_BASE_PATH",
]

OPTIONAL_ENV_SECRETS: List[str] = [
    "ITEL_CABINET_API_TOKEN",
    "EVENTHOUSE_CLUSTER_URL",
    "CLAIMX_API_BASE_PATH",
    "CLAIMX_API_USERNAME",
    "CLAIMX_API_PASSWORD",
]

SECRET_PATTERNS: List[str] = [
    r"token",
    r"password",
    r"secret",
    r"key",
    r"credential",
    r"auth",
    r"bearer",
    r"api[_-]?key",
]


class ConfigValidator:
    """Validates configuration for security and correctness.

    Primary responsibilities:
    1. Prevent secrets from being stored in YAML files
    2. Ensure secrets are loaded from environment variables
    3. Validate environment variables are set
    4. Provide helpful error messages without leaking secrets
    """

    def __init__(self):
        self._secret_patterns = [
            re.compile(pattern, re.IGNORECASE) for pattern in SECRET_PATTERNS
        ]

    def validate_no_secrets_in_yaml(
        self, config: Dict[str, Any], path: str = ""
    ) -> List[str]:
        errors: List[str] = []

        for key, value in config.items():
            current_path = f"{path}.{key}" if path else key

            if isinstance(value, dict):
                errors.extend(self.validate_no_secrets_in_yaml(value, current_path))
                continue

            if self._is_secret_field(key):
                if not self._is_env_var_reference(value):
                    errors.append(
                        f"Secret found in YAML: {current_path}\n"
                        f"  Expected: Environment variable reference (e.g., ${{ENV_VAR}})\n"
                        f"  Found: Hard-coded value\n"
                        f"  Action: Move this secret to an environment variable"
                    )

        return errors

    def load_secrets_from_env(self) -> Dict[str, Optional[str]]:
        secrets: Dict[str, Optional[str]] = {}
        missing_required: List[str] = []

        for var_name in REQUIRED_ENV_SECRETS:
            value = os.getenv(var_name)
            secrets[var_name] = value

            if value is None or value == "":
                missing_required.append(var_name)

        if missing_required:
            error_msg = self._format_missing_secrets_error(missing_required)
            raise ValueError(error_msg)

        for var_name in OPTIONAL_ENV_SECRETS:
            value = os.getenv(var_name)
            secrets[var_name] = value

        return secrets

    def _is_secret_field(self, field_name: str) -> bool:
        for pattern in self._secret_patterns:
            if pattern.search(field_name):
                return True
        return False

    def _is_env_var_reference(self, value: Any) -> bool:
        """Check if value is a proper env var reference (${VAR_NAME}) or allowed non-secret."""
        if value is None or value == "":
            return True

        if not isinstance(value, str):
            return True

        env_var_pattern = r"\$\{[A-Z_][A-Z0-9_]*\}"
        if re.search(env_var_pattern, value):
            return True

        if self._is_non_secret_value(value):
            return True

        return False

    def _is_non_secret_value(self, value: str) -> bool:
        """Prevent false positives for non-sensitive config values that match secret patterns."""
        non_secrets = [
            "bearer",
            "basic",
            "api_key",
            "none",
            "plaintext",
            "ssl",
            "sasl_plaintext",
            "sasl_ssl",
            "oauthbearer",
        ]

        return value.lower() in non_secrets

    def _format_missing_secrets_error(self, missing: List[str]) -> str:
        lines = [
            "Configuration Error: Required environment variables not set",
            "",
            "The following environment variables are required but missing:",
        ]

        for var_name in missing:
            lines.append(f"  - {var_name}")

        lines.extend(
            [
                "",
                "Action Required:",
                "1. Create a .env file in your project root (or set variables in your shell)",
                "2. Add the missing variables with appropriate values",
                "3. NEVER commit secrets to version control",
                "",
                "Example .env file:",
                "  CLAIMX_API_TOKEN=your-token-here",
                "  ONELAKE_BASE_PATH=abfss://workspace@onelake.dfs.fabric.microsoft.com/...",
                "",
                "For production, use a secure secret management system (Azure Key Vault, etc.)",
            ]
        )

        return "\n".join(lines)


def validate_merged_config(
    merged: Dict[str, Any],
    original: Optional[Dict[str, Any]] = None,
) -> List[str]:
    errors: List[str] = []

    if original:
        errors.extend(_validate_structural_equivalence(merged, original))
        return errors

    errors.extend(_validate_shared_settings(merged))
    errors.extend(_validate_domain_completeness(merged, "xact"))
    errors.extend(_validate_domain_completeness(merged, "claimx"))

    return errors


def _validate_structural_equivalence(
    merged: Dict[str, Any],
    original: Dict[str, Any],
    path: str = "",
) -> List[str]:
    errors: List[str] = []

    for key in original:
        current_path = f"{path}.{key}" if path else key

        if key not in merged:
            errors.append(f"Missing required key: {current_path}")
            continue

        orig_value = original[key]
        merged_value = merged[key]

        if type(orig_value) != type(merged_value):
            errors.append(
                f"Type mismatch at {current_path}: "
                f"expected {type(orig_value).__name__}, "
                f"got {type(merged_value).__name__}"
            )
            continue

        if isinstance(orig_value, dict) and isinstance(merged_value, dict):
            errors.extend(
                _validate_structural_equivalence(
                    merged_value, orig_value, current_path
                )
            )

    return errors


def _validate_shared_settings(merged: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    if "kafka" not in merged:
        errors.append("Missing required key: kafka")
        return errors

    kafka = merged["kafka"]

    connection = kafka.get("connection", {})
    if not connection:
        connection = kafka

    if not connection.get("bootstrap_servers"):
        errors.append("Missing required key: kafka.connection.bootstrap_servers")

    if "consumer_defaults" not in kafka:
        errors.append("Missing required key: kafka.consumer_defaults")

    if "producer_defaults" not in kafka:
        errors.append("Missing required key: kafka.producer_defaults")

    return errors


def _validate_domain_completeness(
    merged: Dict[str, Any], domain: str
) -> List[str]:
    errors: List[str] = []

    kafka = merged.get("kafka", {})
    domain_config = kafka.get(domain, {})

    if not domain_config:
        return errors

    if "topics" not in domain_config:
        errors.append(f"Missing required key: kafka.{domain}.topics")
        return errors

    topics = domain_config["topics"]

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

        required_workers = [
            "event_ingester",
            "download_worker",
            "upload_worker",
            "delta_events_writer",
        ]
        for worker in required_workers:
            if worker not in domain_config:
                errors.append(f"Missing required key: kafka.{domain}.{worker}")

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
    """Validate merge rules: deep merge dicts, replace arrays, overlay wins for scalars."""
    errors: List[str] = []

    for key, overlay_value in overlay.items():
        current_path = f"{path}.{key}" if path else key

        if key not in merged:
            errors.append(
                f"Override not applied: {current_path} missing from merged config"
            )
            continue

        merged_value = merged[key]

        if key in base:
            base_value = base[key]

            if isinstance(base_value, dict) and isinstance(overlay_value, dict):
                errors.extend(
                    validate_merge_rules(
                        base_value, overlay_value, merged_value, current_path
                    )
                )

            elif isinstance(overlay_value, list):
                if merged_value != overlay_value:
                    errors.append(
                        f"Array replacement failed at {current_path}: "
                        f"expected {overlay_value}, got {merged_value}"
                    )

            else:
                if merged_value != overlay_value:
                    errors.append(
                        f"Override not applied at {current_path}: "
                        f"expected {overlay_value}, got {merged_value}"
                    )

        else:
            if merged_value != overlay_value:
                errors.append(
                    f"New key not merged at {current_path}: "
                    f"expected {overlay_value}, got {merged_value}"
                )

    return errors


def get_config_summary(config: Dict[str, Any]) -> str:
    summary_lines = []

    kafka = config.get("kafka", {})

    connection = kafka.get("connection", kafka)
    bootstrap = connection.get("bootstrap_servers", "NOT SET")
    summary_lines.append(f"Bootstrap Servers: {bootstrap}")

    xact = kafka.get("xact", {})
    claimx = kafka.get("claimx", {})

    if xact:
        xact_topics = xact.get("topics", {})
        summary_lines.append(f"XACT Topics: {len(xact_topics)} configured")

    if claimx:
        claimx_topics = claimx.get("topics", {})
        summary_lines.append(f"ClaimX Topics: {len(claimx_topics)} configured")

    storage = kafka.get("storage", {})
    if storage:
        summary_lines.append("Storage: Configured")

    return "\n".join(summary_lines)
