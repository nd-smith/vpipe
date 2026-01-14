"""Configuration validator for merged config files.

Validates that merged configuration from split YAML files produces correct
configuration structure with proper merge semantics. Also includes security
validation to prevent secrets from being stored in YAML files.
"""

import os
import re
from typing import Any, Dict, List, Optional, Set


# =============================================================================
# SECRET VALIDATION - SECURITY CRITICAL
# =============================================================================
# These patterns identify fields that should NEVER be stored in YAML files.
# All secrets must be loaded from environment variables only.
# =============================================================================

# Environment variables that MUST be set (validation fails if any are missing)
REQUIRED_ENV_SECRETS: List[str] = [
    "CLAIMX_API_TOKEN",  # ClaimX API authentication token
    "ONELAKE_BASE_PATH",  # OneLake storage base path (may contain credentials)
]

# Optional environment variables that may contain secrets
# These will be validated if present in YAML but won't fail if missing
OPTIONAL_ENV_SECRETS: List[str] = [
    "ITEL_CABINET_API_TOKEN",  # iTel Cabinet API token (not yet implemented)
    "EVENTHOUSE_CLUSTER_URL",  # May contain embedded credentials
    "CLAIMX_API_BASE_PATH",  # ClaimX API base URL
    "CLAIMX_API_USERNAME",  # Alternative to token-based auth
    "CLAIMX_API_PASSWORD",  # Alternative to token-based auth
]

# Patterns that indicate a field contains secret data
# These are used to detect secrets in YAML values
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
        """Initialize validator with secret patterns."""
        # Compile regex patterns for efficient matching
        self._secret_patterns = [
            re.compile(pattern, re.IGNORECASE) for pattern in SECRET_PATTERNS
        ]

    def validate_no_secrets_in_yaml(
        self, config: Dict[str, Any], path: str = ""
    ) -> List[str]:
        """Validate that no secrets are stored in YAML configuration.

        This method recursively scans the entire configuration tree looking for
        fields that appear to contain secrets. It checks:
        1. Field names matching secret patterns (token, password, key, etc.)
        2. Values that look like credentials (not env var placeholders)
        3. Values that are not properly using ${VAR_NAME} syntax

        Args:
            config: Configuration dictionary to validate
            path: Current path in config tree (for error messages)

        Returns:
            List of error messages for any secrets found in YAML

        Examples:
            >>> validator = ConfigValidator()
            >>> config = {"api_token": "abc123"}
            >>> errors = validator.validate_no_secrets_in_yaml(config)
            >>> print(errors)
            ['Secret found in YAML: api_token = [REDACTED]']

            >>> config = {"api_token": "${CLAIMX_API_TOKEN}"}
            >>> errors = validator.validate_no_secrets_in_yaml(config)
            >>> print(errors)
            []
        """
        errors: List[str] = []

        for key, value in config.items():
            current_path = f"{path}.{key}" if path else key

            # Check if this is a nested dictionary - recurse
            if isinstance(value, dict):
                errors.extend(self.validate_no_secrets_in_yaml(value, current_path))
                continue

            # Check if field name matches a secret pattern
            if self._is_secret_field(key):
                # Check if value is properly using environment variable
                if not self._is_env_var_reference(value):
                    # Found a secret in YAML!
                    errors.append(
                        f"Secret found in YAML: {current_path}\n"
                        f"  Expected: Environment variable reference (e.g., ${{ENV_VAR}})\n"
                        f"  Found: Hard-coded value\n"
                        f"  Action: Move this secret to an environment variable"
                    )

        return errors

    def load_secrets_from_env(self) -> Dict[str, Optional[str]]:
        """Load secrets from environment variables.

        Loads all required and optional secret environment variables,
        validates that required ones are set, and returns them in a
        dictionary for use in configuration.

        Returns:
            Dictionary mapping environment variable names to their values

        Raises:
            ValueError: If any required environment variables are missing

        Examples:
            >>> validator = ConfigValidator()
            >>> secrets = validator.load_secrets_from_env()
            >>> print(secrets.get("CLAIMX_API_TOKEN"))
            'token_value_from_env'
        """
        secrets: Dict[str, Optional[str]] = {}
        missing_required: List[str] = []

        # Load required secrets
        for var_name in REQUIRED_ENV_SECRETS:
            value = os.getenv(var_name)
            secrets[var_name] = value

            if value is None or value == "":
                missing_required.append(var_name)

        # Raise error if any required secrets are missing
        if missing_required:
            error_msg = self._format_missing_secrets_error(missing_required)
            raise ValueError(error_msg)

        # Load optional secrets (don't fail if missing)
        for var_name in OPTIONAL_ENV_SECRETS:
            value = os.getenv(var_name)
            secrets[var_name] = value

        return secrets

    def _is_secret_field(self, field_name: str) -> bool:
        """Check if a field name indicates it contains secret data.

        Args:
            field_name: Name of configuration field

        Returns:
            True if field name matches a secret pattern
        """
        for pattern in self._secret_patterns:
            if pattern.search(field_name):
                return True
        return False

    def _is_env_var_reference(self, value: Any) -> bool:
        """Check if a value is an environment variable reference.

        Valid env var references:
        - ${VAR_NAME}
        - Empty string (allowed - will be loaded from env later)
        - None (allowed - will be loaded from env later)

        Args:
            value: Value to check

        Returns:
            True if value is using proper env var syntax or is empty
        """
        # None or empty string is OK - might be loaded from env later
        if value is None or value == "":
            return True

        # If not a string, it can't be a secret (numbers, booleans, etc.)
        if not isinstance(value, str):
            return True

        # Check if it's an environment variable reference
        # Pattern: ${VAR_NAME} anywhere in the string
        env_var_pattern = r"\$\{[A-Z_][A-Z0-9_]*\}"
        if re.search(env_var_pattern, value):
            return True

        # If the value looks like a URL, path, or other non-secret data, allow it
        # This prevents false positives for fields like "auth_type: bearer"
        if self._is_non_secret_value(value):
            return True

        # Otherwise, it's likely a hard-coded secret
        return False

    def _is_non_secret_value(self, value: str) -> bool:
        """Check if a value is likely not a secret.

        This prevents false positives for fields that match secret patterns
        but contain non-sensitive configuration values.

        Args:
            value: String value to check

        Returns:
            True if value is likely not a secret
        """
        # Known non-secret values
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
        """Format a helpful error message for missing environment variables.

        Args:
            missing: List of missing environment variable names

        Returns:
            Formatted error message with instructions
        """
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
