"""Message pipeline configuration from YAML file.

Loads from config/config.yaml with all settings in one place.
Environment variables are supported using ${VAR_NAME} syntax.
"""

import json
import logging
import os
import re
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypedDict

import yaml

# Configure module logger
logger = logging.getLogger(__name__)


# =========================================================================
# TYPEDDICT DEFINITIONS FOR CONFIGURATION STRUCTURE
# =========================================================================


class WorkerConfig(TypedDict, total=False):
    consumer: dict[str, Any]
    producer: dict[str, Any]
    processing: dict[str, Any]


class VeriskDomainConfig(TypedDict, total=False):
    event_ingester: WorkerConfig
    download_worker: WorkerConfig
    upload_worker: WorkerConfig
    enrichment_worker: WorkerConfig
    retry_delays: list[int]


class ClaimXDomainConfig(TypedDict, total=False):
    event_ingester: WorkerConfig
    enrichment_worker: WorkerConfig
    download_worker: WorkerConfig
    retry_delays: list[int]


class StorageConfig(TypedDict, total=False):
    onelake_base_path: str
    onelake_domain_paths: dict[str, str]
    cache_dir: str


class FileLoggingConfig(TypedDict, total=False):
    enabled: bool
    level: str


class EventHubLoggingConfig(TypedDict, total=False):
    enabled: bool
    level: str
    eventhub_name: str
    batch_size: int
    batch_timeout_seconds: float
    max_queue_size: int
    circuit_breaker_threshold: int


class LoggingConfig(TypedDict, total=False):
    level: str
    log_to_stdout: bool
    log_dir: str
    file_logging: FileLoggingConfig
    eventhub_logging: EventHubLoggingConfig


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def get_config_value(env_var: str, yaml_value: str, default: str = "") -> str:
    """Get config value from env var or yaml or default."""
    value = os.getenv(env_var)
    if value:
        return value
    return yaml_value or default


def _expand_env_vars(data: Any) -> Any:
    """Recursively expand ${VAR_NAME} and ${VAR_NAME:-default} environment variables in config data."""
    if isinstance(data, dict):
        return {key: _expand_env_vars(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [_expand_env_vars(item) for item in data]
    elif isinstance(data, str):
        # Support both ${VAR} and ${VAR:-default} syntax
        pattern = r"\$\{([^}:]+)(?::-(([^}]*))?)?\}"

        def replacer(match):
            var_name = match.group(1)
            default_value = (
                match.group(2) if match.group(2) is not None else match.group(0)
            )
            return os.getenv(var_name, default_value)

        return re.sub(pattern, replacer, data)
    else:
        return data


# Default config file: config/config.yaml in src/ directory
DEFAULT_CONFIG_FILE = Path(__file__).parent.parent / "config" / "config.yaml"


@dataclass
class MessageConfig:
    """Pipeline configuration.

    Loads from YAML file with hierarchical structure organized by domain and worker.

    Configuration structure:
        pipeline:
          verisk:                     # Verisk domain
            event_ingester: {...}
            download_worker: {...}
            upload_worker: {...}
          claimx:                     # ClaimX domain
            event_ingester: {...}
            enrichment_worker: {...}
          storage: {...}              # OneLake paths
    """

    # =========================================================================
    # DOMAIN CONFIGURATIONS (verisk and claimx)
    # =========================================================================
    verisk: VeriskDomainConfig = field(default_factory=dict)
    claimx: ClaimXDomainConfig = field(default_factory=dict)

    # =========================================================================
    # STORAGE CONFIGURATION
    # =========================================================================
    onelake_base_path: str = ""
    onelake_domain_paths: dict[str, str] = field(default_factory=dict)
    cache_dir: str = field(
        default_factory=lambda: str(Path(tempfile.gettempdir()) / "pipeline_cache")
    )

    # =========================================================================
    # CLAIMX API CONFIGURATION
    # =========================================================================
    claimx_api_url: str = ""
    claimx_api_token: str = ""
    claimx_api_timeout_seconds: int = 30
    claimx_api_concurrency: int = 20

    # =========================================================================
    # LOGGING CONFIGURATION
    # =========================================================================
    logging_config: LoggingConfig = field(default_factory=dict)

    @property
    def xact(self) -> VeriskDomainConfig:
        """Backwards compatibility alias for verisk domain."""
        return self.verisk

    def _get_domain_config(
        self, domain: str
    ) -> VeriskDomainConfig | ClaimXDomainConfig:
        """Get domain config, supporting legacy 'xact' name."""
        if domain in ("xact", "verisk"):
            return self.verisk
        if domain == "claimx":
            return self.claimx
        raise ValueError(f"Unknown domain: {domain}. Must be 'verisk' or 'claimx'")

    def get_worker_config(
        self,
        domain: str,
        worker_name: str,
        component: str,  # "consumer", "producer", or "processing"
    ) -> dict[str, Any]:
        """Get configuration for a specific worker's component.

        Args:
            domain: Domain name ("verisk" or "claimx", "xact" supported as legacy alias)
            worker_name: Name of the worker (e.g., "download_worker", "event_ingester")
            component: Component type ("consumer", "producer", or "processing")

        Returns:
            dict[str, Any]: Configuration for the specified component.

        Raises:
            ValueError: If domain is not configured or component is invalid.
        """
        domain_config = self._get_domain_config(domain)
        if not domain_config:
            raise ValueError(f"No configuration found for domain: {domain}")

        if component not in ("consumer", "producer", "processing"):
            raise ValueError(
                f"Invalid component: {component}. Must be 'consumer', 'producer', or 'processing'"
            )

        worker_config = domain_config.get(worker_name, {})
        return worker_config.get(component, {})

    def get_topic(self, domain: str, topic_key: str) -> str:
        """Get topic name for domain/topic_key.

        Returns a formatted topic name string in the format "domain.topic_key".
        Used by the dummy data source for testing.
        """
        return f"{domain}.{topic_key}"

    def get_consumer_group(self, domain: str, worker_name: str) -> str:
        """Get consumer group name for a worker.

        First checks for custom group_id in worker config, otherwise constructs from prefix.
        """
        worker_config = self.get_worker_config(domain, worker_name, "consumer")
        if "group_id" in worker_config:
            return worker_config["group_id"]

        domain_config = self._get_domain_config(domain)
        prefix = domain_config.get("consumer_group_prefix", domain)
        return f"{prefix}-{worker_name}"

    def get_retry_delays(self, domain: str) -> list[int]:
        domain_config = self._get_domain_config(domain)
        return domain_config.get("retry_delays", [300, 600, 1200, 2400])

    def get_max_retries(self, domain: str) -> int:
        return len(self.get_retry_delays(domain))

    def get_storage_config(self) -> StorageConfig:
        return StorageConfig(
            onelake_base_path=self.onelake_base_path,
            onelake_domain_paths=self.onelake_domain_paths,
            cache_dir=self.cache_dir,
        )

    def validate(self) -> None:
        for domain_name in ["verisk", "claimx"]:
            domain_config = getattr(self, domain_name)
            if not domain_config:
                continue

            for worker_name, worker_config in domain_config.items():
                if worker_name in [
                    "retry_delays",
                ]:
                    continue

                if "processing" in worker_config:
                    self._validate_processing_settings(
                        worker_config["processing"],
                        f"{domain_name}.{worker_name}.processing",
                    )

        self._validate_urls()
        self._validate_directories()

    @staticmethod
    def _validate_enum(
        settings: dict[str, Any], key: str, valid_values: list[Any], context: str
    ) -> None:
        if key in settings and settings[key] not in valid_values:
            raise ValueError(
                f"{context}: {key} must be one of {valid_values}, "
                f"got '{settings[key]}'"
            )

    @staticmethod
    def _validate_min(
        settings: dict[str, Any],
        key: str,
        min_value: float,
        inclusive: bool,
        context: str,
    ) -> None:
        if key in settings:
            value = settings[key]
            if inclusive and value < min_value:
                raise ValueError(
                    f"{context}: {key} must be >= {min_value}, got {value}"
                )
            elif not inclusive and value <= min_value:
                raise ValueError(f"{context}: {key} must be > {min_value}, got {value}")

    @staticmethod
    def _validate_range(
        settings: dict[str, Any],
        key: str,
        min_value: float,
        max_value: float,
        context: str,
    ) -> None:
        if key in settings:
            value = settings[key]
            if not (min_value <= value <= max_value):
                raise ValueError(
                    f"{context}: {key} must be between {min_value} and {max_value}, got {value}"
                )

    def _validate_processing_settings(
        self, settings: dict[str, Any], context: str
    ) -> None:
        self._validate_range(settings, "concurrency", 1, 50, context)
        self._validate_min(settings, "batch_size", 1, inclusive=True, context=context)
        self._validate_min(
            settings, "timeout_seconds", 0, inclusive=False, context=context
        )
        self._validate_min(
            settings, "flush_timeout_seconds", 0, inclusive=False, context=context
        )

    def _validate_urls(self) -> None:

        if self.claimx_api_url:
            self._validate_url(self.claimx_api_url, "claimx_api_url")

    def _validate_url(self, url: str, field_name: str) -> None:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(f"{field_name} must be HTTP/HTTPS URL: {url}")
        if not parsed.netloc:
            raise ValueError(f"{field_name} missing hostname: {url}")

    def _validate_directories(self) -> None:
        import os
        from pathlib import Path

        if self.cache_dir:
            cache_path = Path(self.cache_dir)
            if cache_path.exists() and not os.access(cache_path, os.W_OK):
                raise ValueError(f"cache_dir not writable: {cache_path}")


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Deep merge overlay into base dict."""
    result = base.copy()
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _get_storage_config(pipeline_data: dict) -> dict:
    """Get storage config from pipeline.storage section only."""
    return pipeline_data.get("storage", {})


def load_config(
    config_path: Path | None = None,
    overrides: dict[str, Any] | None = None,
) -> MessageConfig:
    """Load message pipeline configuration from config.yaml file.

    Loads from single consolidated config/config.yaml file with all settings.

    Environment variables ARE supported using ${VAR_NAME} syntax in YAML files.
    """
    if config_path is None:
        config_path = DEFAULT_CONFIG_FILE

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Expected file: config/config.yaml"
        )

    logger.info("Loading configuration from file: %s", config_path)
    yaml_data = load_yaml(config_path)
    yaml_data = _expand_env_vars(yaml_data)

    if "pipeline" not in yaml_data:
        raise ValueError(
            "Invalid config file: missing 'pipeline:' section\n"
            "See config.yaml.example for correct structure"
        )

    pipeline_config = yaml_data["pipeline"]

    if overrides:
        logger.debug("Applying overrides: %s", list(overrides.keys()))
        pipeline_config = _deep_merge(pipeline_config, overrides)

    verisk_config = pipeline_config.get("verisk", {})
    claimx_config = pipeline_config.get("claimx", {})

    storage = _get_storage_config(pipeline_config)

    claimx_root = yaml_data.get("claimx", {})
    claimx_api = claimx_root.get("api", {})

    claimx_api_token = get_config_value("CLAIMX_API_TOKEN", claimx_api.get("token", ""))
    if not claimx_api_token:
        logger.warning("ClaimX API token not configured")
    else:
        logger.info("ClaimX API authentication configured")

    logging_config = yaml_data.get("logging", {})

    config = MessageConfig(
        verisk=verisk_config,
        claimx=claimx_config,
        onelake_base_path=storage.get("onelake_base_path", ""),
        onelake_domain_paths=storage.get("onelake_domain_paths", {}),
        cache_dir=storage.get("cache_dir")
        or str(Path(tempfile.gettempdir()) / "pipeline_cache"),
        claimx_api_url=get_config_value(
            "CLAIMX_API_URL", claimx_api.get("base_url", "")
        ),
        claimx_api_token=claimx_api_token,
        claimx_api_timeout_seconds=int(
            get_config_value(
                "CLAIMX_API_TIMEOUT_SECONDS", str(claimx_api.get("timeout_seconds", 30))
            )
        ),
        claimx_api_concurrency=int(
            get_config_value(
                "CLAIMX_API_CONCURRENCY", str(claimx_api.get("max_concurrent", 20))
            )
        ),
        logging_config=logging_config,
    )

    logger.debug("Configuration loaded successfully:")
    logger.debug("  - Verisk domain configured: %s", bool(config.verisk))
    logger.debug("  - ClaimX domain configured: %s", bool(config.claimx))

    logger.debug("Validating configuration...")
    config.validate()
    logger.debug("Configuration validation passed")

    return config


_message_config: MessageConfig | None = None


def get_config() -> MessageConfig:
    """Get or load the singleton message config instance."""
    global _message_config
    if _message_config is None:
        _message_config = load_config()
    return _message_config


def set_config(config: MessageConfig) -> None:
    """Set the singleton message config instance (useful for testing)."""
    global _message_config
    _message_config = config


def reset_config() -> None:
    """Reset the singleton config instance (forces reload on next get_config() call)."""
    global _message_config
    _message_config = None


def _cli_main() -> int:
    """CLI entry point for config validation and debugging."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Message Pipeline Configuration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate configuration
  python -m config.config --validate

  # Show merged configuration
  python -m config.config --show-merged

  # Both validate and show
  python -m config.config --validate --show-merged

  # Use custom config directory
  python -m config.config --config /path/to/config --validate

  # JSON output for automation
  python -m config.config --validate --json

  # Enable debug logging
  python -m config.config --validate --verbose
        """,
    )

    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate configuration structure and completeness",
    )
    parser.add_argument(
        "--show-merged",
        action="store_true",
        help="Display merged configuration as YAML",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to config.yaml file (default: src/config/config.yaml)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format instead of human-readable",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(levelname)s: %(message)s",
    )

    if not args.validate and not args.show_merged:
        parser.print_help()
        return 0

    try:
        config = load_config(config_path=args.config)

        config_path = args.config or DEFAULT_CONFIG_FILE
        config_dict = load_yaml(config_path)
        config_dict = _expand_env_vars(config_dict)

        success = True
        output = {}

        if args.validate:
            # Validation happens during load_config(), if we got here it passed
            if args.json:
                output["validation"] = {
                    "passed": True,
                    "errors": [],
                }
            else:
                print("✓ Configuration validation passed")
                print("  - Configuration structure: OK")
                if config.verisk:
                    print("  - Verisk domain: OK")
                if config.claimx:
                    print("  - ClaimX domain: OK")
                print("  - All settings validated: OK")

        if args.show_merged:
            if args.json:
                output["merged_config"] = config_dict
            else:
                print("\nConfiguration:")
                print("=" * 80)
                print(yaml.dump(config_dict, default_flow_style=False, sort_keys=False))
                print("=" * 80)

        if args.json:
            print(json.dumps(output, indent=2))

        return 0 if success else 1

    except FileNotFoundError as e:
        if args.json:
            print(json.dumps({"error": str(e)}))
        else:
            print(f"✗ Error: {e}", file=sys.stderr)
        return 1

    except ValueError as e:
        if args.json:
            print(json.dumps({"error": str(e)}))
        else:
            print(f"✗ Validation error: {e}", file=sys.stderr)
        return 1

    except Exception as e:
        if args.json:
            print(json.dumps({"error": f"Unexpected error: {e}"}))
        else:
            print(f"✗ Unexpected error: {e}", file=sys.stderr)
            if args.verbose:
                import traceback

                traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(_cli_main())
