"""Simulation mode configuration.

Enables local testing without production APIs or cloud infrastructure.
MUST NEVER run in production environments.

VALIDATION GUARANTEE:
All validation happens immediately at config creation time (__post_init__).
If validation fails, a RuntimeError is raised and the config object is NEVER created.
Workers can trust that any SimulationConfig instance is valid - no defensive checks needed.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class SimulationConfig:
    """Configuration for simulation mode.

    Simulation mode allows local testing without access to:
    - ClaimX API (mocked)
    - Azure Storage/OneLake (replaced with local filesystem)
    - Production Eventhouse (uses fixtures)
    - Cloud Delta Lake (replaced with local Delta tables)

    Security: Simulation mode MUST NOT run in production.

    VALIDATION GUARANTEE:
    All validation occurs in __post_init__ and FAILS LOUDLY with RuntimeError.
    No silent failures. No fallbacks. Either config is valid or construction fails.
    Workers can trust the config without defensive try-catch blocks.
    """

    # Master switch - controls whether simulation mode is active
    enabled: bool = False

    # Local filesystem paths for simulated storage
    local_storage_path: Path = Path("/tmp/pcesdopodappv1_simulation")
    local_delta_path: Path = Path("/tmp/pcesdopodappv1_simulation/delta")

    # Allow localhost URLs in download worker (normally blocked)
    allow_localhost_urls: bool = True

    # Fixtures directory for mock data
    fixtures_dir: Path = field(
        default_factory=lambda: Path(__file__).parent / "fixtures"
    )

    # Delta Lake configuration for simulation mode
    truncate_tables_on_start: bool = False  # Clean tables before run
    delta_write_mode: str = "append"  # append, overwrite, merge

    def __post_init__(self):
        """Validate configuration immediately after initialization.

        ALL VALIDATION HAPPENS HERE. FAILS FAST AND LOUD.

        This method enforces the validation guarantee:
        - If this returns, the config is valid
        - If validation fails, RuntimeError is raised (config never created)
        - No silent failures or fallback values

        Validation checks:
        1. Type conversion and normalization (paths)
        2. Path safety (absolute paths required)
        3. Delta write mode validation
        4. Fixtures directory existence (when enabled)
        5. Production environment detection (CRITICAL)

        Raises:
            RuntimeError: If any validation fails
        """
        # Convert and validate paths
        self._validate_and_normalize_paths()

        # Validate delta write mode
        self._validate_delta_write_mode()

        # Validate fixtures directory exists when enabled
        self._validate_fixtures_directory()

        # CRITICAL: Validate not running in production
        self.validate_not_production()

    def _validate_and_normalize_paths(self):
        """Ensure paths are Path objects and validate they're absolute.

        Raises:
            RuntimeError: If path conversion fails or paths are not absolute
        """
        try:
            # Convert to Path objects
            if not isinstance(self.local_storage_path, Path):
                self.local_storage_path = Path(self.local_storage_path)
            if not isinstance(self.local_delta_path, Path):
                self.local_delta_path = Path(self.local_delta_path)
            if not isinstance(self.fixtures_dir, Path):
                self.fixtures_dir = Path(self.fixtures_dir)

            # Validate paths are absolute (relative paths are unsafe)
            if not self.local_storage_path.is_absolute():
                raise RuntimeError(
                    f"Simulation storage path must be absolute, got: {self.local_storage_path}"
                )
            if not self.local_delta_path.is_absolute():
                raise RuntimeError(
                    f"Simulation delta path must be absolute, got: {self.local_delta_path}"
                )
            if not self.fixtures_dir.is_absolute():
                raise RuntimeError(
                    f"Simulation fixtures directory must be absolute, got: {self.fixtures_dir}"
                )
        except (TypeError, ValueError) as e:
            raise RuntimeError(f"Invalid path configuration: {e}") from e

    def _validate_delta_write_mode(self):
        """Validate delta_write_mode is a supported value.

        Raises:
            RuntimeError: If delta_write_mode is not valid
        """
        valid_modes = {"append", "overwrite", "merge"}
        if self.delta_write_mode not in valid_modes:
            raise RuntimeError(
                f"Invalid delta_write_mode: '{self.delta_write_mode}'. "
                f"Must be one of: {', '.join(sorted(valid_modes))}"
            )

    def _validate_fixtures_directory(self):
        """Validate fixtures directory exists when simulation is enabled.

        Raises:
            RuntimeError: If enabled=True but fixtures_dir doesn't exist
        """
        if self.enabled and not self.fixtures_dir.exists():
            raise RuntimeError(
                f"Simulation mode enabled but fixtures directory does not exist: {self.fixtures_dir}. "
                f"Create the directory or disable simulation mode."
            )

    def validate_not_production(self):
        """Ensure simulation mode cannot run in production.

        CRITICAL SECURITY CHECK - FAILS LOUDLY.

        Checks multiple environment variables for production indicators.
        If any production marker is detected, raises RuntimeError immediately.
        No silent failures - this MUST block simulation mode in production.

        Environment variables checked:
        - ENVIRONMENT
        - DEPLOYMENT_ENV
        - APP_ENV
        - AZURE_FUNCTIONS_ENVIRONMENT
        - ASPNETCORE_ENVIRONMENT

        Production indicators:
        - production, prod, live (case-insensitive)

        Raises:
            RuntimeError: If simulation mode is enabled in production environment
        """
        if not self.enabled:
            return  # Not enabled, no check needed

        # Check multiple environment indicators for production
        environment = os.getenv("ENVIRONMENT", "").lower()
        deployment_env = os.getenv("DEPLOYMENT_ENV", "").lower()
        app_env = os.getenv("APP_ENV", "").lower()
        azure_env = os.getenv("AZURE_FUNCTIONS_ENVIRONMENT", "").lower()
        aspnet_env = os.getenv("ASPNETCORE_ENVIRONMENT", "").lower()

        production_indicators = ["production", "prod", "live"]

        for env_var, value in [
            ("ENVIRONMENT", environment),
            ("DEPLOYMENT_ENV", deployment_env),
            ("APP_ENV", app_env),
            ("AZURE_FUNCTIONS_ENVIRONMENT", azure_env),
            ("ASPNETCORE_ENVIRONMENT", aspnet_env),
        ]:
            if any(indicator in value for indicator in production_indicators):
                raise RuntimeError(
                    f"SIMULATION MODE CANNOT RUN IN PRODUCTION. "
                    f"Detected production environment via {env_var}={value}. "
                    f"Simulation mode is for local development and testing only. "
                    f"This is a CRITICAL SECURITY VIOLATION."
                )

    def ensure_directories(self):
        """Create simulation directories if they don't exist."""
        self.local_storage_path.mkdir(parents=True, exist_ok=True)
        self.local_delta_path.mkdir(parents=True, exist_ok=True)
        self.fixtures_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_config_file(cls, config_path: Path | None = None) -> "SimulationConfig":
        """Load simulation config from dedicated simulation.yaml file.

        VALIDATION GUARANTEE:
        All validation happens in __post_init__ after construction.
        If this method returns, the config is guaranteed valid.
        If validation fails, RuntimeError is raised.

        Priority order:
        1. config/simulation.yaml (dedicated simulation config)
        2. config/config.yaml (simulation section in main config)
        3. Default values

        Args:
            config_path: Path to config file (optional, auto-detects)

        Returns:
            SimulationConfig instance (guaranteed valid)

        Raises:
            RuntimeError: If validation fails in __post_init__
            FileNotFoundError: If specified config_path doesn't exist
            yaml.YAMLError: If config file is invalid YAML
        """
        # Auto-detect config path if not provided
        if config_path is None:
            # Try dedicated simulation config first
            sim_config_path = (
                Path(__file__).parent.parent.parent.parent
                / "config"
                / "simulation.yaml"
            )
            if sim_config_path.exists():
                config_path = sim_config_path
            else:
                # Fall back to main config
                config_path = (
                    Path(__file__).parent.parent.parent.parent
                    / "config"
                    / "config.yaml"
                )

        if not config_path.exists():
            # Return default disabled config (still validates in __post_init__)
            return cls()

        try:
            with open(config_path) as f:
                config_data = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise RuntimeError(f"Failed to parse config file {config_path}: {e}") from e

        simulation_data = config_data.get("simulation", {})

        # Environment variable override for enabled flag
        enabled_str = os.getenv(
            "SIMULATION_MODE", str(simulation_data.get("enabled", False))
        ).lower()
        enabled = enabled_str in ("true", "1", "yes")

        # Construct config - validation happens in __post_init__
        return cls(
            enabled=enabled,
            local_storage_path=Path(
                os.getenv(
                    "SIMULATION_STORAGE_PATH",
                    simulation_data.get(
                        "local_storage_path", "/tmp/pcesdopodappv1_simulation"
                    ),
                )
            ),
            local_delta_path=Path(
                os.getenv(
                    "SIMULATION_DELTA_PATH",
                    simulation_data.get(
                        "local_delta_path", "/tmp/pcesdopodappv1_simulation/delta"
                    ),
                )
            ),
            allow_localhost_urls=os.getenv(
                "SIMULATION_ALLOW_LOCALHOST",
                str(simulation_data.get("allow_localhost_urls", True)),
            ).lower()
            in ("true", "1", "yes"),
            fixtures_dir=Path(
                os.getenv(
                    "SIMULATION_FIXTURES_DIR",
                    simulation_data.get(
                        "fixtures_dir", str(Path(__file__).parent / "fixtures")
                    ),
                )
            ),
            truncate_tables_on_start=os.getenv(
                "SIMULATION_TRUNCATE_TABLES",
                str(simulation_data.get("truncate_tables_on_start", False)),
            ).lower()
            in ("true", "1", "yes"),
            delta_write_mode=os.getenv(
                "SIMULATION_DELTA_WRITE_MODE",
                simulation_data.get("delta_write_mode", "append"),
            ),
        )

    @classmethod
    def from_env(cls, enabled: bool = False) -> "SimulationConfig":
        """Create simulation config from environment variables only.

        VALIDATION GUARANTEE:
        All validation happens in __post_init__ after construction.
        If this method returns, the config is guaranteed valid.
        If validation fails, RuntimeError is raised.

        Args:
            enabled: Whether simulation mode is enabled

        Returns:
            SimulationConfig instance (guaranteed valid)

        Raises:
            RuntimeError: If validation fails in __post_init__
        """
        # Construct config - validation happens in __post_init__
        return cls(
            enabled=enabled,
            local_storage_path=Path(
                os.getenv("SIMULATION_STORAGE_PATH", "/tmp/pcesdopodappv1_simulation")
            ),
            local_delta_path=Path(
                os.getenv(
                    "SIMULATION_DELTA_PATH", "/tmp/pcesdopodappv1_simulation/delta"
                )
            ),
            allow_localhost_urls=os.getenv("SIMULATION_ALLOW_LOCALHOST", "true").lower()
            in ("true", "1", "yes"),
            fixtures_dir=Path(
                os.getenv(
                    "SIMULATION_FIXTURES_DIR", str(Path(__file__).parent / "fixtures")
                )
            ),
            truncate_tables_on_start=os.getenv(
                "SIMULATION_TRUNCATE_TABLES", "false"
            ).lower()
            in ("true", "1", "yes"),
            delta_write_mode=os.getenv("SIMULATION_DELTA_WRITE_MODE", "append"),
        )


# ============================================================================
# GLOBAL SIMULATION CONFIG
# ============================================================================
# MUST be initialized via set_simulation_config() during application startup.
# NEVER initialize lazily in worker code - this prevents fail-fast behavior.
#
# Initialization pattern (in __main__.py):
#   if args.simulation_mode:
#       sim_config = SimulationConfig.from_env(enabled=True)  # Validates here
#       set_simulation_config(sim_config)  # Sets global
#
# Workers can then safely call:
#   if is_simulation_mode():  # Safe check, returns False if not initialized
#       config = get_simulation_config()  # Fails fast if not initialized
# ============================================================================

_simulation_config: SimulationConfig | None = None


def is_simulation_mode() -> bool:
    """Check if simulation mode is currently enabled.

    FAIL-FAST GUARANTEE:
    This function NEVER creates config silently. It only checks a config that was
    explicitly initialized via set_simulation_config() during startup.

    If no config exists, this returns False (safe default). Workers that need
    simulation features should call get_simulation_config() to fail fast if
    simulation is required but not initialized.

    Returns:
        True if simulation mode is enabled, False if not enabled or not initialized
    """
    global _simulation_config

    # Safe default: if no config initialized, simulation is not enabled
    if _simulation_config is None:
        return False

    return _simulation_config.enabled


def get_simulation_config() -> SimulationConfig:
    """Get the current simulation configuration.

    FAIL-FAST GUARANTEE:
    This function NEVER creates config silently. It only returns a config that was
    explicitly initialized via set_simulation_config() (typically in __main__.py).

    If no config exists, this raises RuntimeError immediately. Workers should call
    this during startup (not in worker loop) to fail fast if config is missing.

    Returns:
        SimulationConfig instance (guaranteed to be valid)

    Raises:
        RuntimeError: If simulation config was not initialized via set_simulation_config()
                     or if simulation mode is not enabled
    """
    global _simulation_config

    if _simulation_config is None:
        raise RuntimeError(
            "Simulation config not initialized. "
            "Call set_simulation_config() during application startup before creating workers. "
            "This should happen in __main__.py or test setup, NEVER in worker code."
        )

    if not _simulation_config.enabled:
        raise RuntimeError(
            "Simulation mode is not enabled in the initialized config. "
            "Verify SIMULATION_MODE=true environment variable or config file settings."
        )

    return _simulation_config


def set_simulation_config(config: SimulationConfig) -> None:
    """Set the global simulation configuration.

    MUST be called during application startup (__main__.py) to initialize simulation config.

    This function enforces that the config has already been validated:
    - SimulationConfig.__post_init__ validates on construction
    - If config construction succeeded, it's guaranteed valid
    - No additional validation needed here

    Usage:
        # In __main__.py at startup
        if args.simulation_mode:
            sim_config = SimulationConfig.from_env(enabled=True)  # Validates here
            set_simulation_config(sim_config)

        # In tests
        with patch('kafka_pipeline.simulation.config._simulation_config', test_config):
            # test code

    Args:
        config: SimulationConfig instance (already validated via __post_init__)

    Raises:
        TypeError: If config is not a SimulationConfig instance
    """
    global _simulation_config

    if not isinstance(config, SimulationConfig):
        raise TypeError(
            f"config must be SimulationConfig instance, got {type(config).__name__}"
        )

    _simulation_config = config
