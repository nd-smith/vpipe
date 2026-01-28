"""Simulation mode configuration.

Enables local testing without production APIs or cloud infrastructure.
MUST NEVER run in production environments.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

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
    """

    # Master switch - controls whether simulation mode is active
    enabled: bool = False

    # Local filesystem paths for simulated storage
    local_storage_path: Path = Path("/tmp/vpipe_simulation")
    local_delta_path: Path = Path("/tmp/vpipe_simulation/delta")

    # Allow localhost URLs in download worker (normally blocked)
    allow_localhost_urls: bool = True

    # Fixtures directory for mock data
    fixtures_dir: Path = field(default_factory=lambda: Path(__file__).parent / "fixtures")

    # Delta Lake configuration for simulation mode
    truncate_tables_on_start: bool = False  # Clean tables before run
    delta_write_mode: str = "append"  # append, overwrite, merge

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Ensure paths are Path objects
        if not isinstance(self.local_storage_path, Path):
            self.local_storage_path = Path(self.local_storage_path)
        if not isinstance(self.local_delta_path, Path):
            self.local_delta_path = Path(self.local_delta_path)
        if not isinstance(self.fixtures_dir, Path):
            self.fixtures_dir = Path(self.fixtures_dir)

        # CRITICAL: Validate not running in production
        self.validate_not_production()

    def validate_not_production(self):
        """Ensure simulation mode cannot run in production.

        Raises:
            RuntimeError: If simulation mode is enabled in production environment
        """
        if not self.enabled:
            return  # Not enabled, no check needed

        # Check multiple environment indicators for production
        environment = os.getenv("ENVIRONMENT", "").lower()
        deployment_env = os.getenv("DEPLOYMENT_ENV", "").lower()
        app_env = os.getenv("APP_ENV", "").lower()

        production_indicators = ["production", "prod", "live"]

        for env_var, value in [
            ("ENVIRONMENT", environment),
            ("DEPLOYMENT_ENV", deployment_env),
            ("APP_ENV", app_env),
        ]:
            if any(indicator in value for indicator in production_indicators):
                raise RuntimeError(
                    f"SIMULATION MODE CANNOT RUN IN PRODUCTION. "
                    f"Detected production environment via {env_var}={value}. "
                    f"Simulation mode is for local development and testing only."
                )

    def ensure_directories(self):
        """Create simulation directories if they don't exist."""
        self.local_storage_path.mkdir(parents=True, exist_ok=True)
        self.local_delta_path.mkdir(parents=True, exist_ok=True)
        self.fixtures_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_config_file(cls, config_path: Optional[Path] = None) -> "SimulationConfig":
        """Load simulation config from dedicated simulation.yaml file.

        Priority order:
        1. config/simulation.yaml (dedicated simulation config)
        2. config/config.yaml (simulation section in main config)
        3. Default values

        Args:
            config_path: Path to config file (optional, auto-detects)

        Returns:
            SimulationConfig instance
        """
        # Auto-detect config path if not provided
        if config_path is None:
            # Try dedicated simulation config first
            sim_config_path = (
                Path(__file__).parent.parent.parent.parent / "config" / "simulation.yaml"
            )
            if sim_config_path.exists():
                config_path = sim_config_path
            else:
                # Fall back to main config
                config_path = Path(__file__).parent.parent.parent.parent / "config" / "config.yaml"

        if not config_path.exists():
            return cls()  # Return default disabled config

        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f) or {}

        simulation_data = config_data.get("simulation", {})

        # Environment variable override for enabled flag
        enabled_str = os.getenv(
            "SIMULATION_MODE", str(simulation_data.get("enabled", False))
        ).lower()
        enabled = enabled_str in ("true", "1", "yes")

        return cls(
            enabled=enabled,
            local_storage_path=Path(
                os.getenv(
                    "SIMULATION_STORAGE_PATH",
                    simulation_data.get("local_storage_path", "/tmp/vpipe_simulation"),
                )
            ),
            local_delta_path=Path(
                os.getenv(
                    "SIMULATION_DELTA_PATH",
                    simulation_data.get("local_delta_path", "/tmp/vpipe_simulation/delta"),
                )
            ),
            allow_localhost_urls=os.getenv(
                "SIMULATION_ALLOW_LOCALHOST", str(simulation_data.get("allow_localhost_urls", True))
            ).lower()
            in ("true", "1", "yes"),
            fixtures_dir=Path(
                os.getenv(
                    "SIMULATION_FIXTURES_DIR",
                    simulation_data.get("fixtures_dir", str(Path(__file__).parent / "fixtures")),
                )
            ),
            truncate_tables_on_start=os.getenv(
                "SIMULATION_TRUNCATE_TABLES",
                str(simulation_data.get("truncate_tables_on_start", False)),
            ).lower()
            in ("true", "1", "yes"),
            delta_write_mode=os.getenv(
                "SIMULATION_DELTA_WRITE_MODE", simulation_data.get("delta_write_mode", "append")
            ),
        )

    @classmethod
    def from_env(cls, enabled: bool = False) -> "SimulationConfig":
        """Create simulation config from environment variables only.

        Args:
            enabled: Whether simulation mode is enabled

        Returns:
            SimulationConfig instance
        """
        return cls(
            enabled=enabled,
            local_storage_path=Path(os.getenv("SIMULATION_STORAGE_PATH", "/tmp/vpipe_simulation")),
            local_delta_path=Path(
                os.getenv("SIMULATION_DELTA_PATH", "/tmp/vpipe_simulation/delta")
            ),
            allow_localhost_urls=os.getenv("SIMULATION_ALLOW_LOCALHOST", "true").lower()
            in ("true", "1", "yes"),
            fixtures_dir=Path(
                os.getenv("SIMULATION_FIXTURES_DIR", str(Path(__file__).parent / "fixtures"))
            ),
            truncate_tables_on_start=os.getenv("SIMULATION_TRUNCATE_TABLES", "false").lower()
            in ("true", "1", "yes"),
            delta_write_mode=os.getenv("SIMULATION_DELTA_WRITE_MODE", "append"),
        )


# Global simulation config instance
_simulation_config: Optional[SimulationConfig] = None


def is_simulation_mode() -> bool:
    """Check if simulation mode is currently enabled.

    Returns:
        True if simulation mode is enabled, False otherwise
    """
    global _simulation_config

    if _simulation_config is None:
        # Try to load from environment
        enabled_str = os.getenv("SIMULATION_MODE", "false").lower()
        enabled = enabled_str in ("true", "1", "yes")

        if enabled:
            _simulation_config = SimulationConfig.from_env(enabled=True)
        else:
            return False

    return _simulation_config.enabled


def get_simulation_config() -> SimulationConfig:
    """Get the current simulation configuration.

    Returns:
        SimulationConfig instance

    Raises:
        RuntimeError: If simulation mode is not enabled
    """
    global _simulation_config

    if _simulation_config is None:
        # Try to load from environment
        enabled_str = os.getenv("SIMULATION_MODE", "false").lower()
        enabled = enabled_str in ("true", "1", "yes")

        if enabled:
            _simulation_config = SimulationConfig.from_env(enabled=True)
        else:
            raise RuntimeError(
                "Simulation mode is not enabled. Set SIMULATION_MODE=true environment variable."
            )

    if not _simulation_config.enabled:
        raise RuntimeError(
            "Simulation mode is not enabled. Set SIMULATION_MODE=true environment variable."
        )

    return _simulation_config


def set_simulation_config(config: SimulationConfig) -> None:
    """Set the global simulation configuration.

    This is primarily for testing purposes.

    Args:
        config: SimulationConfig instance to use
    """
    global _simulation_config
    _simulation_config = config
