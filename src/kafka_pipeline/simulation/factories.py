"""Factory functions for creating simulation mode workers.

These factories create workers with mocked dependencies for local testing.
All simulation workers are isolated in this module and should use dependency
injection to replace production services with mock implementations.
"""

from typing import Any

from config.config import KafkaConfig
from kafka_pipeline.simulation.config import SimulationConfig


def create_simulation_enrichment_worker(
    config: KafkaConfig,
    simulation_config: SimulationConfig = None,
    domain: str = "claimx",
    **kwargs,
) -> Any:
    """Create enrichment worker with simulation-mode dependencies.

    Returns enrichment worker configured for simulation mode:
    - ClaimX: Uses MockClaimXAPIClient instead of real API
    - XACT: Uses standard worker (no external API needed)

    Args:
        config: Kafka configuration
        simulation_config: Simulation mode configuration (optional, will load from env if None)
        domain: Domain name ("claimx" or "xact")
        **kwargs: Additional arguments passed to worker

    Returns:
        Enrichment worker instance for the specified domain

    Example:
        >>> from config.config import load_config
        >>> config = load_config()
        >>> worker = create_simulation_enrichment_worker(config, domain="claimx")
        >>> # ClaimX worker now uses MockClaimXAPIClient for all API calls
    """
    import logging
    from pathlib import Path
    from kafka_pipeline.simulation.config import get_simulation_config

    logger = logging.getLogger(__name__)

    # Get simulation config if not provided
    if simulation_config is None:
        simulation_config = get_simulation_config()

    if domain == "claimx":
        from kafka_pipeline.simulation.claimx_api_mock import MockClaimXAPIClient
        from kafka_pipeline.claimx.workers.enrichment_worker import (
            ClaimXEnrichmentWorker,
        )

        # Create mock API client with fixtures
        mock_client = MockClaimXAPIClient(fixtures_dir=simulation_config.fixtures_dir)

        # Create enrichment worker with mock client injected
        worker = ClaimXEnrichmentWorker(
            config=config, api_client=mock_client, domain=domain, **kwargs
        )

        logger.info(
            "Created simulation enrichment worker",
            extra={
                "domain": domain,
                "fixtures_dir": str(simulation_config.fixtures_dir),
                "worker_type": "ClaimXEnrichmentWorker",
                "api_client_type": "MockClaimXAPIClient",
            },
        )
    elif domain == "xact":
        from kafka_pipeline.verisk.workers.enrichment_worker import XACTEnrichmentWorker

        # XACT enrichment doesn't use external APIs, so no mocking needed
        worker = XACTEnrichmentWorker(config=config, domain=domain, **kwargs)

        logger.info(
            "Created simulation enrichment worker",
            extra={
                "domain": domain,
                "worker_type": "XACTEnrichmentWorker",
                "note": "XACT enrichment uses plugins only, no API mocking needed",
            },
        )
    else:
        raise ValueError(f"Unsupported domain for simulation enrichment: {domain}")

    return worker


def create_simulation_upload_worker(
    config: KafkaConfig, simulation_config: SimulationConfig, domain: str, **kwargs
) -> Any:
    """Create upload worker with local storage adapter.

    Returns upload worker configured for simulation mode:
    - Uses LocalStorageAdapter instead of OneLake
    - Writes files to /tmp/pcesdopodappv1_simulation/
    - Same directory structure as OneLake

    Args:
        config: Kafka configuration
        simulation_config: Simulation mode configuration
        domain: Domain identifier (e.g., "xact", "claimx")
        **kwargs: Additional arguments passed to worker (e.g., instance_id)

    Returns:
        Upload worker instance with local storage adapter

    Raises:
        ValueError: If domain is not recognized

    Example:
        >>> from kafka_pipeline.simulation import create_simulation_upload_worker
        >>> from kafka_pipeline.simulation.config import SimulationConfig
        >>> from config.config import KafkaConfig
        >>> config = KafkaConfig.from_env()
        >>> sim_config = SimulationConfig.from_env(enabled=True)
        >>> worker = create_simulation_upload_worker(config, sim_config, "claimx")
        >>> # Worker will write to /tmp/pcesdopodappv1_simulation/claimx/
    """
    import logging
    from kafka_pipeline.simulation.storage import LocalStorageAdapter

    logger = logging.getLogger(__name__)

    # Create local storage adapter
    storage_adapter = LocalStorageAdapter(
        base_path=simulation_config.local_storage_path,
        track_metadata=True,  # Enable metadata tracking for debugging
    )

    # Import appropriate worker based on domain
    if domain == "claimx":
        from kafka_pipeline.claimx.workers.upload_worker import ClaimXUploadWorker

        worker = ClaimXUploadWorker(
            config=config,
            domain=domain,
            storage_client=storage_adapter,  # Inject local storage
            **kwargs,
        )
    elif domain == "xact":
        from kafka_pipeline.verisk.workers.upload_worker import UploadWorker

        worker = UploadWorker(
            config=config,
            domain=domain,
            storage_client=storage_adapter,  # Inject local storage
            **kwargs,
        )
    else:
        raise ValueError(f"Unknown domain: {domain}. Supported domains: claimx, xact")

    logger.info(
        "Created simulation upload worker",
        extra={
            "domain": domain,
            "storage_path": str(simulation_config.local_storage_path),
            "worker_type": type(worker).__name__,
            "track_metadata": True,
        },
    )

    return worker


def create_simulation_download_worker(
    config: KafkaConfig,
    simulation_config: SimulationConfig,
    domain: str,
) -> Any:
    """Create download worker with localhost URL support.

    This factory will be implemented in a future work package to create a
    download worker that can download from localhost URLs (normally blocked
    for security) for testing with local file servers.

    Args:
        config: Kafka configuration
        simulation_config: Simulation mode configuration
        domain: Domain identifier (e.g., "xact", "claimx")

    Returns:
        Download worker with localhost support enabled

    Raises:
        NotImplementedError: To be implemented in future work package
    """
    raise NotImplementedError(
        "Simulation download worker will be implemented in a future work package. "
        "This will create a download worker with localhost URL support enabled "
        f"for testing with local file servers for domain '{domain}'."
    )


def create_simulation_delta_writer(
    config: KafkaConfig,
    simulation_config: SimulationConfig,
    domain: str,
) -> Any:
    """Create Delta writer with local storage backend.

    This factory will be implemented in a future work package to create a
    Delta Lake writer that uses local filesystem instead of OneLake/cloud storage.

    Args:
        config: Kafka configuration
        simulation_config: Simulation mode configuration
        domain: Domain identifier (e.g., "xact", "claimx")

    Returns:
        Delta writer with local filesystem backend

    Raises:
        NotImplementedError: To be implemented in future work package
    """
    raise NotImplementedError(
        "Simulation Delta writer will be implemented in a future work package. "
        "This will create a Delta Lake writer that uses local filesystem storage "
        f"instead of OneLake/cloud storage for domain '{domain}'."
    )


def create_simulation_itel_cabinet_worker(
    config: KafkaConfig, simulation_config: SimulationConfig = None, **kwargs
) -> Any:
    """Create iTel Cabinet API worker for simulation mode.

    Returns iTel Cabinet API worker configured for simulation mode:
    - Writes submissions to local files instead of calling iTel API
    - Files written to /tmp/pcesdopodappv1_simulation/itel_submissions/
    - Same data format as API payload for testing

    The worker auto-detects simulation mode via environment variable,
    so this factory simply creates the worker and lets it configure itself.

    Args:
        config: Kafka configuration
        simulation_config: Simulation mode configuration (optional, unused)
        **kwargs: Additional arguments passed to worker initialization

    Returns:
        ItelCabinetApiWorker instance configured for simulation

    Example:
        >>> from config.config import load_config
        >>> from kafka_pipeline.simulation import create_simulation_itel_cabinet_worker
        >>> config = load_config()
        >>> worker = create_simulation_itel_cabinet_worker(config)
        >>> # Worker will write to /tmp/pcesdopodappv1_simulation/itel_submissions/
    """
    import logging
    from kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker import (
        ItelCabinetApiWorker,
    )
    from kafka_pipeline.plugins.shared.connections import ConnectionManager
    import os
    from pathlib import Path
    import yaml

    logger = logging.getLogger(__name__)

    # Load worker configuration
    config_dir = Path(__file__).parent.parent.parent / "config"
    workers_config_path = (
        config_dir / "plugins" / "claimx" / "itel_cabinet_api" / "workers.yaml"
    )

    if not workers_config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {workers_config_path}")

    with open(workers_config_path, "r", encoding="utf-8") as f:
        config_data = yaml.safe_load(f) or {}

    worker_config = config_data.get("workers", {}).get("itel_cabinet_api", {})

    if not worker_config:
        raise ValueError("Worker 'itel_cabinet_api' not found in configuration")

    # Kafka configuration
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    kafka_config = {
        "bootstrap_servers": kafka_servers,
        "input_topic": worker_config["kafka"]["input_topic"],
        "consumer_group": worker_config["kafka"]["consumer_group"],
    }

    # API configuration - simulation mode will override behavior
    api_config = worker_config.get("api", {})

    # Create connection manager (won't be used in simulation mode)
    connection_manager = ConnectionManager()

    # Create worker - it will auto-detect simulation mode
    worker = ItelCabinetApiWorker(
        kafka_config=kafka_config,
        api_config=api_config,
        connection_manager=connection_manager,
    )

    logger.info(
        "Created simulation iTel Cabinet API worker",
        extra={
            "input_topic": kafka_config["input_topic"],
            "consumer_group": kafka_config["consumer_group"],
            "simulation_mode": True,
        },
    )

    return worker
