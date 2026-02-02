"""Simulation mode for local testing without production APIs.

This module provides a complete simulation environment for testing the pipeline
locally without access to production services:

- Mock ClaimX API responses (fixtures-based)
- Local filesystem storage (replaces Azure Storage/OneLake)
- Localhost URL support in download worker
- Local Delta Lake tables

Usage:
    Enable simulation mode via CLI flag:
        python -m pipeline --simulation-mode <worker>

    Or via environment variable:
        SIMULATION_MODE=true python -m pipeline <worker>

Security:
    Simulation mode CANNOT run in production environments. The SimulationConfig
    validates environment variables and raises RuntimeError if production is detected.

Public API:
    - SimulationConfig: Configuration for simulation mode
    - LocalStorageAdapter: Local filesystem adapter mimicking OneLake blob storage
    - create_simulation_enrichment_worker: Factory for enrichment worker with mock API
    - create_simulation_upload_worker: Factory for upload worker with local storage
    - create_simulation_download_worker: Factory for download worker with localhost support
    - create_simulation_delta_writer: Factory for Delta writer with local backend
"""

from pipeline.simulation.claimx_api_mock import MockClaimXAPIClient
from pipeline.simulation.config import (
    SimulationConfig,
    get_simulation_config,
    is_simulation_mode,
)
from pipeline.simulation.factories import (
    create_simulation_delta_writer,
    create_simulation_download_worker,
    create_simulation_enrichment_worker,
    create_simulation_itel_cabinet_worker,
    create_simulation_upload_worker,
)
from pipeline.simulation.storage import LocalStorageAdapter

__all__ = [
    "SimulationConfig",
    "get_simulation_config",
    "is_simulation_mode",
    "create_simulation_enrichment_worker",
    "create_simulation_upload_worker",
    "create_simulation_download_worker",
    "create_simulation_delta_writer",
    "create_simulation_itel_cabinet_worker",
    "MockClaimXAPIClient",
    "LocalStorageAdapter",
]
