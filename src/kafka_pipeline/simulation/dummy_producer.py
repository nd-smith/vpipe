"""Dummy data producer for simulation mode only.

This tool generates realistic test data for the simulation pipeline.
DO NOT USE IN PRODUCTION.

Usage:
    # Via simulation module
    python -m kafka_pipeline.simulation.dummy_producer --domains claimx --max-events 100

    # Or as a module directly
    python -m kafka_pipeline.simulation.dummy_producer

IMPORTANT: This tool only runs in simulation mode. Set SIMULATION_MODE=true.
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

import yaml

from core.logging.context import set_log_context
from core.logging.setup import get_logger

logger = get_logger(__name__)


def parse_args():
    """Parse command-line arguments for dummy data producer."""
    parser = argparse.ArgumentParser(
        description="Generate dummy data for simulation mode testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Generate 100 ClaimX events
    SIMULATION_MODE=true python -m kafka_pipeline.simulation.dummy_producer --domains claimx --max-events 100

    # Generate events at 60/minute for both domains
    SIMULATION_MODE=true python -m kafka_pipeline.simulation.dummy_producer --domains claimx,xact --events-per-minute 60

    # iTel Cabinet workflow testing
    SIMULATION_MODE=true python -m kafka_pipeline.simulation.dummy_producer --plugin-profile itel_cabinet_api --max-events 20

    # Burst mode for load testing
    SIMULATION_MODE=true python -m kafka_pipeline.simulation.dummy_producer --burst-mode --burst-size 100

Environment Variables:
    SIMULATION_MODE=true       # Required - enables simulation mode
    KAFKA_BOOTSTRAP_SERVERS    # Kafka connection (default: localhost:9092)
        """,
    )

    parser.add_argument(
        "--domains",
        type=str,
        default="claimx,xact",
        help="Comma-separated list of domains to generate events for (default: claimx,xact)",
    )

    parser.add_argument(
        "--events-per-minute",
        type=float,
        default=10.0,
        help="Events per minute per domain (default: 10.0)",
    )

    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after N events (default: unlimited)",
    )

    parser.add_argument(
        "--max-runtime-seconds",
        type=int,
        default=None,
        help="Stop after N seconds (default: unlimited)",
    )

    parser.add_argument(
        "--burst-mode",
        action="store_true",
        help="Generate events in bursts for load testing",
    )

    parser.add_argument(
        "--burst-size",
        type=int,
        default=50,
        help="Events per burst when in burst mode (default: 50)",
    )

    parser.add_argument(
        "--burst-interval-seconds",
        type=int,
        default=60,
        help="Seconds between bursts (default: 60)",
    )

    parser.add_argument(
        "--plugin-profile",
        type=str,
        default=None,
        help="Plugin profile to use (e.g., itel_cabinet_api, mixed)",
    )

    parser.add_argument(
        "--itel-trigger-percentage",
        type=float,
        default=0.3,
        help="Percentage of events that trigger iTel (0.0-1.0, default: 0.3)",
    )

    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config file (default: auto-detect config/config.yaml)",
    )

    return parser.parse_args()


async def main():
    """Run dummy data producer in simulation mode."""
    from kafka_pipeline.common.dummy.source import DummyDataSource, DummySourceConfig
    from kafka_pipeline.common.dummy.generators import GeneratorConfig
    from kafka_pipeline.common.dummy.file_server import FileServerConfig
    from kafka_pipeline.simulation import is_simulation_mode, get_simulation_config
    from config.config import load_config

    set_log_context(stage="dummy-producer")

    # CRITICAL: Only run in simulation mode
    if not is_simulation_mode():
        print(
            "ERROR: Dummy data producer can only run in simulation mode.\n"
            "Set SIMULATION_MODE=true environment variable.\n"
            "\n"
            "Example:\n"
            "  export SIMULATION_MODE=true\n"
            "  python -m kafka_pipeline.simulation.dummy_producer --domains claimx --max-events 100\n"
            "\n"
            "Or use the convenience script:\n"
            "  ./scripts/generate_test_data.sh\n"
        )
        sys.exit(1)

    # Parse arguments
    args = parse_args()

    # Load configs
    kafka_config = load_config()
    simulation_config = get_simulation_config()

    logger.info(
        "Starting dummy data producer (simulation mode)",
        extra={
            "simulation_mode": True,
            "domains": args.domains,
            "events_per_minute": args.events_per_minute,
            "max_events": args.max_events,
            "plugin_profile": args.plugin_profile,
        },
    )

    # Parse domains
    domains = [d.strip() for d in args.domains.split(",")]

    # Create generator config
    generator_config = GeneratorConfig(
        base_url="http://localhost:8765",  # Will be updated by file server
        plugin_profile=args.plugin_profile,
        itel_trigger_percentage=args.itel_trigger_percentage,
        include_itel_triggers=(
            args.plugin_profile == "mixed" or args.plugin_profile is None
        ),
    )

    # Create file server config
    file_server_config = FileServerConfig(
        host="0.0.0.0",
        port=8765,
        default_file_size=100_000,
        max_file_size=10_000_000,
    )

    # Check if we should skip embedded file server (for Docker with separate file-server service)
    import os

    skip_embedded_file_server = (
        os.getenv("SKIP_EMBEDDED_FILE_SERVER", "false").lower() == "true"
    )
    external_file_server_url = os.getenv("EXTERNAL_FILE_SERVER_URL")

    # Create dummy source config
    dummy_config = DummySourceConfig(
        kafka=kafka_config,
        generator=generator_config,
        file_server=file_server_config,
        domains=domains,
        events_per_minute=args.events_per_minute,
        burst_mode=args.burst_mode,
        burst_size=args.burst_size,
        burst_interval_seconds=args.burst_interval_seconds,
        max_events=args.max_events,
        max_runtime_seconds=args.max_runtime_seconds,
        skip_embedded_file_server=skip_embedded_file_server,
        external_file_server_url=external_file_server_url,
    )

    logger.info(
        "Dummy source configuration",
        extra={
            "domains": dummy_config.domains,
            "events_per_minute": dummy_config.events_per_minute,
            "burst_mode": dummy_config.burst_mode,
            "max_events": dummy_config.max_events,
            "plugin_profile": generator_config.plugin_profile,
        },
    )

    # Run dummy source
    async with DummyDataSource(dummy_config) as source:
        try:
            await source.run()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, stopping...")
        finally:
            logger.info(
                "Dummy source completed",
                extra={"stats": source.stats, "simulation_mode": True},
            )


if __name__ == "__main__":
    asyncio.run(main())
