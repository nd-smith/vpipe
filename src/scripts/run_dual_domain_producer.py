#!/usr/bin/env python3
"""
Dual-Domain Dummy Data Producer for XACT and ClaimX.

This script generates realistic dummy events for both XACT and ClaimX domains,
with configurable itel Cabinet plugin triggers for testing the complete pipeline.

Features:
- XACT domain events with attachments (photos, documents, reports)
- ClaimX domain events (all event types)
- Configurable percentage of ClaimX events that trigger itel plugin (task_id 32513)
- Interactive mode selection
- Statistics tracking

Usage:
    python scripts/run_dual_domain_producer.py [--mode MODE] [--events N] [--rate R]

Options:
    --mode     Mode: xact, claimx, dual, itel (default: dual)
    --events   Number of events to generate (default: 100)
    --rate     Events per minute (default: 30)
    --itel-pct Percentage of task events that trigger itel (default: 30)

Examples:
    # Generate dual-domain events (default)
    python scripts/run_dual_domain_producer.py

    # Generate only XACT events
    python scripts/run_dual_domain_producer.py --mode xact

    # Generate ClaimX events with 50% itel triggers
    python scripts/run_dual_domain_producer.py --mode claimx --itel-pct 50

    # High-volume test
    python scripts/run_dual_domain_producer.py --events 1000 --rate 120

Requirements:
    - Kafka running on localhost:9094 (or KAFKA_BROKERS env var)
    - Corresponding topics configured
"""

import argparse
import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.config import KafkaConfig
from kafka_pipeline.common.dummy import DummyDataSource, DummySourceConfig
from kafka_pipeline.common.dummy.generators import GeneratorConfig
from kafka_pipeline.common.dummy.file_server import FileServerConfig


@dataclass
class ProducerMode:
    """Configuration for different producer modes."""
    name: str
    description: str
    domains: list
    plugin_profile: str | None
    include_itel_triggers: bool


# Available producer modes
MODES = {
    "xact": ProducerMode(
        name="XACT Only",
        description="Generate only XACT domain events (property claims)",
        domains=["xact"],
        plugin_profile=None,
        include_itel_triggers=False,
    ),
    "claimx": ProducerMode(
        name="ClaimX Only",
        description="Generate ClaimX events with itel triggers",
        domains=["claimx"],
        plugin_profile="mixed",
        include_itel_triggers=True,
    ),
    "dual": ProducerMode(
        name="Dual Domain",
        description="Generate both XACT and ClaimX events with itel triggers",
        domains=["xact", "claimx"],
        plugin_profile="mixed",
        include_itel_triggers=True,
    ),
    "itel": ProducerMode(
        name="itel Cabinet Only",
        description="Generate only itel Cabinet events (task_id 32513)",
        domains=["claimx"],
        plugin_profile="itel_cabinet_api",
        include_itel_triggers=True,
    ),
}


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Dual-Domain Dummy Data Producer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes:
  xact    - XACT domain only (property claim events with attachments)
  claimx  - ClaimX domain only (with itel triggers at specified percentage)
  dual    - Both XACT and ClaimX domains (default)
  itel    - itel Cabinet events only (100% task_id 32513)

Examples:
  %(prog)s                          # Dual-domain, 100 events, 30/min
  %(prog)s --mode xact --events 50  # XACT only, 50 events
  %(prog)s --mode claimx --itel-pct 50  # ClaimX with 50%% itel triggers
        """,
    )

    parser.add_argument(
        "--mode",
        choices=list(MODES.keys()),
        default="dual",
        help="Generation mode (default: dual)",
    )
    parser.add_argument(
        "--events",
        type=int,
        default=100,
        help="Number of events to generate (default: 100, 0 for unlimited)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=30.0,
        help="Events per minute (default: 30)",
    )
    parser.add_argument(
        "--itel-pct",
        type=int,
        default=30,
        help="Percentage of task events to trigger itel plugin (default: 30)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible data (optional)",
    )
    parser.add_argument(
        "--burst",
        action="store_true",
        help="Enable burst mode for load testing",
    )
    parser.add_argument(
        "--burst-size",
        type=int,
        default=50,
        help="Events per burst when burst mode is enabled (default: 50)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def print_banner():
    """Print the startup banner."""
    print()
    print("=" * 80)
    print("  Dual-Domain Dummy Data Producer")
    print("  XACT + ClaimX + itel Cabinet Triggers")
    print("=" * 80)
    print()


def print_mode_info(mode: ProducerMode, args):
    """Print information about the selected mode."""
    print(f"Mode: {mode.name}")
    print(f"  {mode.description}")
    print()
    print("Configuration:")
    print(f"  Domains: {', '.join(mode.domains)}")
    print(f"  Events: {args.events if args.events > 0 else 'Unlimited'}")
    print(f"  Rate: {args.rate} events/minute")
    if mode.include_itel_triggers:
        print(f"  itel Trigger Rate: {args.itel_pct}% of task events")
    print(f"  Burst Mode: {'Enabled' if args.burst else 'Disabled'}")
    if args.seed:
        print(f"  Seed: {args.seed} (reproducible data)")
    print()


def build_config(args, mode: ProducerMode) -> DummySourceConfig:
    """Build the DummySourceConfig from arguments."""
    # Kafka configuration
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9094")

    kafka_config = KafkaConfig(
        bootstrap_servers=kafka_brokers,
        security_protocol="PLAINTEXT",
        xact={
            "topics": {
                "events": "xact.events.raw",
            }
        },
        claimx={
            "topics": {
                "events": "claimx.events.raw",
            }
        },
    )

    # Generator configuration
    generator_config = GeneratorConfig(
        plugin_profile=mode.plugin_profile,
        base_url="http://localhost:8765",
        include_failures=False,
        seed=args.seed,
        itel_trigger_percentage=args.itel_pct / 100.0,
        include_itel_triggers=mode.include_itel_triggers,
    )

    # File server configuration
    file_server_config = FileServerConfig(
        host="0.0.0.0",
        port=8765,
        default_file_size=100_000,
        max_file_size=10_000_000,
    )

    # Main config
    return DummySourceConfig(
        kafka=kafka_config,
        generator=generator_config,
        file_server=file_server_config,
        domains=mode.domains,
        events_per_minute=args.rate,
        burst_mode=args.burst,
        burst_size=args.burst_size,
        burst_interval_seconds=60,
        include_all_event_types=True,
        max_active_claims=100,
        max_events=args.events if args.events > 0 else None,
    )


async def run_producer(config: DummySourceConfig) -> Dict[str, Any]:
    """Run the dummy data producer and return statistics."""
    print("Starting producer...")
    print("Press Ctrl+C to stop")
    print()

    try:
        async with DummyDataSource(config) as source:
            await source.run()
            return source.stats
    except KeyboardInterrupt:
        print("\nStopped by user")
        return {}
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return {}


def print_summary(stats: Dict[str, Any]):
    """Print the final summary."""
    print()
    print("=" * 80)
    print("  Summary")
    print("=" * 80)

    if stats:
        print(f"  Total Events Generated: {stats.get('total_events', 0)}")
        print(f"  Active Claims Tracked: {stats.get('active_claims', 0)}")
        print(f"  Elapsed Time: {stats.get('elapsed_seconds', 0):.1f} seconds")
        if stats.get("elapsed_seconds", 0) > 0:
            eps = stats.get("events_per_second", 0)
            print(f"  Average Rate: {eps:.2f} events/second ({eps * 60:.1f}/minute)")
    else:
        print("  No statistics available")

    print("=" * 80)
    print()


async def main():
    """Main entry point."""
    args = parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Suppress noisy loggers
    if not args.verbose:
        logging.getLogger("aiokafka").setLevel(logging.WARNING)
        logging.getLogger("aiohttp").setLevel(logging.WARNING)

    print_banner()

    # Get the selected mode
    mode = MODES[args.mode]
    print_mode_info(mode, args)

    # Build configuration
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9094")
    print(f"Kafka Brokers: {kafka_brokers}")
    print()

    config = build_config(args, mode)

    # Run the producer
    stats = await run_producer(config)

    # Print summary
    print_summary(stats)

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
