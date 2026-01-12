#!/usr/bin/env python3
"""
Quick test script for itel Cabinet API dummy data producer.

This script runs the dummy data producer with itel Cabinet profile
without requiring changes to config.yaml.

Usage:
    python scripts/run_itel_dummy_producer.py

Requirements:
    - Kafka running on localhost:9092
    - ClaimX topics configured
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.config import KafkaConfig
from kafka_pipeline.common.dummy import DummyDataSource, DummySourceConfig
from kafka_pipeline.common.dummy.generators import GeneratorConfig
from kafka_pipeline.common.dummy.file_server import FileServerConfig


async def main():
    """Run itel Cabinet dummy data producer."""
    print("=" * 80)
    print("itel Cabinet API Dummy Data Producer")
    print("=" * 80)
    print()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Kafka configuration
    # Use port 9094 for external connections (from host machine)
    # Use port 9092 only when running inside Docker network
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9094")
    print(f"Kafka brokers: {kafka_brokers}")

    # For itel Cabinet testing, we can write to either:
    # 1. claimx.events.raw (goes through full pipeline)
    # 2. itel.cabinet.task.tracking (direct to itel worker)
    target_topic = os.getenv("TARGET_TOPIC", "itel.cabinet.task.tracking")

    kafka_config = KafkaConfig(
        bootstrap_servers=kafka_brokers,
        security_protocol="PLAINTEXT",
        claimx={
            "topics": {
                "events": target_topic  # Override to write to itel topic directly
            }
        },
    )

    # Generator configuration with itel Cabinet profile
    generator_config = GeneratorConfig(
        plugin_profile="itel_cabinet_api",  # Use itel Cabinet profile
        base_url="http://localhost:8765",
        include_failures=False,
        seed=None,  # Random data each run (use seed=42 for reproducible)
    )

    # File server configuration
    file_server_config = FileServerConfig(
        host="0.0.0.0",
        port=8765,
        default_file_size=100_000,
        max_file_size=10_000_000,
    )

    # Dummy source configuration
    config = DummySourceConfig(
        kafka=kafka_config,
        generator=generator_config,
        file_server=file_server_config,
        domains=["claimx"],  # Only ClaimX for itel Cabinet
        events_per_minute=10.0,  # 10 events per minute
        burst_mode=False,
        include_all_event_types=True,
        max_active_claims=100,
        max_events=10,  # Generate 10 events then stop (remove for unlimited)
    )

    print()
    print("Configuration:")
    print(f"  - Plugin Profile: {generator_config.plugin_profile}")
    print(f"  - Domain: claimx")
    print(f"  - Target Topic: {target_topic}")
    print(f"  - Events per minute: {config.events_per_minute}")
    print(f"  - Max events: {config.max_events}")
    print(f"  - Task ID: 32513 (itel Cabinet Repair Form)")
    print()
    print("Starting producer...")
    print("Press Ctrl+C to stop")
    print()

    # Run the dummy source
    try:
        async with DummyDataSource(config) as source:
            await source.run()
    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print()
    print("=" * 80)
    print("Producer finished successfully")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
