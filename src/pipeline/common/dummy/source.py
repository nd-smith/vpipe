"""
Dummy data source for testing the Kafka pipeline.

Generates realistic-looking insurance claim data and produces events
to Kafka topics, allowing pipeline testing without external dependencies.
"""

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from config.config import MessageConfig
from pipeline.common.dummy.file_server import DummyFileServer, FileServerConfig
from pipeline.common.dummy.generators import (
    GeneratorConfig,
    RealisticDataGenerator,
)
from pipeline.common.producer import MessageProducer

logger = logging.getLogger(__name__)


@dataclass
class DummySourceConfig:
    """Configuration for the dummy data source."""

    kafka: MessageConfig
    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    file_server: FileServerConfig = field(default_factory=FileServerConfig)

    # Which domains to generate events for
    domains: list[str] = field(default_factory=lambda: ["verisk", "claimx"])

    # Event generation settings
    events_per_minute: float = 10.0  # Events per minute per domain
    burst_mode: bool = False  # Generate events in bursts
    burst_size: int = 50  # Events per burst when in burst mode
    burst_interval_seconds: int = 60  # Time between bursts

    # Scenario settings
    include_all_event_types: bool = True  # Generate all ClaimX event types
    max_active_claims: int = 100  # Max concurrent claims to track

    # Runtime limits
    max_events: int | None = None  # Stop after N events (None = unlimited)
    max_runtime_seconds: int | None = None  # Stop after N seconds

    # External file server (when running in Docker with separate file server)
    skip_embedded_file_server: bool = False  # Skip starting embedded file server
    external_file_server_url: str | None = None  # URL of external file server


class DummyDataSource:
    """
    Generates and publishes dummy events to Kafka for pipeline testing.

    Can run standalone or integrated with the pipeline. Supports both
    continuous generation and burst mode for load testing.
    """

    def __init__(self, config: DummySourceConfig):
        self.config = config
        self._generator = RealisticDataGenerator(config.generator)
        self._file_server: DummyFileServer | None = None
        self._producers: dict[str, MessageProducer] = {}
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._total_events = 0
        self._start_time: datetime | None = None
        self._pending_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        """Initialize all components."""
        logger.info(
            "Starting DummyDataSource",
            extra={
                "domains": self.config.domains,
                "events_per_minute": self.config.events_per_minute,
                "burst_mode": self.config.burst_mode,
                "max_active_claims": self.config.max_active_claims,
                "include_all_event_types": self.config.include_all_event_types,
            },
        )
        # Log the effective rate for clarity
        total_events_per_min = self.config.events_per_minute * len(self.config.domains)
        logger.info(
            f"Event generation rate: {self.config.events_per_minute}/min per domain, "
            f"{total_events_per_min}/min total across {len(self.config.domains)} domains"
        )

        # Start file server (or use external one)
        if self.config.skip_embedded_file_server:
            if self.config.external_file_server_url:
                logger.info(
                    f"Using external file server at {self.config.external_file_server_url}"
                )
                self._generator.config.base_url = self.config.external_file_server_url
            else:
                logger.warning(
                    "skip_embedded_file_server=True but no external_file_server_url provided"
                )
                # Use default localhost URL anyway
                self._generator.config.base_url = f"http://{self.config.file_server.host}:{self.config.file_server.port}"
        else:
            self._file_server = DummyFileServer(self.config.file_server)
            await self._file_server.start()
            # Update generator base URL to match file server
            self._generator.config.base_url = (
                f"http://{self.config.file_server.host}:{self.config.file_server.port}"
            )
            # Use localhost for local testing with embedded file server
            if self.config.file_server.host == "0.0.0.0":
                self._generator.config.base_url = (
                    f"http://localhost:{self.config.file_server.port}"
                )

        # Create producers for each domain
        for domain in self.config.domains:
            producer = MessageProducer(
                config=self.config.kafka,
                domain=domain,
                worker_name="dummy_source",
            )
            await producer.start()
            self._producers[domain] = producer

        self._running = True
        self._start_time = datetime.now(UTC)

    async def stop(self) -> None:
        """Gracefully shutdown all components."""
        logger.info("Stopping DummyDataSource")
        self._running = False
        self._shutdown_event.set()

        # Cancel pending tasks
        for task in self._pending_tasks:
            task.cancel()
        if self._pending_tasks:
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)

        # Stop producers
        for producer in self._producers.values():
            await producer.stop()

        # Stop file server
        if self._file_server:
            await self._file_server.stop()

        logger.info(
            "DummyDataSource stopped",
            extra={"total_events_generated": self._total_events},
        )

    async def __aenter__(self) -> "DummyDataSource":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.stop()

    async def run(self) -> None:
        """Main event generation loop."""
        if self.config.burst_mode:
            await self._run_burst_mode()
        else:
            await self._run_continuous_mode()

    async def _run_continuous_mode(self) -> None:
        """Generate events at a steady rate."""
        # Calculate interval between events
        events_per_second = self.config.events_per_minute / 60.0
        interval = 1.0 / events_per_second if events_per_second > 0 else 1.0

        logger.info(
            "Starting continuous event generation",
            extra={"interval_seconds": interval},
        )

        while self._running and not self._shutdown_event.is_set():
            # Check limits
            if self._should_stop():
                break

            # Generate events for each domain
            for domain in self.config.domains:
                if not self._running:
                    break

                try:
                    await self._generate_and_send_event(domain)
                except Exception as e:
                    logger.error(
                        f"Error generating event for {domain}: {e}",
                        extra={"domain": domain, "error": str(e)},
                        exc_info=True,
                    )

            # Wait for next interval
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=interval,
                )

            # Cleanup old claims periodically
            self._generator.clear_old_claims(self.config.max_active_claims)

    async def _run_burst_mode(self) -> None:
        """Generate events in bursts for load testing."""
        logger.info(
            "Starting burst mode event generation",
            extra={
                "burst_size": self.config.burst_size,
                "interval_seconds": self.config.burst_interval_seconds,
            },
        )

        while self._running and not self._shutdown_event.is_set():
            if self._should_stop():
                break

            # Generate a burst of events
            logger.info("Generating burst of %s events", self.config.burst_size)

            tasks = []
            for _i in range(self.config.burst_size):
                for domain in self.config.domains:
                    if self._should_stop():
                        break
                    tasks.append(self._generate_and_send_event(domain))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            logger.info(
                "Burst complete",
                extra={
                    "events_in_burst": len(tasks),
                    "total_events": self._total_events,
                },
            )

            # Wait for next burst
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.burst_interval_seconds,
                )

    def _should_stop(self) -> bool:
        """Check if generation should stop based on limits."""
        if self.config.max_events and self._total_events >= self.config.max_events:
            logger.info("Reached max events limit: %s", self.config.max_events)
            return True

        if self.config.max_runtime_seconds and self._start_time:
            elapsed = (datetime.now(UTC) - self._start_time).total_seconds()
            if elapsed >= self.config.max_runtime_seconds:
                logger.info(
                    f"Reached max runtime limit: {self.config.max_runtime_seconds}s"
                )
                return True

        return False

    async def _generate_and_send_event(self, domain: str) -> None:
        """Generate and send a single event for the specified domain."""
        producer = self._producers.get(domain)
        if not producer:
            logger.warning("No producer for domain: %s", domain)
            return

        if domain == "verisk":
            event_data = self._generator.generate_xact_event()
            # Import schema class
            from pipeline.verisk.schemas.events import EventMessage

            event = EventMessage(**event_data)
            key = event_data["traceId"]
            _topic = self.config.kafka.get_topic(domain, "events")

        elif domain == "claimx":
            # Check if we're using a plugin profile
            plugin_profile = self.config.generator.plugin_profile

            if plugin_profile == "itel_cabinet_api":
                # Generate itel Cabinet specific event
                event_data = self._generator.generate_itel_cabinet_event()
                logger.debug(
                    f"Generated itel Cabinet event | "
                    f"event_type={event_data['event_type']} | "
                    f"project_id={event_data['project_id']} | "
                    f"task_id={event_data['raw_data'].get('taskId')}"
                )
            elif plugin_profile == "mixed" or (
                plugin_profile is None and self.config.generator.include_itel_triggers
            ):
                # Mixed mode - generate events with configurable itel trigger rate
                event_data = self._generator.generate_claimx_event_mixed()
                # Log if this is an itel-triggering event
                task_id = event_data.get("raw_data", {}).get("taskId")
                if task_id == 32513:
                    logger.debug(
                        f"Generated itel Cabinet trigger event | "
                        f"event_type={event_data['event_type']} | "
                        f"project_id={event_data['project_id']}"
                    )
            else:
                # Generate standard ClaimX event
                event_data = self._generator.generate_claimx_event()

            # Import schema class
            from pipeline.claimx.schemas.events import ClaimXEventMessage

            event = ClaimXEventMessage(**event_data)
            key = event_data["event_id"]
            _topic = self.config.kafka.get_topic(domain, "events")

        else:
            logger.warning("Unknown domain: %s", domain)
            return

        await producer.send(value=event, key=key)
        self._total_events += 1

        if self._total_events % 100 == 0:
            logger.info(
                "Event generation progress",
                extra={
                    "total_events": self._total_events,
                    "active_claims": self._generator.get_active_claims_count(),
                },
            )

    async def generate_batch(
        self,
        domain: str,
        count: int,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Generate a batch of events without sending to Kafka.

        Useful for testing or seeding data.
        """
        events = []
        for _ in range(count):
            if domain == "verisk":
                event_data = self._generator.generate_xact_event()
            elif domain == "claimx":
                event_data = self._generator.generate_claimx_event(
                    event_type=event_type
                )
            else:
                raise ValueError(f"Unknown domain: {domain}")
            events.append(event_data)
        return events

    @property
    def stats(self) -> dict[str, Any]:
        """Return current statistics."""
        elapsed = 0.0
        if self._start_time:
            elapsed = (datetime.now(UTC) - self._start_time).total_seconds()

        return {
            "total_events": self._total_events,
            "active_claims": self._generator.get_active_claims_count(),
            "elapsed_seconds": elapsed,
            "events_per_second": self._total_events / elapsed if elapsed > 0 else 0,
            "running": self._running,
        }


def load_dummy_source_config(
    kafka_config: MessageConfig,
    dummy_config: dict[str, Any] | None = None,
) -> DummySourceConfig:
    """
    Load DummySourceConfig from a dictionary (typically from config.yaml).

    Args:
        kafka_config: The loaded Kafka configuration
        dummy_config: Optional dictionary with dummy source settings

    Returns:
        DummySourceConfig instance
    """
    dummy_config = dummy_config or {}

    # Generator config
    gen_cfg = dummy_config.get("generator", {})
    generator_config = GeneratorConfig(
        seed=gen_cfg.get("seed"),
        base_url=gen_cfg.get("base_url", "http://localhost:8765"),
        include_failures=gen_cfg.get("include_failures", False),
        failure_rate=gen_cfg.get("failure_rate", 0.05),
        plugin_profile=gen_cfg.get("plugin_profile"),  # Support plugin profiles
        itel_trigger_percentage=gen_cfg.get(
            "itel_trigger_percentage", 0.3
        ),  # 30% default
        include_itel_triggers=gen_cfg.get(
            "include_itel_triggers", True
        ),  # Enabled by default
    )

    # File server config
    fs_cfg = dummy_config.get("file_server", {})
    file_server_config = FileServerConfig(
        host=fs_cfg.get("host", "0.0.0.0"),
        port=fs_cfg.get("port", 8765),
        default_file_size=fs_cfg.get("default_file_size", 100_000),
        max_file_size=fs_cfg.get("max_file_size", 10_000_000),
    )

    return DummySourceConfig(
        kafka=kafka_config,
        generator=generator_config,
        file_server=file_server_config,
        domains=dummy_config.get("domains", ["verisk", "claimx"]),
        events_per_minute=dummy_config.get("events_per_minute", 10.0),
        burst_mode=dummy_config.get("burst_mode", False),
        burst_size=dummy_config.get("burst_size", 50),
        burst_interval_seconds=dummy_config.get("burst_interval_seconds", 60),
        include_all_event_types=dummy_config.get("include_all_event_types", True),
        max_active_claims=dummy_config.get("max_active_claims", 100),
        max_events=dummy_config.get("max_events"),
        max_runtime_seconds=dummy_config.get("max_runtime_seconds"),
    )
