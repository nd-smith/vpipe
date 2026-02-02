"""Helper utilities for simulation integration tests."""

import asyncio
import time
from pathlib import Path
from typing import Dict, List, Optional

from pipeline.simulation import get_simulation_config


class KafkaTopicInspector:
    """Helper for inspecting Kafka topics during tests."""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers

    async def get_message_count(self, topic: str) -> int:
        """Get approximate message count in a topic.

        Args:
            topic: Topic name

        Returns:
            Approximate number of messages
        """
        # Use kafka-run-class to get end offsets
        proc = await asyncio.create_subprocess_exec(
            "kafka-run-class",
            "kafka.tools.GetOffsetShell",
            "--broker-list",
            self.bootstrap_servers,
            "--topic",
            topic,
            "--time",
            "-1",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            return 0

        # Parse output: topic:partition:offset
        total = 0
        for line in stdout.decode().strip().split("\n"):
            if ":" in line:
                parts = line.split(":")
                if len(parts) >= 3:
                    total += int(parts[2])

        return total

    async def wait_for_messages(
        self,
        topic: str,
        min_count: int,
        timeout: float = 30.0,
        check_interval: float = 2.0,
    ) -> bool:
        """Wait for a topic to have at least N messages.

        Args:
            topic: Topic name
            min_count: Minimum message count to wait for
            timeout: Maximum time to wait in seconds
            check_interval: How often to check

        Returns:
            True if min_count reached, False if timeout
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            count = await self.get_message_count(topic)
            if count >= min_count:
                return True

            await asyncio.sleep(check_interval)

        return False


class SimulationMetrics:
    """Collect and report metrics from simulation run."""

    def __init__(self, simulation_dir: Path):
        self.simulation_dir = simulation_dir
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

    def start(self):
        """Mark start of simulation."""
        self.start_time = time.time()

    def end(self):
        """Mark end of simulation."""
        self.end_time = time.time()

    def get_duration(self) -> float:
        """Get simulation duration in seconds."""
        if self.start_time is None:
            return 0.0
        end = self.end_time or time.time()
        return end - self.start_time

    def count_files(self, domain: str) -> int:
        """Count uploaded files for a domain."""
        storage_path = self.simulation_dir / domain
        if not storage_path.exists():
            return 0
        return len(list(storage_path.rglob("*")))

    def count_delta_tables(self) -> int:
        """Count valid Delta tables."""
        delta_dir = self.simulation_dir / "delta"
        if not delta_dir.exists():
            return 0

        count = 0
        for table_dir in delta_dir.iterdir():
            if table_dir.is_dir() and (table_dir / "_delta_log").exists():
                count += 1

        return count

    def get_delta_table_row_count(self, table_name: str) -> int:
        """Get row count for a Delta table.

        Args:
            table_name: Name of the Delta table

        Returns:
            Number of rows, or 0 if table doesn't exist or can't be read
        """
        try:
            import polars as pl
        except ImportError:
            return 0

        table_path = self.simulation_dir / "delta" / table_name
        if not table_path.exists():
            return 0

        try:
            df = pl.read_delta(str(table_path))
            return len(df)
        except Exception:
            return 0

    def report(self) -> Dict[str, any]:
        """Generate metrics report.

        Returns:
            Dictionary of metrics
        """
        return {
            "duration_seconds": self.get_duration(),
            "files_claimx": self.count_files("claimx"),
            "files_xact": self.count_files("xact"),
            "delta_tables": self.count_delta_tables(),
            "projects_rows": self.get_delta_table_row_count("claimx_projects"),
            "contacts_rows": self.get_delta_table_row_count("claimx_contacts"),
        }

    def print_report(self):
        """Print metrics report to console."""
        metrics = self.report()

        print("\n" + "=" * 60)
        print("Simulation Metrics")
        print("=" * 60)
        print(f"Duration: {metrics['duration_seconds']:.1f}s")
        print(f"Files (ClaimX): {metrics['files_claimx']}")
        print(f"Files (XACT): {metrics['files_xact']}")
        print(f"Delta Tables: {metrics['delta_tables']}")
        print(f"Project Rows: {metrics['projects_rows']}")
        print(f"Contact Rows: {metrics['contacts_rows']}")
        print("=" * 60)


def assert_files_uploaded(storage_path: Path, min_files: int = 0):
    """Assert that files were uploaded to storage.

    Args:
        storage_path: Path to storage directory
        min_files: Minimum number of files expected

    Raises:
        AssertionError: If assertion fails
    """
    assert storage_path.exists(), f"Storage path does not exist: {storage_path}"

    file_count = len(list(storage_path.rglob("*")))

    if min_files > 0:
        assert (
            file_count >= min_files
        ), f"Expected at least {min_files} files, found {file_count}"


def assert_delta_tables_written(delta_path: Path, expected_tables: List[str]):
    """Assert that Delta tables were created.

    Args:
        delta_path: Path to Delta tables directory
        expected_tables: List of expected table names

    Raises:
        AssertionError: If any expected table is missing
    """
    assert delta_path.exists(), f"Delta path does not exist: {delta_path}"

    missing_tables = []

    for table_name in expected_tables:
        table_path = delta_path / table_name
        delta_log = table_path / "_delta_log"

        if not table_path.exists() or not delta_log.exists():
            missing_tables.append(table_name)

    assert (
        len(missing_tables) == 0
    ), f"Missing Delta tables: {', '.join(missing_tables)}"


def assert_no_errors_in_logs(log_dir: Path = Path("/tmp"), pattern: str = "pcesdopodappv1_*.log"):
    """Assert that no ERROR level logs were written.

    Args:
        log_dir: Directory containing log files
        pattern: Glob pattern for log files

    Raises:
        AssertionError: If errors found in logs
    """
    import subprocess

    result = subprocess.run(
        ["grep", "-l", "ERROR", *log_dir.glob(pattern)],
        capture_output=True,
        text=True,
    )

    # grep returns 0 if matches found, 1 if no matches
    if result.returncode == 0:
        error_files = result.stdout.strip().split("\n")
        raise AssertionError(
            f"Found ERROR level logs in {len(error_files)} files: {error_files}"
        )
