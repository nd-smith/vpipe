"""Default configuration values shared across ClaimX workers."""

from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerDefaults:
    """Default configuration values for ClaimX workers."""

    CONCURRENCY: int = 10
    BATCH_SIZE: int = 20
    MAX_POLL_RECORDS: int = 100
    CYCLE_LOG_INTERVAL_SECONDS: int = 30
