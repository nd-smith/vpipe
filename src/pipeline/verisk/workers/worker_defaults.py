"""Default configuration values for Verisk workers."""

from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerDefaults:
    """Default configuration values shared across workers."""

    CONCURRENCY: int = 10
    BATCH_SIZE: int = 20
    MAX_POLL_RECORDS: int = 100
    CYCLE_LOG_INTERVAL_SECONDS: int = 30
