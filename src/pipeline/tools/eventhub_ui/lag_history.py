"""Append consumer lag snapshots to a local CSV for trend analysis."""

import csv
import io
import os
import threading
from datetime import UTC, datetime
from pathlib import Path

from pipeline.tools.eventhub_ui.lag import ConsumerGroupLag

_lock = threading.Lock()

DEFAULT_CSV_PATH = Path(
    os.getenv("LAG_HISTORY_CSV", "/tmp/consumer_lag_history.csv")
)

_HEADER = [
    "timestamp",
    "eventhub_name",
    "consumer_group",
    "total_lag",
    "partition_count",
]


def _ensure_header(path: Path) -> None:
    if not path.exists() or path.stat().st_size == 0:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow(_HEADER)


def record_snapshot(
    results: list[dict],
    path: Path = DEFAULT_CSV_PATH,
) -> None:
    """Append one row per consumer group with the current lag values.

    `results` is the same list[dict] built in lag_overview:
    each dict has {"hub": EventHubInfo, "worker_name": str, "lag": ConsumerGroupLag}.
    """
    now = datetime.now(UTC).isoformat(timespec="seconds")

    with _lock:
        _ensure_header(path)
        with open(path, "a", newline="") as f:
            writer = csv.writer(f)
            for r in results:
                lag: ConsumerGroupLag = r["lag"]
                writer.writerow([
                    now,
                    lag.eventhub_name,
                    lag.consumer_group,
                    lag.total_lag if lag.total_lag is not None else "",
                    len(lag.partitions),
                ])


def get_trends(
    max_points: int = 30,
    path: Path = DEFAULT_CSV_PATH,
) -> dict[tuple[str, str], list[int | None]]:
    """Return recent lag values per (eventhub_name, consumer_group).

    Reads the CSV tail and returns the last `max_points` values for each group.
    Values are ints or None (for partial/missing data).
    """
    if not path.exists():
        return {}

    # Read all rows (CSV is append-only, so order = chronological)
    with _lock:
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

    # Group by (eventhub, consumer_group), keep last N
    from collections import defaultdict

    series: dict[tuple[str, str], list[int | None]] = defaultdict(list)
    for row in rows:
        eh = row.get("eventhub_name", "")
        cg = row.get("consumer_group", "")
        raw_lag = row.get("total_lag", "")
        lag_val = int(raw_lag) if raw_lag not in ("", None) else None
        series[(eh, cg)].append(lag_val)

    # Trim to last max_points
    return {k: v[-max_points:] for k, v in series.items()}


def read_csv(path: Path = DEFAULT_CSV_PATH) -> str:
    """Return the full CSV contents as a string (for download)."""
    if not path.exists():
        buf = io.StringIO()
        csv.writer(buf).writerow(_HEADER)
        return buf.getvalue()
    return path.read_text()
