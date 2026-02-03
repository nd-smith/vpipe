"""
KQL Event Poller for polling events from Microsoft Fabric Eventhouse.

Polls Eventhouse at configurable intervals, applies deduplication,
and writes events to a configurable sink (Kafka, JSON file, etc.).
"""

import asyncio
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Optional

from config.config import KafkaConfig
from core.logging.setup import get_logger
from pipeline.common.eventhouse.kql_client import (
    EventhouseConfig,
    KQLClient,
)
from pipeline.common.eventhouse.sinks import (
    EventSink,
    create_kafka_sink,
)

logger = get_logger(__name__)


# Default checkpoint directory
DEFAULT_CHECKPOINT_DIR = Path(".checkpoints")


@dataclass
class PollerCheckpoint:
    """
    Checkpoint state for resuming the poller after restart.

    Stores the composite key (ingestion_time, trace_id) of the last processed
    record to enable exact resume without duplicates or gaps.
    """

    last_ingestion_time: str  # ISO format UTC timestamp
    last_trace_id: str  # trace_id of the last processed record
    updated_at: str  # When checkpoint was written (for debugging)

    @classmethod
    def from_file(cls, path: Path) -> Optional["PollerCheckpoint"]:
        """
        Load checkpoint from JSON file.
        """
        if not path.exists():
            logger.info("No checkpoint file found", extra={"path": str(path)})
            return None

        try:
            with open(path) as f:
                data = json.load(f)

            checkpoint = cls(
                last_ingestion_time=data["last_ingestion_time"],
                last_trace_id=data["last_trace_id"],
                updated_at=data.get("updated_at", ""),
            )
            return checkpoint

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(
                "Failed to load checkpoint, starting fresh",
                extra={"path": str(path), "error": str(e)},
            )
            return None

    def save(self, path: Path) -> bool:
        """
        Save checkpoint to JSON file.

        Uses atomic write pattern: write to temp file, then os.replace().
        """
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.updated_at = datetime.now(UTC).isoformat()

            temp_path = path.with_suffix(".tmp")

            with open(temp_path, "w") as f:
                json.dump(asdict(self), f, indent=2)

            # Atomic replace (works on POSIX and modern Windows)
            os.replace(temp_path, path)
            return True

        except OSError as e:
            logger.error("Failed to save checkpoint", extra={"error": str(e)})
            return False

    def to_datetime(self) -> datetime:
        """Parse last_ingestion_time to offset-aware UTC datetime."""
        dt = datetime.fromisoformat(self.last_ingestion_time.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt


@dataclass
class PollerConfig:
    """Configuration for KQL Event Poller."""

    eventhouse: EventhouseConfig
    kafka: KafkaConfig | None = None  # Optional when using custom sink
    event_schema_class: type | None = None
    domain: str = "verisk"
    poll_interval_seconds: int = 30
    batch_size: int = 1000
    source_table: str = "Events"
    column_mapping: dict[str, str] = field(default_factory=dict)
    events_table_path: str = ""
    max_kafka_lag: int = 10_000
    lag_check_interval_seconds: int = 60
    backfill_start_stamp: str | None = None
    backfill_stop_stamp: str | None = None
    bulk_backfill: bool = False
    checkpoint_path: Path | None = None
    # If set, use this column name instead of ingestion_time() function
    # e.g., "IngestionTime" for claimx which has an actual column
    ingestion_time_column: str | None = None
    # Optional custom sink - if not provided, uses KafkaSink with kafka config
    sink: EventSink | None = None


class KQLEventPoller:
    """
    Polls Eventhouse for new events and writes them to a configurable sink.

    Supports multiple output sinks via dependency injection:
    - KafkaSink: Write to Kafka topics (default, for pipeline integration)
    - JsonFileSink: Write to JSON Lines files (for debugging/testing)
    - Custom sinks: Implement the EventSink protocol
    """

    def __init__(self, config: PollerConfig):
        """Initialize the poller."""
        self.config = config
        self._running = False
        self._shutdown_event = asyncio.Event()

        if config.event_schema_class is None:
            from pipeline.verisk.schemas.events import EventMessage

            self._event_schema_class = EventMessage
        else:
            self._event_schema_class = config.event_schema_class

        self._kql_client: KQLClient | None = None
        self._sink: EventSink | None = config.sink
        self._owns_sink = config.sink is None  # Track if we created the sink
        self._last_poll_time: datetime | None = None
        self._consecutive_empty_polls = 0
        self._total_events_fetched = 0
        self._total_polls = 0
        self._seen_trace_ids: set[str] = set()
        self._duplicate_count = 0
        self._backfill_mode = True
        self._backfill_start_time: datetime | None = None
        self._backfill_stop_time: datetime | None = None

        if config.backfill_start_stamp:
            self._backfill_start_time = self._parse_timestamp(
                config.backfill_start_stamp
            )
        if config.backfill_stop_stamp:
            self._backfill_stop_time = self._parse_timestamp(config.backfill_stop_stamp)

        self._last_ingestion_time: datetime | None = None
        self._last_trace_id: str | None = None
        self._checkpoint_path = config.checkpoint_path or (
            DEFAULT_CHECKPOINT_DIR / f"poller_{config.domain}.json"
        )
        self._load_checkpoint()
        self._pending_tasks: set[asyncio.Task] = set()

    @property
    def _trace_id_col(self) -> str | None:
        """Get the KQL column name for the unique ID (default: traceId). Returns None if disabled."""
        col = self.config.column_mapping.get("trace_id")
        if col == "None" or col is None:
            return None
        return col

    @property
    def _pagination_col(self) -> str:
        """Column used for secondary sort in composite-key pagination.

        Returns the configured trace_id column if available, otherwise
        ``_row_hash`` — a synthetic column generated in the KQL query
        from ``hash_sha256(tostring(pack_all()))``.  This ensures every
        table can be paginated correctly even without a natural unique
        identifier.
        """
        return self._trace_id_col or "_row_hash"

    def _parse_timestamp(self, ts_str: str) -> datetime:
        """Helper to ensure all parsed timestamps are offset-aware UTC."""
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt

    def _load_checkpoint(self) -> None:
        """Load checkpoint and initialize state."""
        self._checkpoint = PollerCheckpoint.from_file(self._checkpoint_path)
        if self._checkpoint:
            self._last_ingestion_time = self._checkpoint.to_datetime()
            self._last_trace_id = self._checkpoint.last_trace_id
            if self._backfill_start_time is None:
                self._backfill_start_time = self._last_ingestion_time

    def _save_checkpoint(self, ingestion_time: datetime, trace_id: str) -> None:
        """Saves current progress to disk."""
        if ingestion_time.tzinfo is None:
            ingestion_time = ingestion_time.replace(tzinfo=UTC)

        checkpoint = PollerCheckpoint(
            last_ingestion_time=ingestion_time.isoformat(),
            last_trace_id=trace_id,
            updated_at="",
        )
        checkpoint.save(self._checkpoint_path)
        self._last_ingestion_time = ingestion_time
        self._last_trace_id = trace_id

    async def start(self) -> None:
        """Initialize all components."""
        logger.info("Starting KQLEventPoller components")
        self._kql_client = KQLClient(self.config.eventhouse)
        await self._kql_client.connect()

        # Test eventhouse connectivity before initializing Kafka sink
        await self._test_eventhouse_connectivity()

        # Use provided sink or create default KafkaSink
        if self._sink is None:
            if self.config.kafka is None:
                raise ValueError("Either sink or kafka config must be provided")
            self._sink = create_kafka_sink(
                kafka_config=self.config.kafka,
                domain=self.config.domain,
                worker_name="eventhouse_poller",
            )
            self._owns_sink = True

        await self._sink.start()
        self._running = True
        logger.info(
            "KQLEventPoller started", extra={"sink_type": type(self._sink).__name__}
        )

    async def _test_eventhouse_connectivity(self) -> None:
        """Test eventhouse connectivity by reading a small sample of records.

        Queries up to 10 records from the source table and streams them to
        logging and stdout to confirm we can read data from the eventhouse
        before attempting to initialize Kafka.
        """
        test_limit = 10
        table = self.config.source_table
        query = f"{table} | take {test_limit}"

        logger.info(
            "=== Eventhouse Connectivity Test ===",
            extra={
                "domain": self.config.domain,
                "source_table": table,
                "cluster_url": self.config.eventhouse.cluster_url,
                "database": self.config.eventhouse.database,
                "sample_size": test_limit,
            },
        )

        try:
            result = await self._kql_client.execute_query(query)

            if not result.rows:
                logger.warning(
                    "Eventhouse connectivity test returned 0 rows. "
                    "Connection succeeded but table may be empty.",
                    extra={
                        "source_table": table,
                        "query_duration_ms": round(result.query_duration_ms, 2),
                    },
                )
                print(
                    f"[CONNECTIVITY TEST] Connected to eventhouse successfully. "
                    f"Table '{table}' returned 0 rows (may be empty)."
                )
                return

            logger.info(
                "Eventhouse connectivity test PASSED",
                extra={
                    "rows_returned": len(result.rows),
                    "query_duration_ms": round(result.query_duration_ms, 2),
                    "source_table": table,
                },
            )
            print(
                f"[CONNECTIVITY TEST] SUCCESS - Read {len(result.rows)} records "
                f"from '{table}' in {result.query_duration_ms:.0f}ms"
            )
            print(f"[CONNECTIVITY TEST] Columns: {list(result.rows[0].keys())}")

            for i, row in enumerate(result.rows):
                logger.info(
                    f"Eventhouse sample record {i + 1}/{len(result.rows)}",
                    extra={"record": row},
                )
                print(
                    f"[CONNECTIVITY TEST] Record {i + 1}: {json.dumps(row, default=str)}"
                )

            logger.info("=== Eventhouse Connectivity Test Complete ===")

        except Exception as e:
            logger.error(
                "Eventhouse connectivity test FAILED",
                extra={
                    "source_table": table,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            print(f"[CONNECTIVITY TEST] FAILED - Could not read from '{table}': {e}")
            raise

    # FIXED: Restored Asynchronous Context Manager Protocol
    async def __aenter__(self) -> "KQLEventPoller":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.stop()

    async def stop(self) -> None:
        """Gracefully shutdown components."""
        logger.info("Stopping KQLEventPoller")
        self._running = False
        self._shutdown_event.set()

        # Only stop sink if we created it (owns_sink=True)
        if self._sink and self._owns_sink:
            await self._sink.stop()
        if self._kql_client:
            await self._kql_client.close()

    async def run(self) -> None:
        """Main polling loop."""
        if self.config.bulk_backfill and self._backfill_mode:
            await self._bulk_backfill()
            return

        while self._running and not self._shutdown_event.is_set():
            try:
                await self._poll_cycle()
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.poll_interval_seconds,
                )
            except TimeoutError:
                pass
            except Exception as e:
                logger.error("Error in poll cycle", extra={"error": str(e)})

    async def _bulk_backfill(self) -> None:
        """Execute paginated bulk backfill."""
        now = datetime.now(UTC)
        start = self._backfill_start_time or (now - timedelta(hours=1))
        stop = self._backfill_stop_time or now

        while True:
            query = self._build_query(
                self.config.source_table,
                start,
                stop,
                self.config.batch_size,
                self._last_trace_id,
            )

            result = await self._kql_client.execute_query(query)
            if not result.rows:
                break

            await self._process_filtered_results(result.rows)

            last = result.rows[-1]
            l_time_raw = last.get("ingestion_time", last.get("$IngestionTime"))
            l_time = (
                l_time_raw
                if isinstance(l_time_raw, datetime)
                else datetime.fromisoformat(str(l_time_raw).replace("Z", "+00:00"))
            )
            if l_time.tzinfo is None:
                l_time = l_time.replace(tzinfo=UTC)

            l_tid = str(last.get(self._pagination_col, ""))
            self._save_checkpoint(l_time, l_tid)
            start = l_time

            if len(result.rows) < self.config.batch_size:
                break

    async def _poll_cycle(self) -> None:
        """Execute single poll cycle."""
        now = datetime.now(UTC)
        poll_from = self._last_ingestion_time or (now - timedelta(hours=1))

        query = self._build_query(
            self.config.source_table,
            poll_from,
            now,
            self.config.batch_size,
            self._last_trace_id,
        )
        result = await self._kql_client.execute_query(query)
        if not result.rows:
            return

        rows = self._filter_checkpoint_rows(result.rows)
        await self._process_filtered_results(rows)

        last = result.rows[-1]
        l_time_raw = last.get("ingestion_time", last.get("$IngestionTime"))
        l_time = (
            l_time_raw
            if isinstance(l_time_raw, datetime)
            else datetime.fromisoformat(str(l_time_raw).replace("Z", "+00:00"))
        )
        if l_time.tzinfo is None:
            l_time = l_time.replace(tzinfo=UTC)

        l_tid = str(last.get(self._pagination_col, ""))
        self._save_checkpoint(l_time, l_tid)

    def _filter_checkpoint_rows(self, rows: list[dict]) -> list[dict]:
        """Filter out rows already covered by the current checkpoint.

        Uses the composite key (ingestion_time, pagination_col) to decide
        which rows are new.  The pagination_col is either the configured
        trace_id column or the synthetic ``_row_hash``.
        """
        if not self._last_ingestion_time:
            return rows

        cp_time = self._last_ingestion_time
        cp_tid = self._last_trace_id
        pg_col = self._pagination_col
        filtered = []

        for r in rows:
            t_raw = r.get("ingestion_time", r.get("$IngestionTime"))
            r_time = (
                t_raw
                if isinstance(t_raw, datetime)
                else datetime.fromisoformat(str(t_raw).replace("Z", "+00:00"))
            )
            if r_time.tzinfo is None:
                r_time = r_time.replace(tzinfo=UTC)

            if r_time < cp_time:
                continue
            if r_time == cp_time and cp_tid:
                r_tid = str(r.get(pg_col, ""))
                if r_tid <= cp_tid:
                    continue

            filtered.append(r)
        return filtered

    def _build_query(self, table, p_from, p_to, limit, cp_tid) -> str:
        """Constructs paginated KQL query.

        Always uses composite-key pagination (ingestion_time, pagination_col)
        to handle tables where many rows share the same ingestion timestamp.
        When no trace_id column is configured, a synthetic ``_row_hash``
        column is generated from ``hash_sha256(tostring(pack_all()))`` to
        provide a deterministic secondary sort key.
        """
        f_str = p_from.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        t_str = p_to.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        pg_col = self._pagination_col

        # Use configured column name or ingestion_time() function
        ing_col = self.config.ingestion_time_column
        ing_expr = ing_col if ing_col else "ingestion_time()"

        # Extend clauses MUST come before WHERE so that _row_hash and
        # the normalised ingestion_time column exist when referenced.
        extend_parts = []
        if not self._trace_id_col:
            extend_parts.append("| extend _row_hash = hash_sha256(tostring(pack_all()))")
        extend_parts.append(
            f"| extend ingestion_time = {ing_expr}"
            if ing_col
            else "| extend ingestion_time = ingestion_time()"
        )
        extend_clause = " ".join(extend_parts)

        # Composite-key pagination: (ingestion_time, pagination_col)
        # After the extends, ingestion_time is a column so we reference
        # it by name rather than via the function.
        if cp_tid:
            esc = cp_tid.replace("'", "\\'")
            where = f"| where ingestion_time > datetime({f_str}) or (ingestion_time == datetime({f_str}) and strcmp(tostring({pg_col}), '{esc}') > 0)"
        else:
            # First query or no checkpoint — no secondary key to compare
            where = f"| where ingestion_time > datetime({f_str})"

        order_clause = f"order by ingestion_time asc, {pg_col} asc"

        return f"{table} {extend_clause} {where} | where ingestion_time < datetime({t_str}) | {order_clause} | take {limit}"

    async def _process_filtered_results(self, rows: list[dict]) -> int:
        """Processes rows and writes to configured sink."""
        for row in rows:
            event = self._event_schema_class.from_eventhouse_row(row)
            # Use configured column if available, otherwise rely on schema generation
            if self._trace_id_col and row.get(self._trace_id_col):
                eid = str(row.get(self._trace_id_col))
            else:
                # Use schema-generated or default ID
                # Note: use 'or' to handle None values since getattr returns None
                # when the attribute exists but has value None
                eid = (
                    getattr(event, "event_id", None)
                    or getattr(event, "trace_id", None)
                    or str(hash(str(event)))
                )

            await self._sink.write(key=eid, event=event)
        return len(rows)

    @property
    def stats(self) -> dict:
        """Returns fetched event statistics."""
        return {"total_fetched": self._total_events_fetched}
