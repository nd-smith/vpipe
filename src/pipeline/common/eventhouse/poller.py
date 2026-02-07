"""
KQL Event Poller for polling events from Microsoft Fabric Eventhouse.

Polls Eventhouse at configurable intervals, applies deduplication,
and writes events to a configurable sink (Kafka, JSON file, etc.).
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from config.config import MessageConfig
from pipeline.common.eventhouse.kql_client import (
    EventhouseConfig,
    KQLClient,
)
from pipeline.common.eventhouse.poller_checkpoint_store import (
    PollerCheckpoint,
    PollerCheckpointStore,
    create_poller_checkpoint_store,
)
from pipeline.common.eventhouse.sinks import (
    EventSink,
    create_message_sink,
)
from pipeline.common.health import HealthCheckServer

logger = logging.getLogger(__name__)


@dataclass
class PollerConfig:
    """Configuration for KQL Event Poller."""

    eventhouse: EventhouseConfig
    kafka: MessageConfig | None = None  # Optional when using custom sink
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
    # If set, use this column name instead of ingestion_time() function
    # e.g., "IngestionTime" for claimx which has an actual column
    ingestion_time_column: str | None = None
    # Optional custom sink - if not provided, uses MessageSink with kafka config
    sink: EventSink | None = None
    health_port: int = 8080  # Health check server port
    # Optional custom checkpoint store - if not provided, creates from config
    checkpoint_store: PollerCheckpointStore | None = None


class KQLEventPoller:
    """
    Polls Eventhouse for new events and writes them to a configurable sink.

    Supports multiple output sinks via dependency injection:
    - MessageSink: Write to message transports (default, for pipeline integration)
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
        self._checkpoint_store: PollerCheckpointStore | None = config.checkpoint_store
        self._owns_checkpoint_store = config.checkpoint_store is None
        self._pending_tasks: set[asyncio.Task] = set()

        # Health check server
        self.health_server = HealthCheckServer(
            port=config.health_port,
            worker_name=f"{config.domain}-poller",
        )

    @property
    def _trace_id_col(self) -> str | None:
        """Get the KQL column name for the unique ID (default: traceId). Returns None if disabled."""
        col = self.config.column_mapping.get("trace_id")
        if col == "None" or col is None:
            return None
        return col

    def _parse_timestamp(self, ts_str: str) -> datetime:
        """Helper to ensure all parsed timestamps are offset-aware UTC."""
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt

    def _parse_row_time(self, row: dict) -> datetime:
        """Extract and normalize ingestion_time from a result row."""
        t_raw = row.get("ingestion_time", row.get("$IngestionTime"))
        t = (
            t_raw
            if isinstance(t_raw, datetime)
            else datetime.fromisoformat(str(t_raw).replace("Z", "+00:00"))
        )
        if t.tzinfo is None:
            t = t.replace(tzinfo=UTC)
        return t

    async def _load_checkpoint(self) -> None:
        """Load checkpoint and initialize state."""
        if self._checkpoint_store is None:
            return

        checkpoint = await self._checkpoint_store.load()
        if checkpoint:
            self._last_ingestion_time = checkpoint.to_datetime()
            self._last_trace_id = checkpoint.last_trace_id
            if self._backfill_start_time is None:
                self._backfill_start_time = self._last_ingestion_time

    async def _save_checkpoint(self, ingestion_time: datetime, trace_id: str) -> None:
        """Saves current progress to checkpoint store."""
        if self._checkpoint_store is None:
            return

        if ingestion_time.tzinfo is None:
            ingestion_time = ingestion_time.replace(tzinfo=UTC)

        checkpoint = PollerCheckpoint(
            last_ingestion_time=ingestion_time.isoformat(),
            last_trace_id=trace_id,
            updated_at="",
        )
        await self._checkpoint_store.save(checkpoint)
        self._last_ingestion_time = ingestion_time
        self._last_trace_id = trace_id

    async def start(self) -> None:
        """Initialize all components."""
        print("\n" + "="*80)
        print("[POLLER STARTUP] Starting KQLEventPoller components")
        print("="*80)
        logger.info("Starting KQLEventPoller components")

        print("[POLLER STARTUP] Step 1/6: Starting health check server...")
        await self.health_server.start()
        print(f"[POLLER STARTUP] Health check server started on port {self.health_server.actual_port}")

        print(f"\n[POLLER STARTUP] Step 2/6: Initializing checkpoint store")
        if self._checkpoint_store is None:
            self._checkpoint_store = await create_poller_checkpoint_store(
                domain=self.config.domain
            )
            self._owns_checkpoint_store = True
        print(f"[POLLER STARTUP]   - Type: {type(self._checkpoint_store).__name__}")

        print(f"[POLLER STARTUP] Loading checkpoint...")
        await self._load_checkpoint()
        if self._last_ingestion_time:
            print(f"[POLLER STARTUP]   - Resuming from: {self._last_ingestion_time.isoformat()}")
        else:
            print(f"[POLLER STARTUP]   - No checkpoint found, starting fresh")

        print(f"\n[POLLER STARTUP] Step 3/6: Creating KQLClient for Eventhouse")
        print(f"[POLLER STARTUP]   - Cluster: {self.config.eventhouse.cluster_url}")
        print(f"[POLLER STARTUP]   - Database: {self.config.eventhouse.database}")
        print(f"[POLLER STARTUP]   - Source table: {self.config.source_table}")
        self._kql_client = KQLClient(self.config.eventhouse)

        print(f"\n[POLLER STARTUP] Step 4/6: Connecting to Eventhouse...")
        await self._kql_client.connect()
        print("[POLLER STARTUP] KQLClient connection initialized (client created)")

        # Test eventhouse connectivity before initializing Kafka sink
        print(f"\n[POLLER STARTUP] Step 5/6: Testing Eventhouse connectivity")
        print(f"[POLLER STARTUP]   - Will query: {self.config.source_table} | take 10")
        print(f"[POLLER STARTUP]   - This is the FIRST ACTUAL NETWORK REQUEST")
        print("[POLLER STARTUP]   - This will authenticate and establish TCP connection")
        await self._test_eventhouse_connectivity()
        print("[POLLER STARTUP] Connectivity test passed! Eventhouse connection confirmed.")

        # Use provided sink or create default MessageSink
        print(f"\n[POLLER STARTUP] Step 6/6: Initializing output sink")
        if self._sink is None:
            if self.config.kafka is None:
                raise ValueError("Either sink or kafka config must be provided")
            print(f"[POLLER STARTUP]   - Creating message sink")
            print(f"[POLLER STARTUP]   - Domain: {self.config.domain}")
            self._sink = create_message_sink(
                message_config=self.config.kafka,
                domain=self.config.domain,
                worker_name="eventhouse_poller",
            )
            self._owns_sink = True

        print(f"[POLLER STARTUP] Starting sink ({type(self._sink).__name__})...")
        await self._sink.start()
        print(f"[POLLER STARTUP] Sink started successfully")

        self._running = True

        # Mark health server as ready after successful startup
        self.health_server.set_ready(kafka_connected=True)

        print("\n" + "="*80)
        print("[POLLER STARTUP] All components started successfully!")
        print(f"[POLLER STARTUP]   - Health server: port {self.health_server.actual_port}")
        print(f"[POLLER STARTUP]   - Checkpoint store: {type(self._checkpoint_store).__name__}")
        print(f"[POLLER STARTUP]   - Eventhouse: {self.config.eventhouse.cluster_url}/{self.config.eventhouse.database}")
        print(f"[POLLER STARTUP]   - Source table: {self.config.source_table}")
        print(f"[POLLER STARTUP]   - Output sink: {type(self._sink).__name__}")
        print("="*80 + "\n")

        logger.info(
            "KQLEventPoller started",
            extra={
                "checkpoint_store_type": type(self._checkpoint_store).__name__,
                "sink_type": type(self._sink).__name__,
                "health_port": self.health_server.actual_port,
            },
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

        print("\n" + "#"*80)
        print("[CONNECTIVITY TEST] Starting Eventhouse connectivity test")
        print("#"*80)
        print(f"[CONNECTIVITY TEST] Domain: {self.config.domain}")
        print(f"[CONNECTIVITY TEST] Cluster: {self.config.eventhouse.cluster_url}")
        print(f"[CONNECTIVITY TEST] Database: {self.config.eventhouse.database}")
        print(f"[CONNECTIVITY TEST] Source table: {table}")
        print(f"[CONNECTIVITY TEST] Test query: {query}")
        print(f"[CONNECTIVITY TEST] Sample size: {test_limit} rows")
        print("#"*80)

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
            print("\n[CONNECTIVITY TEST] Executing test query...")
            print("[CONNECTIVITY TEST] This will trigger the first real connection to Eventhouse")
            print("[CONNECTIVITY TEST] >>> Making network request to Kusto endpoint...")
            print("[CONNECTIVITY TEST] >>> This will:")
            print("[CONNECTIVITY TEST] >>>   1. Authenticate using configured credentials")
            print("[CONNECTIVITY TEST] >>>   2. Establish TCP connection to cluster")
            print("[CONNECTIVITY TEST] >>>   3. Execute the test query")
            print("[CONNECTIVITY TEST] >>> Waiting for response...\n")
            result = await self._kql_client.execute_query(query)
            print(f"\n[CONNECTIVITY TEST] ✓ Test query completed successfully!")
            print(f"[CONNECTIVITY TEST] ✓ Network connection established and working!")

            if not result.rows:
                logger.warning(
                    "Eventhouse connectivity test returned 0 rows. "
                    "Connection succeeded but table may be empty.",
                    extra={
                        "source_table": table,
                        "query_duration_ms": round(result.query_duration_ms, 2),
                    },
                )
                print("\n" + "#"*80)
                print(f"[CONNECTIVITY TEST] ✓ Connected to Eventhouse successfully!")
                print(f"[CONNECTIVITY TEST] Table '{table}' returned 0 rows (may be empty)")
                print(f"[CONNECTIVITY TEST] Query duration: {result.query_duration_ms:.0f}ms")
                print("#"*80 + "\n")
                return

            logger.info(
                "Eventhouse connectivity test PASSED",
                extra={
                    "rows_returned": len(result.rows),
                    "query_duration_ms": round(result.query_duration_ms, 2),
                    "source_table": table,
                },
            )
            print("\n" + "#"*80)
            print(f"[CONNECTIVITY TEST] ✓ SUCCESS!")
            print(f"[CONNECTIVITY TEST] Read {len(result.rows)} records from '{table}'")
            print(f"[CONNECTIVITY TEST] Query duration: {result.query_duration_ms:.0f}ms")
            print(f"[CONNECTIVITY TEST] Columns: {list(result.rows[0].keys())}")
            print("#"*80)

            print("\n[CONNECTIVITY TEST] Sample records:")
            for i, row in enumerate(result.rows):
                logger.info(
                    f"Eventhouse sample record {i + 1}/{len(result.rows)}",
                    extra={"record": row},
                )
                print(f"[CONNECTIVITY TEST]   Record {i + 1}: {json.dumps(row, default=str)}")

            print("\n" + "#"*80)
            print("[CONNECTIVITY TEST] Eventhouse connectivity test COMPLETE ✓")
            print("#"*80 + "\n")

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
            print("\n" + "#"*80)
            print(f"[CONNECTIVITY TEST] ✗ FAILED")
            print(f"[CONNECTIVITY TEST] Could not read from table '{table}'")
            print(f"[CONNECTIVITY TEST] Error type: {type(e).__name__}")
            print(f"[CONNECTIVITY TEST] Error message: {str(e)[:500]}")
            print("#"*80 + "\n")
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

        # Only close checkpoint store if we created it
        if self._checkpoint_store and self._owns_checkpoint_store:
            await self._checkpoint_store.close()

        await self.health_server.stop()

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

            if self._trace_id_col:
                # Composite-key path
                await self._process_filtered_results(result.rows)
                last = result.rows[-1]
                l_time = self._parse_row_time(last)
                l_tid = str(last.get(self._trace_id_col, ""))
                await self._save_checkpoint(l_time, l_tid)
                start = l_time
            else:
                # Timestamp-only path with boundary handling
                rows, cp_time = self._resolve_batch_boundary(result.rows)
                if cp_time is None:
                    stuck = self._parse_row_time(result.rows[0])
                    rows = await self._drain_timestamp(stuck, stop)
                    cp_time = stuck
                await self._process_filtered_results(rows)
                await self._save_checkpoint(cp_time, "")
                start = cp_time

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
            logger.debug(
                "Poll cycle returned 0 rows",
                extra={"checkpoint_time": poll_from.isoformat()},
            )
            return

        if self._trace_id_col:
            # Composite-key path: filter duplicates, save with trace_id
            rows = self._filter_checkpoint_rows(result.rows)
            if not rows:
                return
            await self._process_filtered_results(rows)
            last = result.rows[-1]
            l_time = self._parse_row_time(last)
            l_tid = str(last.get(self._trace_id_col, ""))
            await self._save_checkpoint(l_time, l_tid)
        else:
            # Timestamp-only path: handle batch boundary
            rows, cp_time = self._resolve_batch_boundary(result.rows)
            if cp_time is None:
                # Stuck — all rows share one timestamp and batch is full
                stuck = self._parse_row_time(result.rows[0])
                rows = await self._drain_timestamp(stuck, now)
                cp_time = stuck
            logger.info(
                "Poll cycle",
                extra={
                    "query_rows": len(result.rows),
                    "processing": len(rows),
                    "checkpoint_time": cp_time.isoformat(),
                    "prev_checkpoint": poll_from.isoformat(),
                },
            )
            await self._process_filtered_results(rows)
            await self._save_checkpoint(cp_time, "")

    def _filter_checkpoint_rows(self, rows: list[dict]) -> list[dict]:
        """Filter out rows already covered by the current checkpoint.

        Used only for composite-key pagination (tables with trace_id).
        """
        if not self._last_ingestion_time:
            return rows

        cp_time = self._last_ingestion_time
        cp_tid = self._last_trace_id
        tid_col = self._trace_id_col
        filtered = []

        for r in rows:
            r_time = self._parse_row_time(r)
            if r_time < cp_time:
                continue
            if r_time == cp_time and cp_tid and tid_col:
                r_tid = str(r.get(tid_col, ""))
                if r_tid <= cp_tid:
                    continue
            filtered.append(r)
        return filtered

    def _resolve_batch_boundary(
        self, rows: list[dict]
    ) -> tuple[list[dict], datetime | None]:
        """Decide which rows to process and where to checkpoint.

        Handles three cases for timestamp-only pagination:

        1. Batch not full (< batch_size rows): process all, checkpoint
           at the last row's timestamp.
        2. Batch full with mixed timestamps: trim rows at the last
           (boundary) timestamp — they may be incomplete.  Checkpoint at
           the latest timestamp *before* the boundary.  Trimmed rows are
           re-fetched next cycle.
        3. Batch full with a single timestamp: return (rows, None) to
           signal that the caller must drain all rows at that timestamp.

        Returns (rows_to_process, checkpoint_time).
        checkpoint_time is None in case 3.
        """
        if len(rows) < self.config.batch_size:
            return rows, self._parse_row_time(rows[-1])

        # Batch is full — check whether it was cut mid-timestamp
        last_time = self._parse_row_time(rows[-1])
        first_time = self._parse_row_time(rows[0])

        if first_time == last_time:
            # All rows share one timestamp — caller must drain
            return rows, None

        # Trim rows at the boundary timestamp (may be incomplete)
        safe = [r for r in rows if self._parse_row_time(r) < last_time]
        return safe, self._parse_row_time(safe[-1])

    async def _drain_timestamp(
        self, timestamp: datetime, upper_bound: datetime
    ) -> list[dict]:
        """Fetch ALL rows at a specific timestamp.

        Used when a full batch contains only one timestamp, meaning the
        normal ``take batch_size`` query cannot advance past it.
        """
        t_str = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        ing_col = self.config.ingestion_time_column
        ing_bin = ing_col if ing_col else "bin(ingestion_time(), 1microsecond)"

        query = (
            f"{self.config.source_table} "
            f"| extend ingestion_time = {ing_bin} "
            f"| where ingestion_time == datetime({t_str})"
        )

        logger.warning(
            "Draining all rows at stuck timestamp",
            extra={"timestamp": timestamp.isoformat()},
        )
        result = await self._kql_client.execute_query(query)
        logger.info(
            "Drained stuck timestamp",
            extra={
                "timestamp": timestamp.isoformat(),
                "row_count": len(result.rows),
            },
        )
        return result.rows

    def _build_query(self, table, p_from, p_to, limit, cp_tid) -> str:
        """Constructs paginated KQL query.

        For tables with a trace_id column, uses composite-key pagination
        (ingestion_time, trace_id) with KQL strcmp for deterministic
        ordering.  For tables without trace_id, uses simple timestamp-only
        pagination — boundary handling is done in _resolve_batch_boundary.

        bin(ingestion_time(), 1microsecond) truncates KQL's 100-nanosecond
        ticks to microsecond precision so comparisons match the checkpoint
        saved by Python datetime.
        """
        f_str = p_from.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        t_str = p_to.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        ing_col = self.config.ingestion_time_column
        ing_bin = ing_col if ing_col else "bin(ingestion_time(), 1microsecond)"
        extend = f"| extend ingestion_time = {ing_bin}"

        if self._trace_id_col and cp_tid:
            esc = cp_tid.replace("'", "\\'")
            where = (
                f"| where ingestion_time > datetime({f_str}) "
                f"or (ingestion_time == datetime({f_str}) "
                f"and strcmp(tostring({self._trace_id_col}), '{esc}') > 0)"
            )
        else:
            where = f"| where ingestion_time > datetime({f_str})"

        upper = f"| where ingestion_time < datetime({t_str})"

        if self._trace_id_col:
            order = f"order by ingestion_time asc, {self._trace_id_col} asc"
        else:
            order = "order by ingestion_time asc"

        return f"{table} {extend} {where} {upper} | {order} | take {limit}"

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
