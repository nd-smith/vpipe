"""
Pipeline verification utility for diagnosing message count discrepancies.

This script helps identify:
1. Duplicate events in the xact_events Delta table
2. Discrepancies between Eventhouse source and Kafka topics
3. Message flow issues through the pipeline

Usage:
    python -m kafka_pipeline.common.verify_pipeline --table-path <path> --since 2026-01-02
"""

import argparse
import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import polars as pl

from core.auth.credentials import get_storage_options

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class PipelineVerifier:
    """Verifies pipeline data integrity and identifies discrepancies."""

    def __init__(self, xact_events_table_path: str):
        """
        Initialize verifier.

        Args:
            xact_events_table_path: Path to xact_events Delta table
        """
        self.table_path = xact_events_table_path
        self._storage_options: Optional[dict] = None

    def _get_storage_options(self) -> dict:
        """Get storage options for Delta table access."""
        if self._storage_options is None:
            self._storage_options = get_storage_options()
        return self._storage_options

    def analyze_events(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> dict:
        """
        Analyze events in Delta table for duplicates and discrepancies.

        Args:
            since: Only analyze events after this time
            until: Only analyze events before this time

        Returns:
            Dictionary with analysis results
        """
        logger.info(f"Analyzing events in {self.table_path}")

        # Default to last 48 hours if no time range specified
        if since is None:
            since = datetime.now(timezone.utc) - timedelta(hours=48)
        if until is None:
            until = datetime.now(timezone.utc)

        since_date = since.date()
        until_date = until.date()

        try:
            # Load events with partition pruning
            df = (
                pl.scan_delta(
                    self.table_path,
                    storage_options=self._get_storage_options(),
                )
                # Partition filter for file-level pruning
                .filter(pl.col("event_date") >= since_date)
                .filter(pl.col("event_date") <= until_date)
                # Row-level filter for precise time window
                .filter(pl.col("ingested_at") >= since)
                .filter(pl.col("ingested_at") <= until)
                .collect()
            )

            total_rows = len(df)
            unique_trace_ids = df["trace_id"].n_unique()
            duplicate_count = total_rows - unique_trace_ids

            results = {
                "time_range": {
                    "since": since.isoformat(),
                    "until": until.isoformat(),
                },
                "total_rows": total_rows,
                "unique_trace_ids": unique_trace_ids,
                "duplicate_rows": duplicate_count,
                "duplicate_percentage": round(
                    (duplicate_count / total_rows * 100) if total_rows > 0 else 0, 2
                ),
            }

            # Analyze duplicates if present
            if duplicate_count > 0:
                # Find trace_ids with duplicates
                duplicate_analysis = (
                    df.group_by("trace_id")
                    .agg(pl.count().alias("count"))
                    .filter(pl.col("count") > 1)
                    .sort("count", descending=True)
                )

                # Get sample duplicates
                sample_duplicates = duplicate_analysis.head(10).to_dicts()
                max_duplicates = duplicate_analysis["count"].max()
                trace_ids_with_duplicates = len(duplicate_analysis)

                results["duplicate_analysis"] = {
                    "trace_ids_with_duplicates": trace_ids_with_duplicates,
                    "max_duplicates_per_trace_id": max_duplicates,
                    "sample_duplicates": sample_duplicates,
                }

                # Analyze temporal pattern of duplicates
                duplicated_trace_ids = set(duplicate_analysis["trace_id"].to_list())
                duplicate_df = df.filter(
                    pl.col("trace_id").is_in(list(duplicated_trace_ids)[:100])
                )

                # Check if duplicates have different ingested_at times
                if len(duplicate_df) > 0:
                    temporal_analysis = (
                        duplicate_df.group_by("trace_id")
                        .agg([
                            pl.col("ingested_at").min().alias("first_ingested"),
                            pl.col("ingested_at").max().alias("last_ingested"),
                            pl.count().alias("count"),
                        ])
                        .with_columns([
                            (pl.col("last_ingested") - pl.col("first_ingested"))
                            .dt.total_seconds()
                            .alias("time_span_seconds"),
                        ])
                    )

                    avg_time_span = temporal_analysis["time_span_seconds"].mean()
                    max_time_span = temporal_analysis["time_span_seconds"].max()

                    results["temporal_analysis"] = {
                        "avg_time_span_between_duplicates_seconds": round(
                            avg_time_span or 0, 2
                        ),
                        "max_time_span_between_duplicates_seconds": round(
                            max_time_span or 0, 2
                        ),
                    }

            # Event type distribution
            type_distribution = (
                df.group_by("status_subtype")
                .agg(pl.count().alias("count"))
                .sort("count", descending=True)
                .head(10)
                .to_dicts()
            )
            results["event_type_distribution"] = type_distribution

            # Check for events with/without attachments
            # Note: attachment_count might be a different column name
            if "attachment_count" in df.columns:
                events_with_attachments = (
                    df.filter(pl.col("attachment_count") > 0).shape[0]
                )
                events_without_attachments = (
                    df.filter(pl.col("attachment_count") == 0).shape[0]
                )
                results["attachment_analysis"] = {
                    "events_with_attachments": events_with_attachments,
                    "events_without_attachments": events_without_attachments,
                }

            logger.info(f"Analysis complete: {unique_trace_ids} unique events, {duplicate_count} duplicates")
            return results

        except FileNotFoundError:
            logger.error(f"Delta table not found at {self.table_path}")
            return {"error": "Delta table not found", "table_path": self.table_path}
        except Exception as e:
            logger.error(f"Analysis failed: {e}", exc_info=True)
            return {"error": str(e)}

    def get_hourly_distribution(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> list:
        """
        Get hourly distribution of events to identify polling patterns.

        Returns:
            List of dicts with hour and count
        """
        if since is None:
            since = datetime.now(timezone.utc) - timedelta(hours=48)
        if until is None:
            until = datetime.now(timezone.utc)

        since_date = since.date()
        until_date = until.date()

        try:
            df = (
                pl.scan_delta(
                    self.table_path,
                    storage_options=self._get_storage_options(),
                )
                .filter(pl.col("event_date") >= since_date)
                .filter(pl.col("event_date") <= until_date)
                .filter(pl.col("ingested_at") >= since)
                .filter(pl.col("ingested_at") <= until)
                .with_columns([
                    pl.col("ingested_at").dt.truncate("1h").alias("hour"),
                ])
                .group_by("hour")
                .agg([
                    pl.count().alias("total_rows"),
                    pl.col("trace_id").n_unique().alias("unique_events"),
                ])
                .sort("hour")
                .collect()
            )

            return df.to_dicts()

        except Exception as e:
            logger.error(f"Failed to get hourly distribution: {e}")
            return []


def print_report(results: dict) -> None:
    """Print formatted analysis report."""
    print("\n" + "=" * 80)
    print("PIPELINE VERIFICATION REPORT")
    print("=" * 80)

    if "error" in results:
        print(f"\nERROR: {results['error']}")
        return

    print(f"\nTime Range: {results['time_range']['since']} to {results['time_range']['until']}")

    print("\n--- Summary ---")
    print(f"Total rows in Delta table:     {results['total_rows']:,}")
    print(f"Unique trace_ids:              {results['unique_trace_ids']:,}")
    print(f"Duplicate rows:                {results['duplicate_rows']:,}")
    print(f"Duplicate percentage:          {results['duplicate_percentage']}%")

    if "duplicate_analysis" in results:
        print("\n--- Duplicate Analysis ---")
        da = results["duplicate_analysis"]
        print(f"Trace IDs with duplicates:     {da['trace_ids_with_duplicates']:,}")
        print(f"Max duplicates per trace_id:   {da['max_duplicates_per_trace_id']}")
        print("\nSample duplicate trace_ids:")
        for item in da["sample_duplicates"][:5]:
            print(f"  {item['trace_id']}: {item['count']} occurrences")

    if "temporal_analysis" in results:
        print("\n--- Temporal Pattern ---")
        ta = results["temporal_analysis"]
        print(f"Avg time between duplicates:   {ta['avg_time_span_between_duplicates_seconds']}s")
        print(f"Max time between duplicates:   {ta['max_time_span_between_duplicates_seconds']}s")

    if "attachment_analysis" in results:
        print("\n--- Attachment Analysis ---")
        aa = results["attachment_analysis"]
        print(f"Events WITH attachments:       {aa['events_with_attachments']:,}")
        print(f"Events WITHOUT attachments:    {aa['events_without_attachments']:,}")

    if "event_type_distribution" in results:
        print("\n--- Event Type Distribution (top 10) ---")
        for item in results["event_type_distribution"]:
            print(f"  {item.get('status_subtype', 'unknown')}: {item['count']:,}")

    print("\n" + "=" * 80)

    # Recommendations
    print("\n--- RECOMMENDATIONS ---")
    if results["duplicate_rows"] > 0:
        dup_pct = results["duplicate_percentage"]
        if dup_pct > 10:
            print("⚠️  HIGH DUPLICATE RATE detected!")
            print("   Possible causes:")
            print("   1. KQL Poller overlap_minutes causing re-polling")
            print("   2. Multiple poller instances running simultaneously")
            print("   3. Daily maintenance job not running or failing")
            print("   4. Eventhouse source has higher duplicate rate than expected (>0.5%)")
            print("")
            print("   Recommended actions:")
            print("   1. Check for multiple poller pods/processes")
            print("   2. Verify daily maintenance job is running (see fabric_notebooks/)")
            print("   3. Check Eventhouse source duplicate rate")
        elif dup_pct > 1:
            print("⚠️  Moderate duplicate rate detected.")
            print("   This may be expected due to the 5-minute polling overlap window.")
            print("   Daily maintenance job will clean these up.")
            print("   Monitor to ensure rate doesn't increase.")
        else:
            print("✓  Duplicate rate within acceptable range (<1%)")
            print("   Daily maintenance job will clean up remaining duplicates.")
    else:
        print("✓  No duplicates detected in Delta table")

    print("\n" + "=" * 80)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Verify pipeline data integrity and identify discrepancies"
    )
    parser.add_argument(
        "--table-path",
        default=os.getenv("XACT_EVENTS_TABLE_PATH", ""),
        help="Path to xact_events Delta table (default: XACT_EVENTS_TABLE_PATH env var)",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Analyze events since this datetime (ISO format, e.g., 2026-01-02T00:00:00Z)",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=48,
        help="Analyze events from last N hours (default: 48)",
    )
    parser.add_argument(
        "--hourly",
        action="store_true",
        help="Show hourly distribution of events",
    )

    args = parser.parse_args()

    if not args.table_path:
        print("Error: --table-path required or set XACT_EVENTS_TABLE_PATH env var")
        return 1

    # Parse time range
    if args.since:
        since = datetime.fromisoformat(args.since.replace("Z", "+00:00"))
    else:
        since = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    until = datetime.now(timezone.utc)

    verifier = PipelineVerifier(args.table_path)

    # Run analysis
    results = verifier.analyze_events(since=since, until=until)
    print_report(results)

    # Show hourly distribution if requested
    if args.hourly:
        print("\n--- Hourly Event Distribution ---")
        hourly = verifier.get_hourly_distribution(since=since, until=until)
        for item in hourly:
            hour_str = item["hour"].strftime("%Y-%m-%d %H:00") if item.get("hour") else "unknown"
            total = item.get("total_rows", 0)
            unique = item.get("unique_events", 0)
            dup = total - unique
            print(f"  {hour_str}: {total:,} rows ({unique:,} unique, {dup:,} duplicates)")

    return 0


if __name__ == "__main__":
    exit(main())
