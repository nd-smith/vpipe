#!/usr/bin/env python3
"""Inspect Delta Lake tables written during simulation.

Usage:
    python scripts/inspect_simulation_delta.py
    python scripts/inspect_simulation_delta.py /custom/path/to/delta

This script examines all ClaimX entity Delta tables in the simulation directory,
showing row counts, schemas, and sample data. Useful for verifying that the
entity delta writer is functioning correctly in simulation mode.
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional

try:
    import polars as pl
except ImportError:
    print("Error: polars is required. Install with: pip install polars")
    sys.exit(1)


# Expected schemas for validation
EXPECTED_SCHEMAS: Dict[str, List[str]] = {
    "claimx_projects": [
        "project_id", "project_number", "master_file_name", "secondary_number",
        "status", "created_date", "date_of_loss", "type_of_loss", "cause_of_loss",
        "loss_description", "customer_first_name", "customer_last_name",
        "street1", "city", "state_province", "zip_postcode", "primary_email",
        "primary_phone", "custom_attribute1", "custom_attribute2", "custom_attribute3",
        "coverages", "contents_task_sent", "contents_task_at", "xa_autolink_fail",
        "xa_autolink_fail_at", "event_id", "created_at", "updated_at", "last_enriched_at"
    ],
    "claimx_contacts": [
        "project_id", "contact_email", "contact_type", "first_name", "last_name",
        "phone_number", "is_primary_contact", "master_file_name", "task_assignment_id",
        "video_collaboration_id", "event_id", "created_at", "last_enriched_at",
        "created_date", "phone_country_code", "updated_at"
    ],
    "claimx_attachment_metadata": [
        "media_id", "project_id", "task_assignment_id", "file_type", "file_name",
        "media_description", "media_comment", "latitude", "longitude", "gps_source",
        "taken_date", "full_download_link", "expires_at", "event_id", "created_at",
        "updated_at", "last_enriched_at"
    ],
    "claimx_tasks": [
        "assignment_id", "task_id", "project_id", "assignee_id", "assignor_id",
        "date_assigned", "date_completed", "status", "stp_enabled", "mfn",
        "event_id", "created_at", "updated_at", "last_enriched_at"
    ],
    "claimx_task_templates": [
        "task_id", "comp_id", "name", "description", "form_id", "form_name",
        "enabled", "is_default", "is_manual_delivery", "is_external_link_delivery",
        "provide_portal_access", "notify_assigned_send_recipient",
        "notify_assigned_send_recipient_sms", "notify_assigned_subject",
        "notify_task_completed", "notify_completed_subject", "allow_resubmit",
        "auto_generate_pdf", "modified_by", "modified_by_id", "modified_date",
        "event_id", "created_at", "updated_at", "last_enriched_at"
    ],
    "claimx_external_links": [
        "link_id", "assignment_id", "project_id", "link_code", "url",
        "notification_access_method", "country_id", "state_id", "created_date",
        "accessed_count", "last_accessed", "event_id", "created_at", "updated_at"
    ],
    "claimx_video_collab": [
        "video_collaboration_id", "claim_id", "mfn", "claim_number", "policy_number",
        "email_user_name", "claim_rep_first_name", "claim_rep_last_name",
        "claim_rep_full_name", "number_of_videos", "number_of_photos",
        "number_of_viewers", "session_count", "total_time_seconds", "total_time",
        "created_date", "live_call_first_session", "live_call_last_session",
        "company_id", "company_name", "guid", "event_id", "created_at",
        "updated_at", "last_enriched_at"
    ],
}


def validate_schema(table_name: str, df: pl.DataFrame) -> None:
    """Validate table schema matches expectations.

    Args:
        table_name: Name of the table being validated
        df: DataFrame to validate

    Prints warnings for missing or extra columns.
    """
    if table_name not in EXPECTED_SCHEMAS:
        print(f"   ℹ️  No schema validation available for {table_name}")
        return

    expected_cols = set(EXPECTED_SCHEMAS[table_name])
    actual_cols = set(df.columns)

    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    if missing:
        print(f"   ⚠️  Missing columns: {', '.join(sorted(missing))}")
    if extra:
        print(f"   ℹ️  Extra columns: {', '.join(sorted(extra))}")
    if not missing and not extra:
        print(f"   ✅ Schema matches expected definition")


def format_sample_data(row: dict, max_width: int = 80) -> str:
    """Format a sample row for display.

    Args:
        row: Dictionary representing a table row
        max_width: Maximum width for formatted output

    Returns:
        Formatted string representation of the row
    """
    lines = []
    for key, value in row.items():
        value_str = str(value)
        if len(value_str) > max_width - len(key) - 5:
            value_str = value_str[:max_width - len(key) - 8] + "..."
        lines.append(f"     {key}: {value_str}")
    return "\n".join(lines)


def inspect_delta_table(table_path: Path, table_name: str) -> None:
    """Inspect a single Delta table.

    Args:
        table_path: Path to the Delta table directory
        table_name: Name of the table for display
    """
    if not table_path.exists():
        print(f"❌ {table_name}: Not found at {table_path}")
        return

    try:
        # Read Delta table using Polars
        # Delta Lake stores metadata in _delta_log subdirectory
        delta_log_path = table_path / "_delta_log"
        if not delta_log_path.exists():
            print(f"⚠️  {table_name}: Directory exists but no _delta_log found (not a Delta table)")
            return

        # Read the Delta table
        df = pl.read_delta(str(table_path))

        print(f"✅ {table_name}: {len(df)} rows")
        print(f"   Path: {table_path}")
        print(f"   Columns ({len(df.columns)}): {', '.join(df.columns[:10])}")
        if len(df.columns) > 10:
            print(f"   ... and {len(df.columns) - 10} more columns")

        # Validate schema
        validate_schema(table_name, df)

        # Show sample data if available
        if len(df) > 0:
            print(f"   Sample row (first):")
            sample = df.head(1).to_dicts()[0]
            print(format_sample_data(sample))
        else:
            print(f"   (No data)")

        print()

    except Exception as e:
        print(f"⚠️  {table_name}: Error reading - {e}")
        print()


def get_table_size_info(table_path: Path) -> Optional[str]:
    """Get size information for a Delta table.

    Args:
        table_path: Path to the Delta table directory

    Returns:
        Human-readable size string, or None if not available
    """
    if not table_path.exists():
        return None

    try:
        total_size = 0
        for file in table_path.rglob("*"):
            if file.is_file():
                total_size += file.stat().st_size

        # Convert to human-readable format
        for unit in ['B', 'KB', 'MB', 'GB']:
            if total_size < 1024.0:
                return f"{total_size:.1f} {unit}"
            total_size /= 1024.0
        return f"{total_size:.1f} TB"
    except Exception:
        return None


def inspect_delta_tables(delta_path: Path, show_sizes: bool = True) -> None:
    """Inspect all Delta tables in simulation directory.

    Args:
        delta_path: Path to the directory containing Delta tables
        show_sizes: Whether to show table sizes
    """
    tables = [
        "claimx_projects",
        "claimx_contacts",
        "claimx_attachment_metadata",
        "claimx_tasks",
        "claimx_task_templates",
        "claimx_external_links",
        "claimx_video_collab",
    ]

    print(f"Delta Lake Tables Inspection")
    print(f"{'=' * 80}")
    print(f"Base path: {delta_path}")
    print()

    # Check if base path exists
    if not delta_path.exists():
        print(f"❌ Delta path does not exist: {delta_path}")
        print()
        print("To create simulation Delta tables, run:")
        print("  SIMULATION_MODE=true python -m kafka_pipeline claimx-entity-delta-writer")
        return

    # Inspect each table
    total_rows = 0
    tables_found = 0

    for table_name in tables:
        table_path = delta_path / table_name
        inspect_delta_table(table_path, table_name)

        # Track statistics
        if table_path.exists() and (table_path / "_delta_log").exists():
            tables_found += 1
            try:
                df = pl.read_delta(str(table_path))
                total_rows += len(df)
            except Exception:
                pass

    # Summary
    print(f"{'=' * 80}")
    print(f"Summary:")
    print(f"  Tables found: {tables_found}/{len(tables)}")
    print(f"  Total rows: {total_rows}")

    if show_sizes:
        print()
        print(f"Table Sizes:")
        for table_name in tables:
            table_path = delta_path / table_name
            size_info = get_table_size_info(table_path)
            if size_info:
                print(f"  {table_name}: {size_info}")


def main():
    """Main entry point for the inspection script."""
    # Get delta path from command line or use default
    if len(sys.argv) > 1:
        delta_path = Path(sys.argv[1])
    else:
        delta_path = Path("/tmp/vpipe_simulation/delta")

    try:
        inspect_delta_tables(delta_path)
    except KeyboardInterrupt:
        print("\nInspection interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Error during inspection: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
