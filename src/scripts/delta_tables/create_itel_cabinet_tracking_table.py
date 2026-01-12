#!/usr/bin/env python3
"""
Create iTel Cabinet Task Tracking Delta Table

This script initializes the Delta table for tracking iTel cabinet task lifecycles.
The table stores enriched ClaimX task data with complete status change history.

Usage:
    python scripts/delta_tables/create_itel_cabinet_tracking_table.py [options]

Options:
    --table-name NAME       Table name (default: itel_cabinet_task_tracking)
    --table-location PATH   Explicit table location (optional, for external tables)
    --catalog NAME          Catalog name (default: current catalog)
    --database NAME         Database/schema name (default: default)
    --drop-existing         Drop table if it already exists
    --dry-run               Print DDL without executing
"""

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_table_schema() -> StructType:
    """Define the schema for iTel cabinet task tracking table.

    Returns:
        StructType with all columns defined
    """
    return StructType([
        # =====================================================================
        # Event Identification
        # =====================================================================
        StructField("event_id", StringType(), nullable=False,
                   metadata={"comment": "Unique identifier for the ClaimX event"}),
        StructField("event_type", StringType(), nullable=False,
                   metadata={"comment": "Type of event (CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED, etc.)"}),
        StructField("event_timestamp", TimestampType(), nullable=False,
                   metadata={"comment": "When the event occurred in ClaimX"}),
        StructField("processed_timestamp", TimestampType(), nullable=False,
                   metadata={"comment": "When the event was processed by the worker"}),

        # =====================================================================
        # Task Identification
        # =====================================================================
        StructField("task_id", IntegerType(), nullable=False,
                   metadata={"comment": "ClaimX task template ID (always 32513 for iTel cabinet)"}),
        StructField("assignment_id", IntegerType(), nullable=True,
                   metadata={"comment": "Unique ID for this task assignment instance"}),
        StructField("project_id", StringType(), nullable=False,
                   metadata={"comment": "ClaimX project ID this task belongs to"}),

        # =====================================================================
        # Status Tracking
        # =====================================================================
        StructField("task_status", StringType(), nullable=False,
                   metadata={"comment": "Current task status (assigned, in_progress, completed, etc.)"}),
        StructField("task_name", StringType(), nullable=True,
                   metadata={"comment": "Human-readable task name from ClaimX"}),

        # =====================================================================
        # User Tracking
        # =====================================================================
        StructField("assigned_to_user_id", IntegerType(), nullable=True,
                   metadata={"comment": "ID of user assigned to the task"}),
        StructField("assigned_by_user_id", IntegerType(), nullable=True,
                   metadata={"comment": "ID of user who made the assignment"}),

        # =====================================================================
        # Timestamps
        # =====================================================================
        StructField("task_created_at", TimestampType(), nullable=True,
                   metadata={"comment": "When the task was originally created"}),
        StructField("task_completed_at", TimestampType(), nullable=True,
                   metadata={"comment": "When the task was completed (null if not yet completed)"}),

        # =====================================================================
        # Enriched Data (Complex Types)
        # =====================================================================
        StructField("claimx_task_full", StringType(), nullable=True,
                   metadata={"comment": "Full task details from ClaimX API (JSON string)"}),
        StructField("original_event", StringType(), nullable=True,
                   metadata={"comment": "Original event task data from Kafka (JSON string)"}),
        StructField("project_data", StringType(), nullable=True,
                   metadata={"comment": "Project data from event (JSON string)"}),

        # =====================================================================
        # Metadata
        # =====================================================================
        StructField("tracking_version", StringType(), nullable=True,
                   metadata={"comment": "Version of tracking schema (for schema evolution)"}),
        StructField("source", StringType(), nullable=True,
                   metadata={"comment": "Source system that generated this record"}),

        # =====================================================================
        # Partition Columns (for query performance)
        # =====================================================================
        StructField("year", IntegerType(), nullable=True,
                   metadata={"comment": "Year extracted from event_timestamp (partition column)"}),
        StructField("month", IntegerType(), nullable=True,
                   metadata={"comment": "Month extracted from event_timestamp (partition column)"}),
        StructField("day", IntegerType(), nullable=True,
                   metadata={"comment": "Day extracted from event_timestamp (partition column)"}),
    ])


def create_table(
    spark: SparkSession,
    table_name: str,
    table_location: str = None,
    catalog: str = None,
    database: str = "default",
    drop_existing: bool = False,
    dry_run: bool = False,
) -> None:
    """Create the iTel cabinet tracking Delta table.

    Args:
        spark: SparkSession instance
        table_name: Name for the table
        table_location: Optional explicit path for external table
        catalog: Optional catalog name
        database: Database/schema name (default: default)
        drop_existing: If True, drop existing table first
        dry_run: If True, print DDL without executing
    """
    # Build full table name
    full_table_name = table_name
    if database:
        full_table_name = f"{database}.{table_name}"
    if catalog:
        full_table_name = f"{catalog}.{full_table_name}"

    logger.info(f"Creating Delta table: {full_table_name}")

    # Drop existing table if requested
    if drop_existing:
        drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
        logger.info(f"Dropping existing table: {drop_sql}")
        if not dry_run:
            spark.sql(drop_sql)

    # Get schema
    schema = get_table_schema()

    # Create empty DataFrame with schema
    df = spark.createDataFrame([], schema)

    # Build table creation
    writer = df.write.format("delta")

    # Configure partitioning
    partition_cols = ["year", "month", "day"]
    writer = writer.partitionBy(*partition_cols)

    # Configure table properties
    writer = writer.option("delta.enableChangeDataFeed", "true")  # Enable CDC
    writer = writer.option("delta.autoOptimize.optimizeWrite", "true")  # Auto-optimize
    writer = writer.option("delta.autoOptimize.autoCompact", "true")  # Auto-compact

    # Set table location if provided
    if table_location:
        writer = writer.option("path", table_location)
        logger.info(f"Table location: {table_location}")

    # Generate DDL for dry run
    if dry_run:
        logger.info("=" * 80)
        logger.info("DRY RUN - Table would be created with:")
        logger.info("=" * 80)
        logger.info(f"Full table name: {full_table_name}")
        logger.info(f"Partition columns: {partition_cols}")
        logger.info(f"Schema:")
        df.printSchema()
        logger.info("=" * 80)
        logger.info("Table properties:")
        logger.info("  - delta.enableChangeDataFeed = true")
        logger.info("  - delta.autoOptimize.optimizeWrite = true")
        logger.info("  - delta.autoOptimize.autoCompact = true")
        if table_location:
            logger.info(f"  - location = {table_location}")
        logger.info("=" * 80)
        return

    # Create table
    try:
        writer.saveAsTable(full_table_name)
        logger.info(f"✅ Successfully created table: {full_table_name}")

        # Verify table was created
        spark.sql(f"DESCRIBE EXTENDED {full_table_name}").show(truncate=False)

        # Show sample queries
        logger.info("=" * 80)
        logger.info("Table created successfully! Sample queries:")
        logger.info("=" * 80)
        logger.info(f"-- View table schema:")
        logger.info(f"DESCRIBE {full_table_name};")
        logger.info("")
        logger.info(f"-- Count records:")
        logger.info(f"SELECT COUNT(*) FROM {full_table_name};")
        logger.info("")
        logger.info(f"-- View recent events:")
        logger.info(f"SELECT event_timestamp, task_status, task_name")
        logger.info(f"FROM {full_table_name}")
        logger.info(f"ORDER BY event_timestamp DESC")
        logger.info(f"LIMIT 10;")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"❌ Failed to create table: {e}")
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create iTel Cabinet Task Tracking Delta Table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--table-name",
        default="itel_cabinet_task_tracking",
        help="Table name (default: itel_cabinet_task_tracking)",
    )
    parser.add_argument(
        "--table-location",
        help="Explicit table location path (optional, for external tables)",
    )
    parser.add_argument(
        "--catalog",
        help="Catalog name (default: current catalog)",
    )
    parser.add_argument(
        "--database",
        default="default",
        help="Database/schema name (default: default)",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop table if it already exists",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print DDL without executing",
    )

    args = parser.parse_args()

    # Create Spark session
    logger.info("Creating Spark session...")
    spark = (
        SparkSession.builder
        .appName("Create iTel Cabinet Tracking Table")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    try:
        create_table(
            spark=spark,
            table_name=args.table_name,
            table_location=args.table_location,
            catalog=args.catalog,
            database=args.database,
            drop_existing=args.drop_existing,
            dry_run=args.dry_run,
        )
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
