#!/usr/bin/env python3
"""
Migration: Rename claim_media_id to media_id in claimx_itel_attachments table.

This migration renames the column to match the code schema expectations.

Usage:
    python scripts/delta_tables/migrate_attachments_rename_column.py [--dry-run]
"""

import argparse
from pyspark.sql import SparkSession


ATTACHMENTS_TABLE_PATH = "/Tables/dbo/claimx_itel_attachments"
TABLE_NAME = "claimx_itel_attachments"


def run_migration(spark: SparkSession, dry_run: bool = False):
    """
    Rename claim_media_id column to media_id.

    Args:
        spark: Spark session
        dry_run: If True, only print what would be done
    """
    print("=" * 80)
    print("Migration: Rename claim_media_id -> media_id")
    print("=" * 80)
    print(f"Table: {TABLE_NAME}")
    print(f"Path: {ATTACHMENTS_TABLE_PATH}")

    # Check current schema
    print("\nCurrent schema:")
    print("-" * 80)
    try:
        df = spark.read.format("delta").load(ATTACHMENTS_TABLE_PATH)
        for field in df.schema.fields:
            print(f"  {field.name:30s} {str(field.dataType)}")

        if "claim_media_id" not in df.columns:
            print("\n[INFO] Column 'claim_media_id' not found - migration may have already run")
            if "media_id" in df.columns:
                print("[INFO] Column 'media_id' already exists - no migration needed")
                return
            else:
                print("[ERROR] Neither claim_media_id nor media_id found!")
                return
    except Exception as e:
        print(f"\n[ERROR] Could not read table: {e}")
        return

    if dry_run:
        print("\n[DRY RUN] Would execute:")
        print(f"  ALTER TABLE delta.`{ATTACHMENTS_TABLE_PATH}` RENAME COLUMN claim_media_id TO media_id")
        return

    print("\nExecuting migration...")

    # Rename the column using ALTER TABLE
    rename_sql = f"ALTER TABLE delta.`{ATTACHMENTS_TABLE_PATH}` RENAME COLUMN claim_media_id TO media_id"
    print(f"  SQL: {rename_sql}")

    try:
        spark.sql(rename_sql)
        print("\n✓ Column renamed successfully")

        # Verify the change
        print("\nVerifying new schema:")
        print("-" * 80)
        df = spark.read.format("delta").load(ATTACHMENTS_TABLE_PATH)
        for field in df.schema.fields:
            print(f"  {field.name:30s} {str(field.dataType)}")

        if "media_id" in df.columns and "claim_media_id" not in df.columns:
            print("\n✓ Migration verified successfully")
        else:
            print("\n[WARNING] Migration may not have completed correctly")

    except Exception as e:
        print(f"\n[ERROR] Migration failed: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Rename claim_media_id to media_id in attachments table"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL without executing",
    )
    args = parser.parse_args()

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("MigrateAttachmentsRenameColumn") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        run_migration(spark, dry_run=args.dry_run)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
