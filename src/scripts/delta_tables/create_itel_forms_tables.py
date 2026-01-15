#!/usr/bin/env python3
"""
Create Delta tables for iTel Cabinet form submissions and attachments.

Creates two tables:
- claimx_itel_forms: Cabinet repair form submissions
- claimx_itel_attachments: Media attachments from form questions

Usage:
    python scripts/delta_tables/create_itel_forms_tables.py [--dry-run]
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    BooleanType,
    TimestampType,
)


# Table paths
SUBMISSIONS_TABLE_PATH = "/Tables/dbo/claimx_itel_forms"
ATTACHMENTS_TABLE_PATH = "/Tables/dbo/claimx_itel_attachments"


# Submissions schema
submissions_schema = StructType([
    StructField("assignment_id", LongType(), False),
    StructField("project_id", StringType(), False),  # ClaimX returns project_id as string
    StructField("form_id", StringType(), False),
    StructField("form_response_id", StringType(), False),
    StructField("status", StringType(), False),
    StructField("event_id", StringType(), True),  # Kafka event ID for traceability
    StructField("date_assigned", TimestampType(), False),
    StructField("date_completed", TimestampType(), True),
    StructField("customer_first_name", StringType(), True),
    StructField("customer_last_name", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("assignor_email", StringType(), True),
    StructField("damage_description", StringType(), True),
    StructField("additional_notes", StringType(), True),
    StructField("countertops_lf", IntegerType(), True),

    # Lower Cabinets
    StructField("lower_cabinets_damaged", BooleanType(), True),
    StructField("lower_cabinets_lf", IntegerType(), True),
    StructField("num_damaged_lower_boxes", IntegerType(), True),
    StructField("lower_cabinets_detached", BooleanType(), True),
    StructField("lower_face_frames_doors_drawers_available", StringType(), True),
    StructField("lower_face_frames_doors_drawers_damaged", BooleanType(), True),
    StructField("lower_finished_end_panels_damaged", BooleanType(), True),
    StructField("lower_end_panel_damage_present", BooleanType(), True),
    StructField("lower_counter_type", StringType(), True),

    # Upper Cabinets
    StructField("upper_cabinets_damaged", BooleanType(), True),
    StructField("upper_cabinets_lf", IntegerType(), True),
    StructField("num_damaged_upper_boxes", IntegerType(), True),
    StructField("upper_cabinets_detached", BooleanType(), True),
    StructField("upper_face_frames_doors_drawers_available", StringType(), True),
    StructField("upper_face_frames_doors_drawers_damaged", BooleanType(), True),
    StructField("upper_finished_end_panels_damaged", BooleanType(), True),
    StructField("upper_end_panel_damage_present", BooleanType(), True),

    # Full Height Cabinets
    StructField("full_height_cabinets_damaged", BooleanType(), True),
    StructField("full_height_cabinets_lf", IntegerType(), True),
    StructField("num_damaged_full_height_boxes", IntegerType(), True),
    StructField("full_height_cabinets_detached", BooleanType(), True),
    StructField("full_height_face_frames_doors_drawers_available", StringType(), True),
    StructField("full_height_face_frames_doors_drawers_damaged", BooleanType(), True),
    StructField("full_height_finished_end_panels_damaged", BooleanType(), True),

    # Island Cabinets
    StructField("island_cabinets_damaged", BooleanType(), True),
    StructField("island_cabinets_lf", IntegerType(), True),
    StructField("num_damaged_island_boxes", IntegerType(), True),
    StructField("island_cabinets_detached", BooleanType(), True),
    StructField("island_face_frames_doors_drawers_available", StringType(), True),
    StructField("island_face_frames_doors_drawers_damaged", BooleanType(), True),
    StructField("island_finished_end_panels_damaged", BooleanType(), True),
    StructField("island_end_panel_damage_present", BooleanType(), True),
    StructField("island_counter_type", StringType(), True),

    # Metadata
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
])


# Attachments schema
attachments_schema = StructType([
    StructField("id", LongType(), False),  # Auto-generated
    StructField("assignment_id", LongType(), False),
    StructField("project_id", LongType(), False),  # ClaimX project ID
    StructField("question_key", StringType(), False),
    StructField("question_text", StringType(), False),
    StructField("media_id", LongType(), False),  # ClaimX media ID
    StructField("url", StringType(), True),  # Download URL from ClaimX
    StructField("display_order", IntegerType(), False),
    StructField("created_at", TimestampType(), True),
    StructField("event_id", StringType(), True),  # Kafka event ID for traceability
])


def create_submissions_table(spark: SparkSession, dry_run: bool = False):
    """
    Create claimx_itel_forms Delta table.

    Args:
        spark: Spark session
        dry_run: If True, only print schema without creating table
    """
    print("\n" + "=" * 80)
    print("SUBMISSIONS TABLE: claimx_itel_forms")
    print("=" * 80)
    print(f"Path: {SUBMISSIONS_TABLE_PATH}")
    print(f"\nSchema ({len(submissions_schema.fields)} fields):")
    print("-" * 80)

    for field in submissions_schema.fields:
        nullable = "NULL" if field.nullable else "NOT NULL"
        print(f"  {field.name:50s} {str(field.dataType):20s} {nullable}")

    if dry_run:
        print("\n[DRY RUN] Would create table, but --dry-run flag is set")
        return

    print("\nCreating table...")

    # Create empty DataFrame with schema
    df = spark.createDataFrame([], submissions_schema)

    # Write to Delta table
    df.write.format("delta") \
        .mode("ignore") \
        .option("path", SUBMISSIONS_TABLE_PATH) \
        .saveAsTable("claimx_itel_forms")

    print("✓ Table created successfully")


def create_attachments_table(spark: SparkSession, dry_run: bool = False):
    """
    Create claimx_itel_attachments Delta table.

    Args:
        spark: Spark session
        dry_run: If True, only print schema without creating table
    """
    print("\n" + "=" * 80)
    print("ATTACHMENTS TABLE: claimx_itel_attachments")
    print("=" * 80)
    print(f"Path: {ATTACHMENTS_TABLE_PATH}")
    print(f"\nSchema ({len(attachments_schema.fields)} fields):")
    print("-" * 80)

    for field in attachments_schema.fields:
        nullable = "NULL" if field.nullable else "NOT NULL"
        print(f"  {field.name:50s} {str(field.dataType):20s} {nullable}")

    if dry_run:
        print("\n[DRY RUN] Would create table, but --dry-run flag is set")
        return

    print("\nCreating table...")

    # Create empty DataFrame with schema
    df = spark.createDataFrame([], attachments_schema)

    # Write to Delta table
    df.write.format("delta") \
        .mode("ignore") \
        .option("path", ATTACHMENTS_TABLE_PATH) \
        .saveAsTable("claimx_itel_attachments")

    print("✓ Table created successfully")


def main():
    parser = argparse.ArgumentParser(
        description="Create Delta tables for iTel Cabinet forms"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print schema without creating tables",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("iTel Cabinet Forms - Delta Table Creation")
    print("=" * 80)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("CreateItelFormsTables") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Create both tables
        create_submissions_table(spark, dry_run=args.dry_run)
        create_attachments_table(spark, dry_run=args.dry_run)

        if not args.dry_run:
            print("\n" + "=" * 80)
            print("SUCCESS: Both tables created")
            print("=" * 80)
            print("\nVerify tables:")
            print("  spark.sql('DESCRIBE TABLE claimx_itel_forms').show()")
            print("  spark.sql('DESCRIBE TABLE claimx_itel_attachments').show()")
        else:
            print("\n" + "=" * 80)
            print("DRY RUN COMPLETE")
            print("=" * 80)
            print("\nTo create tables, run without --dry-run flag:")
            print("  python scripts/delta_tables/create_itel_forms_tables.py")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
