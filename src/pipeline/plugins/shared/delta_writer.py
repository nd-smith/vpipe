"""
Delta Table Writer Enrichment Handler

Writes enriched data to Delta Lake tables with support for:
- Schema evolution
- Partitioning
- Column mapping
- Error handling
"""

import logging
from datetime import datetime
from typing import Any

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from pipeline.plugins.shared.enrichment import (
    EnrichmentContext,
    EnrichmentHandler,
    EnrichmentResult,
)

logger = logging.getLogger(__name__)


class DeltaTableWriter(EnrichmentHandler):
    """Enrichment handler that writes data to Delta Lake tables.

    This handler is the final step in an enrichment pipeline, persisting
    enriched data to Delta tables for analysis and querying.

    Config example:
        type: pipeline.plugins.delta_writer:DeltaTableWriter
        config:
          table_name: my_tracking_table
          mode: append
          schema_evolution: true
          column_mapping:
            event_id: event_id
            enriched_data: lookup_result
          partition_by:
            - year
            - month
            - day

    Features:
        - Automatic schema evolution when new fields are added
        - Partitioning support for query performance
        - Column remapping for flexible data models
        - Automatic timestamp generation
        - Error handling with detailed logging
    """

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize Delta table writer."""
        super().__init__(config)
        self.spark: SparkSession | None = None
        self.table_name = self.config.get("table_name")
        self.mode = self.config.get("mode", "append")
        self.schema_evolution = self.config.get("schema_evolution", True)
        self.column_mapping = self.config.get("column_mapping", {})
        self.partition_by = self.config.get("partition_by", [])
        self.table_location = self.config.get("table_location")

        if not self.table_name:
            raise ValueError("Delta table writer requires 'table_name' in config")

    async def initialize(self) -> None:
        """Initialize Spark session and verify table access."""
        try:
            # Get or create Spark session
            self.spark = SparkSession.builder.getOrCreate()

            # Verify table exists or can be created
            if self._table_exists():
                logger.info("Delta table '%s' found", self.table_name)
            else:
                logger.warning(
                    f"Delta table '{self.table_name}' does not exist. "
                    f"It will be created on first write."
                )

        except Exception as e:
            logger.error("Failed to initialize Delta table writer: %s", e)
            raise

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """Write enriched data to Delta table."""
        try:
            # Prepare data for writing
            record = self._prepare_record(context.data)
            record["processed_timestamp"] = datetime.utcnow().isoformat()

            # Add partition columns if needed
            if "event_timestamp" in record and self.partition_by:
                record = self._add_partition_columns(record)
            df = self.spark.createDataFrame([record])

            # Write to Delta table
            self._write_to_delta(df)

            logger.debug("Successfully wrote record to %s", self.table_name)
            return EnrichmentResult.ok(context.data)

        except Exception as e:
            error_msg = f"Failed to write to Delta table '{self.table_name}': {str(e)}"
            logger.exception(error_msg)
            return EnrichmentResult.failed(error_msg)

    def _prepare_record(self, data: dict[str, Any]) -> dict[str, Any]:
        """Prepare record for writing using column mapping."""
        if not self.column_mapping:
            # No mapping - use data as-is
            return data

        record = {}
        for output_col, input_field in self.column_mapping.items():
            if input_field in data:
                record[output_col] = data[input_field]
            else:
                logger.warning(
                    f"Column mapping field '{input_field}' not found in data. "
                    f"Column '{output_col}' will be null."
                )
                record[output_col] = None

        return record

    def _add_partition_columns(self, record: dict[str, Any]) -> dict[str, Any]:
        """Extract partition columns from event_timestamp."""
        timestamp_field = record.get("event_timestamp")
        if not timestamp_field:
            logger.warning("No event_timestamp found for partitioning")
            return record

        try:
            # Parse timestamp (supports ISO 8601 format)
            if isinstance(timestamp_field, str):
                dt = datetime.fromisoformat(timestamp_field.replace("Z", "+00:00"))
            elif isinstance(timestamp_field, datetime):
                dt = timestamp_field
            else:
                logger.warning("Unexpected timestamp type: %s", type(timestamp_field))
                return record

            # Add partition columns if configured
            if "year" in self.partition_by:
                record["year"] = dt.year
            if "month" in self.partition_by:
                record["month"] = dt.month
            if "day" in self.partition_by:
                record["day"] = dt.day

        except Exception as e:
            logger.warning("Failed to extract partition columns: %s", e)

        return record

    def _write_to_delta(self, df: DataFrame) -> None:
        """Write DataFrame to Delta table.
        Exception: If write fails
        """
        writer = df.write.format("delta").mode(self.mode)
        if self.schema_evolution:
            writer = writer.option("mergeSchema", "true")
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        if self.table_location:
            # Write to explicit location
            writer.option("path", self.table_location).saveAsTable(self.table_name)
        else:
            # Catalog-managed table
            writer.saveAsTable(self.table_name)

        logger.info(
            f"Wrote to Delta table '{self.table_name}' "
            f"(mode={self.mode}, schema_evolution={self.schema_evolution})"
        )

    def _table_exists(self) -> bool:
        """Check if Delta table exists."""
        try:
            self.spark.table(self.table_name)
            return True
        except Exception:
            # Check if table exists at location
            if self.table_location:
                try:
                    DeltaTable.forPath(self.spark, self.table_location)
                    return True
                except Exception:
                    pass
            return False

    async def cleanup(self) -> None:
        """Cleanup resources.

        Note: We don't stop the Spark session as it may be shared.
        """
        logger.info("Cleaning up Delta table writer for %s", self.table_name)
        # Don't stop Spark session - it may be shared across handlers


class DeltaTableBatchWriter(DeltaTableWriter):
    """Batch variant of Delta table writer.

    Accumulates multiple records and writes them in batches for better
    performance. Useful when processing high-volume streams.

    Config example:
        type: pipeline.plugins.delta_writer:DeltaTableBatchWriter
        config:
          table_name: my_table
          batch_size: 100
          batch_timeout_seconds: 60.0
          # ... other DeltaTableWriter config
    """

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize batch Delta table writer."""
        super().__init__(config)
        self.batch_size = self.config.get("batch_size", 100)
        self.batch_timeout = self.config.get("batch_timeout_seconds", 60.0)
        self._batch: list[dict[str, Any]] = []
        self._last_flush = None

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """Add record to batch and write when batch is full."""
        import time

        try:
            record = self._prepare_record(context.data)
            record["processed_timestamp"] = datetime.utcnow().isoformat()

            if "event_timestamp" in record and self.partition_by:
                record = self._add_partition_columns(record)
            self._batch.append(record)
            if self._last_flush is None:
                self._last_flush = time.time()

            # Check if should flush
            current_time = time.time()
            time_since_flush = current_time - self._last_flush
            should_flush = (
                len(self._batch) >= self.batch_size or time_since_flush >= self.batch_timeout
            )

            if should_flush:
                df = self.spark.createDataFrame(self._batch)
                self._write_to_delta(df)

                logger.info(
                    f"Flushed batch of {len(self._batch)} records to {self.table_name} "
                    f"(timeout: {time_since_flush:.1f}s)"
                )
                self._batch.clear()
                self._last_flush = current_time

                return EnrichmentResult.ok(context.data)
            else:
                # Added to batch, skip for now
                return EnrichmentResult.skip_message(
                    f"Added to Delta batch ({len(self._batch)}/{self.batch_size})"
                )

        except Exception as e:
            error_msg = f"Failed to batch write to Delta table '{self.table_name}': {str(e)}"
            logger.exception(error_msg)
            return EnrichmentResult.failed(error_msg)

    async def flush(self) -> None:
        """Force flush current batch to Delta table."""
        if not self._batch:
            logger.debug("No records in batch to flush")
            return

        try:
            df = self.spark.createDataFrame(self._batch)
            self._write_to_delta(df)

            logger.info(f"Force flushed {len(self._batch)} records to {self.table_name}")
            self._batch.clear()

        except Exception as e:
            logger.error("Failed to force flush batch: %s", e)
            raise

    async def cleanup(self) -> None:
        """Cleanup and flush remaining records."""
        await self.flush()
        await super().cleanup()
