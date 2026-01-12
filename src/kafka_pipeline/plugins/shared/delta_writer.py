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
from typing import Any, Optional

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from kafka_pipeline.plugins.shared.enrichment import EnrichmentContext, EnrichmentHandler, EnrichmentResult

logger = logging.getLogger(__name__)


class DeltaTableWriter(EnrichmentHandler):
    """Enrichment handler that writes data to Delta Lake tables.

    This handler is the final step in an enrichment pipeline, persisting
    enriched data to Delta tables for analysis and querying.

    Config example:
        type: kafka_pipeline.plugins.delta_writer:DeltaTableWriter
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

    def __init__(self, config: Optional[dict[str, Any]] = None):
        """Initialize Delta table writer.

        Args:
            config: Handler configuration including:
                - table_name: Name of Delta table to write to
                - mode: Write mode (append, overwrite, etc.) - default: append
                - schema_evolution: Enable automatic schema evolution - default: true
                - column_mapping: Dict mapping output columns to input fields
                - partition_by: List of columns to partition by (optional)
                - table_location: Optional explicit table location (default: catalog-managed)
        """
        super().__init__(config)
        self.spark: Optional[SparkSession] = None
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
                logger.info(f"Delta table '{self.table_name}' found")
            else:
                logger.warning(
                    f"Delta table '{self.table_name}' does not exist. "
                    f"It will be created on first write."
                )

        except Exception as e:
            logger.error(f"Failed to initialize Delta table writer: {e}")
            raise

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """Write enriched data to Delta table.

        Args:
            context: Enrichment context containing enriched data

        Returns:
            EnrichmentResult indicating success or failure
        """
        try:
            # Prepare data for writing
            record = self._prepare_record(context.data)

            # Add automatic timestamps
            record["processed_timestamp"] = datetime.utcnow().isoformat()

            # Add partition columns if needed
            if "event_timestamp" in record and self.partition_by:
                record = self._add_partition_columns(record)

            # Convert to DataFrame
            df = self.spark.createDataFrame([record])

            # Write to Delta table
            self._write_to_delta(df)

            logger.debug(f"Successfully wrote record to {self.table_name}")
            return EnrichmentResult.ok(context.data)

        except Exception as e:
            error_msg = f"Failed to write to Delta table '{self.table_name}': {str(e)}"
            logger.exception(error_msg)
            return EnrichmentResult.failed(error_msg)

    def _prepare_record(self, data: dict[str, Any]) -> dict[str, Any]:
        """Prepare record for writing using column mapping.

        Args:
            data: Enriched data from context

        Returns:
            Dict with columns mapped according to configuration
        """
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
        """Extract partition columns from event_timestamp.

        Args:
            record: Record with event_timestamp field

        Returns:
            Record with additional year/month/day columns
        """
        timestamp_field = record.get("event_timestamp")
        if not timestamp_field:
            logger.warning("No event_timestamp found for partitioning")
            return record

        try:
            # Parse timestamp (supports ISO 8601 format)
            if isinstance(timestamp_field, str):
                dt = datetime.fromisoformat(timestamp_field.replace('Z', '+00:00'))
            elif isinstance(timestamp_field, datetime):
                dt = timestamp_field
            else:
                logger.warning(f"Unexpected timestamp type: {type(timestamp_field)}")
                return record

            # Add partition columns if configured
            if "year" in self.partition_by:
                record["year"] = dt.year
            if "month" in self.partition_by:
                record["month"] = dt.month
            if "day" in self.partition_by:
                record["day"] = dt.day

        except Exception as e:
            logger.warning(f"Failed to extract partition columns: {e}")

        return record

    def _write_to_delta(self, df: DataFrame) -> None:
        """Write DataFrame to Delta table.

        Args:
            df: DataFrame to write

        Raises:
            Exception: If write fails
        """
        writer = df.write.format("delta").mode(self.mode)

        # Configure schema evolution
        if self.schema_evolution:
            writer = writer.option("mergeSchema", "true")

        # Configure partitioning
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)

        # Write to table
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
        """Check if Delta table exists.

        Returns:
            True if table exists, False otherwise
        """
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
        logger.info(f"Cleaning up Delta table writer for {self.table_name}")
        # Don't stop Spark session - it may be shared across handlers


class DeltaTableBatchWriter(DeltaTableWriter):
    """Batch variant of Delta table writer.

    Accumulates multiple records and writes them in batches for better
    performance. Useful when processing high-volume streams.

    Config example:
        type: kafka_pipeline.plugins.delta_writer:DeltaTableBatchWriter
        config:
          table_name: my_table
          batch_size: 100
          batch_timeout_seconds: 60.0
          # ... other DeltaTableWriter config
    """

    def __init__(self, config: Optional[dict[str, Any]] = None):
        """Initialize batch Delta table writer.

        Args:
            config: Handler configuration including batch_size and batch_timeout_seconds
        """
        super().__init__(config)
        self.batch_size = self.config.get("batch_size", 100)
        self.batch_timeout = self.config.get("batch_timeout_seconds", 60.0)
        self._batch: list[dict[str, Any]] = []
        self._last_flush = None

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """Add record to batch and write when batch is full.

        Args:
            context: Enrichment context

        Returns:
            EnrichmentResult - skip if added to batch, ok if batch flushed
        """
        import time

        try:
            # Prepare record
            record = self._prepare_record(context.data)
            record["processed_timestamp"] = datetime.utcnow().isoformat()

            if "event_timestamp" in record and self.partition_by:
                record = self._add_partition_columns(record)

            # Add to batch
            self._batch.append(record)

            # Initialize flush time
            if self._last_flush is None:
                self._last_flush = time.time()

            # Check if should flush
            current_time = time.time()
            time_since_flush = current_time - self._last_flush
            should_flush = (
                len(self._batch) >= self.batch_size
                or time_since_flush >= self.batch_timeout
            )

            if should_flush:
                # Flush batch
                df = self.spark.createDataFrame(self._batch)
                self._write_to_delta(df)

                logger.info(
                    f"Flushed batch of {len(self._batch)} records to {self.table_name} "
                    f"(timeout: {time_since_flush:.1f}s)"
                )

                # Reset batch
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
            logger.error(f"Failed to force flush batch: {e}")
            raise

    async def cleanup(self) -> None:
        """Cleanup and flush remaining records."""
        await self.flush()
        await super().cleanup()
