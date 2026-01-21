"""
Tests for InMemoryDeltaTable and related classes.

Verifies that the in-memory implementation correctly mirrors DeltaTableWriter
behavior for use in E2E testing.
"""

from datetime import datetime, date, timezone

import polars as pl
import pytest

from kafka_pipeline.common.storage.inmemory_delta import (
    InMemoryDeltaTable,
    InMemoryDeltaTableWriter,
    InMemoryDeltaTableReader,
    InMemoryDeltaRegistry,
)


class TestInMemoryDeltaTable:
    """Tests for InMemoryDeltaTable core functionality."""

    def test_init_empty_table(self):
        """New table should be empty."""
        table = InMemoryDeltaTable("test://events")

        assert not table.exists()
        assert len(table) == 0
        assert table.get_row_count() == 0
        assert table.get_write_count() == 0
        assert table.get_schema() == {}
        assert table.get_columns() == []

    def test_append_single_dataframe(self):
        """Append should add data and track write."""
        table = InMemoryDeltaTable("test://events")

        df = pl.DataFrame({
            "event_id": ["e1", "e2", "e3"],
            "event_type": ["A", "B", "C"],
            "value": [1, 2, 3],
        })

        rows_written = table.append(df)

        assert rows_written == 3
        assert len(table) == 3
        assert table.get_write_count() == 1
        assert table.exists()
        assert set(table.get_columns()) == {"event_id", "event_type", "value"}

        # Verify data
        result = table.read()
        assert len(result) == 3
        assert result["event_id"].to_list() == ["e1", "e2", "e3"]

    def test_append_multiple_dataframes(self):
        """Multiple appends should accumulate data."""
        table = InMemoryDeltaTable("test://events")

        df1 = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        df2 = pl.DataFrame({"id": [3, 4], "name": ["c", "d"]})

        table.append(df1)
        table.append(df2)

        assert len(table) == 4
        assert table.get_write_count() == 2
        assert table.read()["id"].to_list() == [1, 2, 3, 4]

    def test_append_empty_dataframe(self):
        """Appending empty DataFrame should return 0 and not create table."""
        table = InMemoryDeltaTable("test://events")

        df = pl.DataFrame({"id": []})
        rows = table.append(df)

        assert rows == 0
        assert not table.exists()

    def test_schema_evolution_add_columns(self):
        """New columns should be added with nulls for existing rows."""
        table = InMemoryDeltaTable("test://events")

        # First append with columns A, B
        df1 = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        table.append(df1)

        # Second append with columns A, B, C
        df2 = pl.DataFrame({"a": [3], "b": ["z"], "c": [100]})
        table.append(df2)

        result = table.read()
        assert "c" in result.columns
        # First two rows should have null for column c
        assert result["c"].to_list() == [None, None, 100]

    def test_schema_evolution_missing_columns(self):
        """Missing columns in new data should be filled with nulls."""
        table = InMemoryDeltaTable("test://events")

        # First append with columns A, B, C
        df1 = pl.DataFrame({"a": [1], "b": ["x"], "c": [100]})
        table.append(df1)

        # Second append with only columns A, B
        df2 = pl.DataFrame({"a": [2], "b": ["y"]})
        table.append(df2)

        result = table.read()
        # New row should have null for column c
        assert result["c"].to_list() == [100, None]

    def test_read_with_column_selection(self):
        """Read should support column selection."""
        table = InMemoryDeltaTable("test://events")

        df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        table.append(df)

        result = table.read(columns=["a", "c"])
        assert result.columns == ["a", "c"]
        assert "b" not in result.columns

    def test_read_filtered(self):
        """read_filtered should apply filter expression."""
        table = InMemoryDeltaTable("test://events")

        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "value": [10, 20, 30, 40, 50],
        })
        table.append(df)

        result = table.read_filtered(pl.col("value") > 25)
        assert len(result) == 3
        assert result["id"].to_list() == [3, 4, 5]

    def test_read_filtered_with_limit(self):
        """read_filtered should support limit."""
        table = InMemoryDeltaTable("test://events")

        df = pl.DataFrame({"id": list(range(100))})
        table.append(df)

        result = table.read_filtered(pl.col("id") >= 0, limit=10)
        assert len(result) == 10

    def test_merge_insert_new_rows(self):
        """Merge should insert rows when no matches exist."""
        table = InMemoryDeltaTable("test://entities")

        df = pl.DataFrame({
            "id": ["a", "b", "c"],
            "name": ["Alice", "Bob", "Carol"],
        })

        rows = table.merge(df, merge_keys=["id"])

        assert rows == 3
        assert len(table) == 3

    def test_merge_update_existing_rows(self):
        """Merge should update rows when matches exist."""
        table = InMemoryDeltaTable("test://entities")

        # Initial data
        df1 = pl.DataFrame({
            "id": ["a", "b"],
            "name": ["Alice", "Bob"],
            "score": [100, 200],
        })
        table.merge(df1, merge_keys=["id"])

        # Update with new values
        df2 = pl.DataFrame({
            "id": ["a", "b"],
            "name": ["ALICE", "BOB"],
            "score": [150, 250],
        })
        rows = table.merge(df2, merge_keys=["id"])

        result = table.read()
        assert len(result) == 2
        # Names should be updated
        assert set(result["name"].to_list()) == {"ALICE", "BOB"}

    def test_merge_upsert(self):
        """Merge should handle mix of inserts and updates."""
        table = InMemoryDeltaTable("test://entities")

        # Initial data
        df1 = pl.DataFrame({"id": ["a", "b"], "value": [1, 2]})
        table.merge(df1, merge_keys=["id"])

        # Upsert: update "a", insert "c"
        df2 = pl.DataFrame({"id": ["a", "c"], "value": [10, 30]})
        table.merge(df2, merge_keys=["id"])

        result = table.read().sort("id")
        assert len(result) == 3
        assert result["id"].to_list() == ["a", "b", "c"]
        assert result["value"].to_list() == [10, 2, 30]

    def test_merge_preserve_columns(self):
        """Merge should preserve specified columns on update."""
        table = InMemoryDeltaTable("test://entities")

        # Initial with created_at
        df1 = pl.DataFrame({
            "id": ["a"],
            "name": ["Alice"],
            "created_at": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
        })
        table.merge(df1, merge_keys=["id"])

        # Update - created_at should be preserved
        df2 = pl.DataFrame({
            "id": ["a"],
            "name": ["ALICE"],
            "created_at": [datetime(2024, 6, 1, tzinfo=timezone.utc)],
        })
        table.merge(df2, merge_keys=["id"], preserve_columns=["created_at"])

        result = table.read()
        # created_at should still be the original value
        assert result["created_at"][0] == datetime(2024, 1, 1, tzinfo=timezone.utc)
        # name should be updated
        assert result["name"][0] == "ALICE"

    def test_merge_dedupe_within_batch(self):
        """Merge should dedupe within batch by merge keys."""
        table = InMemoryDeltaTable("test://entities")

        # Batch with duplicate keys - last should win
        df = pl.DataFrame({
            "id": ["a", "a", "a"],
            "value": [1, 2, 3],
        })

        table.merge(df, merge_keys=["id"])

        result = table.read()
        assert len(result) == 1
        assert result["value"][0] == 3  # Last value

    def test_clear(self):
        """Clear should reset table to empty state."""
        table = InMemoryDeltaTable("test://events")

        df = pl.DataFrame({"id": [1, 2, 3]})
        table.append(df)
        assert len(table) == 3

        table.clear()

        assert len(table) == 0
        assert not table.exists()
        assert table.get_write_count() == 0
        assert table.get_schema() == {}

    def test_write_history(self):
        """Write history should track all operations."""
        table = InMemoryDeltaTable("test://events")

        df1 = pl.DataFrame({"id": [1, 2]})
        df2 = pl.DataFrame({"id": [3, 4, 5]})

        table.append(df1, batch_id="batch1")
        table.append(df2, batch_id="batch2")

        history = table.get_write_history()
        assert len(history) == 2
        assert history[0].operation == "append"
        assert history[0].rows_affected == 2
        assert history[0].batch_id == "batch1"
        assert history[1].rows_affected == 3

    def test_get_unique_values(self):
        """get_unique_values should return distinct values for column."""
        table = InMemoryDeltaTable("test://events")

        df = pl.DataFrame({
            "status": ["pending", "completed", "pending", "failed", "completed"],
        })
        table.append(df)

        unique = table.get_unique_values("status")
        assert set(unique) == {"pending", "completed", "failed"}

    def test_partition_column_tracking(self):
        """Partition column should be tracked."""
        table = InMemoryDeltaTable(
            "test://events",
            partition_column="event_date",
        )

        df = pl.DataFrame({
            "id": [1, 2, 3],
            "event_date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 1)],
        })
        table.append(df)

        partitions = table.get_partition_values()
        assert set(partitions) == {date(2024, 1, 1), date(2024, 1, 2)}

    def test_to_dicts(self):
        """to_dicts should return list of dicts."""
        table = InMemoryDeltaTable("test://events")

        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        table.append(df)

        dicts = table.to_dicts()
        assert len(dicts) == 2
        assert dicts[0] == {"id": 1, "name": "a"}
        assert dicts[1] == {"id": 2, "name": "b"}


class TestInMemoryDeltaTableWriter:
    """Tests for InMemoryDeltaTableWriter (DeltaTableWriter interface)."""

    def test_write_rows(self):
        """write_rows should convert dicts to DataFrame and append."""
        writer = InMemoryDeltaTableWriter("test://events")

        rows = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        written = writer.write_rows(rows)

        assert written == 2
        assert len(writer) == 2

    def test_write_rows_empty(self):
        """write_rows with empty list should return 0."""
        writer = InMemoryDeltaTableWriter("test://events")
        assert writer.write_rows([]) == 0

    def test_merge_batched(self):
        """merge_batched should perform merge (batching not needed in memory)."""
        writer = InMemoryDeltaTableWriter("test://entities")

        df = pl.DataFrame({"id": ["a", "b"], "value": [1, 2]})
        rows = writer.merge_batched(df, merge_keys=["id"])

        assert rows == 2
        assert len(writer) == 2

    def test_upsert_with_idempotency(self):
        """upsert_with_idempotency should dedupe by event_id."""
        writer = InMemoryDeltaTableWriter("test://events")

        df = pl.DataFrame({
            "_event_id": ["e1", "e1", "e2"],
            "value": [1, 2, 3],
        })

        rows = writer.upsert_with_idempotency(df, merge_key="_event_id")

        assert rows == 2  # e1 deduped, only last value kept
        result = writer.read()
        assert len(result) == 2


class TestInMemoryDeltaTableReader:
    """Tests for InMemoryDeltaTableReader."""

    def test_read_from_table(self):
        """Reader should read from underlying table."""
        table = InMemoryDeltaTable("test://events")
        df = pl.DataFrame({"id": [1, 2, 3]})
        table.append(df)

        reader = InMemoryDeltaTableReader(table)
        result = reader.read()

        assert len(result) == 3

    def test_read_filtered(self):
        """Reader should support filtered reads."""
        table = InMemoryDeltaTable("test://events")
        df = pl.DataFrame({"id": [1, 2, 3, 4, 5]})
        table.append(df)

        reader = InMemoryDeltaTableReader(table)
        result = reader.read_filtered(pl.col("id") > 3)

        assert len(result) == 2

    def test_exists(self):
        """Reader should report table existence."""
        table = InMemoryDeltaTable("test://events")
        reader = InMemoryDeltaTableReader(table)

        assert not reader.exists()

        table.append(pl.DataFrame({"id": [1]}))

        assert reader.exists()


class TestInMemoryDeltaRegistry:
    """Tests for InMemoryDeltaRegistry."""

    def test_get_table_creates_new(self):
        """get_table should create table if doesn't exist."""
        registry = InMemoryDeltaRegistry()

        table = registry.get_table("events")

        assert table is not None
        assert "events" in registry.list_tables()

    def test_get_table_returns_existing(self):
        """get_table should return same table for same name."""
        registry = InMemoryDeltaRegistry()

        table1 = registry.get_table("events")
        table1.append(pl.DataFrame({"id": [1]}))

        table2 = registry.get_table("events")

        assert table1 is table2
        assert len(table2) == 1

    def test_get_writer(self):
        """get_writer should return writer backed by registry table."""
        registry = InMemoryDeltaRegistry()

        writer = registry.get_writer("events")
        writer.append(pl.DataFrame({"id": [1, 2]}))

        # Data should be visible via get_table
        table = registry.get_table("events")
        assert len(table) == 2

    def test_clear_table(self):
        """clear_table should reset specific table."""
        registry = InMemoryDeltaRegistry()

        registry.get_table("events").append(pl.DataFrame({"id": [1]}))
        registry.get_table("inventory").append(pl.DataFrame({"id": [2]}))

        registry.clear_table("events")

        assert len(registry.get_table("events")) == 0
        assert len(registry.get_table("inventory")) == 1

    def test_clear_all(self):
        """clear_all should reset all tables."""
        registry = InMemoryDeltaRegistry()

        registry.get_table("events").append(pl.DataFrame({"id": [1]}))
        registry.get_table("inventory").append(pl.DataFrame({"id": [2]}))

        registry.clear_all()

        assert len(registry.get_table("events")) == 0
        assert len(registry.get_table("inventory")) == 0

    def test_reset(self):
        """reset should remove all tables."""
        registry = InMemoryDeltaRegistry()

        registry.get_table("events")
        registry.get_table("inventory")
        assert len(registry.list_tables()) == 2

        registry.reset()

        assert len(registry.list_tables()) == 0

    def test_get_stats(self):
        """get_stats should return stats for all tables."""
        registry = InMemoryDeltaRegistry()

        registry.get_table("events").append(pl.DataFrame({"id": [1, 2, 3]}))
        registry.get_table("inventory").append(pl.DataFrame({"x": [1]}))

        stats = registry.get_stats()

        assert "events" in stats
        assert stats["events"]["row_count"] == 3
        assert stats["inventory"]["row_count"] == 1


class TestTypeCasting:
    """Tests for type casting and timezone handling."""

    def test_null_columns_cast_to_string(self):
        """Null-typed columns should be cast to string."""
        table = InMemoryDeltaTable("test://events")

        # DataFrame with null column
        df = pl.DataFrame({
            "id": [1, 2],
            "optional": [None, None],
        })

        table.append(df)

        # Column should be Utf8 (string), not Null
        schema = table.get_schema()
        assert schema["optional"] == pl.Utf8

    def test_datetime_timezone_handling(self):
        """Datetime timezone differences should be handled."""
        table = InMemoryDeltaTable("test://events")

        # First append with UTC
        df1 = pl.DataFrame({
            "id": [1],
            "ts": [datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)],
        })
        table.append(df1)

        # Second append should work even with different representation
        df2 = pl.DataFrame({
            "id": [2],
            "ts": [datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)],
        })
        table.append(df2)

        result = table.read()
        assert len(result) == 2


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_read_empty_table(self):
        """Reading empty table should return empty DataFrame."""
        table = InMemoryDeltaTable("test://events")

        result = table.read()
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0

    def test_read_filtered_empty_table(self):
        """Filtered read on empty table should return empty DataFrame."""
        table = InMemoryDeltaTable("test://events")

        result = table.read_filtered(pl.col("id") > 0)
        assert len(result) == 0

    def test_scan_empty_table(self):
        """Scan on empty table should return empty LazyFrame."""
        table = InMemoryDeltaTable("test://events")

        lf = table.scan()
        result = lf.collect()
        assert len(result) == 0

    def test_repr(self):
        """__repr__ should return informative string."""
        table = InMemoryDeltaTable("test://events")
        table.append(pl.DataFrame({"id": [1, 2, 3]}))

        repr_str = repr(table)
        assert "test://events" in repr_str
        assert "rows=3" in repr_str
