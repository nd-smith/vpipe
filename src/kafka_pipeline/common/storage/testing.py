"""
Testing utilities for in-memory Delta table substitution.

Provides helpers for swapping production Delta writers with in-memory
implementations in tests, enabling full E2E testing without external dependencies.

Usage:
    # Context manager approach
    with patch_delta_writers(registry) as storage:
        # All DeltaTableWriter instantiations now use in-memory storage
        await run_pipeline()

        # Verify results
        events = storage["xact_events"].read()
        assert len(events) == expected_count

    # Fixture approach (in conftest.py)
    @pytest.fixture
    def patched_delta_writers(inmemory_delta_registry, monkeypatch):
        return setup_inmemory_writers(inmemory_delta_registry, monkeypatch)
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING
from unittest.mock import patch

from kafka_pipeline.common.storage.inmemory_delta import (
    InMemoryDeltaTable,
    InMemoryDeltaTableWriter,
    InMemoryDeltaRegistry,
)

if TYPE_CHECKING:
    import polars as pl


def create_writer_factory(
    registry: InMemoryDeltaRegistry,
    table_mapping: Optional[Dict[str, str]] = None,
) -> Callable[..., InMemoryDeltaTableWriter]:
    """
    Create a factory function that returns in-memory writers.

    The factory intercepts DeltaTableWriter instantiation and returns
    an InMemoryDeltaTableWriter backed by the registry.

    Args:
        registry: InMemoryDeltaRegistry to store data
        table_mapping: Optional mapping of path patterns to table names.
                      E.g., {"xact_events": "xact_events", "attachments": "xact_attachments"}

    Returns:
        Factory function with same signature as DeltaTableWriter.__init__

    Example:
        factory = create_writer_factory(registry)
        monkeypatch.setattr(
            "kafka_pipeline.common.storage.delta.DeltaTableWriter",
            factory
        )
    """
    # Default mapping based on common table path patterns
    default_mapping = {
        "xact_events": "xact_events",
        "xact_attachments": "xact_attachments",
        "claimx_events": "claimx_events",
        "claimx_entities": "claimx_entities",
        "claimx_contacts": "claimx_contacts",
        "inventory": "inventory",
        "retry": "retry_queue",
    }
    mapping = table_mapping or default_mapping

    def factory(
        table_path: str,
        timestamp_column: str = "ingested_at",
        partition_column: Optional[str] = None,
        z_order_columns: Optional[List[str]] = None,
    ) -> InMemoryDeltaTableWriter:
        """Factory that creates in-memory writers based on table path."""
        # Determine table name from path
        table_name = "default"
        path_lower = table_path.lower()
        for pattern, name in mapping.items():
            if pattern in path_lower:
                table_name = name
                break

        # Get or create the table in registry
        table = registry.get_table(
            name=table_name,
            timestamp_column=timestamp_column,
            partition_column=partition_column,
            z_order_columns=z_order_columns,
        )

        # Create a writer that shares storage with the registry table
        writer = InMemoryDeltaTableWriter(
            table_path=table_path,
            timestamp_column=timestamp_column,
            partition_column=partition_column,
            z_order_columns=z_order_columns,
        )

        # Share internal storage - this is the key part
        # The writer and registry table share the same DataFrame
        writer._data = table._data
        writer._schema = table._schema

        # Create bidirectional reference so writes update both
        original_append = writer.append

        def synced_append(df: "pl.DataFrame", batch_id: Optional[str] = None) -> int:
            result = original_append(df, batch_id)
            # Sync state back to registry table
            table._data = writer._data
            table._schema = writer._schema
            table._write_count = writer._write_count
            table._write_history = writer._write_history
            table._row_count = writer._row_count
            return result

        original_merge = writer.merge

        def synced_merge(
            df: "pl.DataFrame",
            merge_keys: List[str],
            preserve_columns: Optional[List[str]] = None,
            update_condition: Optional[str] = None,
        ) -> int:
            result = original_merge(df, merge_keys, preserve_columns, update_condition)
            # Sync state back to registry table
            table._data = writer._data
            table._schema = writer._schema
            table._write_count = writer._write_count
            table._write_history = writer._write_history
            table._row_count = writer._row_count
            return result

        writer.append = synced_append
        writer.merge = synced_merge

        return writer

    return factory


@contextmanager
def patch_delta_writers(
    registry: Optional[InMemoryDeltaRegistry] = None,
    table_mapping: Optional[Dict[str, str]] = None,
):
    """
    Context manager that patches DeltaTableWriter to use in-memory storage.

    All instantiations of DeltaTableWriter within the context will use
    in-memory storage backed by the provided registry.

    Args:
        registry: Optional registry (created if not provided)
        table_mapping: Optional path-to-name mapping

    Yields:
        Dict with "registry" and individual table references

    Example:
        with patch_delta_writers() as storage:
            # Run pipeline code that uses DeltaTableWriter
            writer = DeltaTableWriter("path/to/xact_events")
            writer.append(df)

            # Verify in-memory data
            events = storage["xact_events"].read()
            assert len(events) == expected
    """
    if registry is None:
        registry = InMemoryDeltaRegistry()

    factory = create_writer_factory(registry, table_mapping)

    with patch(
        "kafka_pipeline.common.storage.delta.DeltaTableWriter",
        factory,
    ):
        with patch(
            "kafka_pipeline.common.writers.base.DeltaTableWriter",
            factory,
        ):
            yield {
                "registry": registry,
                "xact_events": registry.get_table("xact_events"),
                "xact_attachments": registry.get_table("xact_attachments"),
                "claimx_events": registry.get_table("claimx_events"),
                "claimx_entities": registry.get_table("claimx_entities"),
            }


def setup_inmemory_writers(
    registry: InMemoryDeltaRegistry,
    monkeypatch: Any,
    table_mapping: Optional[Dict[str, str]] = None,
) -> Dict[str, InMemoryDeltaTable]:
    """
    Set up in-memory writers using pytest monkeypatch.

    Alternative to context manager for pytest fixtures.

    Args:
        registry: Registry to store data
        monkeypatch: pytest monkeypatch fixture
        table_mapping: Optional path-to-name mapping

    Returns:
        Dict of table name to InMemoryDeltaTable

    Example:
        @pytest.fixture
        def inmemory_storage(inmemory_delta_registry, monkeypatch):
            return setup_inmemory_writers(inmemory_delta_registry, monkeypatch)

        def test_pipeline(inmemory_storage):
            # Run pipeline
            events = inmemory_storage["xact_events"].read()
    """
    factory = create_writer_factory(registry, table_mapping)

    # Patch both locations where DeltaTableWriter might be imported from
    monkeypatch.setattr(
        "kafka_pipeline.common.storage.delta.DeltaTableWriter",
        factory,
    )
    monkeypatch.setattr(
        "kafka_pipeline.common.writers.base.DeltaTableWriter",
        factory,
    )

    return {
        "registry": registry,
        "xact_events": registry.get_table("xact_events"),
        "xact_attachments": registry.get_table("xact_attachments"),
        "claimx_events": registry.get_table("claimx_events"),
        "claimx_entities": registry.get_table("claimx_entities"),
    }


__all__ = [
    "create_writer_factory",
    "patch_delta_writers",
    "setup_inmemory_writers",
]
