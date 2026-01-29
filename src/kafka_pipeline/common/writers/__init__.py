"""Delta table writers and base classes."""

from kafka_pipeline.common.writers.base import BaseDeltaWriter
from kafka_pipeline.common.writers.delta_writer import DeltaWriter

__all__ = ["BaseDeltaWriter", "DeltaWriter"]
