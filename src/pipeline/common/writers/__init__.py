"""Delta table writers and base classes."""

from pipeline.common.writers.base import BaseDeltaWriter
from pipeline.common.writers.delta_writer import DeltaWriter

__all__ = ["BaseDeltaWriter", "DeltaWriter"]
