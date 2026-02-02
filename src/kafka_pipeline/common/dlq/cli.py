"""
DLQ CLI compatibility module.

Re-exports DLQ CLI classes from verisk module for backward compatibility.
"""

from kafka_pipeline.verisk.dlq.cli import CLITaskManager, DLQCLIManager

__all__ = ["CLITaskManager", "DLQCLIManager"]
