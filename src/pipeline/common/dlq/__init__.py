"""
DLQ compatibility module.

Re-exports DLQ classes from domain-specific modules for backward compatibility.
"""

from pipeline.verisk.dlq.handler import DLQHandler

__all__ = ["DLQHandler"]
