"""
DLQ handler compatibility module.

Re-exports DLQHandler from verisk module for backward compatibility.
"""

from kafka_pipeline.verisk.dlq.handler import DLQHandler

__all__ = ["DLQHandler"]
