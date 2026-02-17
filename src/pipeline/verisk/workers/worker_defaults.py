"""Verisk-specific worker defaults (extends pipeline.common.worker_defaults)."""

# Verisk uses a larger poll batch for delta event workers
MAX_POLL_RECORDS = 5000
