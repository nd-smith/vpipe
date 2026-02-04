"""
Pipeline: Event-driven data processing.

This package implements event-driven architecture for real-time data processing,
replacing the legacy polling-based Delta/Kusto pipeline. Uses Azure Event Hub
for reliable event streaming and message processing.

Subpackages:
    claimx   - ClaimX-specific workers, handlers, and schemas
    verisk   - Verisk-specific workers and schemas
    common   - Shared infrastructure (consumers, producers, retry, DLQ, storage)
    plugins  - Plugin system for extensible processing
    runners  - Worker orchestration and lifecycle management

Architecture:
    events.raw → EventIngester → downloads.pending → DownloadWorker → downloads.results → ResultProcessor → Delta
                                        ↓ (on failure)
                                 downloads.retry.* → (delayed) → DownloadWorker
                                        ↓ (exhausted)
                                 downloads.dlq → DLQHandler

Dependencies:
    - core.*: Reusable components (auth, resilience, logging, etc.)
    - azure-eventhub: Event streaming client
    - pydantic: Message schema validation
"""

from config.config import KafkaConfig

__version__ = "0.1.0"
__all__ = ["KafkaConfig"]
