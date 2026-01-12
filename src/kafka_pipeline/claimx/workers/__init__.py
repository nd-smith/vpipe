"""ClaimX worker processes.

This module contains worker implementations for:
- Event ingestion (ClaimXEventIngesterWorker)
- Entity enrichment (ClaimXEnrichmentWorker)
- Attachment download (ClaimXDownloadWorker)
- OneLake upload (ClaimXUploadWorker)
- Result processing (ClaimXResultProcessor)

Import workers directly from submodules to avoid loading all dependencies:
    from kafka_pipeline.claimx.workers.event_ingester import ClaimXEventIngesterWorker
    from kafka_pipeline.claimx.workers.enrichment_worker import ClaimXEnrichmentWorker
    from kafka_pipeline.claimx.workers.download_worker import ClaimXDownloadWorker
    from kafka_pipeline.claimx.workers.upload_worker import ClaimXUploadWorker
    from kafka_pipeline.claimx.workers.result_processor import ClaimXResultProcessor
"""

# Don't import concrete implementations here to avoid loading
# heavy dependencies (aiokafka, aiohttp, polars, etc.) at package import time.
# Workers should be imported directly from their respective modules.

__all__: list[str] = []
