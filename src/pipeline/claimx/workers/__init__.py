"""ClaimX worker processes.

This module contains worker implementations for:
- Event ingestion (ClaimXEventIngesterWorker)
- Entity enrichment (ClaimXEnrichmentWorker)
- Attachment download (ClaimXDownloadWorker)
- OneLake upload (ClaimXUploadWorker)
- Result processing (ClaimXResultProcessor)

Import workers directly from submodules to avoid loading all dependencies:
    from pipeline.claimx.workers.event_ingester import ClaimXEventIngesterWorker
    from pipeline.claimx.workers.enrichment_worker import ClaimXEnrichmentWorker
    from pipeline.claimx.workers.download_worker import ClaimXDownloadWorker
    from pipeline.claimx.workers.upload_worker import ClaimXUploadWorker
    from pipeline.claimx.workers.result_processor import ClaimXResultProcessor
"""

# Don't import concrete implementations here to avoid loading
# heavy dependencies (aiokafka, aiohttp, polars, etc.) at package import time.
# Workers should be imported directly from their respective modules.

__all__: list[str] = []
