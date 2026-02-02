"""
Xact workers package.

Contains worker implementations for the verisk event processing pipeline:
- Event ingester: Consumes verisk events and produces download tasks
- Download worker: Downloads attachments from xact to cache
- Upload worker: Uploads attachments from cache to OneLake
- Result processor: Processes upload results and writes to Delta tables

Import classes directly from submodules to avoid loading heavy dependencies:
    from pipeline.verisk.workers.event_ingester import EventIngesterWorker
    from pipeline.verisk.workers.result_processor import ResultProcessor
    from pipeline.verisk.workers.upload_worker import UploadWorker
"""

# Do not import workers at package level - they have heavy dependencies (aiokafka)
# Import directly from submodules when needed

__all__: list[str] = []
