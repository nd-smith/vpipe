# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Xact workers package.

Contains worker implementations for the xact event processing pipeline:
- Event ingester: Consumes xact events and produces download tasks
- Download worker: Downloads attachments from xact to cache
- Upload worker: Uploads attachments from cache to OneLake
- Result processor: Processes upload results and writes to Delta tables

Import classes directly from submodules to avoid loading heavy dependencies:
    from kafka_pipeline.xact.workers.event_ingester import EventIngesterWorker
    from kafka_pipeline.xact.workers.result_processor import ResultProcessor
    from kafka_pipeline.xact.workers.upload_worker import UploadWorker
"""

# Do not import workers at package level - they have heavy dependencies (aiokafka)
# Import directly from submodules when needed

__all__: list[str] = []
