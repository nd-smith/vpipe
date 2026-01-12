"""
iTel Cabinet API Worker entry point.

This module provides the entry point for running the iTel Cabinet API Worker
as a standalone module via: python -m kafka_pipeline.workers.itel_cabinet_api_worker

The actual implementation is in kafka_pipeline.plugins.itel_cabinet_api.
"""

import asyncio

from kafka_pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker import main

if __name__ == "__main__":
    asyncio.run(main())
