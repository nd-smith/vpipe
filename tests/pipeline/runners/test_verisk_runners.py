"""Tests for verisk domain worker runners.

Verifies that each runner function:
- Instantiates the correct worker/poller class with expected arguments
- Delegates to the correct execute_* helper from common
- Passes through shutdown_event, instance_id, and domain correctly

Note: Runner functions use local imports for worker classes, so we patch
at the source module (e.g., pipeline.verisk.workers.event_ingester.EventIngesterWorker).
The execute_* helpers are imported at module level and patched on the runners module.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest

from pipeline.runners.verisk_runners import (
    run_delta_events_worker,
    run_download_worker,
    run_dummy_source,
    run_event_ingester,
    run_result_processor,
    run_upload_worker,
    run_xact_enrichment_worker,
    run_xact_retry_scheduler,
)

# ---------------------------------------------------------------------------
# run_event_ingester
# ---------------------------------------------------------------------------


class TestRunEventIngester:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        eh_config = Mock()
        kafka_config = Mock()

        with (
            patch("pipeline.verisk.workers.event_ingester.EventIngesterWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_event_ingester(
                eh_config, kafka_config, shutdown, domain="verisk", instance_id=None
            )

        MockWorker.assert_called_once_with(
            config=eh_config,
            domain="verisk",
            producer_config=kafka_config,
            instance_id=None,
        )
        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="xact-event-ingester",
            shutdown_event=shutdown,
            instance_id=None,
        )

    async def test_passes_instance_id(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.verisk.workers.event_ingester.EventIngesterWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_event_ingester(Mock(), Mock(), shutdown, domain="verisk", instance_id=7)

        assert MockWorker.call_args[1]["instance_id"] == 7
        assert mock_exec.call_args[1]["instance_id"] == 7

    async def test_passes_custom_domain(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.verisk.workers.event_ingester.EventIngesterWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_event_ingester(Mock(), Mock(), shutdown, domain="custom")

        assert MockWorker.call_args[1]["domain"] == "custom"


# ---------------------------------------------------------------------------
# run_delta_events_worker
# ---------------------------------------------------------------------------


class TestRunDeltaEventsWorker:
    async def test_delegates_to_execute_worker_with_producer(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.verisk.workers.delta_events_worker.DeltaEventsWorker") as MockWorker,
            patch("pipeline.common.producer.MessageProducer") as MockProducer,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_producer",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_delta_events_worker(kafka_config, "/delta/events", shutdown, instance_id=2)

        mock_exec.assert_awaited_once_with(
            worker_class=MockWorker,
            producer_class=MockProducer,
            kafka_config=kafka_config,
            domain="verisk",
            stage_name="xact-delta-writer",
            shutdown_event=shutdown,
            worker_kwargs={"events_table_path": "/delta/events"},
            producer_worker_name="delta_events_writer",
            instance_id=2,
        )

    async def test_passes_none_instance_id_by_default(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.verisk.workers.delta_events_worker.DeltaEventsWorker"),
            patch("pipeline.common.producer.MessageProducer"),
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_producer",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_delta_events_worker(Mock(), "/path", shutdown)

        assert mock_exec.call_args[1]["instance_id"] is None


# ---------------------------------------------------------------------------
# run_xact_retry_scheduler
# ---------------------------------------------------------------------------


class TestRunXactRetryScheduler:
    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.common.retry.unified_scheduler.UnifiedRetryScheduler") as MockScheduler,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_xact_retry_scheduler(kafka_config, shutdown, instance_id=1)

        MockScheduler.assert_called_once_with(
            config=kafka_config,
            domain="verisk",
            target_topic_keys=["downloads_pending", "enrichment_pending", "downloads_results"],
            persistence_dir=kafka_config.retry_persistence_dir,
        )
        mock_exec.assert_awaited_once_with(
            MockScheduler.return_value,
            stage_name="xact-retry-scheduler",
            shutdown_event=shutdown,
            instance_id=1,
        )


# ---------------------------------------------------------------------------
# run_xact_enrichment_worker
# ---------------------------------------------------------------------------


class TestRunXactEnrichmentWorker:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.verisk.workers.enrichment_worker.XACTEnrichmentWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_xact_enrichment_worker(kafka_config, shutdown, instance_id=3)

        MockWorker.assert_called_once_with(config=kafka_config, domain="verisk", instance_id=3)
        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="xact-enricher",
            shutdown_event=shutdown,
            instance_id=3,
        )

    async def test_defaults_instance_id_to_none(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.verisk.workers.enrichment_worker.XACTEnrichmentWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_xact_enrichment_worker(Mock(), shutdown)

        assert MockWorker.call_args[1]["instance_id"] is None


# ---------------------------------------------------------------------------
# run_download_worker
# ---------------------------------------------------------------------------


class TestRunDownloadWorker:
    async def test_creates_worker_with_temp_dir_as_path(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()
        kafka_config.temp_dir = "/tmp/downloads"

        with (
            patch("pipeline.verisk.workers.download_worker.DownloadWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_download_worker(kafka_config, shutdown, instance_id=5)

        MockWorker.assert_called_once_with(
            config=kafka_config,
            domain="verisk",
            temp_dir=Path("/tmp/downloads"),
            instance_id=5,
        )

    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()
        kafka_config.temp_dir = "/tmp"

        with (
            patch("pipeline.verisk.workers.download_worker.DownloadWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_download_worker(kafka_config, shutdown, instance_id=None)

        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="xact-download",
            shutdown_event=shutdown,
            instance_id=None,
        )


# ---------------------------------------------------------------------------
# run_upload_worker
# ---------------------------------------------------------------------------


class TestRunUploadWorker:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.verisk.workers.upload_worker.UploadWorker") as MockWorker,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_upload_worker(kafka_config, shutdown, instance_id=2)

        MockWorker.assert_called_once_with(config=kafka_config, domain="verisk", instance_id=2)

    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.verisk.workers.upload_worker.UploadWorker"),
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_upload_worker(kafka_config, shutdown)

        mock_exec.assert_awaited_once()
        assert mock_exec.call_args[1]["stage_name"] == "xact-upload"


# ---------------------------------------------------------------------------
# run_result_processor
# ---------------------------------------------------------------------------


class TestRunResultProcessor:
    async def test_raises_when_delta_enabled_without_inventory_path(self):
        shutdown = asyncio.Event()
        kafka_config = Mock()

        with (
            patch("core.logging.context.set_log_context"),
            pytest.raises(
                ValueError,
                match="inventory_table_path is required when delta writes are enabled",
            ),
        ):
            await run_result_processor(
                kafka_config,
                shutdown,
                enable_delta_writes=True,
                inventory_table_path="",
            )

    async def test_does_not_raise_when_delta_disabled_without_inventory_path(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("core.logging.context.set_log_context"),
            patch("pipeline.verisk.workers.result_processor.ResultProcessor"),
            patch("pipeline.common.producer.MessageProducer"),
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_producer",
                new_callable=AsyncMock,
            ),
        ):
            # Should not raise
            await run_result_processor(
                kafka_config,
                shutdown,
                enable_delta_writes=False,
                inventory_table_path="",
            )

    async def test_delegates_with_delta_paths_when_enabled(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("core.logging.context.set_log_context"),
            patch("pipeline.verisk.workers.result_processor.ResultProcessor") as MockRP,
            patch("pipeline.common.producer.MessageProducer") as MockProd,
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_producer",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_result_processor(
                kafka_config,
                shutdown,
                enable_delta_writes=True,
                inventory_table_path="/delta/inventory",
                failed_table_path="/delta/failed",
                instance_id=4,
            )

        mock_exec.assert_awaited_once()
        kwargs = mock_exec.call_args[1]
        assert kwargs["worker_class"] == MockRP
        assert kwargs["producer_class"] == MockProd
        assert kwargs["domain"] == "verisk"
        assert kwargs["stage_name"] == "xact-result-processor"
        assert kwargs["instance_id"] == 4
        assert kwargs["producer_worker_name"] == "result_processor"

        worker_kwargs = kwargs["worker_kwargs"]
        assert worker_kwargs["inventory_table_path"] == "/delta/inventory"
        assert worker_kwargs["failed_table_path"] == "/delta/failed"
        assert worker_kwargs["batch_size"] == 2000
        assert worker_kwargs["batch_timeout_seconds"] == 5.0

    async def test_nullifies_paths_when_delta_disabled(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("core.logging.context.set_log_context"),
            patch("pipeline.verisk.workers.result_processor.ResultProcessor"),
            patch("pipeline.common.producer.MessageProducer"),
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_producer",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_result_processor(
                kafka_config,
                shutdown,
                enable_delta_writes=False,
                inventory_table_path="/delta/inventory",
                failed_table_path="/delta/failed",
            )

        worker_kwargs = mock_exec.call_args[1]["worker_kwargs"]
        assert worker_kwargs["inventory_table_path"] is None
        assert worker_kwargs["failed_table_path"] is None

    async def test_nullifies_failed_path_when_empty_string(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("core.logging.context.set_log_context"),
            patch("pipeline.verisk.workers.result_processor.ResultProcessor"),
            patch("pipeline.common.producer.MessageProducer"),
            patch(
                "pipeline.runners.verisk_runners.execute_worker_with_producer",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_result_processor(
                kafka_config,
                shutdown,
                enable_delta_writes=True,
                inventory_table_path="/delta/inventory",
                failed_table_path="",
            )

        worker_kwargs = mock_exec.call_args[1]["worker_kwargs"]
        assert worker_kwargs["inventory_table_path"] == "/delta/inventory"
        assert worker_kwargs["failed_table_path"] is None


# ---------------------------------------------------------------------------
# run_dummy_source
# ---------------------------------------------------------------------------


class TestRunDummySource:
    async def test_loads_config_and_starts_source(self):
        shutdown = asyncio.Event()
        kafka_config = Mock()
        dummy_config = {"rate": 10}

        mock_source = AsyncMock()
        mock_source.start = AsyncMock()
        mock_source.stop = AsyncMock()
        mock_source.run = AsyncMock()
        mock_source.stats = {"sent": 100}

        with (
            patch("core.logging.context.set_log_context"),
            patch("pipeline.common.dummy.source.load_dummy_source_config") as mock_load,
            patch(
                "pipeline.common.dummy.source.DummyDataSource",
                return_value=mock_source,
            ) as MockSource,
        ):
            await run_dummy_source(kafka_config, dummy_config, shutdown)

        mock_load.assert_called_once_with(kafka_config, dummy_config)
        MockSource.assert_called_once_with(mock_load.return_value)
        mock_source.start.assert_awaited_once()
        mock_source.run.assert_awaited_once()
        mock_source.stop.assert_awaited()

    async def test_stops_source_on_run_exception(self):
        shutdown = asyncio.Event()
        mock_source = AsyncMock()
        mock_source.start = AsyncMock()
        mock_source.run = AsyncMock(side_effect=RuntimeError("boom"))
        mock_source.stop = AsyncMock()
        mock_source.stats = {}

        with (
            patch("core.logging.context.set_log_context"),
            patch("pipeline.common.dummy.source.load_dummy_source_config"),
            patch(
                "pipeline.common.dummy.source.DummyDataSource",
                return_value=mock_source,
            ),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await run_dummy_source(Mock(), {}, shutdown)

        mock_source.stop.assert_awaited()

    async def test_shutdown_event_triggers_stop(self):
        shutdown = asyncio.Event()
        mock_source = AsyncMock()
        mock_source.start = AsyncMock()
        mock_source.stop = AsyncMock()
        mock_source.stats = {}

        async def slow_run():
            await asyncio.sleep(0.1)

        mock_source.run = AsyncMock(side_effect=slow_run)

        async def trigger_shutdown():
            await asyncio.sleep(0.02)
            shutdown.set()

        with (
            patch("core.logging.context.set_log_context"),
            patch("pipeline.common.dummy.source.load_dummy_source_config"),
            patch(
                "pipeline.common.dummy.source.DummyDataSource",
                return_value=mock_source,
            ),
        ):
            await asyncio.gather(
                run_dummy_source(Mock(), {}, shutdown),
                trigger_shutdown(),
            )

        mock_source.stop.assert_awaited()
