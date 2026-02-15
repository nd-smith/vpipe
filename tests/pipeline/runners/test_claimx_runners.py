"""Tests for claimx domain worker runners.

Verifies that each runner function:
- Instantiates the correct worker/poller class with expected arguments
- Delegates to the correct execute_* helper from common
- Passes through shutdown_event, instance_id, and domain correctly

Note: Runner functions use local imports for worker classes, so we patch
at the source module (e.g., pipeline.claimx.workers.event_ingester.ClaimXEventIngesterWorker).
The execute_* helpers are imported at module level and patched on the runners module.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest

from pipeline.runners.claimx_runners import (
    run_claimx_delta_events_worker,
    run_claimx_download_worker,
    run_claimx_enrichment_worker,
    run_claimx_entity_delta_worker,
    run_claimx_event_ingester,
    run_claimx_result_processor,
    run_claimx_retry_scheduler,
    run_claimx_upload_worker,
)

# ---------------------------------------------------------------------------
# run_claimx_event_ingester
# ---------------------------------------------------------------------------


class TestRunClaimxEventIngester:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.claimx.workers.event_ingester.ClaimXEventIngesterWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_event_ingester(kafka_config, shutdown, instance_id=3)

        MockWorker.assert_called_once_with(config=kafka_config, domain="claimx", instance_id=3)
        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="claimx-ingester",
            shutdown_event=shutdown,
            instance_id=3,
        )

    async def test_defaults_instance_id_to_none(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.claimx.workers.event_ingester.ClaimXEventIngesterWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_claimx_event_ingester(Mock(), shutdown)

        assert MockWorker.call_args[1]["instance_id"] is None


# ---------------------------------------------------------------------------
# run_claimx_enrichment_worker
# ---------------------------------------------------------------------------


class TestRunClaimxEnrichmentWorker:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.claimx.workers.enrichment_worker.ClaimXEnrichmentWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_enrichment_worker(
                kafka_config,
                shutdown,
                enable_delta_writes=True,
                claimx_projects_table_path="/delta/projects",
                instance_id=2,
            )

        MockWorker.assert_called_once_with(
            config=kafka_config,
            domain="claimx",
            enable_delta_writes=True,
            projects_table_path="/delta/projects",
            instance_id=2,
        )
        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="claimx-enricher",
            shutdown_event=shutdown,
            instance_id=2,
        )

    async def test_passes_delta_disabled(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.claimx.workers.enrichment_worker.ClaimXEnrichmentWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_claimx_enrichment_worker(
                Mock(), shutdown, enable_delta_writes=False
            )

        assert MockWorker.call_args[1]["enable_delta_writes"] is False


# ---------------------------------------------------------------------------
# run_claimx_download_worker
# ---------------------------------------------------------------------------


class TestRunClaimxDownloadWorker:
    async def test_creates_worker_with_temp_dir_as_path(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()
        kafka_config.temp_dir = "/tmp/claimx-dl"

        with (
            patch("pipeline.claimx.workers.download_worker.ClaimXDownloadWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_claimx_download_worker(kafka_config, shutdown, instance_id=1)

        MockWorker.assert_called_once_with(
            config=kafka_config,
            domain="claimx",
            temp_dir=Path("/tmp/claimx-dl"),
            instance_id=1,
        )

    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()
        kafka_config.temp_dir = "/tmp"

        with (
            patch("pipeline.claimx.workers.download_worker.ClaimXDownloadWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_download_worker(kafka_config, shutdown)

        mock_exec.assert_awaited_once_with(
            MockWorker.return_value,
            stage_name="claimx-downloader",
            shutdown_event=shutdown,
            instance_id=None,
        )


# ---------------------------------------------------------------------------
# run_claimx_upload_worker
# ---------------------------------------------------------------------------


class TestRunClaimxUploadWorker:
    async def test_creates_worker_with_config(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.claimx.workers.upload_worker.ClaimXUploadWorker") as MockWorker,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ),
        ):
            await run_claimx_upload_worker(kafka_config, shutdown, instance_id=4)

        MockWorker.assert_called_once_with(config=kafka_config, domain="claimx", instance_id=4)

    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.claimx.workers.upload_worker.ClaimXUploadWorker"),
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_upload_worker(Mock(), shutdown)

        assert mock_exec.call_args[1]["stage_name"] == "claimx-uploader"


# ---------------------------------------------------------------------------
# run_claimx_result_processor
# ---------------------------------------------------------------------------


class TestRunClaimxResultProcessor:
    async def test_creates_processor_and_starts_it(self):
        shutdown = asyncio.Event()
        kafka_config = Mock()
        pipeline_config = Mock()
        pipeline_config.claimx_inventory_table_path = "/delta/inventory"

        mock_processor = AsyncMock()
        mock_processor.start = AsyncMock()
        mock_processor.stop = AsyncMock()

        with (
            patch("core.logging.context.set_log_context"),
            patch(
                "pipeline.claimx.workers.result_processor.ClaimXResultProcessor",
                return_value=mock_processor,
            ) as MockRP,
        ):
            await run_claimx_result_processor(
                kafka_config, pipeline_config, shutdown, instance_id=2
            )

        MockRP.assert_called_once_with(
            config=kafka_config,
            inventory_table_path="/delta/inventory",
            instance_id=2,
        )
        mock_processor.start.assert_awaited_once()
        mock_processor.stop.assert_awaited()

    async def test_stops_processor_on_start_exception(self):
        shutdown = asyncio.Event()
        pipeline_config = Mock()
        pipeline_config.claimx_inventory_table_path = "/delta/inventory"

        mock_processor = AsyncMock(spec=["start", "stop"])
        mock_processor.start = AsyncMock(side_effect=RuntimeError("start failed"))
        mock_processor.stop = AsyncMock()

        with (
            patch("core.logging.context.set_log_context"),
            patch(
                "pipeline.claimx.workers.result_processor.ClaimXResultProcessor",
                return_value=mock_processor,
            ),
            patch("pipeline.runners.common._start_with_retry", side_effect=RuntimeError("start failed")),
            pytest.raises(RuntimeError, match="start failed"),
        ):
            await run_claimx_result_processor(Mock(), pipeline_config, shutdown)

        mock_processor.stop.assert_awaited()

    async def test_shutdown_event_triggers_processor_stop(self):
        shutdown = asyncio.Event()
        pipeline_config = Mock()
        pipeline_config.claimx_inventory_table_path = "/delta/inv"

        mock_processor = AsyncMock()
        mock_processor.stop = AsyncMock()

        async def slow_start():
            await asyncio.sleep(0.1)

        mock_processor.start = AsyncMock(side_effect=slow_start)

        async def trigger_shutdown():
            await asyncio.sleep(0.02)
            shutdown.set()

        with (
            patch("core.logging.context.set_log_context"),
            patch(
                "pipeline.claimx.workers.result_processor.ClaimXResultProcessor",
                return_value=mock_processor,
            ),
        ):
            await asyncio.gather(
                run_claimx_result_processor(Mock(), pipeline_config, shutdown),
                trigger_shutdown(),
            )

        mock_processor.stop.assert_awaited()

    async def test_sets_log_context(self):
        shutdown = asyncio.Event()
        pipeline_config = Mock()
        pipeline_config.claimx_inventory_table_path = "/delta/inv"

        mock_processor = AsyncMock()
        mock_processor.start = AsyncMock()
        mock_processor.stop = AsyncMock()

        with (
            patch("pipeline.runners.common.set_log_context") as mock_ctx,
            patch(
                "pipeline.claimx.workers.result_processor.ClaimXResultProcessor",
                return_value=mock_processor,
            ),
        ):
            await run_claimx_result_processor(Mock(), pipeline_config, shutdown)

        mock_ctx.assert_called_once_with(stage="claimx-result-processor")


# ---------------------------------------------------------------------------
# run_claimx_delta_events_worker
# ---------------------------------------------------------------------------


class TestRunClaimxDeltaEventsWorker:
    async def test_delegates_to_execute_worker_with_producer(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch(
                "pipeline.claimx.workers.delta_events_worker.ClaimXDeltaEventsWorker"
            ) as MockWorker,
            patch("pipeline.common.producer.MessageProducer") as MockProducer,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_producer",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_delta_events_worker(
                kafka_config, "/delta/claimx_events", shutdown, instance_id=5
            )

        mock_exec.assert_awaited_once_with(
            worker_class=MockWorker,
            producer_class=MockProducer,
            kafka_config=kafka_config,
            domain="claimx",
            stage_name="claimx-delta-writer",
            shutdown_event=shutdown,
            worker_kwargs={"events_table_path": "/delta/claimx_events"},
            producer_worker_name="delta_events_writer",
            instance_id=5,
        )

    async def test_defaults_instance_id_to_none(self):
        shutdown = asyncio.Event()
        shutdown.set()

        with (
            patch("pipeline.claimx.workers.delta_events_worker.ClaimXDeltaEventsWorker"),
            patch("pipeline.common.producer.MessageProducer"),
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_producer",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_delta_events_worker(Mock(), "/path", shutdown)

        assert mock_exec.call_args[1]["instance_id"] is None


# ---------------------------------------------------------------------------
# run_claimx_retry_scheduler
# ---------------------------------------------------------------------------


class TestRunClaimxRetryScheduler:
    async def test_delegates_to_execute_worker_with_shutdown(self):
        shutdown = asyncio.Event()
        shutdown.set()
        kafka_config = Mock()

        with (
            patch("pipeline.common.retry.unified_scheduler.UnifiedRetryScheduler") as MockScheduler,
            patch(
                "pipeline.runners.claimx_runners.execute_worker_with_shutdown",
                new_callable=AsyncMock,
            ) as mock_exec,
        ):
            await run_claimx_retry_scheduler(kafka_config, shutdown, instance_id=1)

        MockScheduler.assert_called_once_with(
            config=kafka_config,
            domain="claimx",
            target_topic_keys=["downloads_pending", "enrichment_pending", "downloads_results"],
            persistence_dir=kafka_config.retry_persistence_dir,
        )
        mock_exec.assert_awaited_once_with(
            MockScheduler.return_value,
            stage_name="claimx-retry-scheduler",
            shutdown_event=shutdown,
            instance_id=1,
        )


# ---------------------------------------------------------------------------
# run_claimx_entity_delta_worker
# ---------------------------------------------------------------------------


class TestRunClaimxEntityDeltaWorker:
    TABLE_PATHS = {
        "projects_table_path": "/delta/projects",
        "contacts_table_path": "/delta/contacts",
        "media_table_path": "/delta/media",
        "tasks_table_path": "/delta/tasks",
        "task_templates_table_path": "/delta/task_templates",
        "external_links_table_path": "/delta/external_links",
        "video_collab_table_path": "/delta/video_collab",
    }

    async def test_creates_worker_with_all_table_paths(self):
        shutdown = asyncio.Event()
        kafka_config = Mock()

        mock_worker = AsyncMock()
        mock_worker.start = AsyncMock()
        mock_worker.stop = AsyncMock()

        with (
            patch("core.logging.context.set_log_context"),
            patch(
                "pipeline.claimx.workers.entity_delta_worker.ClaimXEntityDeltaWorker",
                return_value=mock_worker,
            ) as MockWorker,
        ):
            await run_claimx_entity_delta_worker(
                kafka_config,
                shutdown_event=shutdown,
                instance_id=3,
                **self.TABLE_PATHS,
            )

        MockWorker.assert_called_once_with(
            config=kafka_config,
            domain="claimx",
            projects_table_path="/delta/projects",
            contacts_table_path="/delta/contacts",
            media_table_path="/delta/media",
            tasks_table_path="/delta/tasks",
            task_templates_table_path="/delta/task_templates",
            external_links_table_path="/delta/external_links",
            video_collab_table_path="/delta/video_collab",
            instance_id=3,
        )

    async def test_starts_worker(self):
        shutdown = asyncio.Event()
        mock_worker = AsyncMock()
        mock_worker.start = AsyncMock()
        mock_worker.stop = AsyncMock()

        with (
            patch("core.logging.context.set_log_context"),
            patch(
                "pipeline.claimx.workers.entity_delta_worker.ClaimXEntityDeltaWorker",
                return_value=mock_worker,
            ),
        ):
            await run_claimx_entity_delta_worker(
                Mock(), shutdown_event=shutdown, **self.TABLE_PATHS
            )

        mock_worker.start.assert_awaited_once()
        mock_worker.stop.assert_awaited()

    async def test_stops_worker_on_start_exception(self):
        shutdown = asyncio.Event()
        mock_worker = AsyncMock(spec=["start", "stop"])
        mock_worker.start = AsyncMock(side_effect=RuntimeError("boom"))
        mock_worker.stop = AsyncMock()

        with (
            patch("core.logging.context.set_log_context"),
            patch(
                "pipeline.claimx.workers.entity_delta_worker.ClaimXEntityDeltaWorker",
                return_value=mock_worker,
            ),
            patch("pipeline.runners.common._start_with_retry", side_effect=RuntimeError("boom")),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await run_claimx_entity_delta_worker(
                Mock(), shutdown_event=shutdown, **self.TABLE_PATHS
            )

        mock_worker.stop.assert_awaited()

    async def test_shutdown_event_triggers_worker_stop(self):
        shutdown = asyncio.Event()
        mock_worker = AsyncMock()
        mock_worker.stop = AsyncMock()

        async def slow_start():
            await asyncio.sleep(0.1)

        mock_worker.start = AsyncMock(side_effect=slow_start)

        async def trigger_shutdown():
            await asyncio.sleep(0.02)
            shutdown.set()

        with (
            patch("core.logging.context.set_log_context"),
            patch(
                "pipeline.claimx.workers.entity_delta_worker.ClaimXEntityDeltaWorker",
                return_value=mock_worker,
            ),
        ):
            await asyncio.gather(
                run_claimx_entity_delta_worker(Mock(), shutdown_event=shutdown, **self.TABLE_PATHS),
                trigger_shutdown(),
            )

        mock_worker.stop.assert_awaited()

    async def test_sets_log_context(self):
        shutdown = asyncio.Event()
        mock_worker = AsyncMock()
        mock_worker.start = AsyncMock()
        mock_worker.stop = AsyncMock()

        with (
            patch("pipeline.runners.common.set_log_context") as mock_ctx,
            patch(
                "pipeline.claimx.workers.entity_delta_worker.ClaimXEntityDeltaWorker",
                return_value=mock_worker,
            ),
        ):
            await run_claimx_entity_delta_worker(
                Mock(), shutdown_event=shutdown, **self.TABLE_PATHS
            )

        mock_ctx.assert_called_once_with(stage="claimx-entity-writer")

    async def test_defaults_instance_id_to_none(self):
        shutdown = asyncio.Event()
        mock_worker = AsyncMock()
        mock_worker.start = AsyncMock()
        mock_worker.stop = AsyncMock()

        with (
            patch("core.logging.context.set_log_context"),
            patch(
                "pipeline.claimx.workers.entity_delta_worker.ClaimXEntityDeltaWorker",
                return_value=mock_worker,
            ) as MockWorker,
        ):
            await run_claimx_entity_delta_worker(
                Mock(), shutdown_event=shutdown, **self.TABLE_PATHS
            )

        assert MockWorker.call_args[1]["instance_id"] is None
