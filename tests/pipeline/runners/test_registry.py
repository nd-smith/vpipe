"""
Tests for worker registry.

Test Coverage:
    - Worker registry structure
    - run_worker_from_registry with valid workers
    - Unknown worker handling
    - Parameter filtering for runner signatures
    - Instance ID propagation
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from pipeline.runners.registry import (
    WORKER_REGISTRY,
    run_worker_from_registry,
)


class TestWorkerRegistry:
    """Tests for WORKER_REGISTRY structure."""

    def test_registry_contains_expected_workers(self):
        """Registry contains all expected worker definitions."""
        # XACT workers
        assert "xact-event-ingester" in WORKER_REGISTRY
        assert "xact-delta-writer" in WORKER_REGISTRY
        assert "xact-enricher" in WORKER_REGISTRY
        assert "xact-download" in WORKER_REGISTRY
        assert "xact-upload" in WORKER_REGISTRY

        # ClaimX workers
        assert "claimx-ingester" in WORKER_REGISTRY
        assert "claimx-enricher" in WORKER_REGISTRY
        assert "claimx-downloader" in WORKER_REGISTRY
        assert "claimx-uploader" in WORKER_REGISTRY
        assert "claimx-delta-writer" in WORKER_REGISTRY

    def test_registry_entries_have_runner(self):
        """All non-deprecated workers have runner function."""
        for _worker_name, worker_def in WORKER_REGISTRY.items():
            if worker_def.get("deprecated"):
                continue
            assert "runner" in worker_def
            assert callable(worker_def["runner"])

    def test_deprecated_workers_have_message(self):
        """Deprecated workers have deprecation message."""
        deprecated_workers = [
            name for name, def_ in WORKER_REGISTRY.items() if def_.get("deprecated")
        ]

        for worker_name in deprecated_workers:
            worker_def = WORKER_REGISTRY[worker_name]
            assert "message" in worker_def
            assert isinstance(worker_def["message"], str)


class TestRunWorkerFromRegistry:
    """Tests for run_worker_from_registry."""

    @pytest.mark.asyncio
    async def test_runs_worker_successfully(self):
        """Runs worker by looking it up in registry."""
        calls = []

        async def mock_runner(pipeline_config, shutdown_event, domain):
            calls.append((pipeline_config, shutdown_event, domain))

        pipeline_config = Mock()
        pipeline_config.domain = "claimx"
        shutdown_event = asyncio.Event()

        with patch.dict(
            "pipeline.runners.registry.WORKER_REGISTRY",
            {"test-worker": {"runner": mock_runner}},
        ):
            await run_worker_from_registry(
                worker_name="test-worker",
                pipeline_config=pipeline_config,
                shutdown_event=shutdown_event,
            )

        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_raises_for_unknown_worker(self):
        """Raises ValueError for unknown worker name."""
        pipeline_config = Mock()
        shutdown_event = asyncio.Event()

        with pytest.raises(ValueError, match="Unknown worker: nonexistent-worker"):
            await run_worker_from_registry(
                worker_name="nonexistent-worker",
                pipeline_config=pipeline_config,
                shutdown_event=shutdown_event,
            )

    @pytest.mark.asyncio
    async def test_passes_instance_id(self):
        """Passes instance_id to runner when provided."""

        async def mock_runner(instance_id, shutdown_event):
            assert instance_id == 5

        pipeline_config = Mock()
        pipeline_config.domain = "claimx"
        shutdown_event = asyncio.Event()

        with patch.dict(
            "pipeline.runners.registry.WORKER_REGISTRY",
            {"test-worker": {"runner": mock_runner}},
        ):
            await run_worker_from_registry(
                worker_name="test-worker",
                pipeline_config=pipeline_config,
                shutdown_event=shutdown_event,
                instance_id=5,
            )

    @pytest.mark.asyncio
    async def test_filters_kwargs_by_runner_signature(self):
        """Only passes kwargs that match runner signature."""

        async def mock_runner(pipeline_config, shutdown_event):
            # Only accepts pipeline_config and shutdown_event
            pass

        pipeline_config = Mock()
        pipeline_config.domain = "claimx"
        shutdown_event = asyncio.Event()

        with patch.dict(
            "pipeline.runners.registry.WORKER_REGISTRY",
            {"test-worker": {"runner": mock_runner}},
        ):
            # Should not raise despite extra kwargs
            await run_worker_from_registry(
                worker_name="test-worker",
                pipeline_config=pipeline_config,
                shutdown_event=shutdown_event,
                enable_delta_writes=True,
                eventhub_config=Mock(),
                local_kafka_config=Mock(),
            )

    @pytest.mark.asyncio
    async def test_passes_kafka_config_parameter(self):
        """Passes kafka_config when runner accepts it."""

        async def mock_runner(kafka_config, shutdown_event):
            assert kafka_config is not None

        pipeline_config = Mock()
        pipeline_config.domain = "claimx"
        shutdown_event = asyncio.Event()
        local_kafka = Mock()

        with patch.dict(
            "pipeline.runners.registry.WORKER_REGISTRY",
            {"test-worker": {"runner": mock_runner}},
        ):
            await run_worker_from_registry(
                worker_name="test-worker",
                pipeline_config=pipeline_config,
                shutdown_event=shutdown_event,
                local_kafka_config=local_kafka,
            )

    @pytest.mark.asyncio
    async def test_passes_domain_from_pipeline_config(self):
        """Passes domain extracted from pipeline_config."""

        async def mock_runner(domain, shutdown_event):
            assert domain == "claimx"

        pipeline_config = Mock()
        pipeline_config.domain = "claimx"
        shutdown_event = asyncio.Event()

        with patch.dict(
            "pipeline.runners.registry.WORKER_REGISTRY",
            {"test-worker": {"runner": mock_runner}},
        ):
            await run_worker_from_registry(
                worker_name="test-worker",
                pipeline_config=pipeline_config,
                shutdown_event=shutdown_event,
            )

    @pytest.mark.asyncio
    async def test_passes_enable_delta_writes(self):
        """Passes enable_delta_writes when runner accepts it."""

        async def mock_runner(enable_delta_writes, shutdown_event):
            assert enable_delta_writes is False

        pipeline_config = Mock()
        pipeline_config.domain = "claimx"
        shutdown_event = asyncio.Event()

        with patch.dict(
            "pipeline.runners.registry.WORKER_REGISTRY",
            {"test-worker": {"runner": mock_runner}},
        ):
            await run_worker_from_registry(
                worker_name="test-worker",
                pipeline_config=pipeline_config,
                shutdown_event=shutdown_event,
                enable_delta_writes=False,
            )
