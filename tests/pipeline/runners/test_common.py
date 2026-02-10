"""
Tests for common worker execution patterns and utilities.

Test Coverage:
    - Cleanup watcher task with cancellation
    - Start with retry logic and exponential backoff
    - Worker execution with shutdown handling
    - Worker execution with producer
    - Poller execution with shutdown handling
    - Graceful shutdown coordination
    - Instance ID handling for multi-instance deployments
"""

import asyncio
from unittest.mock import AsyncMock, Mock, call, patch

import pytest

from pipeline.runners.common import (
    DEFAULT_POLLER_BACKOFF_BASE,
    DEFAULT_POLLER_RETRIES,
    DEFAULT_STARTUP_RETRIES,
    _cleanup_watcher_task,
    _start_with_retry,
    execute_poller_with_shutdown,
    execute_worker_with_producer,
    execute_worker_with_shutdown,
)


class TestCleanupWatcherTask:
    """Tests for _cleanup_watcher_task helper."""

    @pytest.mark.asyncio
    async def test_cancels_and_awaits_task(self):
        """Cancels and awaits watcher task."""
        task_completed = False

        async def watcher():
            nonlocal task_completed
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                task_completed = True
                raise

        task = asyncio.create_task(watcher())
        await asyncio.sleep(0.01)  # Let task start

        await _cleanup_watcher_task(task)

        assert task_completed
        assert task.cancelled()

    @pytest.mark.asyncio
    async def test_suppresses_cancelled_error(self):
        """Suppresses CancelledError from task."""

        async def watcher():
            await asyncio.sleep(10)

        task = asyncio.create_task(watcher())
        await asyncio.sleep(0.01)

        # Should not raise despite CancelledError
        await _cleanup_watcher_task(task)

    @pytest.mark.asyncio
    async def test_suppresses_runtime_error(self):
        """Suppresses RuntimeError (event loop closed)."""

        async def watcher():
            raise RuntimeError("Event loop closed")

        task = asyncio.create_task(watcher())
        await asyncio.sleep(0.01)

        # Should not raise despite RuntimeError
        await _cleanup_watcher_task(task)


class TestStartWithRetry:
    """Tests for _start_with_retry helper."""

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self):
        """Succeeds immediately without retries."""
        start_fn = AsyncMock()

        await _start_with_retry(start_fn, "test_worker")

        start_fn.assert_called_once()

    @pytest.mark.asyncio
    async def test_retries_on_failure(self):
        """Retries on failure with exponential backoff."""
        call_count = 0

        async def failing_start():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Failed to connect")

        with patch("pipeline.runners.common.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await _start_with_retry(failing_start, "test_worker", max_retries=5, backoff_base=2)

        assert call_count == 3
        # Should have slept twice: 2*1=2s, 2*2=4s
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(2)
        mock_sleep.assert_any_call(4)

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self):
        """Raises last exception after max retries exhausted."""

        async def always_fails():
            raise ConnectionError("Always fails")

        with (
            patch("pipeline.runners.common.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(ConnectionError, match="Always fails"),
        ):
            await _start_with_retry(always_fails, "test_worker", max_retries=3, backoff_base=1)

    @pytest.mark.asyncio
    async def test_uses_default_config(self):
        """Uses default retry config from constants."""

        async def always_fails():
            raise ConnectionError("Failed")

        with (
            patch("pipeline.runners.common.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            pytest.raises(ConnectionError),
        ):
            await _start_with_retry(always_fails, "test_worker")

        # Should sleep DEFAULT_STARTUP_RETRIES - 1 times (last attempt doesn't sleep)
        assert mock_sleep.call_count == DEFAULT_STARTUP_RETRIES - 1

    @pytest.mark.asyncio
    async def test_uses_env_var_config(self):
        """Uses env vars for retry configuration."""

        async def always_fails():
            raise ConnectionError("Failed")

        with (
            patch("pipeline.runners.common.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            patch.dict(
                "os.environ",
                {"STARTUP_MAX_RETRIES": "2", "STARTUP_BACKOFF_SECONDS": "10"},
            ),
            pytest.raises(ConnectionError),
        ):
            await _start_with_retry(always_fails, "test_worker")

        # Should sleep 1 time (2 attempts - 1 = 1 sleep)
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_once_with(10)

    @pytest.mark.asyncio
    async def test_constant_backoff_mode(self):
        """Constant backoff uses same delay for all retries."""

        async def always_fails():
            raise ConnectionError("Failed")

        with (
            patch("pipeline.runners.common.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            pytest.raises(ConnectionError),
        ):
            await _start_with_retry(
                always_fails,
                "test_worker",
                max_retries=3,
                backoff_base=10,
                use_constant_backoff=True,
            )

        # Should have 2 sleeps (3 retries - 1)
        assert mock_sleep.call_count == 2
        # All delays should be constant at 10s
        mock_sleep.assert_has_calls([call(10), call(10)])

    @pytest.mark.asyncio
    async def test_linear_backoff_mode(self):
        """Linear backoff increases delays (default behavior)."""

        async def always_fails():
            raise ConnectionError("Failed")

        with (
            patch("pipeline.runners.common.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            pytest.raises(ConnectionError),
        ):
            await _start_with_retry(
                always_fails,
                "test_worker",
                max_retries=3,
                backoff_base=5,
                use_constant_backoff=False,
            )

        assert mock_sleep.call_count == 2
        # Linear: 5s, 10s
        mock_sleep.assert_has_calls([call(5), call(10)])


class TestExecuteWorkerWithShutdown:
    """Tests for execute_worker_with_shutdown."""

    @pytest.mark.asyncio
    async def test_starts_and_stops_worker(self):
        """Starts worker and stops on completion."""
        worker = AsyncMock()
        worker.start = AsyncMock()
        worker.stop = AsyncMock()
        shutdown_event = asyncio.Event()

        # Set shutdown immediately so worker.start returns quickly
        async def quick_start():
            shutdown_event.set()
            await asyncio.sleep(0.01)  # Give watcher time to react

        worker.start.side_effect = quick_start

        await execute_worker_with_shutdown(worker, "test-worker", shutdown_event)

        worker.start.assert_called_once()
        # Stop is called at least once (in finally block, watcher may also call it)
        assert worker.stop.call_count >= 1

    @pytest.mark.asyncio
    async def test_sets_log_context(self):
        """Sets log context with stage name."""
        worker = AsyncMock()
        worker.start = AsyncMock()
        worker.stop = AsyncMock()
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        with patch("pipeline.runners.common.set_log_context") as mock_set_context:
            await execute_worker_with_shutdown(worker, "test-stage", shutdown_event)

        mock_set_context.assert_called_once_with(stage="test-stage")

    @pytest.mark.asyncio
    async def test_sets_log_context_with_instance_id(self):
        """Sets log context with instance ID for multi-instance."""
        worker = AsyncMock()
        worker.start = AsyncMock()
        worker.stop = AsyncMock()
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        with patch("pipeline.runners.common.set_log_context") as mock_set_context:
            await execute_worker_with_shutdown(worker, "test-stage", shutdown_event, instance_id=5)

        mock_set_context.assert_called_once_with(
            stage="test-stage", instance_id=5, worker_id="test-stage-5"
        )

    @pytest.mark.asyncio
    async def test_shutdown_signal_stops_worker(self):
        """Shutdown event triggers worker stop."""
        worker = AsyncMock()
        shutdown_event = asyncio.Event()

        async def worker_start():
            # Wait for shutdown
            await asyncio.sleep(0.1)

        worker.start.side_effect = worker_start
        worker.stop = AsyncMock()

        async def trigger_shutdown():
            await asyncio.sleep(0.05)
            shutdown_event.set()

        await asyncio.gather(
            execute_worker_with_shutdown(worker, "test-worker", shutdown_event),
            trigger_shutdown(),
        )

        # Stop should be called by watcher and finally block
        assert worker.stop.call_count >= 1

    @pytest.mark.asyncio
    async def test_always_stops_worker_on_exception(self):
        """Worker is stopped even if start() raises."""
        worker = AsyncMock()
        worker.start = AsyncMock(side_effect=RuntimeError("Start failed"))
        worker.stop = AsyncMock(return_value=None)
        # Delete health_server so worker doesn't enter error mode
        del worker.health_server
        shutdown_event = asyncio.Event()

        with (
            patch("pipeline.runners.common.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(RuntimeError, match="Start failed"),
        ):
            await execute_worker_with_shutdown(worker, "test-worker", shutdown_event)

        # Stop should still be called in finally block
        worker.stop.assert_called()


class TestExecuteWorkerWithProducer:
    """Tests for execute_worker_with_producer."""

    @pytest.mark.asyncio
    async def test_creates_and_starts_producer_and_worker(self):
        """Creates producer and worker, starts both."""
        worker_class = Mock()
        producer_class = Mock()
        kafka_config = Mock()
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        mock_producer = AsyncMock()
        mock_producer.start = AsyncMock()
        mock_producer.stop = AsyncMock()

        mock_worker = AsyncMock()
        mock_worker.start = AsyncMock()
        mock_worker.stop = AsyncMock()

        with patch("pipeline.common.transport.create_producer", return_value=mock_producer):
            worker_class.return_value = mock_worker

            await execute_worker_with_producer(
                worker_class=worker_class,
                producer_class=producer_class,
                kafka_config=kafka_config,
                domain="claimx",
                stage_name="test-worker",
                shutdown_event=shutdown_event,
            )

        mock_producer.start.assert_called_once()
        mock_worker.start.assert_called_once()
        mock_producer.stop.assert_called_once()
        mock_worker.stop.assert_called()

    @pytest.mark.asyncio
    async def test_passes_worker_kwargs(self):
        """Passes additional kwargs to worker constructor."""
        worker_class = Mock()
        kafka_config = Mock()
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        mock_producer = AsyncMock()
        mock_producer.start = AsyncMock()
        mock_producer.stop = AsyncMock()

        mock_worker = AsyncMock()
        mock_worker.start = AsyncMock()
        mock_worker.stop = AsyncMock()

        with patch("pipeline.common.transport.create_producer", return_value=mock_producer):
            worker_class.return_value = mock_worker

            await execute_worker_with_producer(
                worker_class=worker_class,
                producer_class=None,
                kafka_config=kafka_config,
                domain="claimx",
                stage_name="test-worker",
                shutdown_event=shutdown_event,
                worker_kwargs={"custom_arg": "value"},
            )

        worker_class.assert_called_once()
        call_kwargs = worker_class.call_args[1]
        assert call_kwargs["custom_arg"] == "value"
        assert call_kwargs["config"] == kafka_config
        assert call_kwargs["producer"] == mock_producer
        assert call_kwargs["domain"] == "claimx"

    @pytest.mark.asyncio
    async def test_handles_instance_id(self):
        """Adds instance_id to producer name and worker kwargs."""
        worker_class = Mock()
        kafka_config = Mock()
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        mock_producer = AsyncMock()
        mock_producer.start = AsyncMock()
        mock_producer.stop = AsyncMock()

        mock_worker = AsyncMock()
        mock_worker.start = AsyncMock()
        mock_worker.stop = AsyncMock()

        with (
            patch(
                "pipeline.common.transport.create_producer", return_value=mock_producer
            ) as mock_create_producer,
            patch("pipeline.runners.common.set_log_context"),
        ):
            worker_class.return_value = mock_worker

            await execute_worker_with_producer(
                worker_class=worker_class,
                producer_class=None,
                kafka_config=kafka_config,
                domain="claimx",
                stage_name="test-worker",
                shutdown_event=shutdown_event,
                producer_worker_name="test_worker",
                instance_id=3,
            )

        # Producer should have instance_id in name
        create_call_kwargs = mock_create_producer.call_args[1]
        assert create_call_kwargs["worker_name"] == "test_worker_3"

        # Worker should receive instance_id
        worker_call_kwargs = worker_class.call_args[1]
        assert worker_call_kwargs["instance_id"] == 3

    @pytest.mark.asyncio
    async def test_stops_producer_and_worker_on_shutdown(self):
        """Stops both producer and worker on shutdown."""
        worker_class = Mock()
        kafka_config = Mock()
        shutdown_event = asyncio.Event()

        mock_producer = AsyncMock()
        mock_producer.start = AsyncMock()
        mock_producer.stop = AsyncMock()

        mock_worker = AsyncMock()
        mock_worker.stop = AsyncMock()

        async def worker_start():
            await asyncio.sleep(0.1)

        mock_worker.start = AsyncMock(side_effect=worker_start)

        async def trigger_shutdown():
            await asyncio.sleep(0.05)
            shutdown_event.set()

        with patch("pipeline.common.transport.create_producer", return_value=mock_producer):
            worker_class.return_value = mock_worker

            await asyncio.gather(
                execute_worker_with_producer(
                    worker_class=worker_class,
                    producer_class=None,
                    kafka_config=kafka_config,
                    domain="claimx",
                    stage_name="test-worker",
                    shutdown_event=shutdown_event,
                ),
                trigger_shutdown(),
            )

        mock_worker.stop.assert_called()
        mock_producer.stop.assert_called_once()


class TestExecutePollerWithShutdown:
    """Tests for execute_poller_with_shutdown."""

    @pytest.mark.asyncio
    async def test_starts_poller_as_context_manager(self):
        """Starts poller using async context manager."""
        poller_class = Mock()
        poller_config = Mock()
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        mock_poller = AsyncMock()
        mock_poller.run = AsyncMock()
        mock_poller.stop = AsyncMock()

        async def async_context_manager():
            class PollerContextManager:
                async def __aenter__(self):
                    return mock_poller

                async def __aexit__(self, *args):
                    pass

            return PollerContextManager()

        poller_class.return_value = await async_context_manager()

        await execute_poller_with_shutdown(
            poller_class=poller_class,
            poller_config=poller_config,
            stage_name="test-poller",
            shutdown_event=shutdown_event,
        )

        poller_class.assert_called_once_with(poller_config)
        mock_poller.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_signal_stops_poller(self):
        """Shutdown event triggers poller stop."""
        poller_class = Mock()
        poller_config = Mock()
        shutdown_event = asyncio.Event()

        mock_poller = AsyncMock()
        mock_poller.stop = AsyncMock()

        async def poller_run():
            await asyncio.sleep(0.1)

        mock_poller.run = AsyncMock(side_effect=poller_run)

        class PollerContextManager:
            async def __aenter__(self):
                return mock_poller

            async def __aexit__(self, *args):
                pass

        poller_class.return_value = PollerContextManager()

        async def trigger_shutdown():
            await asyncio.sleep(0.05)
            shutdown_event.set()

        await asyncio.gather(
            execute_poller_with_shutdown(
                poller_class=poller_class,
                poller_config=poller_config,
                stage_name="test-poller",
                shutdown_event=shutdown_event,
            ),
            trigger_shutdown(),
        )

        mock_poller.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_sets_log_context(self):
        """Sets log context with stage name."""
        poller_class = Mock()
        poller_config = Mock()
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        mock_poller = AsyncMock()
        mock_poller.run = AsyncMock()
        mock_poller.stop = AsyncMock()

        class PollerContextManager:
            async def __aenter__(self):
                return mock_poller

            async def __aexit__(self, *args):
                pass

        poller_class.return_value = PollerContextManager()

        with patch("pipeline.runners.common.set_log_context") as mock_set_context:
            await execute_poller_with_shutdown(
                poller_class=poller_class,
                poller_config=poller_config,
                stage_name="test-poller",
                shutdown_event=shutdown_event,
            )

        mock_set_context.assert_called_once_with(stage="test-poller")

    @pytest.mark.asyncio
    async def test_uses_poller_specific_retry_config(self):
        """Pollers use poller-specific retry configuration with constant backoff."""
        poller_class = Mock()
        poller_config = Mock()
        shutdown_event = asyncio.Event()

        mock_poller = AsyncMock()
        mock_poller.stop = AsyncMock()
        # Explicitly delete health_server so hasattr returns False
        del mock_poller.health_server

        call_count = 0

        async def failing_run():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Kusto timeout")

        mock_poller.run = failing_run

        class PollerContextManager:
            def __init__(self):
                self.entered = False

            async def __aenter__(self):
                self.entered = True
                return mock_poller

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                # Don't suppress the exception
                return False

        cm = PollerContextManager()
        poller_class.return_value = cm

        with (
            patch("pipeline.runners.common.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            pytest.raises(ConnectionError, match="Kusto timeout"),
        ):
            await execute_poller_with_shutdown(
                poller_class=poller_class,
                poller_config=poller_config,
                stage_name="test-poller",
                shutdown_event=shutdown_event,
            )

        # Should use DEFAULT_POLLER_RETRIES (10 attempts)
        assert call_count == DEFAULT_POLLER_RETRIES
        # Should sleep 9 times (10 attempts - 1)
        assert mock_sleep.call_count == DEFAULT_POLLER_RETRIES - 1
        # All delays should be constant at DEFAULT_POLLER_BACKOFF_BASE (60s)
        for call_args in mock_sleep.call_args_list:
            assert call_args[0][0] == DEFAULT_POLLER_BACKOFF_BASE

    @pytest.mark.asyncio
    async def test_uses_poller_env_var_config(self):
        """Pollers respect POLLER_STARTUP_* environment variables."""
        poller_class = Mock()
        poller_config = Mock()
        shutdown_event = asyncio.Event()

        mock_poller = AsyncMock()
        mock_poller.stop = AsyncMock()
        # Explicitly delete health_server so hasattr returns False
        del mock_poller.health_server

        call_count = 0

        async def failing_run():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Kusto timeout")

        mock_poller.run = failing_run

        class PollerContextManager:
            async def __aenter__(self):
                return mock_poller

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                # Don't suppress the exception
                return False

        poller_class.return_value = PollerContextManager()

        with (
            patch("pipeline.runners.common.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            patch.dict(
                "os.environ",
                {
                    "POLLER_STARTUP_MAX_RETRIES": "3",
                    "POLLER_STARTUP_BACKOFF_SECONDS": "15",
                },
            ),
            pytest.raises(ConnectionError, match="Kusto timeout"),
        ):
            await execute_poller_with_shutdown(
                poller_class=poller_class,
                poller_config=poller_config,
                stage_name="test-poller",
                shutdown_event=shutdown_event,
            )

        # Should use env var values (3 attempts, 15s delays)
        assert call_count == 3
        assert mock_sleep.call_count == 2
        # All delays should be constant at 15s
        mock_sleep.assert_has_calls([call(15), call(15)])
