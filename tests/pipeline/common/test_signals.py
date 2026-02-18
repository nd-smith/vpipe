"""Tests for pipeline.common.signals module."""

import asyncio
import signal
from unittest.mock import MagicMock, patch

from pipeline.common.signals import setup_shutdown_signal_handlers


class TestSetupShutdownSignalHandlers:
    def test_registers_handlers_unix(self):
        callback = MagicMock()
        loop = MagicMock()
        loop.add_signal_handler = MagicMock()

        with patch("asyncio.get_event_loop", return_value=loop):
            setup_shutdown_signal_handlers(callback)

        assert loop.add_signal_handler.call_count == 2
        calls = loop.add_signal_handler.call_args_list
        sigs = {c[0][0] for c in calls}
        assert signal.SIGTERM in sigs
        assert signal.SIGINT in sigs

    def test_falls_back_on_windows(self):
        callback = MagicMock()
        loop = MagicMock()
        loop.add_signal_handler.side_effect = NotImplementedError

        with patch("asyncio.get_event_loop", return_value=loop), \
             patch("signal.signal") as mock_signal:
            setup_shutdown_signal_handlers(callback)

        assert mock_signal.call_count == 2
        sigs = {c[0][0] for c in mock_signal.call_args_list}
        assert signal.SIGTERM in sigs
        assert signal.SIGINT in sigs
