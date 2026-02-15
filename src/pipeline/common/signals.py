"""Cross-platform signal handler setup for graceful shutdown."""

import asyncio
import logging
import signal
from collections.abc import Callable

logger = logging.getLogger(__name__)


def setup_shutdown_signal_handlers(callback: Callable[[], None]) -> None:
    """Register SIGTERM/SIGINT handlers that invoke callback on signal.

    On Unix, uses the event loop's add_signal_handler(). On Windows,
    falls back to signal.signal() since add_signal_handler() is not supported.
    """
    loop = asyncio.get_event_loop()
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, callback)
    except NotImplementedError:
        def _handler(signum, frame):
            logger.info("Received signal %s, initiating shutdown", signum)
            callback()

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)
