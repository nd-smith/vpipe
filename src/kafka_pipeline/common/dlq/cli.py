"""
Command-line interface for DLQ message management.

Provides CLI commands for manual review and replay of dead-letter queue messages:
- list: Display all DLQ messages
- view: Show detailed information for a specific message
- replay: Send a message back to pending topic for reprocessing
- resolve: Mark a message as handled (commit offset)
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set

from aiokafka.structs import ConsumerRecord
from dotenv import load_dotenv

# Project root directory (where .env file is located)
# cli.py is at src/kafka_pipeline/common/dlq/cli.py, so root is 5 levels up
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent

from config.config import KafkaConfig
from kafka_pipeline.common.dlq.handler import DLQHandler
from kafka_pipeline.xact.schemas.results import FailedDownloadMessage

# Configure logging for CLI
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class CLITaskManager:
    """Manage async tasks with proper cancellation and cleanup."""

    def __init__(self):
        """Initialize task manager with signal handlers."""
        self.tasks: Set[asyncio.Task] = set()
        self._shutdown = False
        self._shutdown_event = asyncio.Event()
        self._original_handlers = {}
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            """Handle shutdown signals."""
            if not self._shutdown:
                logger.info(f"Received signal {signum}, initiating shutdown...")
                self._shutdown = True
                # Set the event in a thread-safe way
                try:
                    loop = asyncio.get_running_loop()
                    loop.call_soon_threadsafe(self._shutdown_event.set)
                except RuntimeError:
                    # No running loop, set directly
                    pass

        # Save original handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            self._original_handlers[sig] = signal.signal(sig, signal_handler)

    def _restore_signal_handlers(self):
        """Restore original signal handlers."""
        for sig, handler in self._original_handlers.items():
            signal.signal(sig, handler)

    async def __aenter__(self):
        """Enter async context manager."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit async context manager and cleanup tasks."""
        await self.shutdown()
        self._restore_signal_handlers()
        return False

    def create_task(self, coro, name: str = None) -> asyncio.Task:
        """
        Create and track a task.

        Args:
            coro: Coroutine to create task from
            name: Optional task name for debugging

        Returns:
            Created asyncio.Task
        """
        task = asyncio.create_task(coro, name=name)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def shutdown(self, timeout: float = 5.0):
        """
        Shutdown all tasks gracefully.

        Args:
            timeout: Maximum time to wait for tasks to cancel (seconds)
        """
        if not self.tasks:
            return

        logger.info(f"Shutting down {len(self.tasks)} tasks...")

        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Wait for all tasks to complete with timeout
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.tasks, return_exceptions=True),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning(f"Task shutdown timed out after {timeout}s")
        except Exception as e:
            logger.error(f"Error during task shutdown: {e}")

        logger.info("Task shutdown complete")

    async def wait_all(self, timeout: float = None):
        """
        Wait for all tasks to complete.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            List of task results
        """
        if not self.tasks:
            return []

        try:
            if timeout:
                return await asyncio.wait_for(
                    asyncio.gather(*self.tasks, return_exceptions=True),
                    timeout=timeout
                )
            else:
                return await asyncio.gather(*self.tasks, return_exceptions=True)
        except asyncio.TimeoutError:
            logger.warning(f"Wait timed out after {timeout}s")
            raise

    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown

    async def wait_for_shutdown(self):
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()


class DLQCLIManager:
    """Manager for DLQ CLI operations."""

    def __init__(self, config: KafkaConfig):
        """
        Initialize DLQ CLI manager.

        Args:
            config: Kafka configuration
        """
        self.config = config
        self.handler = DLQHandler(config)
        self._messages: List[ConsumerRecord] = []

    async def start(self) -> None:
        """Start DLQ handler and fetch messages."""
        logger.info("Starting DLQ CLI manager")
        await self.handler.start()

    async def stop(self) -> None:
        """Stop DLQ handler and cleanup."""
        logger.info("Stopping DLQ CLI manager")
        await self.handler.stop()

    async def fetch_messages(self, limit: int = 100, timeout_ms: int = 5000) -> List[ConsumerRecord]:
        """
        Fetch messages from DLQ topic for review.

        Args:
            limit: Maximum number of messages to fetch
            timeout_ms: Timeout for fetching messages

        Returns:
            List of ConsumerRecord from DLQ topic
        """
        if not self.handler._consumer or not self.handler._consumer._consumer:
            raise RuntimeError("DLQ handler not started. Call start() first.")

        logger.info(f"Fetching up to {limit} messages from DLQ topic (timeout: {timeout_ms}ms)")
        messages = []

        # Fetch messages without committing
        consumer = self.handler._consumer._consumer
        data = await consumer.getmany(timeout_ms=timeout_ms, max_records=limit)

        for topic_partition, records in data.items():
            messages.extend(records)
            if len(messages) >= limit:
                break

        logger.info(f"Fetched {len(messages)} messages from DLQ")
        self._messages = messages
        return messages

    def list_messages(self) -> None:
        """
        List all fetched DLQ messages in table format.
        """
        if not self._messages:
            print("No DLQ messages found.")
            return

        print(f"\n{'='*100}")
        print(f"DLQ Messages ({len(self._messages)} total)")
        print(f"{'='*100}\n")
        print(f"{'Offset':<8} {'Trace ID':<20} {'Retry Count':<12} {'Error Category':<15} {'URL':<45}")
        print(f"{'-'*100}")

        for record in self._messages:
            try:
                dlq_msg = self.handler.parse_dlq_message(record)
                url_display = dlq_msg.attachment_url[:42] + "..." if len(dlq_msg.attachment_url) > 45 else dlq_msg.attachment_url
                print(
                    f"{record.offset:<8} {dlq_msg.trace_id:<20} {dlq_msg.retry_count:<12} "
                    f"{dlq_msg.error_category:<15} {url_display:<45}"
                )
            except Exception as e:
                print(f"{record.offset:<8} [PARSE ERROR: {str(e)}]")

        print(f"{'-'*100}\n")

    def view_message(self, trace_id: str) -> None:
        """
        Display detailed information for a specific message.

        Args:
            trace_id: Trace ID of message to view
        """
        # Find message by trace_id
        record = self._find_message_by_trace_id(trace_id)
        if not record:
            print(f"Error: No message found with trace_id '{trace_id}'")
            return

        try:
            dlq_msg = self.handler.parse_dlq_message(record)

            print(f"\n{'='*80}")
            print(f"DLQ Message Details: {trace_id}")
            print(f"{'='*80}\n")

            print(f"Kafka Metadata:")
            print(f"  Topic:     {record.topic}")
            print(f"  Partition: {record.partition}")
            print(f"  Offset:    {record.offset}")
            print(f"  Key:       {record.key.decode('utf-8') if record.key else 'None'}")
            print()

            print(f"Message Details:")
            print(f"  Trace ID:       {dlq_msg.trace_id}")
            print(f"  Attachment URL: {dlq_msg.attachment_url}")
            print(f"  Retry Count:    {dlq_msg.retry_count}")
            print(f"  Error Category: {dlq_msg.error_category}")
            print(f"  Failed At:      {dlq_msg.failed_at}")
            print()

            print(f"Error Information:")
            print(f"  {dlq_msg.final_error}")
            print()

            print(f"Original Task:")
            print(f"  Event Type:        {dlq_msg.original_task.event_type}")
            print(f"  Event Subtype:     {dlq_msg.original_task.event_subtype}")
            print(f"  Blob Path:         {dlq_msg.original_task.blob_path}")
            print(f"  Original Time:     {dlq_msg.original_task.original_timestamp}")
            print()

            if dlq_msg.original_task.metadata:
                print(f"Metadata:")
                for key, value in dlq_msg.original_task.metadata.items():
                    print(f"  {key}: {value}")
                print()

            print(f"{'='*80}\n")

        except Exception as e:
            print(f"Error parsing message: {e}")
            logger.error(f"Failed to view message {trace_id}", exc_info=True)

    async def replay_message(self, trace_id: str) -> None:
        """
        Replay a message back to pending topic.

        Args:
            trace_id: Trace ID of message to replay
        """
        record = self._find_message_by_trace_id(trace_id)
        if not record:
            print(f"Error: No message found with trace_id '{trace_id}'")
            return

        try:
            print(f"Replaying message {trace_id} to pending topic...")
            await self.handler.replay_message(record)
            print(f"✓ Message {trace_id} replayed successfully")

            # Audit log
            logger.info(
                "DLQ message replayed via CLI",
                extra={
                    "action": "replay",
                    "trace_id": trace_id,
                    "offset": record.offset,
                    "partition": record.partition,
                    "user": "cli",
                },
            )

        except Exception as e:
            print(f"✗ Failed to replay message: {e}")
            logger.error(f"Failed to replay message {trace_id}", exc_info=True)

    async def resolve_message(self, trace_id: str) -> None:
        """
        Mark a message as resolved (commit offset).

        Args:
            trace_id: Trace ID of message to resolve
        """
        record = self._find_message_by_trace_id(trace_id)
        if not record:
            print(f"Error: No message found with trace_id '{trace_id}'")
            return

        try:
            print(f"Marking message {trace_id} as resolved...")
            await self.handler.acknowledge_message(record)
            print(f"✓ Message {trace_id} resolved successfully")

            # Audit log
            logger.info(
                "DLQ message resolved via CLI",
                extra={
                    "action": "resolve",
                    "trace_id": trace_id,
                    "offset": record.offset,
                    "partition": record.partition,
                    "user": "cli",
                },
            )

        except Exception as e:
            print(f"✗ Failed to resolve message: {e}")
            logger.error(f"Failed to resolve message {trace_id}", exc_info=True)

    def _find_message_by_trace_id(self, trace_id: str) -> Optional[ConsumerRecord]:
        """
        Find a message by trace_id.

        Args:
            trace_id: Trace ID to search for

        Returns:
            ConsumerRecord if found, None otherwise
        """
        for record in self._messages:
            try:
                dlq_msg = self.handler.parse_dlq_message(record)
                if dlq_msg.trace_id == trace_id:
                    return record
            except Exception:
                # Skip messages we can't parse
                continue
        return None


async def main_list(args):
    """Execute list command."""
    from config.config import load_config
    config = load_config()
    domain = "xact"
    manager = DLQCLIManager(config)

    # Override default message handler to prevent auto-processing
    # We'll manually fetch without committing
    manager.handler._handle_dlq_message = lambda record: asyncio.sleep(0)

    async with CLITaskManager() as task_manager:
        try:
            # Start producer (needed for replay/resolve later)
            await manager.handler._producer.start() if not manager.handler._producer else None

            # Create consumer manually without starting message loop
            # Inline import: lazy loading for CLI commands (only import what's needed)
            from kafka_pipeline.common.consumer import BaseKafkaConsumer
            manager.handler._consumer = BaseKafkaConsumer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
                topics=[config.get_topic(domain, "dlq")],
                message_handler=lambda r: asyncio.sleep(0),  # No-op handler
            )

            # Start consumer (connects but we'll fetch manually)
            consumer_task = task_manager.create_task(
                manager.handler._consumer.start(),
                name="dlq_consumer"
            )
            await asyncio.sleep(0.5)  # Give consumer time to connect

            # Fetch messages
            await manager.fetch_messages(limit=args.limit, timeout_ms=args.timeout)
            manager.list_messages()

        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
        except Exception as e:
            print(f"Error: {e}")
            logger.error("List command failed", exc_info=True)
            sys.exit(1)
        finally:
            # Stop consumer
            if manager.handler._consumer:
                await manager.handler._consumer.stop()
            await manager.stop()


async def main_view(args):
    """Execute view command."""
    from config.config import load_config
    config = load_config()
    domain = "xact"
    manager = DLQCLIManager(config)

    async with CLITaskManager() as task_manager:
        try:
            # Start and fetch messages
            manager.handler._handle_dlq_message = lambda record: asyncio.sleep(0)
            await manager.handler._producer.start() if not manager.handler._producer else None

            from kafka_pipeline.common.consumer import BaseKafkaConsumer
            manager.handler._consumer = BaseKafkaConsumer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
                topics=[config.get_topic(domain, "dlq")],
                message_handler=lambda r: asyncio.sleep(0),
            )

            consumer_task = task_manager.create_task(
                manager.handler._consumer.start(),
                name="dlq_consumer"
            )
            await asyncio.sleep(0.5)

            await manager.fetch_messages(limit=100, timeout_ms=5000)
            manager.view_message(args.trace_id)

        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
        except Exception as e:
            print(f"Error: {e}")
            logger.error("View command failed", exc_info=True)
            sys.exit(1)
        finally:
            if manager.handler._consumer:
                await manager.handler._consumer.stop()
            await manager.stop()


async def main_replay(args):
    """Execute replay command."""
    from config.config import load_config
    config = load_config()
    domain = "xact"
    manager = DLQCLIManager(config)

    async with CLITaskManager() as task_manager:
        try:
            # Start handler components
            manager.handler._handle_dlq_message = lambda record: asyncio.sleep(0)

            # Start producer for replay
            from kafka_pipeline.common.producer import BaseKafkaProducer
            manager.handler._producer = BaseKafkaProducer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
            )
            await manager.handler._producer.start()

            # Start consumer to fetch messages
            from kafka_pipeline.common.consumer import BaseKafkaConsumer
            manager.handler._consumer = BaseKafkaConsumer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
                topics=[config.get_topic(domain, "dlq")],
                message_handler=lambda r: asyncio.sleep(0),
            )

            consumer_task = task_manager.create_task(
                manager.handler._consumer.start(),
                name="dlq_consumer"
            )
            await asyncio.sleep(0.5)

            await manager.fetch_messages(limit=100, timeout_ms=5000)
            await manager.replay_message(args.trace_id)

        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
        except Exception as e:
            print(f"Error: {e}")
            logger.error("Replay command failed", exc_info=True)
            sys.exit(1)
        finally:
            if manager.handler._consumer:
                await manager.handler._consumer.stop()
            if manager.handler._producer:
                await manager.handler._producer.stop()


async def main_resolve(args):
    """Execute resolve command."""
    from config.config import load_config
    config = load_config()
    domain = "xact"
    manager = DLQCLIManager(config)

    async with CLITaskManager() as task_manager:
        try:
            # Start handler components
            manager.handler._handle_dlq_message = lambda record: asyncio.sleep(0)

            # Start producer (needed by handler)
            from kafka_pipeline.common.producer import BaseKafkaProducer
            manager.handler._producer = BaseKafkaProducer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
            )
            await manager.handler._producer.start()

            # Start consumer
            from kafka_pipeline.common.consumer import BaseKafkaConsumer
            manager.handler._consumer = BaseKafkaConsumer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
                topics=[config.get_topic(domain, "dlq")],
                message_handler=lambda r: asyncio.sleep(0),
            )

            consumer_task = task_manager.create_task(
                manager.handler._consumer.start(),
                name="dlq_consumer"
            )
            await asyncio.sleep(0.5)

            await manager.fetch_messages(limit=100, timeout_ms=5000)
            await manager.resolve_message(args.trace_id)

        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
        except Exception as e:
            print(f"Error: {e}")
            logger.error("Resolve command failed", exc_info=True)
            sys.exit(1)
        finally:
            if manager.handler._consumer:
                await manager.handler._consumer.stop()
            if manager.handler._producer:
                await manager.handler._producer.stop()


def main():
    """Main CLI entry point."""
    # Load environment variables from .env file before any config access
    load_dotenv(PROJECT_ROOT / ".env")

    parser = argparse.ArgumentParser(
        description="DLQ message management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all DLQ messages
  python -m kafka_pipeline.dlq.cli list

  # List with custom limit
  python -m kafka_pipeline.dlq.cli list --limit 50

  # View specific message
  python -m kafka_pipeline.dlq.cli view evt-123

  # Replay message to pending topic
  python -m kafka_pipeline.dlq.cli replay evt-123

  # Mark message as resolved
  python -m kafka_pipeline.dlq.cli resolve evt-123
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # List command
    list_parser = subparsers.add_parser("list", help="List all DLQ messages")
    list_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of messages to fetch (default: 100)",
    )
    list_parser.add_argument(
        "--timeout",
        type=int,
        default=5000,
        help="Timeout in milliseconds for fetching messages (default: 5000)",
    )

    # View command
    view_parser = subparsers.add_parser("view", help="View detailed message information")
    view_parser.add_argument("trace_id", help="Trace ID of message to view")

    # Replay command
    replay_parser = subparsers.add_parser(
        "replay", help="Replay message to pending topic for reprocessing"
    )
    replay_parser.add_argument("trace_id", help="Trace ID of message to replay")

    # Resolve command
    resolve_parser = subparsers.add_parser(
        "resolve", help="Mark message as resolved (commit offset)"
    )
    resolve_parser.add_argument("trace_id", help="Trace ID of message to resolve")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Execute command
    if args.command == "list":
        asyncio.run(main_list(args))
    elif args.command == "view":
        asyncio.run(main_view(args))
    elif args.command == "replay":
        asyncio.run(main_replay(args))
    elif args.command == "resolve":
        asyncio.run(main_resolve(args))


if __name__ == "__main__":
    main()
