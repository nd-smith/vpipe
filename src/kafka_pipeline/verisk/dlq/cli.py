"""
Command-line interface for XACT DLQ message management.
Provides: list, view, replay, and resolve commands.
"""

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent

from config.config import KafkaConfig
from kafka_pipeline.common.types import PipelineMessage, from_consumer_record
from kafka_pipeline.verisk.dlq.handler import DLQHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class CLITaskManager:

    def __init__(self):
        self.tasks: set[asyncio.Task] = set()
        self._shutdown = False
        self._shutdown_event = asyncio.Event()
        self._original_handlers = {}
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        def signal_handler(signum, frame):
            if not self._shutdown:
                logger.info("Received signal %s, initiating shutdown...", signum)
                self._shutdown = True
                # Set the event in a thread-safe way
                try:
                    loop = asyncio.get_running_loop()
                    loop.call_soon_threadsafe(self._shutdown_event.set)
                except RuntimeError:
                    pass

        for sig in (signal.SIGTERM, signal.SIGINT):
            self._original_handlers[sig] = signal.signal(sig, signal_handler)

    def _restore_signal_handlers(self):
        for sig, handler in self._original_handlers.items():
            signal.signal(sig, handler)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()
        self._restore_signal_handlers()
        return False

    def create_task(self, coro, name: str = None) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def shutdown(self, timeout: float = 5.0):
        if not self.tasks:
            return

        logger.info("Shutting down %s tasks...", len(self.tasks))
        for task in self.tasks:
            if not task.done():
                task.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.tasks, return_exceptions=True), timeout=timeout
            )
        except TimeoutError:
            logger.warning("Task shutdown timed out after %ss", timeout)
        except Exception as e:
            logger.exception("Error during task shutdown: %s", e)

        logger.info("Task shutdown complete")

    async def wait_all(self, timeout: float = None):
        if not self.tasks:
            return []

        try:
            if timeout:
                return await asyncio.wait_for(
                    asyncio.gather(*self.tasks, return_exceptions=True), timeout=timeout
                )
            else:
                return await asyncio.gather(*self.tasks, return_exceptions=True)
        except TimeoutError:
            logger.warning("Wait timed out after %ss", timeout)
            raise

    def is_shutdown_requested(self):
        return self._shutdown

    async def wait_for_shutdown(self):
        await self._shutdown_event.wait()


class DLQCLIManager:

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.handler = DLQHandler(config)
        self._messages: list[PipelineMessage] = []

    async def start(self) -> None:
        logger.info("Starting XACT DLQ CLI manager")
        await self.handler.start()

    async def stop(self) -> None:
        logger.info("Stopping XACT DLQ CLI manager")
        await self.handler.stop()

    async def fetch_messages(
        self, limit: int = 100, timeout_ms: int = 5000
    ) -> list[PipelineMessage]:
        if not self.handler._consumer or not self.handler._consumer._consumer:
            raise RuntimeError("DLQ handler not started. Call start() first.")

        logger.info(
            f"Fetching up to {limit} messages from DLQ topic (timeout: {timeout_ms}ms)"
        )
        messages = []
        consumer = self.handler._consumer._consumer
        data = await consumer.getmany(timeout_ms=timeout_ms, max_records=limit)

        for _topic_partition, records in data.items():
            # Convert ConsumerRecord to PipelineMessage
            messages.extend([from_consumer_record(record) for record in records])
            if len(messages) >= limit:
                break

        logger.info("Fetched %s messages from DLQ", len(messages))
        self._messages = messages
        return messages

    def list_messages(self) -> None:
        if not self._messages:
            print("No DLQ messages found.")
            return

        print(f"\n{'='*100}")
        print(f"DLQ Messages ({len(self._messages)} total)")
        print(f"{'='*100}\n")
        print(
            f"{'Offset':<8} {'Trace ID':<20} {'Retry Count':<12} {'Error Category':<15} {'URL':<45}"
        )
        print(f"{'-'*100}")

        for record in self._messages:
            try:
                dlq_msg = self.handler.parse_dlq_message(record)
                url_display = (
                    dlq_msg.attachment_url[:42] + "..."
                    if len(dlq_msg.attachment_url) > 45
                    else dlq_msg.attachment_url
                )
                print(
                    f"{record.offset:<8} {dlq_msg.trace_id:<20} {dlq_msg.retry_count:<12} "
                    f"{dlq_msg.error_category:<15} {url_display:<45}"
                )
            except Exception as e:
                print(f"{record.offset:<8} [PARSE ERROR: {str(e)}]")

        print(f"{'-'*100}\n")

    def view_message(self, trace_id: str) -> None:
        record = self._find_message_by_trace_id(trace_id)
        if not record:
            print(f"Error: No message found with trace_id '{trace_id}'")
            return

        try:
            dlq_msg = self.handler.parse_dlq_message(record)

            print(f"\n{'='*80}")
            print(f"DLQ Message Details: {trace_id}")
            print(f"{'='*80}\n")

            print("Kafka Metadata:")
            print(f"  Topic:     {record.topic}")
            print(f"  Partition: {record.partition}")
            print(f"  Offset:    {record.offset}")
            print(
                f"  Key:       {record.key.decode('utf-8') if record.key else 'None'}"
            )
            print()

            print("Message Details:")
            print(f"  Trace ID:       {dlq_msg.trace_id}")
            print(f"  Attachment URL: {dlq_msg.attachment_url}")
            print(f"  Retry Count:    {dlq_msg.retry_count}")
            print(f"  Error Category: {dlq_msg.error_category}")
            print(f"  Failed At:      {dlq_msg.failed_at}")
            print()

            print("Error Information:")
            print(f"  {dlq_msg.final_error}")
            print()

            print("Original Task:")
            print(f"  Event Type:        {dlq_msg.original_task.event_type}")
            print(f"  Event Subtype:     {dlq_msg.original_task.event_subtype}")
            print(f"  Blob Path:         {dlq_msg.original_task.blob_path}")
            print(f"  Original Time:     {dlq_msg.original_task.original_timestamp}")
            print()

            if dlq_msg.original_task.metadata:
                print("Metadata:")
                for key, value in dlq_msg.original_task.metadata.items():
                    print(f"  {key}: {value}")
                print()

            print(f"{'='*80}\n")

        except Exception as e:
            print(f"Error parsing message: {e}")
            logger.error("Failed to view message %s", trace_id)

    async def replay_message(self, trace_id: str) -> None:
        record = self._find_message_by_trace_id(trace_id)
        if not record:
            print(f"Error: No message found with trace_id '{trace_id}'")
            return

        try:
            print(f"Replaying message {trace_id} to pending topic...")
            await self.handler.replay_message(record)
            print(f"✓ Message {trace_id} replayed successfully")
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
            logger.exception("Failed to replay message %s", trace_id)

    async def resolve_message(self, trace_id: str) -> None:
        record = self._find_message_by_trace_id(trace_id)
        if not record:
            print(f"Error: No message found with trace_id '{trace_id}'")
            return

        try:
            print(f"Marking message {trace_id} as resolved...")
            await self.handler.acknowledge_message(record)
            print(f"✓ Message {trace_id} resolved successfully")
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
            logger.exception("Failed to resolve message %s", trace_id)

    def _find_message_by_trace_id(self, trace_id: str) -> PipelineMessage | None:
        for record in self._messages:
            try:
                dlq_msg = self.handler.parse_dlq_message(record)
                if dlq_msg.trace_id == trace_id:
                    return record
            except Exception:
                continue
        return None


async def main_list(args):
    from config.config import load_config

    config = load_config()
    domain = "verisk"
    manager = DLQCLIManager(config)
    manager.handler._handle_dlq_message = lambda record: asyncio.sleep(0)

    async with CLITaskManager() as task_manager:
        try:
            (
                await manager.handler._producer.start()
                if not manager.handler._producer
                else None
            )
            from kafka_pipeline.common.consumer import BaseKafkaConsumer

            manager.handler._consumer = BaseKafkaConsumer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
                topics=[config.get_topic(domain, "dlq")],
                message_handler=lambda r: asyncio.sleep(0),
            )
            task_manager.create_task(
                manager.handler._consumer.start(), name="dlq_consumer"
            )
            await asyncio.sleep(0.5)
            await manager.fetch_messages(limit=args.limit, timeout_ms=args.timeout)
            manager.list_messages()

        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
        except Exception as e:
            print(f"Error: {e}")
            logger.exception("List command failed")
            sys.exit(1)
        finally:
            if manager.handler._consumer:
                await manager.handler._consumer.stop()
            await manager.stop()


async def main_view(args):
    from config.config import load_config

    config = load_config()
    domain = "verisk"
    manager = DLQCLIManager(config)

    async with CLITaskManager() as task_manager:
        try:
            manager.handler._handle_dlq_message = lambda record: asyncio.sleep(0)
            (
                await manager.handler._producer.start()
                if not manager.handler._producer
                else None
            )

            from kafka_pipeline.common.consumer import BaseKafkaConsumer

            manager.handler._consumer = BaseKafkaConsumer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
                topics=[config.get_topic(domain, "dlq")],
                message_handler=lambda r: asyncio.sleep(0),
            )

            task_manager.create_task(
                manager.handler._consumer.start(), name="dlq_consumer"
            )
            await asyncio.sleep(0.5)

            await manager.fetch_messages(limit=100, timeout_ms=5000)
            manager.view_message(args.trace_id)

        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
        except Exception as e:
            print(f"Error: {e}")
            logger.exception("View command failed")
            sys.exit(1)
        finally:
            if manager.handler._consumer:
                await manager.handler._consumer.stop()
            await manager.stop()


async def main_replay(args):
    from config.config import load_config

    config = load_config()
    domain = "verisk"
    manager = DLQCLIManager(config)

    async with CLITaskManager() as task_manager:
        try:
            manager.handler._handle_dlq_message = lambda record: asyncio.sleep(0)
            from kafka_pipeline.common.producer import BaseKafkaProducer

            manager.handler._producer = BaseKafkaProducer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
            )
            await manager.handler._producer.start()
            from kafka_pipeline.common.consumer import BaseKafkaConsumer

            manager.handler._consumer = BaseKafkaConsumer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
                topics=[config.get_topic(domain, "dlq")],
                message_handler=lambda r: asyncio.sleep(0),
            )

            task_manager.create_task(
                manager.handler._consumer.start(), name="dlq_consumer"
            )
            await asyncio.sleep(0.5)

            await manager.fetch_messages(limit=100, timeout_ms=5000)
            await manager.replay_message(args.trace_id)

        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
        except Exception as e:
            print(f"Error: {e}")
            logger.exception("Replay command failed")
            sys.exit(1)
        finally:
            if manager.handler._consumer:
                await manager.handler._consumer.stop()
            if manager.handler._producer:
                await manager.handler._producer.stop()


async def main_resolve(args):
    from config.config import load_config

    config = load_config()
    domain = "verisk"
    manager = DLQCLIManager(config)

    async with CLITaskManager() as task_manager:
        try:
            manager.handler._handle_dlq_message = lambda record: asyncio.sleep(0)
            from kafka_pipeline.common.producer import BaseKafkaProducer

            manager.handler._producer = BaseKafkaProducer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
            )
            await manager.handler._producer.start()
            from kafka_pipeline.common.consumer import BaseKafkaConsumer

            manager.handler._consumer = BaseKafkaConsumer(
                config=config,
                domain=domain,
                worker_name="dlq_cli",
                topics=[config.get_topic(domain, "dlq")],
                message_handler=lambda r: asyncio.sleep(0),
            )

            task_manager.create_task(
                manager.handler._consumer.start(), name="dlq_consumer"
            )
            await asyncio.sleep(0.5)

            await manager.fetch_messages(limit=100, timeout_ms=5000)
            await manager.resolve_message(args.trace_id)

        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
        except Exception as e:
            print(f"Error: {e}")
            logger.exception("Resolve command failed")
            sys.exit(1)
        finally:
            if manager.handler._consumer:
                await manager.handler._consumer.stop()
            if manager.handler._producer:
                await manager.handler._producer.stop()


def main():
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
    view_parser = subparsers.add_parser(
        "view", help="View detailed message information"
    )
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
