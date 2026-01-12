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
import sys
from datetime import datetime
from typing import List, Optional

from aiokafka.structs import ConsumerRecord

from config.config import KafkaConfig
from kafka_pipeline.common.dlq.handler import DLQHandler
from kafka_pipeline.xact.schemas.results import FailedDownloadMessage

# Configure logging for CLI
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


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
        consumer_start = asyncio.create_task(manager.handler._consumer.start())
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
            consumer_start.cancel()
            try:
                await consumer_start
            except asyncio.CancelledError:
                pass
            await manager.handler._consumer.stop()
        await manager.stop()


async def main_view(args):
    """Execute view command."""
    from config.config import load_config
    config = load_config()
    domain = "xact"
    manager = DLQCLIManager(config)

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

        consumer_start = asyncio.create_task(manager.handler._consumer.start())
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
            consumer_start.cancel()
            try:
                await consumer_start
            except asyncio.CancelledError:
                pass
            await manager.handler._consumer.stop()
        await manager.stop()


async def main_replay(args):
    """Execute replay command."""
    from config.config import load_config
    config = load_config()
    domain = "xact"
    manager = DLQCLIManager(config)

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

        consumer_start = asyncio.create_task(manager.handler._consumer.start())
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
            consumer_start.cancel()
            try:
                await consumer_start
            except asyncio.CancelledError:
                pass
            await manager.handler._consumer.stop()
        if manager.handler._producer:
            await manager.handler._producer.stop()


async def main_resolve(args):
    """Execute resolve command."""
    from config.config import load_config
    config = load_config()
    domain = "xact"
    manager = DLQCLIManager(config)

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

        consumer_start = asyncio.create_task(manager.handler._consumer.start())
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
            consumer_start.cancel()
            try:
                await consumer_start
            except asyncio.CancelledError:
                pass
            await manager.handler._consumer.stop()
        if manager.handler._producer:
            await manager.handler._producer.stop()


def main():
    """Main CLI entry point."""
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
