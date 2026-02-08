"""ClaimX DLQ Management CLI.

Manage ClaimX Dead Letter Queues (DLQs) for enrichment and download workers.
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from dotenv import load_dotenv

from config.config import MessageConfig
from pipeline.claimx.schemas.results import (
    FailedDownloadMessage,
    FailedEnrichmentMessage,
)

# Project root directory (where .env file is located)
# cli.py is at src/pipeline/claimx/dlq/cli.py, so root is 5 levels up
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DLQManager:
    """Manager for ClaimX DLQ operations."""

    def __init__(self, config: MessageConfig):
        """
        Initialize DLQ manager.

        Args:
            config: Kafka configuration
        """
        self.config = config
        self.enrichment_dlq_topic = config.get_topic("claimx", "enrichment.dlq")
        self.enrichment_pending_topic = config.get_topic("claimx", "enrichment.pending")
        self.download_dlq_topic = config.get_topic("claimx", "downloads.dlq")
        self.download_pending_topic = config.get_topic("claimx", "downloads.pending")

    def _get_consumer_config(self) -> dict:
        """Build Kafka consumer configuration."""
        from pipeline.common.kafka_config import build_kafka_security_config

        consumer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
            "group_id": "claimx-dlq-cli",  # Dedicated consumer group
            "enable_auto_commit": False,  # Manual control
            "auto_offset_reset": "earliest",  # Read from beginning
        }

        consumer_config.update(build_kafka_security_config(self.config))
        return consumer_config

    def _get_producer_config(self) -> dict:
        """Build Kafka producer configuration."""
        from pipeline.common.kafka_config import build_kafka_security_config

        producer_config = {
            "bootstrap_servers": self.config.bootstrap_servers,
        }

        producer_config.update(build_kafka_security_config(self.config))
        return producer_config

    async def list_dlq_counts(self) -> dict:
        """
        List message counts for all ClaimX DLQs.

        Returns:
            Dictionary with DLQ topics and their message counts
        """
        consumer_config = self._get_consumer_config()
        consumer = AIOKafkaConsumer(**consumer_config)

        try:
            await consumer.start()

            counts = {}

            for topic_name, topic_label in [
                (self.enrichment_dlq_topic, "Enrichment DLQ"),
                (self.download_dlq_topic, "Download DLQ"),
            ]:
                # Get topic partitions
                partitions = consumer.partitions_for_topic(topic_name)
                if not partitions:
                    counts[topic_label] = {
                        "topic": topic_name,
                        "count": 0,
                        "error": "Topic not found",
                    }
                    continue

                # Get end offsets for all partitions
                topic_partitions = [TopicPartition(topic_name, p) for p in partitions]
                end_offsets = await consumer.end_offsets(topic_partitions)

                # Get beginning offsets for all partitions
                beginning_offsets = await consumer.beginning_offsets(topic_partitions)

                # Calculate total message count
                total_count = sum(
                    end_offsets[tp] - beginning_offsets[tp] for tp in topic_partitions
                )

                counts[topic_label] = {
                    "topic": topic_name,
                    "count": total_count,
                    "partitions": len(partitions),
                }

            return counts

        finally:
            await consumer.stop()

    async def inspect_dlq(
        self,
        dlq_type: str,
        limit: int = 10,
    ) -> list[dict]:
        """
        Inspect messages in a DLQ.

        Args:
            dlq_type: Type of DLQ ("enrichment" or "download")
            limit: Maximum number of messages to retrieve

        Returns:
            List of DLQ messages with metadata
        """
        if dlq_type == "enrichment":
            topic = self.enrichment_dlq_topic
            schema_class = FailedEnrichmentMessage
        elif dlq_type == "download":
            topic = self.download_dlq_topic
            schema_class = FailedDownloadMessage
        else:
            raise ValueError(
                f"Invalid DLQ type: {dlq_type}. Must be 'enrichment' or 'download'"
            )

        consumer_config = self._get_consumer_config()
        consumer = AIOKafkaConsumer(topic, **consumer_config)

        try:
            await consumer.start()

            messages = []
            count = 0

            # Consume messages up to limit
            async for record in consumer:
                if count >= limit:
                    break

                try:
                    # Parse message
                    failed_message = schema_class.model_validate_json(record.value)

                    # Extract key fields
                    message_data = {
                        "partition": record.partition,
                        "offset": record.offset,
                        "timestamp": datetime.fromtimestamp(
                            record.timestamp / 1000
                        ).isoformat(),
                        "key": record.key.decode() if record.key else None,
                    }

                    # Add type-specific fields
                    if dlq_type == "enrichment":
                        message_data.update(
                            {
                                "event_id": failed_message.event_id,
                                "event_type": failed_message.event_type,
                                "project_id": failed_message.project_id,
                                "error_category": failed_message.error_category,
                                "final_error": failed_message.final_error,
                                "retry_count": failed_message.retry_count,
                                "failed_at": failed_message.failed_at.isoformat(),
                            }
                        )
                    else:  # download
                        message_data.update(
                            {
                                "media_id": failed_message.media_id,
                                "project_id": failed_message.project_id,
                                "download_url": failed_message.download_url[:100]
                                + "...",  # Truncate
                                "blob_path": failed_message.blob_path,
                                "error_category": failed_message.error_category,
                                "final_error": failed_message.final_error,
                                "retry_count": failed_message.retry_count,
                                "url_refresh_attempted": failed_message.url_refresh_attempted,
                                "failed_at": failed_message.failed_at.isoformat(),
                            }
                        )

                    messages.append(message_data)
                    count += 1

                except Exception as e:
                    logger.exception(
                        f"Failed to parse message at offset {record.offset}: {e}"
                    )
                    continue

            return messages

        finally:
            await consumer.stop()

    async def replay_messages(
        self,
        dlq_type: str,
        event_ids: list[str] | None = None,
        replay_all: bool = False,
    ) -> int:
        """
        Replay messages from DLQ back to pending topic.

        Extracts original task from DLQ message and sends to pending topic
        with reset retry_count=0 for fresh retry.

        Args:
            dlq_type: Type of DLQ ("enrichment" or "download")
            event_ids: List of specific event/media IDs to replay (None = all)
            replay_all: Replay all messages (overrides event_ids)

        Returns:
            Number of messages replayed
        """
        if dlq_type == "enrichment":
            dlq_topic = self.enrichment_dlq_topic
            pending_topic = self.enrichment_pending_topic
            schema_class = FailedEnrichmentMessage
            id_field = "event_id"
        elif dlq_type == "download":
            dlq_topic = self.download_dlq_topic
            pending_topic = self.download_pending_topic
            schema_class = FailedDownloadMessage
            id_field = "media_id"
        else:
            raise ValueError(
                f"Invalid DLQ type: {dlq_type}. Must be 'enrichment' or 'download'"
            )

        consumer_config = self._get_consumer_config()
        consumer = AIOKafkaConsumer(dlq_topic, **consumer_config)

        producer_config = self._get_producer_config()
        producer = AIOKafkaProducer(**producer_config)

        try:
            await consumer.start()
            await producer.start()

            replayed_count = 0
            event_id_set = set(event_ids) if event_ids else set()

            logger.info("Starting replay from %s to %s", dlq_topic, pending_topic)
            if event_ids:
                logger.info("Filtering for IDs: %s", event_ids)

            # Consume all messages from DLQ
            async for record in consumer:
                try:
                    # Parse DLQ message
                    failed_message = schema_class.model_validate_json(record.value)

                    # Get message ID
                    message_id = getattr(failed_message, id_field)

                    # Check filter
                    if not replay_all and event_ids and message_id not in event_id_set:
                        continue

                    # Extract original task
                    original_task = failed_message.original_task

                    # Reset retry count for fresh retry
                    replayed_task = original_task.model_copy(deep=True)
                    replayed_task.retry_count = 0

                    # Send to pending topic
                    await producer.send(
                        pending_topic,
                        key=message_id.encode(),
                        value=replayed_task.model_dump_json().encode(),
                    )

                    replayed_count += 1
                    logger.info(
                        "Replayed %s=%s to %s", id_field, message_id, pending_topic
                    )

                except Exception as e:
                    logger.error(
                        f"Failed to replay message at offset {record.offset}: {e}"
                    )
                    continue

            await producer.flush()
            logger.info("Successfully replayed %s messages", replayed_count)

            return replayed_count

        finally:
            await producer.stop()
            await consumer.stop()

    async def purge_dlq(
        self,
        dlq_type: str,
        confirm: bool = False,
    ) -> bool:
        """
        Purge all messages from a DLQ.

        WARNING: This permanently deletes messages. Use with caution.

        Args:
            dlq_type: Type of DLQ ("enrichment" or "download")
            confirm: Whether user has confirmed the operation

        Returns:
            True if purge was successful, False if cancelled
        """
        if dlq_type == "enrichment":
            topic = self.enrichment_dlq_topic
        elif dlq_type == "download":
            topic = self.download_dlq_topic
        else:
            raise ValueError(
                f"Invalid DLQ type: {dlq_type}. Must be 'enrichment' or 'download'"
            )

        if not confirm:
            response = input(
                f"\nWARNING: This will permanently delete all messages from {topic}.\n"
                f"Are you sure you want to continue? (yes/no): "
            )
            if response.lower() not in ("yes", "y"):
                print("Purge cancelled.")
                return False

        consumer_config = self._get_consumer_config()
        consumer = AIOKafkaConsumer(topic, **consumer_config)

        try:
            await consumer.start()

            # Get all partitions
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                logger.warning("Topic %s not found or has no partitions", topic)
                return False

            topic_partitions = [TopicPartition(topic, p) for p in partitions]

            for tp in topic_partitions:
                await consumer.seek_to_end(tp)

            # Commit the positions (this marks messages as consumed)
            await consumer.commit()

            logger.info("Successfully purged all messages from %s", topic)
            return True

        finally:
            await consumer.stop()


async def cmd_list(args: argparse.Namespace) -> int:
    """Execute list command."""
    config = MessageConfig.from_env()
    manager = DLQManager(config)

    print("\n📊 ClaimX DLQ Message Counts\n")

    counts = await manager.list_dlq_counts()

    for label, data in counts.items():
        if "error" in data:
            print(f"{label}:")
            print(f"  Topic: {data['topic']}")
            print(f"  Error: {data['error']}\n")
        else:
            print(f"{label}:")
            print(f"  Topic: {data['topic']}")
            print(f"  Messages: {data['count']:,}")
            print(f"  Partitions: {data['partitions']}\n")

    return 0


async def cmd_inspect(args: argparse.Namespace) -> int:
    """Execute inspect command."""
    config = MessageConfig.from_env()
    manager = DLQManager(config)

    print(f"\n🔍 Inspecting {args.dlq_type.capitalize()} DLQ (limit: {args.limit})\n")

    messages = await manager.inspect_dlq(args.dlq_type, limit=args.limit)

    if not messages:
        print("No messages found in DLQ.")
        return 0

    for i, msg in enumerate(messages, 1):
        print(f"Message #{i}:")
        print(json.dumps(msg, indent=2))
        print()

    print(f"Total messages inspected: {len(messages)}")

    return 0


async def cmd_replay(args: argparse.Namespace) -> int:
    """Execute replay command."""
    config = MessageConfig.from_env()
    manager = DLQManager(config)

    event_ids = args.event_ids.split(",") if args.event_ids else None

    if args.all:
        print(f"\n🔄 Replaying ALL messages from {args.dlq_type} DLQ...")
    elif event_ids:
        print(f"\n🔄 Replaying specific messages from {args.dlq_type} DLQ:")
        print(f"   IDs: {event_ids}")
    else:
        print("Error: Must specify either --all or --event-ids")
        return 1

    count = await manager.replay_messages(
        dlq_type=args.dlq_type,
        event_ids=event_ids,
        replay_all=args.all,
    )

    print(f"\n✅ Successfully replayed {count} message(s)")

    return 0


async def cmd_purge(args: argparse.Namespace) -> int:
    """Execute purge command."""
    config = MessageConfig.from_env()
    manager = DLQManager(config)

    print(f"\n🗑️  Purging {args.dlq_type.capitalize()} DLQ...")

    success = await manager.purge_dlq(
        dlq_type=args.dlq_type,
        confirm=args.yes,
    )

    if success:
        print("✅ DLQ purged successfully")
        return 0
    else:
        print("❌ Purge cancelled or failed")
        return 1


def main():
    """Main CLI entry point."""
    # Load environment variables from .env file before any config access
    load_dotenv(PROJECT_ROOT / ".env")

    parser = argparse.ArgumentParser(
        description="ClaimX DLQ Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # List DLQ message counts
    python -m pipeline.claimx.dlq.cli list

    # Inspect enrichment DLQ
    python -m pipeline.claimx.dlq.cli inspect enrichment --limit 5

    # Replay specific messages
    python -m pipeline.claimx.dlq.cli replay download --event-ids media_123,media_456

    # Replay all download DLQ messages
    python -m pipeline.claimx.dlq.cli replay download --all

    # Purge enrichment DLQ
    python -m pipeline.claimx.dlq.cli purge enrichment
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # List command
    parser_list = subparsers.add_parser("list", help="List DLQ message counts")
    parser_list.set_defaults(func=cmd_list)

    # Inspect command
    parser_inspect = subparsers.add_parser("inspect", help="Inspect DLQ messages")
    parser_inspect.add_argument(
        "dlq_type", choices=["enrichment", "download"], help="Which DLQ to inspect"
    )
    parser_inspect.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of messages to retrieve (default: 10)",
    )
    parser_inspect.set_defaults(func=cmd_inspect)

    # Replay command
    parser_replay = subparsers.add_parser(
        "replay", help="Replay DLQ messages to pending topic"
    )
    parser_replay.add_argument(
        "dlq_type", choices=["enrichment", "download"], help="Which DLQ to replay from"
    )
    parser_replay.add_argument(
        "--event-ids",
        type=str,
        help="Comma-separated list of event/media IDs to replay",
    )
    parser_replay.add_argument(
        "--all", action="store_true", help="Replay all messages in DLQ"
    )
    parser_replay.set_defaults(func=cmd_replay)

    # Purge command
    parser_purge = subparsers.add_parser("purge", help="Purge all messages from DLQ")
    parser_purge.add_argument(
        "dlq_type", choices=["enrichment", "download"], help="Which DLQ to purge"
    )
    parser_purge.add_argument(
        "--yes", action="store_true", help="Skip confirmation prompt"
    )
    parser_purge.set_defaults(func=cmd_purge)

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Run command
    try:
        exit_code = asyncio.run(args.func(args))
        return exit_code
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        return 130
    except Exception as e:
        logger.error("Error: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
