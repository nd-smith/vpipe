#!/usr/bin/env python3
"""
Inject a ClaimX event for testing purposes.

Usage:
    python scripts/inject_claimx_event.py --type PROJECT_CREATED --project-id 12345
"""

import argparse
import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

from kafka_pipeline.config import KafkaConfig, get_config
from kafka_pipeline.common.producer import BaseKafkaProducer
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inject_event")


async def inject_event(event_type: str, project_id: str, payload_items: dict = None):
    # Load config
    config = get_config()
    
    # Initialize producer
    producer = BaseKafkaProducer(
        config=config,
        domain="claimx",
        worker_name="injector",
    )
    await producer.start()

    try:
        # Create event payload
        event_id = f"evt_{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc)
        
        # Raw data payload simulation
        raw_data = {
            "projectId": int(project_id) if project_id.isdigit() else project_id,
            "eventType": event_type,
            "timestamp": now.isoformat(),
            **(payload_items or {})
        }

        # Create message object
        message = ClaimXEventMessage(
            event_id=event_id,
            event_type=event_type,
            project_id=str(project_id),
            ingested_at=now,
            raw_data=raw_data
        )

        # Get topic name
        topic = config.get_topic("claimx", "events")
        
        # Send message
        logger.info(f"Injecting {event_type} event to topic {topic}...")
        logger.info(f"Event ID: {event_id}")
        logger.info(f"Project ID: {project_id}")
        
        await producer.send_message(
            topic=topic,
            key=project_id,
            value=message.model_dump(mode="json"),
        )
        logger.info("Event injected successfully!")

    finally:
        await producer.stop()


def main():
    parser = argparse.ArgumentParser(description="Inject ClaimX test event")
    parser.add_argument("--type", default="PROJECT_CREATED", help="Event type")
    parser.add_argument("--project-id", required=True, help="Project ID")
    parser.add_argument("--payload", type=json.loads, default={}, help="Additional JSON payload")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(inject_event(args.type, args.project_id, args.payload))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Injection failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
