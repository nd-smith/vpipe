"""ClaimX Mitigation Task Processing Pipeline."""

import json
import logging
from datetime import datetime

from kafka_pipeline.plugins.shared.connections import ConnectionManager

from .models import (
    MitigationTaskEvent,
    MitigationSubmission,
    ProcessedMitigationTask,
)

logger = logging.getLogger(__name__)

# Valid task IDs for this plugin
VALID_TASK_IDS = {25367, 24454}


class MitigationTaskPipeline:
    """Processing pipeline for ClaimX Mitigation task events.

    Responsibilities:
    1. Parse and validate incoming events
    2. Enrich completed tasks with ClaimX data
    3. Publish to success topic
    """

    def __init__(
        self,
        connection_manager: ConnectionManager,
        kafka_producer,
        config: dict,
    ):
        self.connections = connection_manager
        self.kafka = kafka_producer
        self.config = config
        self.claimx_connection = config.get('claimx_connection', 'claimx_api')
        self.output_topic = config.get('output_topic', 'claimx.mitigation.task.success')

        logger.info(
            "MitigationTaskPipeline initialized",
            extra={
                'claimx_connection': self.claimx_connection,
                'output_topic': self.output_topic,
            }
        )

    async def process(self, raw_message: dict) -> ProcessedMitigationTask:
        """Main processing flow:
        1. Parse and validate event
        2. Enrich with ClaimX data
        3. Publish to success topic
        """
        event = MitigationTaskEvent.from_kafka_message(raw_message)
        logger.info(
            "Processing mitigation task event",
            extra={
                'event_id': event.event_id,
                'assignment_id': event.assignment_id,
                'task_id': event.task_id,
                'task_status': event.task_status,
            }
        )

        self._validate_event(event)

        # Enrich completed task with ClaimX API data
        submission = await self._enrich_task(event)

        # Publish to success topic
        await self._publish_to_success(submission)

        return ProcessedMitigationTask(
            event=event,
            submission=submission,
        )

    def _validate_event(self, event: MitigationTaskEvent) -> None:
        """Validate business rules:
        - task_id is 25367 or 24454
        - task_status is COMPLETED
        """
        if event.task_id not in VALID_TASK_IDS:
            raise ValueError(
                f"Invalid task_id: {event.task_id}. "
                f"Expected one of: {VALID_TASK_IDS}"
            )

        if event.task_status != 'COMPLETED':
            raise ValueError(
                f"Invalid task_status: {event.task_status}. "
                f"Expected: COMPLETED"
            )

        logger.debug("Event validation passed")

    async def _enrich_task(
        self,
        event: MitigationTaskEvent,
    ) -> MitigationSubmission:
        """Fetch and parse task data from ClaimX API.

        1. Fetch task assignment from ClaimX
        2. Parse into flat submission structure
        """
        logger.info(
            "Enriching mitigation task",
            extra={'assignment_id': event.assignment_id}
        )

        task_data = await self._fetch_claimx_assignment(event.assignment_id)
        submission = self._parse_submission(task_data, event)

        logger.info(
            "Task enriched successfully",
            extra={
                'assignment_id': event.assignment_id,
                'claim_media_ids_count': len(submission.claim_media_ids or []),
            }
        )

        return submission

    async def _fetch_claimx_assignment(self, assignment_id: int) -> dict:
        """Fetch assignment data from ClaimX API."""
        endpoint = f"/customTasks/assignment/{assignment_id}"

        logger.debug(f"Fetching assignment from ClaimX", extra={'assignment_id': assignment_id})

        status, response = await self.connections.request_json(
            connection_name=self.claimx_connection,
            method='GET',
            path=endpoint,
            params={'full': 'true'},
        )

        if status < 200 or status >= 300:
            raise Exception(
                f"ClaimX API returned error status {status}: {response}"
            )

        return response

    def _parse_submission(
        self,
        task_data: dict,
        event: MitigationTaskEvent,
    ) -> MitigationSubmission:
        """Parse task data into flat MitigationSubmission."""
        # Extract form data
        form_response = task_data.get("formResponse", {})
        form_id = form_response.get("formId", "")
        form_response_id = form_response.get("_id", "")

        # Get dates from API response (not from event)
        date_assigned = task_data.get("dateAssigned")
        date_completed = task_data.get("dateCompleted")

        # Extract claimMediaIds array
        claim_media_ids = task_data.get("claimMediaIds", [])

        return MitigationSubmission(
            event_id=event.event_id,
            assignment_id=event.assignment_id,
            project_id=event.project_id,
            task_id=event.task_id,
            task_name=event.task_name,
            status=event.task_status,
            form_id=form_id,
            form_response_id=form_response_id,
            date_assigned=date_assigned,
            date_completed=date_completed,
            claim_media_ids=claim_media_ids,
            ingested_at=datetime.utcnow().isoformat(),
        )

    async def _publish_to_success(
        self,
        submission: MitigationSubmission,
    ) -> None:
        """Publish flat enriched data to success topic."""
        logger.info(
            "Publishing to success topic",
            extra={
                'assignment_id': submission.assignment_id,
                'topic': self.output_topic,
            }
        )

        # Flat structure - all fields at top level
        payload = submission.to_flat_dict()
        payload['published_at'] = datetime.utcnow().isoformat()
        payload['source'] = 'mitigation_tracking_worker'

        await self.kafka.send(
            topic=self.output_topic,
            value=json.dumps(payload).encode('utf-8'),
            key=submission.event_id.encode('utf-8'),
        )

        logger.info(
            "Published to success topic successfully",
            extra={'assignment_id': submission.assignment_id}
        )
