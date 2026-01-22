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
        2. Fetch project export for project metadata
        3. Fetch project media and filter to claim_media_ids
        4. Parse into flat submission structure
        """
        logger.info(
            "Enriching mitigation task",
            extra={'assignment_id': event.assignment_id}
        )

        # Fetch task assignment data
        task_data = await self._fetch_claimx_assignment(event.assignment_id)

        # Fetch project export data
        project_id = int(event.project_id)
        project_data = await self._fetch_project_export(project_id)

        # Fetch all project media
        media_list = await self._fetch_project_media(project_id)

        # Parse submission with all enriched data
        submission = self._parse_submission(task_data, event, project_data, media_list)

        logger.info(
            "Task enriched successfully",
            extra={
                'assignment_id': event.assignment_id,
                'media_count': len(media_list),
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

    async def _fetch_project_export(self, project_id: int) -> dict:
        """Fetch project export data from ClaimX API."""
        endpoint = f"/export/project/{project_id}"

        logger.debug("Fetching project export from ClaimX", extra={'project_id': project_id})

        status, response = await self.connections.request_json(
            connection_name=self.claimx_connection,
            method='GET',
            path=endpoint,
            params={},
        )

        if status < 200 or status >= 300:
            logger.warning(
                f"Failed to fetch project export: HTTP {status}",
                extra={'project_id': project_id, 'status': status}
            )
            return {}

        return response

    async def _fetch_project_media(self, project_id: int) -> list[dict]:
        """Fetch all project media from ClaimX API."""
        endpoint = f"/export/project/{project_id}/media"

        logger.debug("Fetching project media from ClaimX", extra={'project_id': project_id})

        status, response = await self.connections.request_json(
            connection_name=self.claimx_connection,
            method='GET',
            path=endpoint,
            params={},
        )

        if status < 200 or status >= 300:
            logger.warning(
                f"Failed to fetch project media: HTTP {status}",
                extra={'project_id': project_id, 'status': status}
            )
            return []

        # Handle different response formats
        if isinstance(response, list):
            media_list = response
        elif isinstance(response, dict):
            if "data" in response:
                media_list = response["data"]
            elif "media" in response:
                media_list = response["media"]
            else:
                media_list = []
        else:
            media_list = []

        logger.info(
            "Fetched project media",
            extra={
                'project_id': project_id,
                'media_count': len(media_list),
            }
        )

        return media_list

    def _parse_submission(
        self,
        task_data: dict,
        event: MitigationTaskEvent,
        project_data: dict,
        media_list: list[dict],
    ) -> MitigationSubmission:
        """Parse task data into flat MitigationSubmission."""
        # Get dates from API response (not from event)
        date_assigned = task_data.get("dateAssigned")
        date_completed = task_data.get("dateCompleted")

        # Extract project data from export/project response
        project = project_data.get("data", {}).get("project", {})
        master_filename = project.get("masterFilename")
        type_of_loss = project.get("typeOfLoss")
        claim_number = project.get("projectNumber")
        policy_number = project.get("secondaryNumber")

        return MitigationSubmission(
            event_id=event.event_id,
            assignment_id=event.assignment_id,
            project_id=event.project_id,
            task_id=event.task_id,
            task_name=event.task_name,
            status=event.task_status,
            master_filename=master_filename,
            type_of_loss=type_of_loss,
            claim_number=claim_number,
            policy_number=policy_number,
            date_assigned=date_assigned,
            date_completed=date_completed,
            media=media_list,
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
