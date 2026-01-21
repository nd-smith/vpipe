"""ClaimX Mitigation Task Processing Pipeline."""

import json
import logging
from datetime import datetime
from typing import Optional

from kafka_pipeline.plugins.shared.connections import ConnectionManager

from .models import (
    MitigationTaskEvent,
    MitigationSubmission,
    MitigationAttachment,
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
        submission, attachments = await self._enrich_task(event)

        # Publish to success topic
        await self._publish_to_success(event, submission, attachments)

        return ProcessedMitigationTask(
            event=event,
            submission=submission,
            attachments=attachments,
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
    ) -> tuple[MitigationSubmission, list[MitigationAttachment]]:
        """Fetch and parse task data from ClaimX API.

        1. Fetch task assignment from ClaimX
        2. Parse form data into submission
        3. Extract attachments with media URLs
        """
        logger.info(
            "Enriching mitigation task",
            extra={'assignment_id': event.assignment_id}
        )

        task_data = await self._fetch_claimx_assignment(event.assignment_id)
        project_id = int(task_data.get("projectId", event.project_id))
        media_url_map = await self._fetch_project_media_urls(project_id)

        # Parse submission and attachments
        submission = self._parse_submission(task_data, event)
        attachments = self._parse_attachments(
            task_data, event, project_id, media_url_map
        )

        logger.info(
            "Task enriched successfully",
            extra={
                'assignment_id': event.assignment_id,
                'attachment_count': len(attachments),
            }
        )

        return submission, attachments

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

    async def _fetch_project_media_urls(self, project_id: int) -> dict[int, str]:
        """Build media_id -> download URL lookup map from ClaimX API."""
        endpoint = f"/export/project/{project_id}/media"

        logger.debug(f"Fetching all project media", extra={'project_id': project_id})

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
            return {}

        if isinstance(response, list):
            media_list = response
        elif isinstance(response, dict):
            if "data" in response:
                media_list = response["data"]
            elif "media" in response:
                media_list = response["media"]
            else:
                media_list = [response]
        else:
            media_list = []

        media_url_map = {}
        for media in media_list:
            media_id = media.get("mediaID")
            download_url = media.get("fullDownloadLink")
            if media_id and download_url:
                media_url_map[media_id] = download_url

        logger.info(
            f"Fetched project media URLs",
            extra={
                'project_id': project_id,
                'total_media': len(media_list),
                'with_urls': len(media_url_map),
            }
        )

        return media_url_map

    def _parse_submission(
        self,
        task_data: dict,
        event: MitigationTaskEvent,
    ) -> MitigationSubmission:
        """Parse task data into MitigationSubmission.

        This is a basic implementation - expand as the model is built out.
        """
        now = datetime.utcnow()

        # Extract form data
        form_response = task_data.get("formResponse", {})
        form_id = form_response.get("formId", "")
        form_response_id = form_response.get("_id", "")

        return MitigationSubmission(
            assignment_id=event.assignment_id,
            project_id=event.project_id,
            form_id=form_id,
            form_response_id=form_response_id,
            status=event.task_status,
            event_id=event.event_id,
            task_id=event.task_id,
            task_name=event.task_name,
            date_assigned=event.task_created_at,
            date_completed=event.task_completed_at,
            ingested_at=now,
            raw_data=json.dumps(task_data),
            created_at=now,
            updated_at=now,
        )

    def _parse_attachments(
        self,
        task_data: dict,
        event: MitigationTaskEvent,
        project_id: int,
        media_url_map: dict[int, str],
    ) -> list[MitigationAttachment]:
        """Extract attachments from task data.

        This is a basic implementation - expand as the model is built out.
        """
        attachments = []
        now = datetime.utcnow()

        form_response = task_data.get("formResponse", {})
        responses = form_response.get("responses", [])

        for response in responses:
            media_list = response.get("media", [])
            control_id = response.get("controlId", "")
            question_key = response.get("questionKey", "")
            question_text = response.get("questionText", "")

            for idx, media in enumerate(media_list):
                media_id = media.get("mediaID") or media.get("mediaId")
                if not media_id:
                    continue

                attachment = MitigationAttachment(
                    assignment_id=event.assignment_id,
                    project_id=project_id,
                    event_id=event.event_id,
                    control_id=control_id,
                    question_key=question_key,
                    question_text=question_text,
                    topic_category="Mitigation",
                    media_id=media_id,
                    url=media_url_map.get(media_id),
                    display_order=idx,
                    created_at=now,
                )
                attachments.append(attachment)

        return attachments

    async def _publish_to_success(
        self,
        event: MitigationTaskEvent,
        submission: MitigationSubmission,
        attachments: list[MitigationAttachment],
    ) -> None:
        """Publish enriched data to success topic."""
        logger.info(
            "Publishing to success topic",
            extra={
                'assignment_id': event.assignment_id,
                'topic': self.output_topic,
            }
        )

        payload = {
            'event_id': event.event_id,
            'event_timestamp': event.event_timestamp,
            'assignment_id': event.assignment_id,
            'project_id': event.project_id,
            'task_id': event.task_id,
            'task_name': event.task_name,
            'submission': submission.to_dict(),
            'attachments': [att.to_dict() for att in attachments],
            'published_at': datetime.utcnow().isoformat(),
            'source': 'mitigation_tracking_worker',
        }

        await self.kafka.send(
            topic=self.output_topic,
            value=json.dumps(payload).encode('utf-8'),
            key=event.event_id.encode('utf-8'),
        )

        logger.info(
            "Published to success topic successfully",
            extra={'assignment_id': event.assignment_id}
        )
