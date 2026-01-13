"""
iTel Cabinet Processing Pipeline.

Clear, explicit flow for processing iTel cabinet task events.
No magic, no abstraction - just readable code.
"""

import json
import logging
from datetime import datetime
from typing import Optional

from kafka_pipeline.plugins.shared.connections import ConnectionManager

from .models import TaskEvent, CabinetSubmission, CabinetAttachment, ProcessedTask
from .parsers import parse_cabinet_form, parse_cabinet_attachments, get_readable_report
from .media_downloader import MediaDownloader

logger = logging.getLogger(__name__)


class ItelCabinetPipeline:
    """
    Processing pipeline for iTel Cabinet task events.

    Responsibilities:
    1. Parse and validate incoming events
    2. Enrich completed tasks with ClaimX data
    3. Write to Delta tables (always)
    4. Publish to API worker topic (when completed)

    Clear top-to-bottom flow - easy to trace and debug.
    """

    def __init__(
        self,
        connection_manager: ConnectionManager,
        delta_writer,  # ItelCabinetDeltaWriter
        kafka_producer,
        config: dict,
        onelake_attachments_path: Optional[str] = None,
    ):
        """
        Initialize pipeline with dependencies.

        Args:
            connection_manager: For ClaimX API calls
            delta_writer: For writing to Delta tables (ItelCabinetDeltaWriter)
            kafka_producer: For publishing to downstream topics
            config: Pipeline configuration
            onelake_attachments_path: OneLake base path for media uploads (from env)
        """
        self.connections = connection_manager
        self.delta = delta_writer
        self.kafka = kafka_producer
        self.config = config

        # Configuration settings
        self.claimx_connection = config.get('claimx_connection', 'claimx_api')
        self.output_topic = config.get('output_topic', 'itel.cabinet.completed')
        self.download_media = config.get('download_media', False)

        # Media downloader (lazy initialized)
        self.onelake_attachments_path = onelake_attachments_path
        self._media_downloader: Optional[MediaDownloader] = None

        logger.info(
            "ItelCabinetPipeline initialized",
            extra={
                'claimx_connection': self.claimx_connection,
                'output_topic': self.output_topic,
                'download_media': self.download_media,
                'has_onelake_path': onelake_attachments_path is not None,
            }
        )

    async def process(self, raw_message: dict) -> ProcessedTask:
        """
        Main processing flow - read this to understand the entire pipeline.

        Flow:
        1. Parse event from Kafka message
        2. Validate business rules
        3. Conditionally enrich (only COMPLETED tasks)
        4. Write to Delta tables (always)
        5. Publish to API worker (only COMPLETED tasks)

        Args:
            raw_message: Raw message from Kafka

        Returns:
            ProcessedTask with all enriched data

        Raises:
            ValueError: If validation fails
            Exception: If processing fails
        """
        # Step 1: Parse and validate
        event = TaskEvent.from_kafka_message(raw_message)
        logger.info(
            "Processing iTel cabinet event",
            extra={
                'event_id': event.event_id,
                'assignment_id': event.assignment_id,
                'task_status': event.task_status,
            }
        )

        self._validate_event(event)

        # Step 2: Conditionally enrich (only for COMPLETED status)
        if event.task_status == 'COMPLETED':
            submission, attachments, readable_report = await self._enrich_completed_task(event)
        else:
            submission, attachments, readable_report = None, [], None
            logger.debug(
                f"Skipping enrichment for non-completed task",
                extra={'task_status': event.task_status}
            )

        # Step 3: Write to Delta tables (always - full or metadata-only)
        await self._write_to_delta(event, submission, attachments)

        # Step 4: Publish to API worker topic (only for COMPLETED)
        if event.task_status == 'COMPLETED' and submission:
            await self._publish_for_api(event, submission, attachments, readable_report)

        return ProcessedTask(
            event=event,
            submission=submission,
            attachments=attachments,
            readable_report=readable_report,
        )

    def _validate_event(self, event: TaskEvent) -> None:
        """
        Validate business rules for iTel cabinet tasks.

        Rules:
        - Must be task_id 32513 (iTel Cabinet Repair Form)
        - Must have valid task_status

        Args:
            event: Parsed event

        Raises:
            ValueError: If validation fails
        """
        # Check task ID
        if event.task_id != 32513:
            raise ValueError(
                f"Invalid task_id: {event.task_id}. "
                f"Expected 32513 (iTel Cabinet Repair Form)"
            )

        # Check task status
        valid_statuses = ['ASSIGNED', 'IN_PROGRESS', 'COMPLETED']
        if event.task_status not in valid_statuses:
            raise ValueError(
                f"Invalid task_status: {event.task_status}. "
                f"Expected one of: {valid_statuses}"
            )

        logger.debug("Event validation passed")

    async def _enrich_completed_task(
        self,
        event: TaskEvent,
    ) -> tuple[CabinetSubmission, list[CabinetAttachment], dict]:
        """
        Fetch and parse full task data for completed tasks.

        Flow:
        1. Fetch task details from ClaimX API
        2. Parse cabinet form data
        3. Parse attachments
        4. Generate readable report for API consumption
        5. Optionally download media files

        Args:
            event: Task event

        Returns:
            Tuple of (submission, attachments, readable_report)

        Raises:
            Exception: If ClaimX API call or parsing fails
        """
        logger.info(
            "Enriching completed task",
            extra={'assignment_id': event.assignment_id}
        )

        # Fetch from ClaimX API
        task_data = await self._fetch_claimx_assignment(event.assignment_id)

        # Parse form data
        submission = parse_cabinet_form(task_data, event.event_id)
        # Get project_id from task_data (as int for delta schema)
        project_id = int(task_data.get("projectId", event.project_id))
        attachments = parse_cabinet_attachments(
            task_data,
            event.assignment_id,
            project_id,
            event.event_id,
        )

        # Generate readable report for API consumption
        readable_report = get_readable_report(task_data, event.event_id)

        logger.info(
            "Task enriched successfully",
            extra={
                'assignment_id': event.assignment_id,
                'attachment_count': len(attachments),
            }
        )

        # Download media files if configured
        if self.download_media and attachments:
            if not self.onelake_attachments_path:
                logger.warning(
                    "Media download enabled but ITEL_ATTACHMENTS_PATH not configured - skipping downloads"
                )
            else:
                attachments = await self._download_media_files(
                    attachments, project_id, event.assignment_id
                )

        return submission, attachments, readable_report

    async def _fetch_claimx_assignment(self, assignment_id: int) -> dict:
        """
        Fetch assignment details from ClaimX API.

        Args:
            assignment_id: Assignment ID to fetch

        Returns:
            Assignment data from API

        Raises:
            Exception: If API call fails
        """
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

    async def _download_media_files(
        self,
        attachments: list[CabinetAttachment],
        project_id: int,
        assignment_id: int,
    ) -> list[CabinetAttachment]:
        """
        Download media files for attachments and upload to OneLake.

        Creates MediaDownloader on first call (lazy initialization).
        Continues on partial failures - logs errors but returns all attachments.

        Args:
            attachments: Attachments to download media for
            project_id: Project ID for path construction
            assignment_id: Assignment ID for path construction

        Returns:
            Attachments with blob_path updated for successful downloads
        """
        if not attachments:
            return attachments

        logger.info(
            "Downloading media files",
            extra={
                'assignment_id': assignment_id,
                'attachment_count': len(attachments),
            }
        )

        # Lazy initialize media downloader
        if self._media_downloader is None:
            self._media_downloader = MediaDownloader(
                connection_manager=self.connections,
                onelake_base_path=self.onelake_attachments_path,
                claimx_connection=self.claimx_connection,
            )

        # Download and upload media
        try:
            async with self._media_downloader:
                updated_attachments = await self._media_downloader.download_and_upload(
                    attachments, project_id, assignment_id
                )

            logger.info(
                "Media download complete",
                extra={
                    'assignment_id': assignment_id,
                    'total': len(attachments),
                    'with_blob_path': sum(1 for a in updated_attachments if a.blob_path),
                }
            )

            return updated_attachments

        except Exception as e:
            logger.error(
                f"Media download failed: {e}",
                extra={'assignment_id': assignment_id},
                exc_info=True,
            )
            # Return original attachments even if download failed
            return attachments

    async def _write_to_delta(
        self,
        event: TaskEvent,
        submission: Optional[CabinetSubmission],
        attachments: list[CabinetAttachment],
    ) -> None:
        """
        Write to Delta tables.

        Writes:
        - Full submission data (if completed)
        - OR metadata-only row (if not completed)
        - Attachments (if any)

        Args:
            event: Task event
            submission: Parsed submission (None if not completed)
            attachments: Parsed attachments
        """
        logger.info(
            "Writing to Delta tables",
            extra={'assignment_id': event.assignment_id}
        )

        # Build submission row (full or metadata-only)
        if submission:
            submission_row = submission.to_dict()
        else:
            submission_row = self._build_metadata_row(event)

        # Write submission
        await self.delta.write_submission(submission_row)

        # Write attachments (if any)
        if attachments:
            attachment_rows = [att.to_dict() for att in attachments]
            await self.delta.write_attachments(attachment_rows)

        logger.info(
            "Delta write complete",
            extra={
                'assignment_id': event.assignment_id,
                'has_submission': submission is not None,
                'attachment_count': len(attachments),
            }
        )

    def _build_metadata_row(self, event: TaskEvent) -> dict:
        """
        Build minimal submission row for non-completed statuses.

        For tracking purposes, we write metadata even if task isn't completed.

        Args:
            event: Task event

        Returns:
            Minimal submission dict
        """
        now = datetime.utcnow()

        return {
            'assignment_id': event.assignment_id,
            'project_id': event.project_id,
            'task_id': event.task_id,
            'task_name': event.task_name,
            'task_status': event.task_status,
            'event_id': event.event_id,
            'event_type': event.event_type,
            'event_timestamp': event.event_timestamp,
            'assigned_to_user_id': event.assigned_to_user_id,
            'assigned_by_user_id': event.assigned_by_user_id,
            'task_created_at': event.task_created_at,
            'task_completed_at': event.task_completed_at,
            'updated_at': now.isoformat(),
            # Form-specific fields will be NULL
            'form_id': None,
            'customer_first_name': None,
            'customer_last_name': None,
        }

    async def _publish_for_api(
        self,
        event: TaskEvent,
        submission: CabinetSubmission,
        attachments: list[CabinetAttachment],
        readable_report: Optional[dict],
    ) -> None:
        """
        Publish to API worker topic.

        Builds payload with submission, attachments, and readable report for iTel API worker.

        Args:
            event: Task event
            submission: Parsed submission
            attachments: Parsed attachments
            readable_report: Topic-organized report for API consumption
        """
        logger.info(
            "Publishing to API worker topic",
            extra={
                'assignment_id': event.assignment_id,
                'topic': self.output_topic,
            }
        )

        # Build payload for API worker
        payload = {
            # Event metadata
            'event_id': event.event_id,
            'event_timestamp': event.event_timestamp,

            # Task identifiers
            'assignment_id': event.assignment_id,
            'project_id': event.project_id,
            'task_id': event.task_id,

            # Parsed data (ready for API transformation)
            'submission': submission.to_dict(),
            'attachments': [att.to_dict() for att in attachments],
            'readable_report': readable_report,  # NEW: Topic-organized format

            # Metadata
            'published_at': datetime.utcnow().isoformat(),
            'source': 'itel_cabinet_tracking_worker',
        }

        # Publish to Kafka - use event_id as key for consistent partitioning
        await self.kafka.send(
            topic=self.output_topic,
            value=json.dumps(payload).encode('utf-8'),
            key=event.event_id.encode('utf-8'),
        )

        logger.info(
            "Published to API worker successfully",
            extra={'assignment_id': event.assignment_id}
        )
