"""iTel Cabinet Processing Pipeline."""

import json
import logging
from datetime import UTC, datetime

from pipeline.plugins.shared.connections import (
    ConnectionManager,
    is_http_error,
)

from .models import CabinetAttachment, CabinetSubmission, ProcessedTask, TaskEvent
from .parsers import get_readable_report, parse_cabinet_attachments, parse_cabinet_form

logger = logging.getLogger(__name__)


class ItelCabinetPipeline:
    """Processing pipeline for iTel Cabinet task events.

    Responsibilities:
    1. Parse and validate incoming events
    2. Enrich completed tasks with ClaimX data
    3. Write to Delta tables (always)
    4. Publish to API worker topic (when completed)
    """

    def __init__(
        self,
        connection_manager: ConnectionManager,
        submissions_writer,  # ItelSubmissionsDeltaWriter
        attachments_writer,  # ItelAttachmentsDeltaWriter
        producer,
        config: dict,
    ):
        self.connections = connection_manager
        self.submissions_writer = submissions_writer
        self.attachments_writer = attachments_writer
        self.producer = producer
        self.config = config
        self.claimx_connection = config.get("claimx_connection", "claimx_api")
        self.output_topic = config.get("output_topic", "pcesdopodappv1-itel-cabinet-completed")

        logger.info(
            "ItelCabinetPipeline initialized",
            extra={
                "claimx_connection": self.claimx_connection,
                "output_topic": self.output_topic,
            },
        )

    async def process(self, raw_message: dict) -> ProcessedTask:
        """Main processing flow:
        1. Parse and validate event
        2. Conditionally enrich (COMPLETED tasks only)
        3. Write to Delta (always)
        4. Publish to API worker (COMPLETED tasks only)
        """
        event = TaskEvent.from_message(raw_message)
        logger.info(
            "Processing iTel cabinet event",
            extra={
                "event_id": event.event_id,
                "assignment_id": event.assignment_id,
                "task_status": event.task_status,
            },
        )

        self._validate_event(event)
        if event.task_status == "COMPLETED":
            submission, attachments, readable_report = await self._enrich_completed_task(event)
        else:
            submission, attachments, readable_report = None, [], None
            logger.debug(
                "Skipping enrichment for non-completed task",
                extra={"task_status": event.task_status},
            )

        await self._write_to_delta(event, submission, attachments)
        if event.task_status == "COMPLETED" and submission:
            await self._publish_for_api(event, submission, attachments, readable_report)

        return ProcessedTask(
            event=event,
            submission=submission,
            attachments=attachments,
            readable_report=readable_report,
        )

    def _validate_event(self, event: TaskEvent) -> None:
        """Validate task_status is a known value."""
        valid_statuses = ["ASSIGNED", "IN_PROGRESS", "COMPLETED"]
        if event.task_status not in valid_statuses:
            raise ValueError(
                f"Invalid task_status: {event.task_status}. Expected one of: {valid_statuses}"
            )

        logger.debug("Event validation passed")

    async def _enrich_completed_task(
        self,
        event: TaskEvent,
    ) -> tuple[CabinetSubmission, list[CabinetAttachment], dict]:
        """Fetch and parse completed task:
        1. Fetch task from ClaimX API
        2. Fetch project media for URL lookup
        3. Parse form, attachments, and readable report
        """
        logger.info("Enriching completed task", extra={"assignment_id": event.assignment_id})

        task_data = await self._fetch_claimx_assignment(event.assignment_id)
        project_id = int(task_data.get("projectId", event.project_id))
        media_url_map = await self._fetch_project_media_urls(project_id)

        submission = parse_cabinet_form(task_data, event.event_id)

        # Parse attachments with URL enrichment
        attachments = parse_cabinet_attachments(
            task_data,
            event.assignment_id,
            project_id,
            event.event_id,
            media_url_map,
        )

        readable_report = get_readable_report(task_data, event.event_id, media_url_map)

        logger.info(
            "Task enriched successfully",
            extra={
                "assignment_id": event.assignment_id,
                "attachment_count": len(attachments),
            },
        )

        return submission, attachments, readable_report

    async def _fetch_claimx_assignment(self, assignment_id: int) -> dict:
        endpoint = f"/customTasks/assignment/{assignment_id}"

        logger.debug("Fetching assignment from ClaimX", extra={"assignment_id": assignment_id})

        status, response = await self.connections.request_json(
            connection_name=self.claimx_connection,
            method="GET",
            path=endpoint,
            params={"full": "true"},
        )

        if is_http_error(status):
            raise RuntimeError(f"ClaimX API returned error status {status}: {response}")

        return response

    async def _fetch_project_media_urls(self, project_id: int) -> dict[int, str]:
        """Build media_id -> download URL lookup map from ClaimX API."""
        endpoint = f"/export/project/{project_id}/media"

        logger.debug("Fetching all project media", extra={"project_id": project_id})

        status, response = await self.connections.request_json(
            connection_name=self.claimx_connection,
            method="GET",
            path=endpoint,
            params={},  # No mediaIds param = get all media
        )

        if is_http_error(status):
            logger.warning(
                "Failed to fetch project media: HTTP %s",
                status,
                extra={"project_id": project_id, "status": status},
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
            "Fetched project media URLs",
            extra={
                "project_id": project_id,
                "total_media": len(media_list),
                "with_urls": len(media_url_map),
            },
        )

        # Resolve ClaimX URLs to S3 pre-signed URLs
        resolved_media_url_map = {}
        for media_id, claimx_url in media_url_map.items():
            s3_url = await self._resolve_redirect_url(claimx_url)
            resolved_media_url_map[media_id] = s3_url

        logger.info(
            "Resolved %d media URLs to S3",
            len(resolved_media_url_map),
            extra={"project_id": project_id},
        )

        return resolved_media_url_map

    def _is_s3_url(self, url: str) -> bool:
        """
        Check if URL is already an S3 URL.

        S3 URLs can have various formats:
        - https://s3.amazonaws.com/bucket/key
        - https://bucket.s3.amazonaws.com/key
        - https://bucket.s3.region.amazonaws.com/key

        Args:
            url: URL to check

        Returns:
            True if URL is an S3 URL, False otherwise
        """
        if not url:
            return False

        url_lower = url.lower()
        return (
            "s3.amazonaws.com" in url_lower
            or "s3-" in url_lower  # Handles s3-region.amazonaws.com patterns
        )

    async def _resolve_redirect_url(self, claimx_url: str) -> str:
        """
        Follow 302 redirect from ClaimX URL to get S3 pre-signed URL.

        ClaimX media URLs require auth and redirect to S3 - we need the final S3 URL
        since receivers won't have ClaimX auth.

        Some files (large mp4/mov) already link directly to S3, so we skip redirect
        resolution for those.

        Args:
            claimx_url: ClaimX media download URL or direct S3 URL

        Returns:
            S3 pre-signed URL from Location header, or original URL if already S3 or redirect fails
        """
        if self._is_s3_url(claimx_url):
            logger.debug(
                "URL is already S3, skipping redirect resolution",
                extra={"url_prefix": claimx_url[:80]},
            )
            return claimx_url

        try:
            response = await self.connections.request_url(
                connection_name=self.claimx_connection,
                method="HEAD",
                url=claimx_url,
                allow_redirects=False,
                timeout_override=10,
            )
            if response.status in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                if location:
                    logger.debug(
                        "Resolved ClaimX URL to S3",
                        extra={
                            "status": response.status,
                            "claimx_url_prefix": claimx_url[:80],
                            "s3_url_prefix": location[:80],
                        },
                    )
                    return location

            logger.warning(
                "No redirect found for ClaimX URL (status %s)",
                response.status,
                extra={"url_prefix": claimx_url[:80], "status": response.status},
            )
            return claimx_url

        except Exception as e:
            logger.warning(
                "Failed to resolve redirect URL: %s",
                e,
                extra={"url_prefix": claimx_url[:80]},
            )
            return claimx_url

    async def _write_to_delta(
        self,
        event: TaskEvent,
        submission: CabinetSubmission | None,
        attachments: list[CabinetAttachment],
    ) -> None:
        logger.info("Writing to Delta tables", extra={"assignment_id": event.assignment_id})
        submission_row = submission.to_dict() if submission else self._build_metadata_row(event)

        await self.submissions_writer.write(submission_row)
        if attachments:
            attachment_rows = [att.to_dict() for att in attachments]
            await self.attachments_writer.write(attachment_rows)

        logger.info(
            "Delta write complete",
            extra={
                "assignment_id": event.assignment_id,
                "has_submission": submission is not None,
                "attachment_count": len(attachments),
            },
        )

    def _build_metadata_row(self, event: TaskEvent) -> dict:
        """Build minimal submission row for non-completed statuses."""
        now = datetime.now(UTC)

        return {
            "assignment_id": event.assignment_id,
            "project_id": event.project_id,
            "task_id": event.task_id,
            "task_name": event.task_name,
            "task_status": event.task_status,
            "event_id": event.event_id,
            "event_type": event.event_type,
            "event_timestamp": event.event_timestamp,
            "assigned_to_user_id": event.assigned_to_user_id,
            "assigned_by_user_id": event.assigned_by_user_id,
            "task_created_at": event.task_created_at,
            "task_completed_at": event.task_completed_at,
            "updated_at": now.isoformat(),
            "form_id": None,
            "customer_first_name": None,
            "customer_last_name": None,
        }

    async def _publish_for_api(
        self,
        event: TaskEvent,
        submission: CabinetSubmission,
        attachments: list[CabinetAttachment],
        readable_report: dict | None,
    ) -> None:
        logger.info(
            "Publishing to API worker topic",
            extra={
                "assignment_id": event.assignment_id,
                "topic": self.output_topic,
            },
        )

        payload = {
            "event_id": event.event_id,
            "event_timestamp": event.event_timestamp,
            "assignment_id": event.assignment_id,
            "project_id": event.project_id,
            "task_id": event.task_id,
            "submission": submission.to_dict(),
            "attachments": [att.to_dict() for att in attachments],
            "readable_report": readable_report,
            "published_at": datetime.now(UTC).isoformat(),
            "source": "itel_cabinet_tracking_worker",
        }
        await self.producer.send(
            topic=self.output_topic,
            value=json.dumps(payload).encode("utf-8"),
            key=event.event_id.encode("utf-8"),
        )

        logger.info(
            "Published to API worker successfully",
            extra={"assignment_id": event.assignment_id},
        )
