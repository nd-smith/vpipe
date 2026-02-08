"""
Plugin framework base classes.

Provides domain-agnostic plugin infrastructure that works with
both XACT and ClaimX pipelines.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

from pydantic import BaseModel

if TYPE_CHECKING:
    from pipeline.claimx.schemas.entities import EntityRowsMessage


async def resolve_claimx_project_id(
    claim_number: str,
    connection_manager,
    connection_name: str = "claimx_api",
) -> int | None:
    """
    Helper function to resolve ClaimX project ID from claim number.

    Useful for plugins in Verisk domain that need to work with ClaimX but only
    have the claim number available.

    Args:
        claim_number: The claim number (e.g., "ABC123456")
        connection_manager: ConnectionManager instance for API requests
        connection_name: Named connection to use (default: "claimx_api")

    Returns:
        ClaimX project ID if found, None otherwise

    Example:
        from pipeline.plugins.shared.base import resolve_claimx_project_id

        class MyXACTPlugin(Plugin):
            async def execute(self, context: PluginContext) -> PluginResult:
                claim_number = context.message.claim_number

                # Resolve ClaimX project ID
                project_id = await resolve_claimx_project_id(
                    claim_number,
                    self.connection_manager
                )

                if not project_id:
                    return PluginResult.skip("ClaimX project not found")

                # Now create task with the resolved ID
                return PluginResult.create_claimx_task(
                    project_id=project_id,
                    task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
                    task_data={...}
                )
    """
    try:
        response = await connection_manager.request(
            connection_name=connection_name,
            method="GET",
            path="/export/project/projectId",
            params={"projectNumber": claim_number},  # API uses projectNumber param
        )

        if response.status >= 400:
            return None

        response_data = await response.json()

        # API may return just the ID as a number, or in a dict
        if isinstance(response_data, int):
            return response_data
        elif isinstance(response_data, dict):
            return response_data.get("projectId") or response_data.get("id")

    except Exception:
        return None

    return None


class Domain(str, Enum):
    """Pipeline domains."""

    XACT = "xact"
    CLAIMX = "claimx"


class PipelineStage(str, Enum):
    """
    Pipeline stages where plugins can execute.

    Stages map to worker processing points in each domain.
    """

    # Common
    EVENT_INGEST = "event_ingest"

    # XACT-specific
    DOWNLOAD_QUEUED = "download_queued"
    DOWNLOAD_COMPLETE = "download_complete"
    UPLOAD_COMPLETE = "upload_complete"

    # ClaimX-specific
    ENRICHMENT_QUEUED = "enrichment_queued"
    ENRICHMENT_COMPLETE = "enrichment_complete"
    ENTITY_WRITE = "entity_write"

    # Shared
    RETRY = "retry"
    DLQ = "dlq"
    ERROR = "error"


class ActionType(str, Enum):
    """Available plugin action types."""

    PUBLISH_TO_TOPIC = "publish_to_topic"
    HTTP_WEBHOOK = "http_webhook"
    SEND_EMAIL = "send_email"
    CREATE_CLAIMX_TASK = "create_claimx_task"
    LOG = "log"
    ADD_HEADER = "add_header"
    FILTER = "filter"
    METRIC = "metric"


@dataclass
class PluginAction:
    """An action to be executed by a plugin."""

    action_type: ActionType
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class PluginResult:
    """Result returned by plugin execution."""

    success: bool
    actions: list[PluginAction] = field(default_factory=list)
    message: str | None = None
    terminate_pipeline: bool = False

    @classmethod
    def success(cls, message: str = None) -> "PluginResult":
        return cls(success=True, message=message)

    @classmethod
    def skip(cls, reason: str) -> "PluginResult":
        return cls(success=True, message=f"Skipped: {reason}")

    @classmethod
    def publish(
        cls,
        topic: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> "PluginResult":
        return cls(
            success=True,
            actions=[
                PluginAction(
                    action_type=ActionType.PUBLISH_TO_TOPIC,
                    params={
                        "topic": topic,
                        "payload": payload,
                        "headers": headers or {},
                    },
                )
            ],
        )

    @classmethod
    def webhook(
        cls,
        url: str,
        method: str = "POST",
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> "PluginResult":
        return cls(
            success=True,
            actions=[
                PluginAction(
                    action_type=ActionType.HTTP_WEBHOOK,
                    params={
                        "url": url,
                        "method": method,
                        "body": body or {},
                        "headers": headers or {},
                    },
                )
            ],
        )

    @classmethod
    def log(cls, level: str, message: str) -> "PluginResult":
        return cls(
            success=True,
            actions=[
                PluginAction(
                    action_type=ActionType.LOG,
                    params={"level": level, "message": message},
                )
            ],
        )

    @classmethod
    def filter_out(cls, reason: str) -> "PluginResult":
        return cls(
            success=True,
            terminate_pipeline=True,
            message=f"Filtered: {reason}",
            actions=[
                PluginAction(action_type=ActionType.FILTER, params={"reason": reason})
            ],
        )

    @classmethod
    def email(
        cls,
        to: str | list[str],
        subject: str,
        body: str,
        *,
        connection: str = "email_service",
        cc: str | list[str] | None = None,
        bcc: str | list[str] | None = None,
        reply_to: str | None = None,
        html: bool = False,
        template_id: str | None = None,
        template_data: dict[str, Any] | None = None,
    ) -> "PluginResult":
        params = {
            "connection": connection,
            "to": to if isinstance(to, list) else [to],
            "subject": subject,
            "body": body,
            "html": html,
        }
        if cc:
            params["cc"] = cc if isinstance(cc, list) else [cc]
        if bcc:
            params["bcc"] = bcc if isinstance(bcc, list) else [bcc]
        if reply_to:
            params["reply_to"] = reply_to
        if template_id:
            params["template_id"] = template_id
        if template_data:
            params["template_data"] = template_data

        return cls(
            success=True,
            actions=[PluginAction(action_type=ActionType.SEND_EMAIL, params=params)],
        )

    @classmethod
    def create_claimx_task(
        cls,
        task_type: str,
        task_data: dict[str, Any],
        *,
        project_id: int | None = None,
        claim_number: str | None = None,
        connection: str = "claimx_api",
        use_primary_contact_as_sender: bool = True,
        sender_username: str | None = None,
    ) -> "PluginResult":
        if not project_id and not claim_number:
            raise ValueError("Either project_id or claim_number must be provided")

        params = {
            "connection": connection,
            "project_id": project_id,
            "claim_number": claim_number,
            "task_type": task_type,
            "task_data": task_data,
            "use_primary_contact_as_sender": use_primary_contact_as_sender,
            "sender_username": sender_username,
        }

        return cls(
            success=True,
            actions=[
                PluginAction(action_type=ActionType.CREATE_CLAIMX_TASK, params=params)
            ],
        )


@dataclass
class PluginContext:
    """
    Context passed to plugins during execution.

    Domain-agnostic: works with any message type via the generic
    'message' field and domain-specific data in 'data' dict.
    """

    # Domain identification
    domain: Domain
    stage: PipelineStage

    # The raw message (varies by domain/stage)
    message: BaseModel

    # Common identifiers extracted for convenience
    event_id: str
    event_type: str | None = None
    project_id: str | None = None

    # Domain-specific processed data
    # ClaimX: {"entities": EntityRowsMessage, "handler_result": HandlerResult}
    # XACT: {"file_metadata": {...}, "download_result": {...}}
    data: dict[str, Any] = field(default_factory=dict)

    # Mutable headers (plugins can add)
    headers: dict[str, str] = field(default_factory=dict)

    # Error context (for ERROR/RETRY stages)
    error: Exception | None = None
    retry_count: int = 0

    # Timestamp
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def get_claimx_entities(self) -> Optional["EntityRowsMessage"]:
        """Get ClaimX EntityRowsMessage if available."""
        return self.data.get("entities")

    def get_tasks(self) -> list[dict[str, Any]]:
        """Get task rows from ClaimX entities."""
        entities = self.get_claimx_entities()
        if entities:
            return entities.tasks
        return []

    def get_first_task(self) -> dict[str, Any] | None:
        """Get first task row if available."""
        tasks = self.get_tasks()
        return tasks[0] if tasks else None


class Plugin(ABC):
    """
    Base class for plugins.

    Subclasses define:
      - Which domains/stages/event_types they handle
      - The execute() method with business logic
    """

    # Plugin metadata
    name: str = "unnamed_plugin"

    # Filtering: which contexts trigger this plugin
    # Empty list = no filter (matches all)
    domains: list[Domain] = []
    stages: list[PipelineStage] = []
    event_types: list[str] = []

    # Execution priority (lower = runs first)
    priority: int = 100

    # Default configuration (can be overridden)
    default_config: dict[str, Any] = {}

    def __init__(self, config: dict[str, Any] | None = None):
        """
        Initialize plugin with optional config override.

        Args:
            config: Config dict to merge with default_config
        """
        self.logger = logging.getLogger(__name__)
        self.config = {**self.default_config, **(config or {})}

    def should_run(self, context: PluginContext) -> bool:
        """
        Check if this plugin should run for the given context.

        Args:
            context: Plugin execution context

        Returns:
            True if plugin should execute
        """
        if self.domains and context.domain not in self.domains:
            return False

        if self.stages and context.stage not in self.stages:
            return False

        return not (self.event_types and context.event_type not in self.event_types)

    @abstractmethod
    async def execute(self, context: PluginContext) -> PluginResult:
        """
        Execute the plugin logic.

        Args:
            context: Plugin execution context with message and data

        Returns:
            PluginResult with success status and actions to perform
        """
        pass
