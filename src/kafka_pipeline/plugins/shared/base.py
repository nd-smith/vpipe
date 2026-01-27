# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""
Plugin framework base classes.

Provides domain-agnostic plugin infrastructure that works with
both XACT and ClaimX pipelines.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from pydantic import BaseModel

from kafka_pipeline.common.logging import LoggedClass


async def resolve_claimx_project_id(
    claim_number: str,
    connection_manager,
    connection_name: str = "claimx_api",
) -> Optional[int]:
    """
    Helper function to resolve ClaimX project ID from claim number.

    Useful for plugins in XACT domain that need to work with ClaimX but only
    have the claim number available.

    Args:
        claim_number: The claim number (e.g., "ABC123456")
        connection_manager: ConnectionManager instance for API requests
        connection_name: Named connection to use (default: "claimx_api")

    Returns:
        ClaimX project ID if found, None otherwise

    Example:
        from kafka_pipeline.plugins.shared.base import resolve_claimx_project_id

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
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PluginResult:
    """Result returned by plugin execution."""

    success: bool
    actions: List[PluginAction] = field(default_factory=list)
    message: Optional[str] = None
    terminate_pipeline: bool = False

    @classmethod
    def success(cls, message: str = None) -> "PluginResult":
        """Create a successful result with no actions."""
        return cls(success=True, message=message)

    @classmethod
    def skip(cls, reason: str) -> "PluginResult":
        """Create a skip result (conditions not met)."""
        return cls(success=True, message=f"Skipped: {reason}")

    @classmethod
    def publish(
        cls,
        topic: str,
        payload: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
    ) -> "PluginResult":
        """Create a result that publishes to a Kafka topic."""
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
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> "PluginResult":
        """Create a result that calls an HTTP webhook."""
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
        """Create a result that logs a message."""
        return cls(
            success=True,
            actions=[
                PluginAction(
                    action_type=ActionType.LOG, params={"level": level, "message": message}
                )
            ],
        )

    @classmethod
    def filter_out(cls, reason: str) -> "PluginResult":
        """Create a result that stops pipeline processing."""
        return cls(
            success=True,
            terminate_pipeline=True,
            message=f"Filtered: {reason}",
            actions=[PluginAction(action_type=ActionType.FILTER, params={"reason": reason})],
        )

    @classmethod
    def email(
        cls,
        to: Union[str, List[str]],
        subject: str,
        body: str,
        *,
        connection: str = "email_service",
        cc: Optional[Union[str, List[str]]] = None,
        bcc: Optional[Union[str, List[str]]] = None,
        reply_to: Optional[str] = None,
        html: bool = False,
        template_id: Optional[str] = None,
        template_data: Optional[Dict[str, Any]] = None,
    ) -> "PluginResult":
        """
        Create a result that sends an email.

        Args:
            to: Recipient email address(es)
            subject: Email subject line
            body: Email body (plain text or HTML based on 'html' flag)
            connection: Named connection for email service (default: "email_service")
            cc: CC recipient(s)
            bcc: BCC recipient(s)
            reply_to: Reply-to address
            html: If True, body is treated as HTML
            template_id: Optional template ID for templated emails
            template_data: Data to populate template variables

        Returns:
            PluginResult with SEND_EMAIL action
        """
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
        task_data: Dict[str, Any],
        *,
        project_id: Optional[int] = None,
        claim_number: Optional[str] = None,
        connection: str = "claimx_api",
        use_primary_contact_as_sender: bool = True,
        sender_username: Optional[str] = None,
    ) -> "PluginResult":
        """
        Create a result that creates a ClaimX task via /import/project/actions API.

        Args:
            task_type: Action type (e.g., "CUSTOM_TASK_ASSIGN_EXTERNAL_LINK")
            task_data: Task-specific data payload containing:
                - customTaskName: Name of the custom task
                - customTaskId: ID of the custom task
                - notificationType: Type of notification (e.g., "COPY_EXTERNAL_LINK_URL")
                - Additional fields as required by the task type
            project_id: ClaimX project ID (required if claim_number not provided)
            claim_number: Claim number (required if project_id not provided)
                Will be used to fetch the ClaimX project ID via API
            connection: Named connection for ClaimX API (default: "claimx_api")
            use_primary_contact_as_sender: Use primary contact as sender (default: True)
            sender_username: Sender username (optional, defaults to config value)

        Returns:
            PluginResult with CREATE_CLAIMX_TASK action

        Examples:
            # With ClaimX project ID (ClaimX domain events)
            PluginResult.create_claimx_task(
                project_id=12345,
                task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
                task_data={
                    "customTaskName": "Review Documentation",
                    "customTaskId": 456,
                    "notificationType": "COPY_EXTERNAL_LINK_URL",
                }
            )

            # With claim number (XACT domain events)
            PluginResult.create_claimx_task(
                claim_number="ABC123456",
                task_type="CUSTOM_TASK_ASSIGN_EXTERNAL_LINK",
                task_data={
                    "customTaskName": "Review Documentation",
                    "customTaskId": 456,
                    "notificationType": "COPY_EXTERNAL_LINK_URL",
                }
            )
        """
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
            actions=[PluginAction(action_type=ActionType.CREATE_CLAIMX_TASK, params=params)],
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
    event_type: Optional[str] = None
    project_id: Optional[str] = None

    # Domain-specific processed data
    # ClaimX: {"entities": EntityRowsMessage, "handler_result": HandlerResult}
    # XACT: {"file_metadata": {...}, "download_result": {...}}
    data: Dict[str, Any] = field(default_factory=dict)

    # Mutable headers (plugins can add)
    headers: Dict[str, str] = field(default_factory=dict)

    # Error context (for ERROR/RETRY stages)
    error: Optional[Exception] = None
    retry_count: int = 0

    # Timestamp
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def get_claimx_entities(self) -> Optional["EntityRowsMessage"]:
        """Get ClaimX EntityRowsMessage if available."""
        return self.data.get("entities")

    def get_tasks(self) -> List[Dict[str, Any]]:
        """Get task rows from ClaimX entities."""
        entities = self.get_claimx_entities()
        if entities:
            return entities.tasks
        return []

    def get_first_task(self) -> Optional[Dict[str, Any]]:
        """Get first task row if available."""
        tasks = self.get_tasks()
        return tasks[0] if tasks else None

    def get_field(self, path: str) -> Any:
        """
        Get field value using dot notation path.

        Supports:
          - message.field_name
          - data.entities.tasks[0].task_id
          - headers.x-custom

        Args:
            path: Dot-notation path to field

        Returns:
            Field value or None if not found
        """
        import re

        parts = path.split(".")
        root = parts[0]

        # Get root object
        if root == "message":
            obj = self.message
        elif root == "data":
            obj = self.data
        elif root == "headers":
            return self.headers.get(".".join(parts[1:])) if len(parts) > 1 else self.headers
        elif root == "event_id":
            return self.event_id
        elif root == "event_type":
            return self.event_type
        elif root == "project_id":
            return self.project_id
        else:
            return None

        # Navigate remaining path
        for part in parts[1:]:
            if obj is None:
                return None

            # Handle array indexing: tasks[0]
            array_match = re.match(r"^(\w+)\[(\d+)\]$", part)
            if array_match:
                attr_name, index = array_match.groups()
                if isinstance(obj, dict):
                    obj = obj.get(attr_name)
                elif hasattr(obj, attr_name):
                    obj = getattr(obj, attr_name)
                else:
                    return None

                if obj and isinstance(obj, (list, tuple)) and len(obj) > int(index):
                    obj = obj[int(index)]
                else:
                    return None
            elif isinstance(obj, dict):
                obj = obj.get(part)
            elif hasattr(obj, part):
                obj = getattr(obj, part)
            else:
                return None

        return obj


class Plugin(LoggedClass, ABC):
    """
    Base class for plugins.

    Subclasses define:
      - Which domains/stages/event_types they handle
      - The execute() method with business logic

    Provides logging infrastructure via LoggedClass:
      - self._logger: Logger instance
      - self._log(level, msg, **extra): Log with context
      - self._log_exception(exc, msg, **extra): Exception logging
    """

    # Plugin metadata
    name: str = "unnamed_plugin"
    description: str = ""
    version: str = "1.0.0"

    # Filtering: which contexts trigger this plugin
    # Empty list = no filter (matches all)
    domains: List[Domain] = []
    stages: List[PipelineStage] = []
    event_types: List[str] = []

    # Execution priority (lower = runs first)
    priority: int = 100

    # Default configuration (can be overridden)
    default_config: Dict[str, Any] = {}

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize plugin with optional config override.

        Args:
            config: Config dict to merge with default_config
        """
        super().__init__()
        self.config = {**self.default_config, **(config or {})}
        self._enabled = True

    @property
    def enabled(self):
        return self._enabled

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    def should_run(self, context: PluginContext) -> bool:
        """
        Check if this plugin should run for the given context.

        Args:
            context: Plugin execution context

        Returns:
            True if plugin should execute
        """
        if not self._enabled:
            return False

        if self.domains and context.domain not in self.domains:
            return False

        if self.stages and context.stage not in self.stages:
            return False

        if self.event_types and context.event_type not in self.event_types:
            return False

        return True

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

    async def on_load(self) -> None:
        """Called when plugin is loaded. Override for initialization."""
        pass

    async def on_unload(self) -> None:
        """Called when plugin is unloaded. Override for cleanup."""
        pass
