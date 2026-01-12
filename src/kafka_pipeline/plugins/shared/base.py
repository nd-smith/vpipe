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
            actions=[PluginAction(
                action_type=ActionType.PUBLISH_TO_TOPIC,
                params={
                    "topic": topic,
                    "payload": payload,
                    "headers": headers or {},
                }
            )]
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
            actions=[PluginAction(
                action_type=ActionType.HTTP_WEBHOOK,
                params={
                    "url": url,
                    "method": method,
                    "body": body or {},
                    "headers": headers or {},
                }
            )]
        )

    @classmethod
    def log(cls, level: str, message: str) -> "PluginResult":
        """Create a result that logs a message."""
        return cls(
            success=True,
            actions=[PluginAction(
                action_type=ActionType.LOG,
                params={"level": level, "message": message}
            )]
        )

    @classmethod
    def filter_out(cls, reason: str) -> "PluginResult":
        """Create a result that stops pipeline processing."""
        return cls(
            success=True,
            terminate_pipeline=True,
            message=f"Filtered: {reason}",
            actions=[PluginAction(
                action_type=ActionType.FILTER,
                params={"reason": reason}
            )]
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
            array_match = re.match(r'^(\w+)\[(\d+)\]$', part)
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
    def enabled(self) -> bool:
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
