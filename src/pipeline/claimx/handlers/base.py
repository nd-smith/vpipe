"""Base handler classes and registry for ClaimX event processing."""

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    TypeVar,
)

from core.types import ErrorCategory
from pipeline.claimx.api_client import ClaimXApiClient, ClaimXApiError
from pipeline.claimx.handlers.utils import LOG_ERROR_TRUNCATE_SHORT
from pipeline.claimx.schemas.entities import EntityRowsMessage
from pipeline.claimx.schemas.events import ClaimXEventMessage
from pipeline.common.logging import extract_log_context
from pipeline.common.metrics import (
    claimx_handler_duration_seconds,
    claimx_handler_events_total,
)

if TYPE_CHECKING:
    from pipeline.claimx.handlers.project_cache import ProjectCache

logger = logging.getLogger(__name__)


class EnrichmentResult:
    """Result from processing a single ClaimX event."""

    def __init__(
        self,
        event: ClaimXEventMessage,
        success: bool,
        rows: EntityRowsMessage | None = None,
        error: str | None = None,
        error_category: ErrorCategory | None = None,
        is_retryable: bool = True,
        api_calls: int = 0,
        duration_ms: int = 0,
    ):
        self.event = event
        self.success = success
        self.rows = rows or EntityRowsMessage()
        self.error = error
        self.error_category = error_category
        self.is_retryable = is_retryable
        self.api_calls = api_calls
        self.duration_ms = duration_ms


class HandlerResult:
    """Aggregated result from processing a batch of events."""

    def __init__(
        self,
        handler_name: str,
        total: int,
        succeeded: int,
        failed: int,
        failed_permanent: int,
        skipped: int,
        rows: EntityRowsMessage,
        errors: list[str],
        duration_seconds: float,
        api_calls: int,
    ):
        self.handler_name = handler_name
        self.total = total
        self.succeeded = succeeded
        self.failed = failed
        self.failed_permanent = failed_permanent
        self.skipped = skipped
        self.rows = rows
        self.errors = errors
        self.duration_seconds = duration_seconds
        self.api_calls = api_calls


def _make_error_result(
    event: ClaimXEventMessage,
    error: Exception,
    start_time: datetime,
    api_calls: int,
    category: ErrorCategory,
    is_retryable: bool,
) -> EnrichmentResult:
    duration_ms = int((datetime.now(UTC) - start_time).total_seconds() * 1000)
    return EnrichmentResult(
        event=event,
        success=False,
        error=str(error),
        error_category=category,
        is_retryable=is_retryable,
        api_calls=api_calls,
        duration_ms=duration_ms,
    )


F = TypeVar("F", bound=Callable[..., Awaitable[EnrichmentResult]])


def with_api_error_handling(
    api_calls: int = 1,
    log_context: Callable[[ClaimXEventMessage], dict] | None = None,
) -> Callable[
    [Callable[..., Awaitable[EnrichmentResult]]],
    Callable[[Any, ClaimXEventMessage], Awaitable[EnrichmentResult]],
]:
    """Decorator for standardized API error handling with extract_log_context."""

    def decorator(
        fn: Callable[..., Awaitable[EnrichmentResult]],
    ) -> Callable[[Any, ClaimXEventMessage], Awaitable[EnrichmentResult]]:
        async def wrapper(self: Any, event: ClaimXEventMessage) -> EnrichmentResult:
            start_time = datetime.now(UTC)
            handler_name = getattr(self, "name", self.__class__.__name__)

            try:
                return await fn(self, event, start_time)

            except ClaimXApiError as e:
                # Use extract_log_context, but allow override via log_context param
                event_log_context = extract_log_context(event)
                if log_context:
                    event_log_context.update(log_context(event))

                duration_ms = int(
                    (datetime.now(UTC) - start_time).total_seconds() * 1000
                )
                logger.warning(
                    "API error",
                    extra={
                        "handler_name": handler_name,
                        "error_message": str(e)[:LOG_ERROR_TRUNCATE_SHORT],
                        "error_category": e.category.value if e.category else None,
                        "http_status": e.status_code,
                        "duration_ms": duration_ms,
                        **event_log_context,
                    },
                )
                return _make_error_result(
                    event,
                    e,
                    start_time,
                    api_calls,
                    category=e.category,
                    is_retryable=e.is_retryable,
                )

            except Exception as e:
                # Use extract_log_context for consistent failure logging
                event_log_context = extract_log_context(event)
                if log_context:
                    event_log_context.update(log_context(event))

                duration_ms = int(
                    (datetime.now(UTC) - start_time).total_seconds() * 1000
                )
                logger.error(
                    "Unexpected error",
                    extra={
                        "handler_name": handler_name,
                        "duration_ms": duration_ms,
                        **event_log_context,
                    },
                    exc_info=True,
                )
                return _make_error_result(
                    event,
                    e,
                    start_time,
                    api_calls,
                    category=ErrorCategory.TRANSIENT,
                    is_retryable=True,
                )

        # Copy only name and doc, not signature
        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper

    return decorator


class EventHandler(ABC):
    """Base class for ClaimX event handlers.

    Processes events by type and returns entity rows for Delta tables.
    """

    event_types: list[str] = []
    supports_batching: bool = False
    batch_key: str | None = None

    def __init__(
        self,
        client: ClaimXApiClient,
        project_cache: Optional["ProjectCache"] = None,
    ):
        self.client = client
        self.project_cache = project_cache

    @property
    def name(self) -> str:
        return self.__class__.__name__

    async def ensure_project_exists(
        self,
        project_id: int,
        source_event_id: str,
    ) -> "EntityRowsMessage":
        """
        Ensure project exists in warehouse by fetching project data.

        This is used by handlers to perform in-flight project verification,
        ensuring the project record exists before adding related entities.

        Args:
            project_id: Project ID to verify
            source_event_id: Source event ID for traceability

        Returns:
            EntityRowsMessage containing project and contact rows
        """
        from pipeline.claimx.handlers.project import ProjectHandler

        project_handler = ProjectHandler(self.client, project_cache=self.project_cache)
        return await project_handler.fetch_project_data(
            project_id,
            source_event_id=source_event_id,
        )

    @abstractmethod
    async def handle_event(self, event: ClaimXEventMessage) -> EnrichmentResult:
        pass

    async def handle_batch(
        self, events: list[ClaimXEventMessage]
    ) -> list[EnrichmentResult]:
        """Default processes events independently. Override for optimized batch processing."""
        import asyncio

        if not events:
            return []

        logger.debug(
            "Processing event batch",
            extra={
                "handler_name": self.name,
                "batch_size": len(events),
            },
        )

        start_time = datetime.now(UTC)
        tasks = [self.handle_event(event) for event in events]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        final_results = []
        success_count = 0
        error_count = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                event = events[i]
                error_count += 1
                logger.error(
                    "Handler failed for event",
                    extra={
                        "handler_name": self.name,
                        **extract_log_context(event),
                    },
                    exc_info=True,
                )
                # Create failure result
                final_results.append(
                    EnrichmentResult(
                        event=event,
                        success=False,
                        error=str(result),
                        error_category=ErrorCategory.TRANSIENT,
                        is_retryable=True,
                        api_calls=0,
                        duration_ms=0,
                    )
                )
            else:
                final_results.append(result)
                if result.success:
                    success_count += 1
                else:
                    error_count += 1

        elapsed_ms = (datetime.now(UTC) - start_time).total_seconds() * 1000
        logger.debug(
            "Batch complete",
            extra={
                "handler_name": self.name,
                "batch_size": len(events),
                "records_succeeded": success_count,
                "records_failed": error_count,
                "duration_ms": round(elapsed_ms, 2),
            },
        )

        return final_results

    async def process(self, events: list[ClaimXEventMessage]) -> HandlerResult:
        if not events:
            logger.debug(
                "No events to process",
                extra={
                    "handler_name": self.name,
                },
            )
            return HandlerResult(
                handler_name=self.name,
                total=0,
                succeeded=0,
                failed=0,
                failed_permanent=0,
                skipped=0,
                rows=EntityRowsMessage(),
                errors=[],
                duration_seconds=0.0,
                api_calls=0,
            )

        start_time = datetime.now(UTC)

        # Log event type distribution
        event_type_counts: dict[str, int] = {}
        for event in events:
            event_type_counts[event.event_type] = (
                event_type_counts.get(event.event_type, 0) + 1
            )

        logger.debug(
            "Processing events",
            extra={
                "handler_name": self.name,
                "total_events": len(events),
                "supports_batching": self.supports_batching,
                "batch_key": self.batch_key,
                "event_type_distribution": event_type_counts,
            },
        )

        if self.supports_batching and self.batch_key:
            results = await self._process_batched(events)
        else:
            results = await self.handle_batch(events)

        handler_result = self._aggregate_results(results, start_time)

        logger.debug(
            "Handler processing complete",
            extra={
                "handler_name": self.name,
                "records_processed": handler_result.total,
                "records_succeeded": handler_result.succeeded,
                "records_failed": handler_result.failed,
                "records_failed_permanent": handler_result.failed_permanent,
                "api_calls": handler_result.api_calls,
                "duration_seconds": round(handler_result.duration_seconds, 3),
            },
        )

        return handler_result

    async def _process_batched(
        self, events: list[ClaimXEventMessage]
    ) -> list[EnrichmentResult]:
        """Process events in batches grouped by batch_key, concurrently."""
        import asyncio

        # Group by batch key
        groups: dict[Any, list[ClaimXEventMessage]] = defaultdict(list)
        for event in events:
            key = getattr(event, self.batch_key, None)
            groups[key].append(event)

        logger.debug(
            "Processing batched events",
            extra={
                "handler_name": self.name,
                "total_events": len(events),
                "batch_key": self.batch_key,
                "group_count": len(groups),
                "group_sizes": {str(k): len(v) for k, v in groups.items()},
            },
        )

        # Process all groups concurrently
        async def process_group(key: Any, group_events: list[ClaimXEventMessage]):
            return key, await self.handle_batch(group_events)

        tasks = [process_group(k, evts) for k, evts in groups.items()]
        group_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten results, preserving order
        results = []
        groups_succeeded = 0
        groups_failed = 0

        for group_result in group_results:
            if isinstance(group_result, Exception):
                groups_failed += 1
                logger.error(
                    "Batch group processing failed",
                    extra={
                        "handler_name": self.name,
                        "batch_key": self.batch_key,
                    },
                    exc_info=True,
                )
                continue
            key, batch_results = group_result
            groups_succeeded += 1
            results.extend(batch_results)

        logger.debug(
            "Batched processing complete",
            extra={
                "handler_name": self.name,
                "groups_succeeded": groups_succeeded,
                "groups_failed": groups_failed,
                "total_results": len(results),
            },
        )

        return results

    def _aggregate_results(
        self,
        results: list[EnrichmentResult],
        start_time: datetime,
    ) -> HandlerResult:
        duration_seconds = (datetime.now(UTC) - start_time).total_seconds()

        total = 0
        succeeded = 0
        failed = 0
        failed_permanent = 0
        api_calls = 0
        all_rows = EntityRowsMessage()
        errors = []

        for enrichment_result in results:
            if enrichment_result is None:
                logger.error(
                    "Invalid handler result",
                    extra={
                        "handler_name": self.name,
                    },
                    exc_info=True,
                )
                continue
            total += 1
            api_calls += enrichment_result.api_calls

            if enrichment_result.success:
                succeeded += 1
                all_rows.merge(enrichment_result.rows)
            else:
                failed += 1
                if not enrichment_result.is_retryable:
                    failed_permanent += 1
                if enrichment_result.error:
                    errors.append(enrichment_result.error)

        # Log entity row counts
        row_counts = {
            "projects": len(all_rows.projects),
            "contacts": len(all_rows.contacts),
            "media": len(all_rows.media),
            "tasks": len(all_rows.tasks),
            "task_templates": len(all_rows.task_templates),
            "external_links": len(all_rows.external_links),
            "video_collab": len(all_rows.video_collab),
        }
        non_zero_counts = {k: v for k, v in row_counts.items() if v > 0}

        if non_zero_counts:
            logger.debug(
                "Aggregated entity rows",
                extra={
                    "handler_name": self.name,
                    "entity_counts": non_zero_counts,
                    "total_rows": sum(non_zero_counts.values()),
                },
            )

        if succeeded > 0:
            # Calculate average duration per successful event
            avg_duration = (
                duration_seconds / succeeded if succeeded else duration_seconds
            )
            claimx_handler_duration_seconds.labels(
                handler_name=self.name, status="success"
            ).observe(avg_duration)

        if failed > 0:
            # Calculate average duration per failed event
            avg_duration = duration_seconds / failed if failed else duration_seconds
            claimx_handler_duration_seconds.labels(
                handler_name=self.name, status="failed"
            ).observe(avg_duration)

        claimx_handler_events_total.labels(
            handler_name=self.name, status="success"
        ).inc(succeeded)
        claimx_handler_events_total.labels(handler_name=self.name, status="failed").inc(
            failed
        )

        return HandlerResult(
            handler_name=self.name,
            total=total,
            succeeded=succeeded,
            failed=failed,
            failed_permanent=failed_permanent,
            skipped=0,
            rows=all_rows,
            errors=errors,
            duration_seconds=duration_seconds,
            api_calls=api_calls,
        )


class NoOpHandler(EventHandler):
    """Extracts data directly from event payload without API calls."""

    supports_batching = False

    async def handle_event(self, event: ClaimXEventMessage) -> EnrichmentResult:
        start_time = datetime.now(UTC)

        try:
            rows = self.extract_rows(event)
            duration_ms = int(
                (datetime.now(UTC) - start_time).total_seconds() * 1000
            )

            row_counts = {
                "projects": len(rows.projects),
                "contacts": len(rows.contacts),
                "media": len(rows.media),
                "tasks": len(rows.tasks),
            }
            non_zero = {k: v for k, v in row_counts.items() if v > 0}

            logger.debug(
                "NoOp handler extracted rows",
                extra={
                    "handler_name": self.name,
                    "entity_counts": non_zero if non_zero else None,
                    "duration_ms": duration_ms,
                    **extract_log_context(event),
                },
            )

            return EnrichmentResult(
                event=event,
                success=True,
                rows=rows,
                api_calls=0,
                duration_ms=duration_ms,
            )

        except Exception as e:
            logger.error(
                "Error extracting rows from event",
                extra={
                    "handler_name": self.name,
                    **extract_log_context(event),
                },
                exc_info=True,
            )
            duration_ms = int(
                (datetime.now(UTC) - start_time).total_seconds() * 1000
            )

            return EnrichmentResult(
                event=event,
                success=False,
                error=str(e),
                error_category=ErrorCategory.TRANSIENT,
                is_retryable=True,
                api_calls=0,
                duration_ms=duration_ms,
            )

    @abstractmethod
    def extract_rows(self, event: ClaimXEventMessage) -> EntityRowsMessage:
        pass


# Module-level handler registry
_HANDLERS: dict[str, type[EventHandler]] = {}


class HandlerRegistry:
    """Registry mapping event types to handler classes."""

    def get_handler_class(self, event_type: str) -> type[EventHandler] | None:
        return _HANDLERS.get(event_type)

    def get_handler(
        self,
        event_type: str,
        client: ClaimXApiClient,
    ) -> EventHandler | None:
        handler_class = _HANDLERS.get(event_type)
        if handler_class is None:
            logger.debug(
                "No handler found for event type",
                extra={
                    "event_type": event_type,
                },
            )
            return None
        return handler_class(client)

    def group_events_by_handler(
        self,
        events: list[ClaimXEventMessage],
    ) -> dict[type[EventHandler], list[ClaimXEventMessage]]:
        groups: dict[type[EventHandler], list[ClaimXEventMessage]] = defaultdict(list)
        unhandled_types: dict[str, int] = {}

        for event in events:
            handler_class = self.get_handler_class(event.event_type)
            if handler_class:
                groups[handler_class].append(event)
            else:
                unhandled_types[event.event_type] = (
                    unhandled_types.get(event.event_type, 0) + 1
                )

        handler_distribution = {cls.__name__: len(evts) for cls, evts in groups.items()}

        logger.debug(
            "Grouped events by handler",
            extra={
                "total_events": len(events),
                "handler_count": len(groups),
                "handler_distribution": handler_distribution,
                "unhandled_event_types": unhandled_types if unhandled_types else None,
            },
        )

        if unhandled_types:
            for event_type, count in unhandled_types.items():
                logger.warning(
                    "No handler for event type",
                    extra={
                        "event_type": event_type,
                        "event_count": count,
                    },
                )

        return dict(groups)

    def get_registered_handlers(self) -> dict[str, str]:
        return {
            event_type: handler.__name__ for event_type, handler in _HANDLERS.items()
        }


def get_handler_registry() -> HandlerRegistry:
    """Get the handler registry instance."""
    return HandlerRegistry()


def register_handler(cls: type[EventHandler]) -> type[EventHandler]:
    """Decorator to register a handler class for its event_types."""
    for event_type in cls.event_types:
        if event_type in _HANDLERS:
            logger.warning(
                "Overwriting handler registration",
                extra={
                    "event_type": event_type,
                    "old_handler": _HANDLERS[event_type].__name__,
                    "new_handler": cls.__name__,
                },
            )
        _HANDLERS[event_type] = cls
        logger.debug(
            "Registered handler",
            extra={
                "handler_name": cls.__name__,
                "event_type": event_type,
                "supports_batching": cls.supports_batching,
                "batch_key": cls.batch_key,
            },
        )
    return cls
