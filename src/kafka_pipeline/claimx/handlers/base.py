"""
Base handler classes and registry for ClaimX event processing.

Provides the EventHandler abstract base class, handler registry for routing events,
and decorator utilities for error handling and handler registration.
"""

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Type, TypeVar, TYPE_CHECKING

from kafka_pipeline.claimx.api_client import ClaimXApiClient, ClaimXApiError
from kafka_pipeline.claimx.schemas.events import ClaimXEventMessage
from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage

from core.types import ErrorCategory
from core.logging import get_logger, log_exception, log_with_context
from kafka_pipeline.common.logging import extract_log_context
from kafka_pipeline.common.metrics import (
    claimx_handler_duration_seconds,
    claimx_handler_events_total,
)

if TYPE_CHECKING:
    from kafka_pipeline.claimx.handlers.project_cache import ProjectCache

logger = get_logger(__name__)


class EnrichmentResult:
    """
    Result from processing a single ClaimX event.

    Attributes:
        event: Original event that was processed
        success: Whether processing succeeded
        rows: Entity rows extracted from API (if successful)
        error: Error message (if failed)
        error_category: Error classification for retry logic
        is_retryable: Whether this error can be retried
        api_calls: Number of API calls made
        duration_ms: Processing time in milliseconds
    """

    def __init__(
        self,
        event: ClaimXEventMessage,
        success: bool,
        rows: Optional[EntityRowsMessage] = None,
        error: Optional[str] = None,
        error_category: Optional[ErrorCategory] = None,
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
    """
    Aggregated result from processing a batch of events.

    Attributes:
        handler_name: Name of the handler class
        total: Total events processed
        succeeded: Number of successful events
        failed: Number of failed events
        failed_permanent: Number of permanently failed events
        skipped: Number of skipped events
        rows: All extracted entity rows
        event_logs: Log entries for each event
        errors: List of error messages
        duration_seconds: Total processing time in seconds
        api_calls: Total API calls made
    """

    def __init__(
        self,
        handler_name: str,
        total: int,
        succeeded: int,
        failed: int,
        failed_permanent: int,
        skipped: int,
        rows: EntityRowsMessage,
        event_logs: List[Dict[str, Any]],
        errors: List[str],
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
        self.event_logs = event_logs
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
    """Create standardized error result."""
    duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
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
    log_context: Optional[Callable[[ClaimXEventMessage], dict]] = None,
) -> Callable[
    [Callable[..., Awaitable[EnrichmentResult]]],
    Callable[[Any, ClaimXEventMessage], Awaitable[EnrichmentResult]],
]:
    """
    Decorator for standardized API error handling in handlers.

    Uses extract_log_context for consistent failure logging.

    Args:
        api_calls: Number of API calls made in the decorated function
        log_context: Optional function to extract additional context from event

    Returns:
        Decorator function
    """

    def decorator(
        fn: Callable[..., Awaitable[EnrichmentResult]],
    ) -> Callable[[Any, ClaimXEventMessage], Awaitable[EnrichmentResult]]:
        async def wrapper(self: Any, event: ClaimXEventMessage) -> EnrichmentResult:
            start_time = datetime.now(timezone.utc)
            handler_name = getattr(self, "name", self.__class__.__name__)

            try:
                return await fn(self, event, start_time)

            except ClaimXApiError as e:
                # Use extract_log_context, but allow override via log_context param
                ctx = extract_log_context(event)
                if log_context:
                    ctx.update(log_context(event))

                duration_ms = int(
                    (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                )
                log_with_context(
                    logger,
                    logging.WARNING,
                    "API error",
                    handler_name=handler_name,
                    error_message=str(e)[:200],
                    error_category=e.category.value if e.category else None,
                    http_status=e.status_code,
                    duration_ms=duration_ms,
                    **ctx,
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
                ctx = extract_log_context(event)
                if log_context:
                    ctx.update(log_context(event))

                duration_ms = int(
                    (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                )
                log_exception(
                    logger,
                    e,
                    "Unexpected error",
                    handler_name=handler_name,
                    duration_ms=duration_ms,
                    **ctx,
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
    """
    Base class for ClaimX event handlers.

    Each handler processes specific event types and returns entity rows
    to be written to Delta tables.

    Attributes:
        event_types: List of event type strings this handler processes
        supports_batching: Whether this handler supports batch processing
        batch_key: Field to batch by (e.g., "project_id")
        client: ClaimX API client for fetching data
    """

    # Event types this handler processes
    event_types: List[str] = []

    # Whether this handler supports batch processing
    supports_batching: bool = False

    # Field to batch by (e.g., "project_id")
    batch_key: Optional[str] = None

    def __init__(
        self,
        client: ClaimXApiClient,
        project_cache: Optional["ProjectCache"] = None,
    ):
        """
        Initialize handler with API client.

        Args:
            client: ClaimX API client instance
            project_cache: Optional project cache for in-flight verification
        """
        self.client = client
        self.project_cache = project_cache

    @property
    def name(self) -> str:
        """Handler name for logging."""
        return self.__class__.__name__

    @abstractmethod
    async def handle_event(self, event: ClaimXEventMessage) -> EnrichmentResult:
        """
        Process a single event.

        Args:
            event: ClaimX event to process

        Returns:
            EnrichmentResult with entity rows or error info
        """
        pass

    async def handle_batch(
        self, events: List[ClaimXEventMessage]
    ) -> List[EnrichmentResult]:
        """
        Process a batch of events concurrently.

        Default implementation processes events independently.
        Override for optimized batch processing.

        Args:
            events: List of events to process

        Returns:
            List of results, one per event
        """
        import asyncio

        if not events:
            return []

        log_with_context(
            logger,
            logging.DEBUG,
            "Processing event batch",
            handler_name=self.name,
            batch_size=len(events),
        )

        start_time = datetime.now(timezone.utc)
        tasks = [self.handle_event(event) for event in events]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        final_results = []
        success_count = 0
        error_count = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                event = events[i]
                error_count += 1
                log_exception(
                    logger,
                    result,
                    "Handler failed for event",
                    handler_name=self.name,
                    **extract_log_context(event),
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

        elapsed_ms = (
            datetime.now(timezone.utc) - start_time
        ).total_seconds() * 1000
        log_with_context(
            logger,
            logging.DEBUG,
            "Batch complete",
            handler_name=self.name,
            batch_size=len(events),
            records_succeeded=success_count,
            records_failed=error_count,
            duration_ms=round(elapsed_ms, 2),
        )

        return final_results

    async def process(self, events: List[ClaimXEventMessage]) -> HandlerResult:
        """
        Entry point for processing events.

        Handles batching logic and aggregates results.

        Args:
            events: Events to process

        Returns:
            HandlerResult with aggregated stats and rows
        """
        if not events:
            log_with_context(
                logger,
                logging.DEBUG,
                "No events to process",
                handler_name=self.name,
            )
            return HandlerResult(
                handler_name=self.name,
                total=0,
                succeeded=0,
                failed=0,
                failed_permanent=0,
                skipped=0,
                rows=EntityRowsMessage(),
                event_logs=[],
                errors=[],
                duration_seconds=0.0,
                api_calls=0,
            )

        start_time = datetime.now(timezone.utc)

        # Log event type distribution
        event_type_counts: Dict[str, int] = {}
        for e in events:
            event_type_counts[e.event_type] = (
                event_type_counts.get(e.event_type, 0) + 1
            )

        log_with_context(
            logger,
            logging.DEBUG,
            "Processing events",
            handler_name=self.name,
            total_events=len(events),
            supports_batching=self.supports_batching,
            batch_key=self.batch_key,
            event_type_distribution=event_type_counts,
        )

        if self.supports_batching and self.batch_key:
            results = await self._process_batched(events)
        else:
            results = await self.handle_batch(events)

        handler_result = self._aggregate_results(results, start_time)

        log_with_context(
            logger,
            logging.DEBUG,
            "Handler processing complete",
            handler_name=self.name,
            records_processed=handler_result.total,
            records_succeeded=handler_result.succeeded,
            records_failed=handler_result.failed,
            records_failed_permanent=handler_result.failed_permanent,
            api_calls=handler_result.api_calls,
            duration_seconds=round(handler_result.duration_seconds, 3),
        )

        return handler_result

    async def _process_batched(
        self, events: List[ClaimXEventMessage]
    ) -> List[EnrichmentResult]:
        """Process events in batches grouped by batch_key, concurrently."""
        import asyncio

        # Group by batch key
        groups: Dict[Any, List[ClaimXEventMessage]] = defaultdict(list)
        for event in events:
            key = getattr(event, self.batch_key, None)
            groups[key].append(event)

        log_with_context(
            logger,
            logging.DEBUG,
            "Processing batched events",
            handler_name=self.name,
            total_events=len(events),
            batch_key=self.batch_key,
            group_count=len(groups),
            group_sizes={str(k): len(v) for k, v in groups.items()},
        )

        # Process all groups concurrently
        async def process_group(key: Any, group_events: List[ClaimXEventMessage]):
            return key, await self.handle_batch(group_events)

        tasks = [process_group(k, evts) for k, evts in groups.items()]
        group_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten results, preserving order
        results = []
        groups_succeeded = 0
        groups_failed = 0

        for item in group_results:
            if isinstance(item, Exception):
                groups_failed += 1
                log_exception(
                    logger,
                    item,
                    "Batch group processing failed",
                    handler_name=self.name,
                    batch_key=self.batch_key,
                )
                continue
            key, batch_results = item
            groups_succeeded += 1
            results.extend(batch_results)

        log_with_context(
            logger,
            logging.DEBUG,
            "Batched processing complete",
            handler_name=self.name,
            groups_succeeded=groups_succeeded,
            groups_failed=groups_failed,
            total_results=len(results),
        )

        return results

    def _aggregate_results(
        self,
        results: List[EnrichmentResult],
        start_time: datetime,
    ) -> HandlerResult:
        """Aggregate individual results into HandlerResult."""
        duration_seconds = (
            datetime.now(timezone.utc) - start_time
        ).total_seconds()

        total = 0
        succeeded = 0
        failed = 0
        failed_permanent = 0
        api_calls = 0
        all_rows = EntityRowsMessage()
        event_logs = []
        errors = []

        for result in results:
            if result is None:
                log_exception(
                    logger,
                    Exception("Handler returned None result"),
                    "Invalid handler result",
                    handler_name=self.name,
                )
                continue
            total += 1
            api_calls += result.api_calls

            if result.success:
                succeeded += 1
                all_rows.merge(result.rows)
            else:
                failed += 1
                if not result.is_retryable:
                    failed_permanent += 1
                if result.error:
                    errors.append(result.error)

            # Build event log entry
            now = datetime.now(timezone.utc)
            log_entry = {
                "event_id": result.event.event_id,
                "event_type": result.event.event_type,
                "project_id": result.event.project_id,
                "status": (
                    "success"
                    if result.success
                    else (
                        "failed_permanent"
                        if not result.is_retryable
                        else "failed"
                    )
                ),
                "error_message": result.error,
                "error_category": (
                    result.error_category.value if result.error_category else None
                ),
                "api_calls": result.api_calls,
                "duration_ms": result.duration_ms,
                "retry_count": 0,
                "processed_at": now.isoformat(),
                "processed_date": now.date().isoformat(),
            }
            event_logs.append(log_entry)

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
            log_with_context(
                logger,
                logging.DEBUG,
                "Aggregated entity rows",
                handler_name=self.name,
                entity_counts=non_zero_counts,
                total_rows=sum(non_zero_counts.values()),
            )

        # Record per-handler metrics
        # Record duration for succeeded events
        if succeeded > 0:
            # Calculate average duration per successful event
            avg_duration = duration_seconds / succeeded if succeeded else duration_seconds
            claimx_handler_duration_seconds.labels(
                handler_name=self.name, status="success"
            ).observe(avg_duration)

        # Record duration for failed events
        if failed > 0:
            # Calculate average duration per failed event
            avg_duration = duration_seconds / failed if failed else duration_seconds
            claimx_handler_duration_seconds.labels(
                handler_name=self.name, status="failed"
            ).observe(avg_duration)

        # Record event counts
        claimx_handler_events_total.labels(
            handler_name=self.name, status="success"
        ).inc(succeeded)
        claimx_handler_events_total.labels(
            handler_name=self.name, status="failed"
        ).inc(failed)

        return HandlerResult(
            handler_name=self.name,
            total=total,
            succeeded=succeeded,
            failed=failed,
            failed_permanent=failed_permanent,
            skipped=0,
            rows=all_rows,
            event_logs=event_logs,
            errors=errors,
            duration_seconds=duration_seconds,
            api_calls=api_calls,
        )


class NoOpHandler(EventHandler):
    """
    Handler for events that don't require API calls.

    Extracts data directly from the event payload.
    Subclasses must implement extract_rows() method.
    """

    supports_batching = False

    async def handle_event(self, event: ClaimXEventMessage) -> EnrichmentResult:
        """Extract rows from event payload without API call."""
        start_time = datetime.now(timezone.utc)

        try:
            rows = self.extract_rows(event)
            duration_ms = int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            )

            # Log extraction result
            row_counts = {
                "projects": len(rows.projects),
                "contacts": len(rows.contacts),
                "media": len(rows.media),
                "tasks": len(rows.tasks),
            }
            non_zero = {k: v for k, v in row_counts.items() if v > 0}

            log_with_context(
                logger,
                logging.DEBUG,
                "NoOp handler extracted rows",
                handler_name=self.name,
                entity_counts=non_zero if non_zero else None,
                duration_ms=duration_ms,
                **extract_log_context(event),
            )

            return EnrichmentResult(
                event=event,
                success=True,
                rows=rows,
                api_calls=0,
                duration_ms=duration_ms,
            )

        except Exception as e:
            log_exception(
                logger,
                e,
                "Error extracting rows from event",
                handler_name=self.name,
                **extract_log_context(event),
            )
            duration_ms = int(
                (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
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
        """
        Extract entity rows from event payload.

        Args:
            event: ClaimX event with payload data

        Returns:
            EntityRowsMessage extracted from payload
        """
        pass


# Global handler registry
_handler_registry: Optional["HandlerRegistry"] = None


class HandlerRegistry:
    """Registry mapping event types to handler classes."""

    def __init__(self):
        self._handlers: Dict[str, Type[EventHandler]] = {}
        log_with_context(
            logger,
            logging.DEBUG,
            "HandlerRegistry initialized",
        )

    def register(self, handler_class: Type[EventHandler]) -> None:
        """Register a handler class for its event types."""
        for event_type in handler_class.event_types:
            if event_type in self._handlers:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Overwriting handler registration",
                    event_type=event_type,
                    old_handler=self._handlers[event_type].__name__,
                    new_handler=handler_class.__name__,
                )
            self._handlers[event_type] = handler_class
            log_with_context(
                logger,
                logging.DEBUG,
                "Registered handler",
                handler_name=handler_class.__name__,
                event_type=event_type,
                supports_batching=handler_class.supports_batching,
                batch_key=handler_class.batch_key,
            )

    def get_handler(
        self,
        event_type: str,
        client: ClaimXApiClient,
    ) -> Optional[EventHandler]:
        """Get handler instance for event type."""
        handler_class = self._handlers.get(event_type)
        if handler_class is None:
            log_with_context(
                logger,
                logging.DEBUG,
                "No handler found for event type",
                event_type=event_type,
            )
            return None
        return handler_class(client)

    def get_handler_class(
        self, event_type: str
    ) -> Optional[Type[EventHandler]]:
        """Get handler class for event type."""
        return self._handlers.get(event_type)

    def group_events_by_handler(
        self,
        events: List[ClaimXEventMessage],
    ) -> Dict[Type[EventHandler], List[ClaimXEventMessage]]:
        """Group events by their handler class."""
        groups: Dict[Type[EventHandler], List[ClaimXEventMessage]] = (
            defaultdict(list)
        )
        unhandled_types: Dict[str, int] = {}

        for event in events:
            handler_class = self.get_handler_class(event.event_type)
            if handler_class:
                groups[handler_class].append(event)
            else:
                unhandled_types[event.event_type] = (
                    unhandled_types.get(event.event_type, 0) + 1
                )

        # Log grouping results
        handler_distribution = {
            cls.__name__: len(evts) for cls, evts in groups.items()
        }

        log_with_context(
            logger,
            logging.DEBUG,
            "Grouped events by handler",
            total_events=len(events),
            handler_count=len(groups),
            handler_distribution=handler_distribution,
            unhandled_event_types=unhandled_types if unhandled_types else None,
        )

        if unhandled_types:
            for event_type, count in unhandled_types.items():
                log_with_context(
                    logger,
                    logging.WARNING,
                    "No handler for event type",
                    event_type=event_type,
                    event_count=count,
                )

        return dict(groups)

    def get_registered_handlers(self) -> Dict[str, str]:
        """Get map of event_type -> handler_name for diagnostics."""
        return {
            event_type: handler.__name__
            for event_type, handler in self._handlers.items()
        }


def get_handler_registry() -> HandlerRegistry:
    """Get or create global handler registry."""
    global _handler_registry
    if _handler_registry is None:
        _handler_registry = HandlerRegistry()
        log_with_context(
            logger,
            logging.DEBUG,
            "Created global handler registry",
        )
    return _handler_registry


def reset_registry() -> None:
    """Reset global registry (for testing)."""
    global _handler_registry
    if _handler_registry is not None:
        log_with_context(
            logger,
            logging.DEBUG,
            "Resetting handler registry",
            registered_handlers=len(_handler_registry._handlers),
        )
    _handler_registry = None


def register_handler(cls: Type[EventHandler]) -> Type[EventHandler]:
    """
    Decorator to register a handler class.

    Usage:
        @register_handler
        class MyHandler(EventHandler):
            event_types = ["MY_EVENT"]
            ...

    Args:
        cls: Handler class to register

    Returns:
        The same handler class (unchanged)
    """
    registry = get_handler_registry()
    registry.register(cls)
    return cls
