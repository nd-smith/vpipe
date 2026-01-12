"""
ClaimX event handlers for Kafka pipeline.

Provides handler classes for processing ClaimX events and enriching them with
data from the ClaimX API.

Handler Types:
    - ProjectHandler: Handles PROJECT_CREATED, PROJECT_MFN_ADDED events
    - MediaHandler: Handles PROJECT_FILE_ADDED events
    - TaskHandler: Handles CUSTOM_TASK_ASSIGNED, CUSTOM_TASK_COMPLETED events
    - PolicyholderHandler: Handles POLICYHOLDER_INVITED, POLICYHOLDER_JOINED events
    - VideoCollabHandler: Handles VIDEO_COLLABORATION_INVITE_SENT, VIDEO_COLLABORATION_COMPLETED events
    - XALinkingHandler: Handles PROJECT_AUTO_XA_LINKING_UNSUCCESSFUL events

Base Classes:
    - EventHandler: Abstract base class for all handlers
    - NoOpHandler: Base for handlers that don't need API calls
    - HandlerRegistry: Registry for routing events to handlers

Usage:
    >>> from kafka_pipeline.claimx.api_client import ClaimXApiClient
    >>> from kafka_pipeline.claimx.handlers import get_handler_registry
    >>>
    >>> # Handlers are auto-registered on first registry access
    >>> registry = get_handler_registry()
    >>> handler = registry.get_handler("PROJECT_CREATED", api_client)
    >>> result = await handler.handle_event(event)
"""

# Import base classes (lightweight, no heavy deps)
from kafka_pipeline.claimx.handlers.base import (
    EventHandler,
    NoOpHandler,
    EnrichmentResult,
    HandlerResult,
    HandlerRegistry,
    get_handler_registry as _get_handler_registry_base,
    reset_registry,
    register_handler,
)
from kafka_pipeline.claimx.handlers.project_cache import ProjectCache

# Flag to track if handlers have been registered
_handlers_registered = False


def _ensure_handlers_registered() -> None:
    """Lazily import and register all handlers on first access."""
    global _handlers_registered
    if _handlers_registered:
        return

    # Import handlers to trigger @register_handler decorator
    from kafka_pipeline.claimx.handlers import project  # noqa: F401
    from kafka_pipeline.claimx.handlers import media  # noqa: F401
    from kafka_pipeline.claimx.handlers import task  # noqa: F401
    from kafka_pipeline.claimx.handlers import contact  # noqa: F401
    from kafka_pipeline.claimx.handlers import video  # noqa: F401
    from kafka_pipeline.claimx.handlers import xa_linking  # noqa: F401

    _handlers_registered = True


def get_handler_registry() -> HandlerRegistry:
    """
    Get the handler registry with all handlers registered.

    This function lazily loads and registers all handlers on first call,
    avoiding heavy dependency loading at import time.

    Returns:
        HandlerRegistry with all handlers registered
    """
    _ensure_handlers_registered()
    return _get_handler_registry_base()


# For explicit imports of specific handlers, users can still do:
# from kafka_pipeline.claimx.handlers.project import ProjectHandler

__all__ = [
    # Base classes
    "EventHandler",
    "NoOpHandler",
    "EnrichmentResult",
    "HandlerResult",
    "HandlerRegistry",
    "ProjectCache",
    "get_handler_registry",
    "reset_registry",
    "register_handler",
]
