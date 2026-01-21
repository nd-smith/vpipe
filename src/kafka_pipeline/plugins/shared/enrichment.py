"""
Plugin Enrichment Framework

Provides infrastructure for enriching/transforming data in plugin workers
before sending to external systems. Supports data transformation, external
lookups, validation, and batching.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

from kafka_pipeline.plugins.shared.connections import ConnectionManager
from kafka_pipeline.common.logging import LoggedClass

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentContext:
    """Context passed to enrichment handlers.

    Attributes:
        message: The original message from Kafka
        data: Mutable dict for handler data passing
        metadata: Read-only metadata about the message
        connection_manager: HTTP connection manager for external lookups
    """

    message: dict[str, Any]
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    connection_manager: Optional[ConnectionManager] = None

    def get(self, key: str, default: Any = None) -> Any:
        """Get value from data dict."""
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set value in data dict."""
        self.data[key] = value


@dataclass
class EnrichmentResult:
    """Result from enrichment handler execution.

    Attributes:
        success: Whether enrichment succeeded
        data: Enriched data (typically modified context.data)
        skip: If True, skip downstream handlers and don't send to API
        error: Error message if enrichment failed
    """

    success: bool
    data: Optional[dict[str, Any]] = None
    skip: bool = False
    error: Optional[str] = None

    @classmethod
    def ok(cls, data: dict[str, Any]) -> "EnrichmentResult":
        """Create successful result."""
        return cls(success=True, data=data)

    @classmethod
    def skip_message(cls, reason: str) -> "EnrichmentResult":
        """Create skip result (don't process further)."""
        logger.info(f"Skipping message: {reason}")
        return cls(success=True, skip=True)

    @classmethod
    def failed(cls, error: str) -> "EnrichmentResult":
        """Create failed result."""
        logger.error(f"Enrichment failed: {error}")
        return cls(success=False, error=error)


class EnrichmentHandler(LoggedClass, ABC):
    """Base class for enrichment handlers.

    Handlers are executed in sequence to transform/enrich data before
    sending to external APIs. Each handler receives the context and can:
    - Transform data (modify context.data)
    - Fetch additional data from external sources
    - Validate and filter messages (skip unwanted ones)
    - Aggregate/batch data

    Handlers should be stateless and async.

    Provides logging infrastructure via LoggedClass:
    - self._logger: Logger instance
    - self._log(level, msg, **extra): Log with automatic context
    - self._log_exception(exc, msg, **extra): Exception logging with context
    """

    def __init__(self, config: Optional[dict[str, Any]] = None):
        """Initialize handler with configuration.


        """
        self.config = config or {}
        self.name = self.__class__.__name__
        super().__init__()  # Initialize LoggedClass

    @abstractmethod
    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """Enrich the message data.
        """
        pass

    async def initialize(self) -> None:
        """Initialize handler (called once at startup).

        Override to perform one-time setup (e.g., load lookup tables).
        """
        pass

    async def cleanup(self) -> None:
        """Cleanup handler resources (called at shutdown).

        Override to perform cleanup (e.g., close connections).
        """
        pass


class TransformHandler(EnrichmentHandler):
    """Handler for data transformation using field mappings.

    Supports:
    - Field extraction/renaming
    - Nested field access (dot notation)
    - Default values
    - Simple expressions (in future)

    Config example:
        type: transform
        config:
          mappings:
            output_field: input_field
            project_name: task.project.name
            status: task_status
          defaults:
            priority: "normal"
    """

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """Transform data using configured mappings."""
        mappings = self.config.get("mappings", {})
        defaults = self.config.get("defaults", {})

        output = {}
        for output_key, input_path in mappings.items():
            value = self._get_nested(context.message, input_path)
            if value is not None:
                output[output_key] = value
            elif output_key in defaults:
                output[output_key] = defaults[output_key]
                logger.debug(f"Using default value for {output_key}: {defaults[output_key]}")

        # Apply defaults for missing fields
        for key, default_value in defaults.items():
            if key not in output:
                output[key] = default_value

        # Merge with existing data
        context.data.update(output)

        logger.debug(f"Transformed {len(mappings)} fields")
        return EnrichmentResult.ok(context.data)

    def _get_nested(self, obj: dict, path: str) -> Any:
        """Get nested field using dot notation.
        """
        parts = path.split(".")
        current = obj

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None

            if current is None:
                return None

        return current


class ValidationHandler(EnrichmentHandler):
    """Handler for business logic validation and filtering.

    Supports:
    - Required field validation
    - Value constraints (min/max, allowed values)
    - Custom validation rules
    - Conditional filtering

    Config example:
        type: validation
        config:
          required_fields:
            - project_id
            - task_id
          field_rules:
            status:
              allowed_values: [completed, verified]
            priority:
              min: 1
              max: 10
          skip_if:
            field: task_status
            equals: cancelled
    """

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """Validate data and optionally skip message."""
        # Check skip conditions first
        skip_if = self.config.get("skip_if")
        if skip_if:
            field = skip_if.get("field")
            expected = skip_if.get("equals")
            value = self._get_value(context, field)

            if value == expected:
                return EnrichmentResult.skip_message(
                    f"Field '{field}' equals '{expected}'"
                )
        required_fields = self.config.get("required_fields", [])
        for field in required_fields:
            value = self._get_value(context, field)
            if value is None:
                return EnrichmentResult.failed(
                    f"Required field '{field}' is missing"
                )
        field_rules = self.config.get("field_rules", {})
        for field, rules in field_rules.items():
            value = self._get_value(context, field)

            if value is None:
                continue  # Skip validation if field not present (use required_fields for that)
            if "allowed_values" in rules:
                if value not in rules["allowed_values"]:
                    return EnrichmentResult.failed(
                        f"Field '{field}' value '{value}' not in allowed values: {rules['allowed_values']}"
                    )

            # Check min/max
            if "min" in rules and value < rules["min"]:
                return EnrichmentResult.failed(
                    f"Field '{field}' value {value} below minimum {rules['min']}"
                )

            if "max" in rules and value > rules["max"]:
                return EnrichmentResult.failed(
                    f"Field '{field}' value {value} above maximum {rules['max']}"
                )

        logger.debug("Validation passed")
        return EnrichmentResult.ok(context.data)

    def _get_value(self, context: EnrichmentContext, field: str) -> Any:
        """Get field value from context (checks both data and message)."""
        # Check enriched data first
        if field in context.data:
            return context.data[field]

        # Fall back to original message
        if field in context.message:
            return context.message[field]
        if "." in field:
            parts = field.split(".")
            obj = context.message
            for part in parts:
                if isinstance(obj, dict):
                    obj = obj.get(part)
                else:
                    return None
                if obj is None:
                    return None
            return obj

        return None


class LookupHandler(EnrichmentHandler):
    """Handler for external data lookup.

    Fetches additional data from external APIs to enrich the message.

    Config example:
        type: lookup
        config:
          connection: external_api
          endpoint: /v1/projects/{project_id}
          path_params:
            project_id: project_id  # Use field from message
          query_params:
            include: details
          result_field: project_details
          cache_ttl: 300  # Cache for 5 minutes
    """

    def __init__(self, config: Optional[dict[str, Any]] = None):
        self._cache: dict[str, Any] = {}
        self._cache_timestamps: dict[str, float] = {}
        super().__init__(config)

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """Fetch data from external API and add to context."""
        if not context.connection_manager:
            return EnrichmentResult.failed("No connection manager available")

        connection_name = self.config.get("connection")
        if not connection_name:
            return EnrichmentResult.failed("No connection specified in config")

        # Build endpoint with path params
        endpoint = self.config.get("endpoint", "")
        path_params = self.config.get("path_params", {})

        for param_name, field_name in path_params.items():
            value = context.message.get(field_name) or context.data.get(field_name)
            if value is None:
                return EnrichmentResult.failed(
                    f"Path parameter '{param_name}' field '{field_name}' not found"
                )
            endpoint = endpoint.replace(f"{{{param_name}}}", str(value))
        query_params = {}
        config_params = self.config.get("query_params", {})
        for param_name, value_or_field in config_params.items():
            # Check if it's a field reference or literal value
            if isinstance(value_or_field, str) and value_or_field in context.message:
                query_params[param_name] = context.message[value_or_field]
            elif isinstance(value_or_field, str) and value_or_field in context.data:
                query_params[param_name] = context.data[value_or_field]
            else:
                query_params[param_name] = value_or_field
        cache_key = f"{connection_name}:{endpoint}:{query_params}"
        cache_ttl = self.config.get("cache_ttl", 0)

        if cache_ttl > 0 and cache_key in self._cache:
            import time

            age = time.time() - self._cache_timestamps.get(cache_key, 0)
            if age < cache_ttl:
                logger.debug(f"Cache hit for {endpoint} (age: {age:.1f}s)")
                cached_data = self._cache[cache_key]
                result_field = self.config.get("result_field", "lookup_result")
                context.data[result_field] = cached_data
                return EnrichmentResult.ok(context.data)
        try:
            status, response_data = await context.connection_manager.request_json(
                connection_name=connection_name,
                method="GET",
                path=endpoint,
                params=query_params if query_params else None,
            )

            if status >= 400:
                return EnrichmentResult.failed(
                    f"API request failed with status {status}: {response_data}"
                )

            # Store result in context
            result_field = self.config.get("result_field", "lookup_result")
            context.data[result_field] = response_data
            if cache_ttl > 0:
                import time

                self._cache[cache_key] = response_data
                self._cache_timestamps[cache_key] = time.time()

            logger.debug(f"Lookup successful: {endpoint}")
            return EnrichmentResult.ok(context.data)

        except Exception as e:
            logger.exception(f"Lookup failed: {e}")
            return EnrichmentResult.failed(f"Lookup exception: {str(e)}")


class BatchingHandler(EnrichmentHandler):
    """Handler for batching multiple messages.

    Collects messages and sends them in batches to reduce API calls.
    This handler is special - it accumulates state across messages.

    Config example:
        type: batching
        config:
          batch_size: 10
          batch_timeout_seconds: 5.0
          batch_field: items
    """

    def __init__(self, config: Optional[dict[str, Any]] = None):
        self._batch: list[dict[str, Any]] = []
        self._batch_lock = asyncio.Lock()
        self._last_flush = asyncio.get_event_loop().time()
        super().__init__(config)

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """Add message to batch, return batch when ready."""
        batch_size = self.config.get("batch_size", 10)
        batch_timeout = self.config.get("batch_timeout_seconds", 5.0)

        async with self._batch_lock:
            # Add current message to batch
            self._batch.append(context.data.copy())

            current_time = asyncio.get_event_loop().time()
            time_since_last_flush = current_time - self._last_flush

            # Check if we should flush
            should_flush = (
                len(self._batch) >= batch_size or time_since_last_flush >= batch_timeout
            )

            if should_flush:
                batch_field = self.config.get("batch_field", "items")
                batch_data = {batch_field: self._batch.copy(), "batch_size": len(self._batch)}

                logger.info(
                    f"Flushing batch of {len(self._batch)} messages "
                    f"(timeout: {time_since_last_flush:.1f}s)"
                )
                self._batch.clear()
                self._last_flush = current_time

                # Return batch for processing
                return EnrichmentResult.ok(batch_data)
            else:
                # Skip this message (it's in the batch, will be sent later)
                return EnrichmentResult.skip_message(
                    f"Added to batch ({len(self._batch)}/{batch_size})"
                )

    async def flush(self) -> Optional[dict[str, Any]]:
        """Force flush current batch.
        """
        async with self._batch_lock:
            if not self._batch:
                return None

            batch_field = self.config.get("batch_field", "items")
            batch_data = {batch_field: self._batch.copy(), "batch_size": len(self._batch)}

            logger.info(f"Force flushing batch of {len(self._batch)} messages")

            self._batch.clear()
            self._last_flush = asyncio.get_event_loop().time()

            return batch_data


# Registry of built-in handler types
BUILTIN_HANDLERS = {
    "transform": TransformHandler,
    "validation": ValidationHandler,
    "lookup": LookupHandler,
    "batching": BatchingHandler,
}


def create_handler_from_config(config: dict[str, Any]) -> EnrichmentHandler:
    """Create enrichment handler from configuration.
    """
    handler_type = config.get("type")
    if not handler_type:
        raise ValueError("Handler config missing 'type' field")

    handler_config = config.get("config", {})

    # Check built-in handlers first
    if handler_type in BUILTIN_HANDLERS:
        handler_class = BUILTIN_HANDLERS[handler_type]
        return handler_class(config=handler_config)

    # Try to import custom handler class
    if ":" in handler_type:
        module_path, class_name = handler_type.rsplit(":", 1)
        try:
            import importlib

            module = importlib.import_module(module_path)
            handler_class = getattr(module, class_name)
            return handler_class(config=handler_config)
        except Exception as e:
            raise ValueError(f"Failed to load handler '{handler_type}': {e}")

    raise ValueError(
        f"Unknown handler type '{handler_type}'. "
        f"Available: {list(BUILTIN_HANDLERS.keys())}"
    )


class EnrichmentPipeline:
    """Chain of enrichment handlers executed in sequence."""

    def __init__(self, handlers: list[EnrichmentHandler]):
        """Initialize pipeline with handlers.


        """
        self.handlers = handlers

    async def execute(
        self, message: dict[str, Any], connection_manager: Optional[ConnectionManager] = None
    ) -> EnrichmentResult:
        """Execute all handlers in sequence.
        """
        context = EnrichmentContext(
            message=message,
            data=message.copy(),  # Start with message as initial data
            connection_manager=connection_manager,
        )

        for handler in self.handlers:
            try:
                result = await handler.enrich(context)

                if not result.success:
                    logger.error(f"Handler {handler.name} failed: {result.error}")
                    return result

                if result.skip:
                    logger.debug(f"Handler {handler.name} skipped message")
                    return result

                # Update context with enriched data
                if result.data:
                    context.data = result.data

            except Exception as e:
                logger.exception(f"Handler {handler.name} raised exception: {e}")
                return EnrichmentResult.failed(f"Exception in {handler.name}: {str(e)}")
        return EnrichmentResult.ok(context.data)

    async def initialize(self) -> None:
        """Initialize all handlers."""
        for handler in self.handlers:
            try:
                await handler.initialize()
                logger.info(f"Initialized handler: {handler.name}")
            except Exception as e:
                logger.error(f"Failed to initialize handler {handler.name}: {e}")
                raise

    async def cleanup(self) -> None:
        """Cleanup all handlers."""
        for handler in self.handlers:
            try:
                await handler.cleanup()
                logger.info(f"Cleaned up handler: {handler.name}")
            except Exception as e:
                logger.error(f"Failed to cleanup handler {handler.name}: {e}")
