"""Shared configuration for consumer tuning parameters."""

from dataclasses import dataclass


@dataclass
class ConsumerConfig:
    """Bundled tuning parameters for create_consumer / create_batch_consumer.

    Groups the optional knobs that control consumer behavior (batching,
    prefetch, commit strategy, etc.) so that factory functions in
    ``transport.py`` accept a single config object instead of many
    individual keyword arguments.

    Any field left at its default matches the previous hard-coded default
    in the factory functions, so callers that don't need to override
    anything can simply omit the parameter.
    """

    enable_message_commit: bool = True
    instance_id: str | None = None
    prefetch: int = 300
    starting_position: str | None = None
    batch_size: int = 20
    max_batch_size: int | None = None
    batch_timeout_ms: int = 1000
    checkpoint_interval: int = 1
