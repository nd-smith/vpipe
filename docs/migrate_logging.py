#!/usr/bin/env python3
"""Migrate logging calls from custom helpers to standard logging."""

import re
import sys
from pathlib import Path


def migrate_file(file_path: Path) -> bool:
    """Migrate a single file. Returns True if changes were made."""
    content = file_path.read_text()
    original = content

    # Remove imports of deprecated helpers
    content = re.sub(
        r'from core\.logging import get_logger, log_exception, log_with_context\n',
        '',
        content
    )
    content = re.sub(
        r'from core\.logging import get_logger, log_with_context\n',
        '',
        content
    )
    content = re.sub(
        r'from core\.logging import get_logger, log_exception\n',
        '',
        content
    )
    content = re.sub(
        r'from core\.logging import MessageLogContext, get_logger, log_exception, log_with_context\n',
        'from core.logging import MessageLogContext\n',
        content
    )
    content = re.sub(
        r'from core\.logging\.setup import get_logger\n',
        '',
        content
    )

    # Add logging import if not present and logger is used
    if 'logger = logging.getLogger(__name__)' in content or 'get_logger(__name__)' in content:
        if 'import logging' not in content:
            # Add after other stdlib imports
            content = re.sub(
                r'(import asyncio\n)',
                r'\1import logging\n',
                content
            )
            # Fallback: add at top after docstring
            if 'import logging' not in content:
                content = re.sub(
                    r'(""".*?"""\n\n)',
                    r'\1import logging\n',
                    content,
                    flags=re.DOTALL
                )

    # Change logger creation
    content = re.sub(
        r'logger = get_logger\(__name__\)',
        'logger = logging.getLogger(__name__)',
        content
    )

    # Fix log_with_context calls - simple pattern
    # Handles: log_with_context(logger, logging.LEVEL, "msg", key=value, ...)
    def replace_log_with_context(match):
        level = match.group(1)
        msg = match.group(2)
        rest = match.group(3)

        # Convert level to method name
        level_map = {
            'logging.DEBUG': 'debug',
            'logging.INFO': 'info',
            'logging.WARNING': 'warning',
            'logging.ERROR': 'error',
            'logging.CRITICAL': 'critical',
        }
        method = level_map.get(level, 'info')

        if rest.strip():
            return f'logger.{method}(\n            {msg},\n            extra={{{rest}\n            }},\n        )'
        else:
            return f'logger.{method}({msg})'

    # Pattern for log_with_context
    pattern = r'log_with_context\(\s*logger,\s*(logging\.\w+),\s*([^,]+),\s*(.*?)\s*\)'

    # This is a simplified pattern - for complex cases we need manual intervention

    # Fix log_exception calls
    # log_exception(logger, exc, "msg", key=value, ...) -> logger.error("msg", extra={key: value}, exc_info=exc)
    def replace_log_exception(match):
        exc = match.group(1)
        msg = match.group(2)
        rest = match.group(3)

        if rest.strip():
            return f'logger.error(\n                {msg},\n                extra={{{rest}\n                }},\n                exc_info={exc},\n            )'
        else:
            return f'logger.error({msg}, exc_info={exc})'

    pattern_exc = r'log_exception\(\s*logger,\s*([^,]+),\s*([^,]+),\s*(.*?)\s*\)'

    changed = content != original

    if changed:
        file_path.write_text(content)
        print(f"Updated: {file_path}")

    return changed


def main():
    """Migrate all specified files."""
    files_to_migrate = [
        # Common pipeline files
        "src/pipeline/common/eventhub/producer.py",
        "src/pipeline/common/eventhub/consumer.py",
        "src/pipeline/common/eventhub/checkpoint_store.py",
        "src/pipeline/common/producer.py",
        "src/pipeline/common/consumer.py",
        "src/pipeline/common/auth.py",
        "src/pipeline/common/storage/delta.py",
        "src/pipeline/common/storage/onelake.py",
        "src/pipeline/plugins/shared/registry.py",
        "src/pipeline/plugins/shared/enrichment.py",
        "src/pipeline/plugins/shared/loader.py",
        "src/pipeline/plugins/shared/base.py",
        # ClaimX workers
        "src/pipeline/claimx/workers/download_worker.py",
        "src/pipeline/claimx/workers/enrichment_worker.py",
        "src/pipeline/claimx/workers/entity_delta_worker.py",
        "src/pipeline/claimx/workers/event_ingester.py",
        "src/pipeline/claimx/workers/result_processor.py",
        "src/pipeline/claimx/workers/upload_worker.py",
        # Verisk workers
        "src/pipeline/verisk/workers/download_worker.py",
        "src/pipeline/verisk/workers/enrichment_worker.py",
        "src/pipeline/verisk/workers/event_ingester.py",
        "src/pipeline/verisk/workers/result_processor.py",
        "src/pipeline/verisk/workers/upload_worker.py",
        "src/pipeline/verisk/workers/delta_events_worker.py",
        # Retry handlers
        "src/pipeline/claimx/retry/download_handler.py",
        "src/pipeline/claimx/retry/enrichment_handler.py",
        "src/pipeline/verisk/retry/download_handler.py",
        "src/pipeline/verisk/retry/enrichment_handler.py",
        # DLQ handlers
        "src/pipeline/claimx/dlq/handler.py",
        "src/pipeline/verisk/dlq/handler.py",
        # Common retry/delta
        "src/pipeline/common/retry/delta_handler.py",
        "src/pipeline/common/retry/unified_scheduler.py",
        # Handlers and utils
        "src/pipeline/claimx/handlers/utils.py",
        "src/pipeline/claimx/handlers/project_cache.py",
        "src/pipeline/claimx/workers/download_factory.py",
        "src/pipeline/claimx/workers/delta_events_worker.py",
        "src/pipeline/claimx/writers/delta_entities.py",
        # Common
        "src/pipeline/common/__init__.py",
        "src/pipeline/common/transport.py",
        "src/pipeline/common/eventhouse/poller.py",
        "src/pipeline/common/eventhouse/sinks.py",
        "src/pipeline/common/writers/base.py",
        # Core
        "src/core/security/url_validation.py",
    ]

    root = Path("/home/nick/projects/vpipe")
    changed_count = 0

    for file_rel in files_to_migrate:
        file_path = root / file_rel
        if file_path.exists():
            if migrate_file(file_path):
                changed_count += 1
        else:
            print(f"Not found: {file_path}", file=sys.stderr)

    print(f"\nMigrated {changed_count} files")


if __name__ == "__main__":
    main()
