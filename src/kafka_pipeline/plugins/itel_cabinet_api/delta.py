"""
Delta table writer for iTel Cabinet data.

Simple wrapper around DeltaWriter for the two iTel tables.
"""

import logging
from typing import Any

from kafka_pipeline.common.writers.delta_writer import DeltaWriter

logger = logging.getLogger(__name__)


class ItelCabinetDeltaWriter:
    """
    Writes to both iTel Cabinet Delta tables.

    Wraps two DeltaWriter instances for simpler interface.
    """

    def __init__(self, submissions_table_path: str, attachments_table_path: str):
        """
        Initialize writers for both tables.

        Args:
            submissions_table_path: Full path to submissions Delta table
            attachments_table_path: Full path to attachments Delta table
        """
        self.submissions_writer = DeltaWriter(
            table_path=submissions_table_path,
            timestamp_column="updated_at",
        )

        self.attachments_writer = DeltaWriter(
            table_path=attachments_table_path,
            timestamp_column="created_at",
        )

        logger.info(
            "Initialized iTel Cabinet Delta writers",
            extra={
                'submissions_table': submissions_table_path,
                'attachments_table': attachments_table_path,
            }
        )

    async def write_submission(self, submission_row: dict) -> bool:
        """
        Write submission to Delta table using MERGE (upsert on assignment_id).

        Args:
            submission_row: Submission data dict

        Returns:
            True if successful
        """
        return await self.submissions_writer.merge_records(
            records=[submission_row],
            merge_keys=["assignment_id"],
            preserve_columns=["created_at"],
        )

    async def write_attachments(self, attachment_rows: list[dict]) -> bool:
        """
        Write attachments to Delta table using MERGE.

        Args:
            attachment_rows: List of attachment dicts

        Returns:
            True if successful
        """
        if not attachment_rows:
            return True

        return await self.attachments_writer.merge_records(
            records=attachment_rows,
            merge_keys=["assignment_id", "claim_media_id"],
            preserve_columns=["created_at"],
        )
