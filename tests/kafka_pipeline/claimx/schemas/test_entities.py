"""
Tests for ClaimX entity row schemas.

Validates EntityRowsMessage Pydantic model and helper methods.
"""

import pytest

from kafka_pipeline.claimx.schemas.entities import EntityRowsMessage


class TestEntityRowsMessageCreation:
    """Test EntityRowsMessage instantiation."""

    def test_create_empty(self):
        """EntityRowsMessage can be created empty."""
        rows = EntityRowsMessage()

        assert rows.projects == []
        assert rows.contacts == []
        assert rows.media == []
        assert rows.tasks == []
        assert rows.task_templates == []
        assert rows.external_links == []
        assert rows.video_collab == []

    def test_create_with_project_rows(self):
        """EntityRowsMessage can be created with project rows."""
        rows = EntityRowsMessage(
            projects=[
                {"project_id": "proj_001", "project_name": "Claim 2024"},
                {"project_id": "proj_002", "project_name": "Claim 2025"}
            ]
        )

        assert len(rows.projects) == 2
        assert rows.projects[0]["project_id"] == "proj_001"

    def test_create_with_multiple_entity_types(self):
        """EntityRowsMessage can contain multiple entity types."""
        rows = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}],
            contacts=[{"contact_id": "con_001"}],
            media=[{"media_id": "media_001"}]
        )

        assert len(rows.projects) == 1
        assert len(rows.contacts) == 1
        assert len(rows.media) == 1

    def test_create_with_all_entity_types(self):
        """EntityRowsMessage supports all 7 entity types."""
        rows = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}],
            contacts=[{"contact_id": "con_001"}],
            media=[{"media_id": "media_001"}],
            tasks=[{"task_id": "task_001"}],
            task_templates=[{"template_id": "tmpl_001"}],
            external_links=[{"link_id": "link_001"}],
            video_collab=[{"collaboration_id": "video_001"}]
        )

        assert len(rows.projects) == 1
        assert len(rows.contacts) == 1
        assert len(rows.media) == 1
        assert len(rows.tasks) == 1
        assert len(rows.task_templates) == 1
        assert len(rows.external_links) == 1
        assert len(rows.video_collab) == 1


class TestEntityRowsMessageIsEmpty:
    """Test is_empty method."""

    def test_is_empty_when_created_empty(self):
        """is_empty returns True for empty EntityRowsMessage."""
        rows = EntityRowsMessage()
        assert rows.is_empty() is True

    def test_is_empty_false_when_has_projects(self):
        """is_empty returns False when projects exist."""
        rows = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}]
        )
        assert rows.is_empty() is False

    def test_is_empty_false_when_has_contacts(self):
        """is_empty returns False when contacts exist."""
        rows = EntityRowsMessage(
            contacts=[{"contact_id": "con_001"}]
        )
        assert rows.is_empty() is False

    def test_is_empty_false_when_has_media(self):
        """is_empty returns False when media exists."""
        rows = EntityRowsMessage(
            media=[{"media_id": "media_001"}]
        )
        assert rows.is_empty() is False

    def test_is_empty_false_when_has_tasks(self):
        """is_empty returns False when tasks exist."""
        rows = EntityRowsMessage(
            tasks=[{"task_id": "task_001"}]
        )
        assert rows.is_empty() is False

    def test_is_empty_false_when_has_task_templates(self):
        """is_empty returns False when task_templates exist."""
        rows = EntityRowsMessage(
            task_templates=[{"template_id": "tmpl_001"}]
        )
        assert rows.is_empty() is False

    def test_is_empty_false_when_has_external_links(self):
        """is_empty returns False when external_links exist."""
        rows = EntityRowsMessage(
            external_links=[{"link_id": "link_001"}]
        )
        assert rows.is_empty() is False

    def test_is_empty_false_when_has_video_collab(self):
        """is_empty returns False when video_collab exists."""
        rows = EntityRowsMessage(
            video_collab=[{"collaboration_id": "video_001"}]
        )
        assert rows.is_empty() is False

    def test_is_empty_false_when_has_multiple_types(self):
        """is_empty returns False when multiple types exist."""
        rows = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}],
            media=[{"media_id": "media_001"}]
        )
        assert rows.is_empty() is False


class TestEntityRowsMessageRowCount:
    """Test row_count method."""

    def test_row_count_zero_when_empty(self):
        """row_count returns 0 for empty EntityRowsMessage."""
        rows = EntityRowsMessage()
        assert rows.row_count() == 0

    def test_row_count_with_single_type(self):
        """row_count returns correct count for single entity type."""
        rows = EntityRowsMessage(
            projects=[
                {"project_id": "proj_001"},
                {"project_id": "proj_002"},
                {"project_id": "proj_003"}
            ]
        )
        assert rows.row_count() == 3

    def test_row_count_with_multiple_types(self):
        """row_count sums across all entity types."""
        rows = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}, {"project_id": "proj_002"}],
            contacts=[{"contact_id": "con_001"}],
            media=[{"media_id": "media_001"}, {"media_id": "media_002"}, {"media_id": "media_003"}]
        )
        assert rows.row_count() == 6  # 2 + 1 + 3

    def test_row_count_with_all_types(self):
        """row_count includes all 7 entity types."""
        rows = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}],
            contacts=[{"contact_id": "con_001"}],
            media=[{"media_id": "media_001"}],
            tasks=[{"task_id": "task_001"}],
            task_templates=[{"template_id": "tmpl_001"}],
            external_links=[{"link_id": "link_001"}],
            video_collab=[{"collaboration_id": "video_001"}]
        )
        assert rows.row_count() == 7


class TestEntityRowsMessageMerge:
    """Test merge method."""

    def test_merge_empty_into_empty(self):
        """Merging two empty messages results in empty message."""
        rows1 = EntityRowsMessage()
        rows2 = EntityRowsMessage()

        rows1.merge(rows2)

        assert rows1.is_empty()

    def test_merge_non_empty_into_empty(self):
        """Merging non-empty message into empty message."""
        rows1 = EntityRowsMessage()
        rows2 = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}]
        )

        rows1.merge(rows2)

        assert len(rows1.projects) == 1
        assert rows1.projects[0]["project_id"] == "proj_001"

    def test_merge_empty_into_non_empty(self):
        """Merging empty message into non-empty message."""
        rows1 = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}]
        )
        rows2 = EntityRowsMessage()

        rows1.merge(rows2)

        assert len(rows1.projects) == 1

    def test_merge_same_type_extends_list(self):
        """Merging messages with same entity type extends the list."""
        rows1 = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}]
        )
        rows2 = EntityRowsMessage(
            projects=[{"project_id": "proj_002"}, {"project_id": "proj_003"}]
        )

        rows1.merge(rows2)

        assert len(rows1.projects) == 3
        assert rows1.projects[0]["project_id"] == "proj_001"
        assert rows1.projects[1]["project_id"] == "proj_002"
        assert rows1.projects[2]["project_id"] == "proj_003"

    def test_merge_different_types(self):
        """Merging messages with different entity types."""
        rows1 = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}]
        )
        rows2 = EntityRowsMessage(
            contacts=[{"contact_id": "con_001"}],
            media=[{"media_id": "media_001"}]
        )

        rows1.merge(rows2)

        assert len(rows1.projects) == 1
        assert len(rows1.contacts) == 1
        assert len(rows1.media) == 1

    def test_merge_all_types(self):
        """Merging messages with all entity types."""
        rows1 = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}],
            contacts=[{"contact_id": "con_001"}],
            media=[{"media_id": "media_001"}],
            tasks=[{"task_id": "task_001"}]
        )
        rows2 = EntityRowsMessage(
            projects=[{"project_id": "proj_002"}],
            task_templates=[{"template_id": "tmpl_001"}],
            external_links=[{"link_id": "link_001"}],
            video_collab=[{"collaboration_id": "video_001"}]
        )

        rows1.merge(rows2)

        assert len(rows1.projects) == 2
        assert len(rows1.contacts) == 1
        assert len(rows1.media) == 1
        assert len(rows1.tasks) == 1
        assert len(rows1.task_templates) == 1
        assert len(rows1.external_links) == 1
        assert len(rows1.video_collab) == 1
        assert rows1.row_count() == 8

    def test_merge_updates_row_count(self):
        """Merge correctly updates row_count."""
        rows1 = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}]
        )
        rows2 = EntityRowsMessage(
            contacts=[{"contact_id": "con_001"}, {"contact_id": "con_002"}]
        )

        initial_count = rows1.row_count()
        rows1.merge(rows2)
        final_count = rows1.row_count()

        assert initial_count == 1
        assert final_count == 3


class TestEntityRowsMessageSerialization:
    """Test JSON serialization and deserialization."""

    def test_model_dump_empty(self):
        """model_dump works for empty EntityRowsMessage."""
        rows = EntityRowsMessage()
        dumped = rows.model_dump()

        assert dumped["projects"] == []
        assert dumped["contacts"] == []
        assert dumped["media"] == []

    def test_model_dump_with_data(self):
        """model_dump includes all entity data."""
        rows = EntityRowsMessage(
            projects=[{"project_id": "proj_001", "project_name": "Test"}],
            media=[{"media_id": "media_001", "file_name": "test.jpg"}]
        )
        dumped = rows.model_dump()

        assert len(dumped["projects"]) == 1
        assert dumped["projects"][0]["project_id"] == "proj_001"
        assert len(dumped["media"]) == 1
        assert dumped["media"][0]["media_id"] == "media_001"

    def test_model_dump_json(self):
        """model_dump_json produces valid JSON."""
        rows = EntityRowsMessage(
            projects=[{"project_id": "proj_001"}]
        )
        json_str = rows.model_dump_json()

        assert isinstance(json_str, str)
        assert "proj_001" in json_str

    def test_model_validate_from_dict(self):
        """Can validate and create from dict."""
        data = {
            "projects": [{"project_id": "proj_001"}],
            "contacts": [],
            "media": [{"media_id": "media_001"}],
            "tasks": [],
            "task_templates": [],
            "external_links": [],
            "video_collab": []
        }

        rows = EntityRowsMessage.model_validate(data)

        assert len(rows.projects) == 1
        assert len(rows.media) == 1


class TestEntityRowsMessageEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_entity_rows_can_be_complex_dicts(self):
        """Entity rows can contain complex nested dictionaries."""
        rows = EntityRowsMessage(
            projects=[
                {
                    "project_id": "proj_001",
                    "project_name": "Complex Project",
                    "metadata": {
                        "created_by": "user@example.com",
                        "tags": ["urgent", "property"],
                        "nested": {
                            "level2": {
                                "value": "deep"
                            }
                        }
                    }
                }
            ]
        )

        assert rows.projects[0]["metadata"]["tags"][0] == "urgent"
        assert rows.projects[0]["metadata"]["nested"]["level2"]["value"] == "deep"

    def test_multiple_merges(self):
        """Multiple consecutive merges work correctly."""
        rows1 = EntityRowsMessage(projects=[{"project_id": "proj_001"}])
        rows2 = EntityRowsMessage(projects=[{"project_id": "proj_002"}])
        rows3 = EntityRowsMessage(projects=[{"project_id": "proj_003"}])

        rows1.merge(rows2)
        rows1.merge(rows3)

        assert len(rows1.projects) == 3
        assert rows1.row_count() == 3

    def test_large_row_count(self):
        """Can handle large number of entity rows."""
        rows = EntityRowsMessage(
            media=[{"media_id": f"media_{i}"} for i in range(100)]
        )

        assert rows.row_count() == 100
        assert not rows.is_empty()
