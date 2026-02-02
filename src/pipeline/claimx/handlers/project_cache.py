"""
Project cache for in-flight verification.

Maintains a simple set of project IDs that have been processed
to avoid redundant API calls.
"""

from core.logging import get_logger

logger = get_logger(__name__)


class ProjectCache:
    """
    Simple in-memory set of project IDs that have been processed.

    Used by handlers to avoid redundant API calls when verifying
    that project data exists before writing child entities.

    Delta merge handles duplicate project rows gracefully, so this
    cache only optimizes for the common case (same project, multiple events
    in a single worker session).
    """

    def __init__(self, ttl_seconds: int | None = None):
        """Initialize cache. ttl_seconds parameter ignored (kept for compatibility)."""
        self._projects: set[str] = set()
        logger.debug("ProjectCache initialized")

    def has(self, project_id: str) -> bool:
        """Check if project ID is in cache."""
        return project_id in self._projects

    def add(self, project_id: str) -> None:
        """Add project ID to cache."""
        self._projects.add(project_id)

    def load_from_ids(self, project_ids: list[str]) -> int:
        """
        Preload cache with existing project IDs.

        Args:
            project_ids: List of project IDs to preload

        Returns:
            Number of unique IDs loaded
        """
        before_size = len(self._projects)
        self._projects.update(str(pid) for pid in project_ids)
        loaded_count = len(self._projects) - before_size

        logger.info(
            "Preloaded project cache",
            extra={
                "loaded_count": loaded_count,
                "total_provided": len(project_ids),
                "cache_size": len(self._projects),
            },
        )
        return loaded_count

    def size(self) -> int:
        """Get current cache size."""
        return len(self._projects)

    def clear(self) -> None:
        """Clear all cached project IDs."""
        self._projects.clear()
