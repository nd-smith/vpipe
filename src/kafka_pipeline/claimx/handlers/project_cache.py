"""
Project cache for in-flight verification.

Prevents redundant API calls for project data by maintaining
an in-memory cache of project IDs that have been processed.

Supports TTL-based expiration for periodic refresh from Delta table.
Thread-safe for use across async tasks within a single worker instance.
"""

import time
from typing import Dict, List, Optional

from core.logging import get_logger

logger = get_logger(__name__)

# Default TTL of 30 minutes
DEFAULT_TTL_SECONDS = 1800


class ProjectCache:
    """
    In-memory cache of project IDs that have been processed.

    Used by handlers to avoid redundant API calls when verifying
    that project data exists before writing child entities (tasks, media, etc.).

    The cache is maintained per-worker instance and persists for the
    lifetime of the worker. It's not shared across workers or sessions,
    but that's acceptable because:
    - Delta merge handles duplicate project rows gracefully
    - Cache only optimizes for the common case (same project, multiple events)
    - Race conditions between workers are rare and harmless

    Features:
    - TTL-based expiration for periodic refresh
    - Optional preloading from Delta table at startup
    - Lazy expiration (items are removed when checked, not proactively)

    Usage:
        cache = ProjectCache(ttl_seconds=1800)  # 30 minute TTL

        # Optionally preload from Delta at startup
        existing_ids = await fetch_project_ids_from_delta()
        cache.load_from_ids(existing_ids)

        # Check if project is cached (respects TTL)
        if cache.has(project_id):
            # Skip API call
            pass
        else:
            # Fetch from API and add to cache
            project_rows = await fetch_project_data(project_id)
            cache.add(project_id)
    """

    def __init__(self, ttl_seconds: Optional[int] = None):
        """
        Initialize cache with optional TTL.

        Args:
            ttl_seconds: Time-to-live in seconds. If None, uses DEFAULT_TTL_SECONDS.
                         Set to 0 to disable TTL (items never expire).
        """
        self._ttl_seconds = (
            ttl_seconds if ttl_seconds is not None else DEFAULT_TTL_SECONDS
        )
        # Maps project_id -> timestamp when added
        self._projects: Dict[str, float] = {}
        self._hits = 0
        self._misses = 0
        self._expirations = 0
        logger.debug(
            "ProjectCache initialized", extra={"ttl_seconds": self._ttl_seconds}
        )

    def has(self, project_id: str) -> bool:
        """
        Check if project ID is in cache and not expired.

        Args:
            project_id: Project ID to check

        Returns:
            True if project is cached and not expired, False otherwise
        """
        if project_id not in self._projects:
            self._misses += 1
            return False

        # Check TTL if enabled
        if self._ttl_seconds > 0:
            added_at = self._projects[project_id]
            if time.monotonic() - added_at > self._ttl_seconds:
                # Entry expired - remove it
                del self._projects[project_id]
                self._expirations += 1
                self._misses += 1
                logger.debug(
                    "Project cache entry expired",
                    extra={
                        "project_id": project_id,
                        "ttl_seconds": self._ttl_seconds,
                    },
                )
                return False

        self._hits += 1
        return True

    def add(self, project_id: str) -> None:
        """
        Add project ID to cache with current timestamp.

        Args:
            project_id: Project ID to add
        """
        is_new = project_id not in self._projects
        self._projects[project_id] = time.monotonic()
        if is_new:
            logger.debug(
                "Added project to cache",
                extra={
                    "project_id": project_id,
                    "cache_size": len(self._projects),
                },
            )

    def load_from_ids(self, project_ids: List[str]) -> int:
        """
        Preload cache with existing project IDs (e.g., from Delta table).

        All loaded entries get the same timestamp (current time), so they'll
        expire together after TTL.

        Args:
            project_ids: List of project IDs to preload

        Returns:
            Number of unique IDs loaded
        """
        now = time.monotonic()
        loaded_count = 0
        for pid in project_ids:
            pid_str = str(pid)
            if pid_str not in self._projects:
                self._projects[pid_str] = now
                loaded_count += 1

        logger.info(
            "Preloaded project cache from Delta",
            extra={
                "loaded_count": loaded_count,
                "total_provided": len(project_ids),
                "cache_size": len(self._projects),
                "ttl_seconds": self._ttl_seconds,
            },
        )
        return loaded_count

    def size(self) -> int:
        """Get current cache size (includes potentially expired entries)."""
        return len(self._projects)

    def clear(self) -> None:
        """Clear all cached project IDs."""
        count = len(self._projects)
        self._projects.clear()
        logger.debug("Cleared project cache", extra={"cleared_count": count})

    def get_stats(self) -> Dict[str, int]:
        """
        Get cache statistics.

        Returns:
            Dict with hits, misses, expirations, and size
        """
        return {
            "hits": self._hits,
            "misses": self._misses,
            "expirations": self._expirations,
            "size": len(self._projects),
            "hit_rate_pct": (
                round(self._hits / (self._hits + self._misses) * 100, 1)
                if (self._hits + self._misses) > 0
                else 0.0
            ),
        }

    def reset_stats(self) -> None:
        """Reset cache statistics (useful for periodic reporting)."""
        self._hits = 0
        self._misses = 0
        self._expirations = 0
