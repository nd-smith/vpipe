"""
Path resolution module.

Provides path generation logic for storage organization:
- Blob path generation for different event types
- OneLake path construction

Components:
    - generate_blob_path(): Generate storage path based on event subtype
    - get_onelake_path_for_event(): Combine base path with domain-specific path
"""

from core.paths.resolver import generate_blob_path, get_onelake_path_for_event

__all__ = [
    "generate_blob_path",
    "get_onelake_path_for_event",
]
