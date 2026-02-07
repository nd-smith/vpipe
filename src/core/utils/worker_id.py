"""Worker ID generation using coolnames for unique, memorable identifiers."""

from coolname import generate_slug


def generate_worker_id(prefix: str = "") -> str:
    """Generate a unique, memorable worker ID using coolnames.

    Uses coolnames library to create human-readable identifiers that are easier
    to trace in logs compared to machine names or UUIDs.

    Args:
        prefix: Optional prefix to prepend to the generated ID (e.g., "claimx-ingester")

    Returns:
        A unique worker ID in the format "prefix-word1-word2-word3" or "word1-word2-word3"

    Examples:
        >>> generate_worker_id()
        'brave-golden-tiger'
        >>> generate_worker_id("claimx-ingester")
        'claimx-ingester-swift-blue-falcon'
    """
    coolname_id = generate_slug(3)

    if prefix:
        return f"{prefix}-{coolname_id}"

    return coolname_id
