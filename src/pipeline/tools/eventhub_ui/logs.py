"""OneLake log browsing and filtering."""

import json
import os
from dataclasses import dataclass

from pipeline.common.storage.onelake import OneLakeClient


@dataclass
class LogEntry:
    ts: str
    level: str
    logger: str
    message: str
    domain: str
    stage: str
    worker_id: str
    trace_id: str
    raw: dict  # full parsed JSON for detail view


def get_onelake_log_path() -> str:
    """Get the OneLake log path from config."""
    from config.config import DEFAULT_CONFIG_FILE, _expand_env_vars, load_yaml

    path = os.getenv("ONELAKE_LOG_PATH")
    if path:
        return path

    if DEFAULT_CONFIG_FILE.exists():
        data = load_yaml(DEFAULT_CONFIG_FILE)
        data = _expand_env_vars(data)
        path = data.get("observability", {}).get("onelake_log_path", "")
        if path:
            return path

    raise ValueError(
        "No OneLake log path configured. Set ONELAKE_LOG_PATH."
    )


def _get_client() -> OneLakeClient:
    return OneLakeClient(base_path=get_onelake_log_path())


def list_log_domains() -> list[str]:
    """List available domain directories under logs/."""
    client = _get_client()
    entries = client.list_directory("logs")
    return sorted(
        e["name"].split("/")[-1]
        for e in entries
        if e["is_directory"]
    )


def list_log_dates(domain: str) -> list[str]:
    """List available date directories for a domain."""
    client = _get_client()
    entries = client.list_directory(f"logs/{domain}")
    return sorted(
        (e["name"].split("/")[-1] for e in entries if e["is_directory"]),
        reverse=True,
    )


@dataclass
class LogFileInfo:
    name: str
    path: str  # relative path for download
    size: int
    last_modified: str


def list_log_files(domain: str, date: str) -> list[LogFileInfo]:
    """List log files for a domain/date."""
    from concurrent.futures import ThreadPoolExecutor

    client = _get_client()
    prefix = f"logs/{domain}/{date}"
    entries = client.list_directory(prefix)

    files = []
    subdirs = []
    for e in entries:
        if e["is_directory"]:
            subdirs.append(e["name"])
        else:
            files.append(LogFileInfo(
                name=e["name"].split("/")[-1],
                path=e["name"],
                size=e["size"],
                last_modified=str(e["last_modified"] or ""),
            ))

    # Fetch archive subdirectories in parallel
    if subdirs:
        with ThreadPoolExecutor(max_workers=min(len(subdirs), 4)) as pool:
            results = pool.map(client.list_directory, subdirs)
        for archive_entries in results:
            for ae in archive_entries:
                if not ae["is_directory"]:
                    files.append(LogFileInfo(
                        name=ae["name"].split("/")[-1],
                        path=ae["name"],
                        size=ae["size"],
                        last_modified=str(ae["last_modified"] or ""),
                    ))

    return sorted(files, key=lambda f: f.name, reverse=True)


def _parse_log_entry(
    line: str,
    level: str | None,
    search_lower: str | None,
    trace_id: str | None,
) -> LogEntry | None:
    """Parse a single JSON log line and apply filters. Returns None if filtered out."""
    line = line.strip()
    if not line:
        return None

    try:
        raw = json.loads(line)
    except json.JSONDecodeError:
        return None

    entry_level = raw.get("level", "")
    entry_message = raw.get("message", "")
    entry_trace_id = raw.get("trace_id", "")

    if level and entry_level != level:
        return None
    if trace_id and trace_id not in entry_trace_id:
        return None
    if search_lower and search_lower not in entry_message.lower():
        return None

    return LogEntry(
        ts=raw.get("ts", ""),
        level=entry_level,
        logger=raw.get("logger", ""),
        message=entry_message,
        domain=raw.get("domain", ""),
        stage=raw.get("stage", ""),
        worker_id=raw.get("worker_id", ""),
        trace_id=entry_trace_id,
        raw=raw,
    )


def read_log_file(
    path: str,
    level: str | None = None,
    search: str | None = None,
    trace_id: str | None = None,
    tail: int = 500,
) -> list[LogEntry]:
    """Download and parse a log file, applying filters.

    Args:
        path: Relative path to the log file in OneLake.
        level: Filter to this log level (e.g. "ERROR").
        search: Free-text search in message field.
        trace_id: Filter to this trace_id.
        tail: Max number of lines to return (from end of file).
    """
    client = _get_client()
    content = client.download_bytes(path)
    text = content.decode("utf-8", errors="replace")

    lines = text.strip().splitlines()

    # Take last N lines first (most recent), then filter
    if len(lines) > tail:
        lines = lines[-tail:]

    search_lower = search.lower() if search else None

    entries = []
    for line in lines:
        entry = _parse_log_entry(line, level, search_lower, trace_id)
        if entry is not None:
            entries.append(entry)

    return entries
