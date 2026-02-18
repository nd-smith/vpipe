"""Logging setup and configuration."""

import contextlib
import logging
import os
import secrets
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

from core.logging.context import set_log_context
from core.logging.filters import StageContextFilter
from core.logging.formatters import ConsoleFormatter, JSONFormatter

# Default settings
DEFAULT_LOG_DIR = Path("logs")
DEFAULT_ROTATION_WHEN = "H"  # When to rotate: 'H' (hourly), 'midnight', etc.
DEFAULT_ROTATION_INTERVAL = 1  # Interval for rotation (every 1 hour)
DEFAULT_BACKUP_COUNT = 24  # Keep 24 hours of logs by default
DEFAULT_CONSOLE_LEVEL = logging.INFO
DEFAULT_FILE_LEVEL = logging.DEBUG


@dataclass
class LoggingConfig:
    """Configuration for logging handlers and formatting.

    Groups the many optional parameters for setup_logging() and
    setup_multi_worker_logging() into a single object.
    """

    json_format: bool = True
    console_level: int = DEFAULT_CONSOLE_LEVEL
    file_level: int = DEFAULT_FILE_LEVEL
    rotation_when: str = DEFAULT_ROTATION_WHEN
    rotation_interval: int = DEFAULT_ROTATION_INTERVAL
    backup_count: int = DEFAULT_BACKUP_COUNT
    suppress_noisy: bool = True
    use_instance_id: bool = True
    log_to_stdout: bool = False
    eventhub_config: dict[str, Any] | None = None
    enable_eventhub_logging: bool = True


def _resolve_onelake_log_path() -> str:
    """Resolve OneLake log path from env var or config.yaml."""
    path = os.getenv("ONELAKE_LOG_PATH")
    if path:
        return path

    try:
        from config.config import DEFAULT_CONFIG_FILE, _expand_env_vars, load_yaml

        if DEFAULT_CONFIG_FILE.exists():
            data = load_yaml(DEFAULT_CONFIG_FILE)
            data = _expand_env_vars(data)
            path = data.get("observability", {}).get("onelake_log_path", "")
            if path:
                return path
    except Exception:
        pass

    return ""


def _generate_instance_id() -> str:
    """Generate unique human-readable instance ID using coolname."""
    from coolname import generate_slug

    return generate_slug(2)


# Noisy loggers to suppress
NOISY_LOGGERS = [
    "azure.core.pipeline.policies.http_logging_policy",
    "azure.eventhub",
    "azure.eventhub._pyamqp",
    "azure.identity",
    "azure.kusto",
    "azure.kusto.data",
    "azure.kusto.data.security",
    "uamqp",
    "urllib3",
    "aiohttp",
    "aiokafka",
]


class LogArchiver:
    """Moves rotated log files into an archive directory."""

    def __init__(self, archive_dir: Path):
        self.archive_dir = archive_dir
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def archive_rotated_files(self, log_dir: Path, base_name: str) -> None:
        """Move rotated backup files (base_name.*) from log_dir to archive_dir."""
        for rotated_file in log_dir.glob(f"{base_name}.*"):
            archive_file = self.archive_dir / rotated_file.name
            try:
                shutil.move(str(rotated_file), str(archive_file))
            except Exception as e:
                print(f"Warning: Failed to archive {rotated_file}: {e}", file=sys.stderr)


class OneLakeLogUploader:
    """Uploads archived log files to OneLake and manages local retention."""

    def __init__(self, onelake_client, log_retention_hours: int):
        self.onelake_client = onelake_client
        self.log_retention_hours = log_retention_hours

    def upload_archived(self, archive_dir: Path, base_name: str, log_dir: Path) -> None:
        """Upload archived rotated files to OneLake."""
        for rotated_file in archive_dir.glob(f"{base_name}.*"):
            try:
                relative_path = rotated_file.resolve().relative_to(log_dir.parent.parent)
                onelake_path = f"logs/{relative_path}"
                self.onelake_client.upload_file(
                    relative_path=onelake_path,
                    local_path=str(rotated_file),
                    overwrite=True,
                )
            except Exception as e:
                print(
                    f"Warning: Failed to upload log {rotated_file}: {e}",
                    file=sys.stderr,
                )

    def cleanup_old_logs(self, log_dir: Path, archive_dir: Path) -> None:
        """Remove log files older than retention period."""
        if self.log_retention_hours <= 0:
            return

        cutoff_time = time.time() - (self.log_retention_hours * 3600)
        for directory in [log_dir, archive_dir]:
            if not directory.exists():
                continue
            for log_file in directory.glob("*.log*"):
                try:
                    if log_file.stat().st_mtime < cutoff_time:
                        log_file.unlink()
                        print(f"Cleaned up old log: {log_file.name}", file=sys.stderr)
                except Exception as e:
                    print(f"Warning: Failed to cleanup {log_file}: {e}", file=sys.stderr)


class PipelineFileHandler(TimedRotatingFileHandler):
    """Single file handler with optional archiving, size-based rollover, and OneLake upload.

    Combines time-based rotation (from TimedRotatingFileHandler) with optional
    size-based rotation, automatic archiving of rotated files, and OneLake upload.

    Args:
        filename: Log file path.
        archiver: Optional LogArchiver to move rotated files to an archive dir.
        uploader: Optional OneLakeLogUploader to upload and clean up rotated files.
        max_bytes: Maximum file size before rotation (0 to disable size-based rotation).
    """

    def __init__(
        self,
        filename,
        *,
        archiver: LogArchiver | None = None,
        uploader: OneLakeLogUploader | None = None,
        max_bytes: int = 0,
        **kwargs,
    ):
        super().__init__(filename, **kwargs)
        self.archiver = archiver
        self.uploader = uploader
        self.max_bytes = max_bytes

        # Store archive_dir for crash log upload to find archived files
        self.archive_dir = archiver.archive_dir if archiver else None
        # Store onelake_client for crash log upload to reuse
        self.onelake_client = uploader.onelake_client if uploader else None

        if uploader:
            log_path = Path(self.baseFilename)
            uploader.cleanup_old_logs(log_path.parent, archiver.archive_dir)

    def shouldRollover(self, record):
        if super().shouldRollover(record):
            return 1

        if self.max_bytes > 0:
            if self.stream is None:
                self.stream = self._open()
            msg = f"{self.format(record)}\n"
            self.stream.seek(0, 2)
            if self.stream.tell() + len(msg.encode("utf-8")) >= self.max_bytes:
                return 1

        return 0

    def doRollover(self):
        super().doRollover()

        log_path = Path(self.baseFilename)
        log_dir = log_path.parent
        base_name = log_path.name

        if self.archiver:
            self.archiver.archive_rotated_files(log_dir, base_name)

        if self.uploader and self.archiver:
            self.uploader.upload_archived(self.archiver.archive_dir, base_name, log_dir)
        elif self.uploader:
            self.uploader.cleanup_old_logs(log_dir, log_dir)


def get_log_file_path(
    log_dir: Path,
    domain: str | None = None,
    stage: str | None = None,
    instance_id: str | None = None,
) -> Path:
    """
    Build log file path with domain/date subfolder structure.

    New Structure: {log_dir}/{domain}/{YYYY-MM-DD}/{domain}_{stage}_{MMDD}_{HHMM}_{phrase}.log

    Examples:
        logs/xact/2026-01-05/xact_download_0105_1430_0.log
        logs/claimx/2026-01-05/claimx_enricher_0105_0930_1.log

    When instance_id is provided, it's appended to the filename to prevent
    file locking conflicts when multiple workers of the same type run
    concurrently. If instance_id is not provided, an ordinal number is
    generated.

    Args:
        log_dir: Base log directory
        domain: Pipeline domain (xact, claimx, kafka)
        stage: Stage name (ingest, download, etc.)
        instance_id: Unique instance identifier (human-readable phrase or will be generated)

    Returns:
        Full path to log file
    """
    now = datetime.now()
    date_folder = now.strftime("%Y-%m-%d")
    date_str = now.strftime("%m%d")  # Simpler: MMDD instead of YYYYMMDD
    time_str = now.strftime("%H%M")  # HHMM for time

    # Build base filename
    if domain and stage:
        base_name = f"{domain}_{stage}_{date_str}_{time_str}"
    elif domain:
        base_name = f"{domain}_{date_str}_{time_str}"
    elif stage:
        base_name = f"{stage}_{date_str}_{time_str}"
    else:
        base_name = f"pipeline_{date_str}_{time_str}"

    # Generate or use instance ID (ordinal number)
    phrase = instance_id or _generate_instance_id()

    # Append instance ID to filename
    filename = f"{base_name}_{phrase}.log"

    # Build path with subfolders
    if domain:
        log_path = log_dir / domain / date_folder / filename
    else:
        log_path = log_dir / date_folder / filename

    return log_path


def _create_eventhub_handler(
    connection_string: str,
    eventhub_name: str,
    level: int,
    batch_size: int,
    batch_timeout_seconds: float,
    max_queue_size: int,
    circuit_breaker_threshold: int,
) -> logging.Handler | None:
    """
    Create EventHub log handler with configuration.

    Returns None if handler creation fails (logs error, does not raise).
    This prevents EventHub connectivity issues from blocking application startup.
    """
    logger = logging.getLogger(__name__)
    try:
        from core.logging.eventhub_handler import EventHubLogHandler

        handler = EventHubLogHandler(
            connection_string=connection_string,
            eventhub_name=eventhub_name,
            batch_size=batch_size,
            batch_timeout_seconds=batch_timeout_seconds,
            max_queue_size=max_queue_size,
            circuit_breaker_threshold=circuit_breaker_threshold,
        )
        handler.setLevel(level)
        handler.setFormatter(JSONFormatter())

        logger.info(
            "EventHub log handler created",
            extra={
                "eventhub_name": eventhub_name,
                "level": logging.getLevelName(level),
                "batch_size": batch_size,
            },
        )
        return handler

    except Exception as e:
        logger.error(
            "Failed to create EventHub log handler - continuing with file logging only",
            extra={"error": str(e), "eventhub_name": eventhub_name},
            exc_info=True,
        )
        return None


def _build_file_handler(
    log_file: Path,
    log_dir: Path,
    backup_count: int,
    rotation_when: str = DEFAULT_ROTATION_WHEN,
    rotation_interval: int = DEFAULT_ROTATION_INTERVAL,
) -> PipelineFileHandler:
    """Build a PipelineFileHandler with archiver and optional OneLake uploader.

    Reads OneLake configuration from environment variables (LOG_UPLOAD_ENABLED,
    ONELAKE_LOG_PATH, LOG_MAX_SIZE_MB, LOG_ROTATION_MINUTES, LOG_RETENTION_HOURS).
    When upload is enabled, uses size+time rotation and uploads on rotate.
    Otherwise, uses standard time-based rotation with archiving only.
    """
    # Calculate archive directory: logs/archive/domain/date
    try:
        relative_path = log_file.relative_to(log_dir)
        archive_dir = log_dir / "archive" / relative_path.parent
    except ValueError:
        archive_dir = log_file.parent / "archive"

    upload_enabled = os.getenv("LOG_UPLOAD_ENABLED", "false").lower() == "true"
    max_size_mb = int(os.getenv("LOG_MAX_SIZE_MB", "5"))
    rotation_minutes = int(os.getenv("LOG_ROTATION_MINUTES", "5"))
    retention_hours = int(os.getenv("LOG_RETENTION_HOURS", "2"))

    onelake_client = None
    if upload_enabled:
        onelake_log_path = _resolve_onelake_log_path()
        if not onelake_log_path:
            print(
                "Warning: LOG_UPLOAD_ENABLED=true but ONELAKE_LOG_PATH not configured, "
                "disabling log upload",
                file=sys.stderr,
            )
            upload_enabled = False
        else:
            try:
                from pipeline.common.storage.onelake import OneLakeClient

                onelake_client = OneLakeClient(base_path=onelake_log_path)
            except Exception as e:
                print(
                    f"Warning: Failed to initialize OneLake client for log upload: {e}",
                    file=sys.stderr,
                )
                upload_enabled = False

    archiver = LogArchiver(archive_dir)

    if upload_enabled and onelake_client:
        uploader = OneLakeLogUploader(onelake_client, retention_hours)
        return PipelineFileHandler(
            log_file,
            when="M",
            interval=rotation_minutes,
            backupCount=backup_count,
            encoding="utf-8",
            archiver=archiver,
            uploader=uploader,
            max_bytes=max_size_mb * 1024 * 1024,
        )

    return PipelineFileHandler(
        log_file,
        when=rotation_when,
        interval=rotation_interval,
        backupCount=backup_count,
        encoding="utf-8",
        archiver=archiver,
    )


def _reset_root_logger() -> logging.Logger:
    """Clear root logger handlers (preserving pytest handlers) and set level to DEBUG."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    for h in root_logger.handlers[:]:
        if type(h).__module__.startswith(("_pytest", "pytest")):
            continue
        h.close()
        root_logger.removeHandler(h)
    return root_logger


def _attach_eventhub_handler(root_logger: logging.Logger, cfg: LoggingConfig) -> None:
    """Create and attach EventHub log handler if enabled and configured."""
    if not (cfg.enable_eventhub_logging and cfg.eventhub_config):
        return

    eh = cfg.eventhub_config
    print(f"[STARTUP] Creating EventHub log handler for: {eh['eventhub_name']}")
    eventhub_handler = _create_eventhub_handler(
        connection_string=eh["connection_string"],
        eventhub_name=eh["eventhub_name"],
        level=eh["level"],
        batch_size=eh["batch_size"],
        batch_timeout_seconds=eh["batch_timeout_seconds"],
        max_queue_size=eh["max_queue_size"],
        circuit_breaker_threshold=eh["circuit_breaker_threshold"],
    )
    if eventhub_handler:
        root_logger.addHandler(eventhub_handler)
        print("[STARTUP] EventHub log handler created and attached successfully")
    else:
        print("[STARTUP] EventHub log handler creation FAILED - check error logs")


def _create_console_handler(cfg: LoggingConfig) -> logging.StreamHandler:
    """Create a console StreamHandler with appropriate level and formatting."""
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(cfg.file_level if cfg.log_to_stdout else cfg.console_level)
    handler.setFormatter(ConsoleFormatter())
    return handler


def _create_file_formatter(cfg: LoggingConfig) -> logging.Formatter:
    """Create file formatter based on config (JSON or plain text)."""
    if cfg.json_format:
        return JSONFormatter()
    return logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )


def _suppress_noisy_loggers() -> None:
    """Set noisy third-party loggers to ERROR level."""
    for logger_name in NOISY_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.ERROR)


def _build_logging_config(kwargs: dict[str, Any]) -> LoggingConfig:
    """Build a LoggingConfig from keyword arguments, consuming known fields."""
    config_fields = {f.name for f in LoggingConfig.__dataclass_fields__.values()}
    config_kwargs = {k: kwargs.pop(k) for k in list(kwargs) if k in config_fields}
    return LoggingConfig(**config_kwargs)


def setup_logging(
    name: str = "pipeline",
    stage: str | None = None,
    domain: str | None = None,
    log_dir: Path | None = None,
    worker_id: str | None = None,
    config: LoggingConfig | None = None,
    **kwargs,
) -> logging.Logger:
    """
    Configure logging with console and auto-archiving time-based rotating file handlers.

    Log files are organized by domain and date with human-readable names:
        logs/xact/2026-01-05/xact_download_0105_1430_happy-tiger.log
        logs/claimx/2026-01-05/claimx_enricher_0105_0930_calm-ocean.log

    When use_instance_id is True (default), a human-readable phrase is added to
    the log filename to prevent file locking conflicts when multiple workers of
    the same type run concurrently.

    Rotated backup files (with timestamps like .2026-01-05) are automatically moved to
    an 'archive' subdirectory to keep the main log directory clean:
        logs/xact/2026-01-05/archive/xact_download_0105_1430_happy-tiger.log.2026-01-05

    Args:
        name: Logger name and log file prefix
        stage: Stage name for per-stage log files (ingest/download/retry)
        domain: Pipeline domain (xact, claimx, kafka)
        log_dir: Directory for log files (default: ./logs)
        worker_id: Worker identifier for context
        config: LoggingConfig with handler/formatter settings (or pass individual kwargs)
        **kwargs: Individual LoggingConfig fields for backward compatibility

    Returns:
        Configured logger instance
    """
    cfg = config or _build_logging_config(kwargs)
    log_dir = log_dir or DEFAULT_LOG_DIR

    if worker_id:
        set_log_context(worker_id=worker_id)
    if stage:
        set_log_context(stage=stage)
    if domain:
        set_log_context(domain=domain)

    root_logger = _reset_root_logger()
    _attach_eventhub_handler(root_logger, cfg)
    root_logger.addHandler(_create_console_handler(cfg))

    # File handler — always created regardless of log_to_stdout
    instance_id = _generate_instance_id() if cfg.use_instance_id else None
    log_file = get_log_file_path(log_dir, domain=domain, stage=stage, instance_id=instance_id)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = _build_file_handler(
        log_file, log_dir, cfg.backup_count, cfg.rotation_when, cfg.rotation_interval
    )
    file_handler.setLevel(cfg.file_level)
    file_handler.setFormatter(_create_file_formatter(cfg))
    root_logger.addHandler(file_handler)

    if cfg.suppress_noisy:
        _suppress_noisy_loggers()

    logger = logging.getLogger(name)
    logger.debug(
        f"Logging initialized: file={log_file}, json={cfg.json_format}, stdout={cfg.log_to_stdout}",
        extra={"stage": stage or "pipeline", "domain": domain or "unknown"},
    )

    return logger


def setup_multi_worker_logging(
    workers: list[str],
    domain: str = "kafka",
    log_dir: Path | None = None,
    config: LoggingConfig | None = None,
    **kwargs,
) -> logging.Logger:
    """
    Configure logging with per-worker auto-archiving time-based file handlers.

    Creates one PipelineFileHandler per worker type, each filtered
    to only receive logs from that worker's context. Also creates
    a combined log file that receives all logs.

    Log files are organized by domain and date with human-readable names:
        logs/kafka/2026-01-05/kafka_download_0105_1430_happy-tiger.log
        logs/kafka/2026-01-05/kafka_upload_0105_1430_happy-tiger.log
        logs/kafka/2026-01-05/pipeline_0105_1430_happy-tiger.log  (combined)

    Args:
        workers: List of worker stage names (e.g., ["download", "upload"])
        domain: Pipeline domain (default: "kafka")
        log_dir: Directory for log files (default: ./logs)
        config: LoggingConfig with handler/formatter settings (or pass individual kwargs)
        **kwargs: Individual LoggingConfig fields for backward compatibility

    Returns:
        Configured logger instance
    """
    cfg = config or _build_logging_config(kwargs)
    log_dir = log_dir or DEFAULT_LOG_DIR

    root_logger = _reset_root_logger()
    _attach_eventhub_handler(root_logger, cfg)
    root_logger.addHandler(_create_console_handler(cfg))

    # File handlers — always created regardless of log_to_stdout
    instance_id = _generate_instance_id() if cfg.use_instance_id else None
    file_formatter = _create_file_formatter(cfg)

    # Per-worker file handlers (filtered by stage context)
    for worker in workers:
        log_file = get_log_file_path(
            log_dir, domain=domain, stage=worker, instance_id=instance_id
        )
        log_file.parent.mkdir(parents=True, exist_ok=True)

        handler = _build_file_handler(
            log_file, log_dir, cfg.backup_count, cfg.rotation_when, cfg.rotation_interval
        )
        handler.setLevel(cfg.file_level)
        handler.setFormatter(file_formatter)
        handler.addFilter(StageContextFilter(worker))
        root_logger.addHandler(handler)

    # Combined file handler (no filter — receives all logs)
    combined_file = get_log_file_path(
        log_dir, domain=domain, stage="pipeline", instance_id=instance_id
    )
    combined_file.parent.mkdir(parents=True, exist_ok=True)

    combined_handler = _build_file_handler(
        combined_file, log_dir, cfg.backup_count, cfg.rotation_when, cfg.rotation_interval
    )
    combined_handler.setLevel(cfg.file_level)
    combined_handler.setFormatter(file_formatter)
    root_logger.addHandler(combined_handler)

    if cfg.suppress_noisy:
        _suppress_noisy_loggers()

    logger = logging.getLogger("pipeline")
    logger.debug(
        f"Multi-worker logging initialized: workers={workers}, domain={domain}, stdout_only={cfg.log_to_stdout}",
        extra={"stage": "pipeline", "domain": domain},
    )

    return logger


def upload_crash_logs(reason: str = "") -> None:
    """Upload active log files to OneLake after a fatal crash.

    Called when any worker fatally crashes to ensure crash logs are
    preserved in OneLake for post-mortem debugging, regardless of whether
    periodic log upload is enabled via LOG_UPLOAD_ENABLED.

    This is a best-effort operation: failures are logged but never raised.

    Args:
        reason: Description of why logs are being uploaded (e.g., error message)
    """
    try:
        _do_crash_log_upload(reason)
    except Exception as e:
        # Last-resort fallback: never let crash upload break the error-mode flow
        print(f"Warning: Crash log upload failed unexpectedly: {e}", file=sys.stderr)


def _flush_eventhub_handlers(crash_logger: logging.Logger) -> None:
    """Flush EventHub log handlers during crash to send pending logs."""
    try:
        from core.logging.eventhub_handler import EventHubLogHandler

        for handler in logging.getLogger().handlers:
            if isinstance(handler, EventHubLogHandler):
                try:
                    crash_logger.debug(
                        "Flushing EventHub handler during crash",
                        extra={"queue_size": handler.log_queue.qsize()},
                    )
                    handler.close()
                    crash_logger.debug("EventHub handler flushed successfully")
                except Exception as e:
                    crash_logger.warning(
                        "Failed to flush EventHub handler during crash",
                        extra={"error": str(e)},
                    )
    except ImportError:
        pass


def _collect_archived_files(archive_dir: Any) -> list[Path]:
    """Collect non-empty archived log files from an archive directory."""
    archive_path = Path(archive_dir) if not isinstance(archive_dir, Path) else archive_dir
    if not archive_path.exists():
        return []
    files = []
    for archived in archive_path.glob("*.log*"):
        try:
            if archived.stat().st_size > 0:
                files.append(archived)
        except OSError:
            pass
    return files


def _collect_crash_log_files() -> tuple[list[Path], Any]:
    """Flush file handlers, collect log file paths, and find OneLake client.

    Returns (deduplicated_log_files, onelake_client_or_None).
    """
    log_files: list[Path] = []
    onelake_client = None

    for handler in logging.getLogger().handlers:
        if not isinstance(handler, logging.FileHandler):
            continue

        with contextlib.suppress(Exception):
            handler.flush()

        log_path = Path(handler.baseFilename)
        if log_path.exists() and log_path.stat().st_size > 0:
            log_files.append(log_path)

        if hasattr(handler, "onelake_client") and handler.onelake_client is not None:
            onelake_client = handler.onelake_client

        if hasattr(handler, "archive_dir"):
            log_files.extend(_collect_archived_files(handler.archive_dir))

    # Deduplicate while preserving order
    seen: set = set()
    unique_files: list[Path] = []
    for f in log_files:
        resolved = f.resolve()
        if resolved not in seen:
            seen.add(resolved)
            unique_files.append(f)

    return unique_files, onelake_client


def _get_or_create_onelake_client(
    existing_client: Any, crash_logger: logging.Logger
) -> Any:
    """Return existing OneLake client or create one from config. Returns None on failure."""
    if existing_client is not None:
        return existing_client

    onelake_log_path = _resolve_onelake_log_path()
    if not onelake_log_path:
        crash_logger.warning(
            "No OneLake path configured for crash log upload (set ONELAKE_LOG_PATH)"
        )
        return None

    try:
        from pipeline.common.storage.onelake import OneLakeClient

        return OneLakeClient(base_path=onelake_log_path)
    except Exception as e:
        crash_logger.warning(
            "Failed to create OneLake client for crash log upload",
            extra={"error": str(e)},
        )
        return None


def _upload_files_to_onelake(
    log_files: list[Path], onelake_client: Any, reason: str, crash_logger: logging.Logger
) -> None:
    """Upload log files to OneLake under crash/ prefix."""
    crash_logger.info(
        "Uploading crash logs to OneLake",
        extra={"file_count": len(log_files), "reason": reason},
    )

    date_str = datetime.now().strftime("%Y-%m-%d")
    uploaded = 0

    for log_file in log_files:
        try:
            onelake_path = f"logs/crash/{date_str}/{log_file.name}"
            onelake_client.upload_file(
                relative_path=onelake_path,
                local_path=str(log_file),
                overwrite=True,
            )
            uploaded += 1
            crash_logger.debug("Uploaded crash log", extra={"file": log_file.name})
        except Exception as e:
            crash_logger.warning(
                "Failed to upload crash log file",
                extra={"file": log_file.name, "error": str(e)},
            )

    if uploaded > 0:
        crash_logger.info(
            "Crash log upload complete",
            extra={"uploaded": uploaded, "total": len(log_files)},
        )
    else:
        crash_logger.warning("No crash logs were successfully uploaded")


def _do_crash_log_upload(reason: str) -> None:
    """Internal implementation for crash log upload."""
    crash_logger = logging.getLogger("core.logging.crash_upload")

    _flush_eventhub_handlers(crash_logger)

    log_files, onelake_client = _collect_crash_log_files()
    if not log_files:
        crash_logger.warning("No log files found for crash upload")
        return

    onelake_client = _get_or_create_onelake_client(onelake_client, crash_logger)
    if onelake_client is None:
        return

    _upload_files_to_onelake(log_files, onelake_client, reason, crash_logger)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.

    Use this instead of logging.getLogger() to ensure consistent naming.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def log_worker_startup(
    logger: logging.Logger,
    worker_name: str,
    kafka_bootstrap_servers: str | None = None,
    input_topic: str | None = None,
    output_topic: str | None = None,
    consumer_group: str | None = None,
    extra_config: dict | None = None,
) -> None:
    """
    Log standard worker startup information including Kafka configuration.

    Call this at worker startup to ensure consistent logging of important
    configuration that aids debugging (especially bootstrap server mismatches).

    Args:
        logger: Logger instance to use
        worker_name: Name of the worker starting up
        kafka_bootstrap_servers: Kafka bootstrap servers (logs env var if not provided)
        input_topic: Input topic the worker consumes from
        output_topic: Output topic the worker produces to
        consumer_group: Consumer group ID
        extra_config: Additional configuration to log
    """
    # Get bootstrap servers from env if not provided
    if kafka_bootstrap_servers is None:
        kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "not set")

    logger.info("=" * 70)
    logger.info("Starting %s", worker_name)
    logger.info("=" * 70)
    logger.info("Kafka bootstrap servers: %s", kafka_bootstrap_servers)

    if input_topic:
        logger.info("Input topic: %s", input_topic)
    if output_topic:
        logger.info("Output topic: %s", output_topic)
    if consumer_group:
        logger.info("Consumer group: %s", consumer_group)

    if extra_config:
        for key, value in extra_config.items():
            logger.info("%s: %s", key, value)

    logger.info("=" * 70)


def generate_cycle_id() -> str:
    """
    Generate unique cycle identifier.

    Format: c-YYYYMMDD-HHMMSS-XXXX where XXXX is random hex.

    Returns:
        Unique cycle ID string
    """
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    suffix = secrets.token_hex(2)
    return f"c-{ts}-{suffix}"
