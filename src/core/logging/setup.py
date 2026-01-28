# Copyright (c) 2024-2026 nickdsmith. All Rights Reserved.
# SPDX-License-Identifier: PROPRIETARY
#
# This file is proprietary and confidential. Unauthorized copying of this file,
# via any medium is strictly prohibited.

"""Logging setup and configuration."""

import io
import logging
import os
import secrets
import shutil
import sys
import time
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import List, Optional

import coolname

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

# Noisy loggers to suppress
NOISY_LOGGERS = [
    "azure.core.pipeline.policies.http_logging_policy",
    "azure.identity",
    "urllib3",
    "aiohttp",
    "aiokafka",
]


class ArchivingTimedRotatingFileHandler(TimedRotatingFileHandler):
    """
    Custom TimedRotatingFileHandler that automatically moves rotated files to an archive folder.

    When a log file is rotated (e.g., file.log -> file.log.2026-01-22), the backup files
    are automatically moved to an 'archive' subdirectory to keep the main log
    directory clean.

    Example:
        Before rotation:
            logs/xact/2026-01-05/xact_download_0105_1430_happy-tiger.log

        After rotation (daily at midnight):
            logs/xact/2026-01-05/xact_download_0105_1430_happy-tiger.log (new file)
            logs/xact/2026-01-05/archive/xact_download_0105_1430_happy-tiger.log.2026-01-05
            logs/xact/2026-01-05/archive/xact_download_0105_1430_happy-tiger.log.2026-01-04

    If archive_dir is provided, rotated files are moved there instead.
    """

    def __init__(
        self,
        filename,
        when="midnight",
        interval=1,
        backupCount=0,
        encoding=None,
        delay=False,
        utc=False,
        archive_dir=None,
    ):
        super().__init__(filename, when, interval, backupCount, encoding, delay, utc)
        if archive_dir:
            self.archive_dir = Path(archive_dir)
        else:
            # Fallback to subdirectory behavior
            log_path = Path(self.baseFilename)
            self.archive_dir = log_path.parent / "archive"

        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def doRollover(self):
        super().doRollover()

        # Move rotated files to archive
        # TimedRotatingFileHandler appends timestamps like .2026-01-22 to rotated files
        log_path = Path(self.baseFilename)
        log_dir = log_path.parent
        base_name = log_path.name

        # Find all rotated backup files (they have timestamps appended)
        for rotated_file in log_dir.glob(f"{base_name}.*"):
            # Skip the current log file itself
            if rotated_file == log_path:
                continue

            archive_file = self.archive_dir / rotated_file.name
            try:
                shutil.move(str(rotated_file), str(archive_file))
            except Exception as e:
                # Log to stderr if we can't move the file (don't use logger to avoid recursion)
                print(f"Warning: Failed to archive {rotated_file}: {e}", file=sys.stderr)


class OneLakeRotatingFileHandler(ArchivingTimedRotatingFileHandler):
    """
    Enhanced log handler with time + size rotation triggers and OneLake upload.

    Rotates logs when EITHER condition is met:
    - Time limit reached (e.g., every 15 minutes)
    - Size limit reached (e.g., 50 MB)

    On rotation:
    - Uploads rotated file to OneLake
    - Deletes local file after successful upload
    - Keeps only recent logs locally

    On initialization:
    - Cleans up old log files from previous runs

    Environment variables:
        LOG_UPLOAD_ENABLED: Enable OneLake upload (default: false)
        LOG_MAX_SIZE_MB: Max log file size before rotation (default: 50)
        LOG_ROTATION_MINUTES: Time-based rotation interval (default: 15)
        LOG_RETENTION_HOURS: Keep logs locally for N hours (default: 2)
        ONELAKE_LOG_PATH: OneLake path for logs (default: {ONELAKE_BASE_PATH}/logs)
    """

    def __init__(
        self,
        filename,
        when="M",
        interval=15,
        backupCount=0,
        encoding=None,
        delay=False,
        utc=False,
        archive_dir=None,
        max_bytes=52428800,  # 50 MB default
        onelake_client=None,
        log_retention_hours=2,
    ):
        super().__init__(filename, when, interval, backupCount, encoding, delay, utc, archive_dir)
        self.max_bytes = max_bytes
        self.onelake_client = onelake_client
        self.log_retention_hours = log_retention_hours
        self.upload_enabled = os.getenv("LOG_UPLOAD_ENABLED", "false").lower() == "true"

        # Clean up old logs on initialization
        self._cleanup_old_logs()

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.
        Rollover happens if EITHER time OR size limit is reached.
        """
        # Check time-based rollover (from parent class)
        if super().shouldRollover(record):
            return 1

        # Check size-based rollover
        if self.stream is None:
            self.stream = self._open()

        if self.max_bytes > 0:
            msg = "%s\n" % self.format(record)
            self.stream.seek(0, 2)  # Go to end of file
            if self.stream.tell() + len(msg.encode('utf-8')) >= self.max_bytes:
                return 1

        return 0

    def doRollover(self):
        """
        Perform rollover, then upload to OneLake and cleanup.
        """
        # Get rotated file path before rollover
        log_path = Path(self.baseFilename)

        # Do the actual rotation (parent class handles this)
        super().doRollover()

        # Upload and cleanup rotated files
        if self.upload_enabled and self.onelake_client:
            self._upload_and_cleanup_rotated_files()
        else:
            # If upload disabled, still cleanup old files
            self._cleanup_old_logs()

    def _upload_and_cleanup_rotated_files(self):
        """Upload rotated files to OneLake and delete after success."""
        log_path = Path(self.baseFilename)
        log_dir = log_path.parent
        base_name = log_path.name

        # Find rotated files in archive directory
        for rotated_file in self.archive_dir.glob(f"{base_name}.*"):
            try:
                # Build OneLake path: logs/{domain}/{date}/{filename}
                relative_path = rotated_file.relative_to(log_dir.parent.parent)
                onelake_path = f"logs/{relative_path}"

                # Upload to OneLake
                self.onelake_client.upload_file(
                    relative_path=onelake_path,
                    local_path=str(rotated_file),
                    overwrite=True,
                )

                # Delete local file after successful upload
                rotated_file.unlink()
                print(f"Uploaded and deleted log: {rotated_file.name}", file=sys.stderr)

            except Exception as e:
                print(f"Warning: Failed to upload log {rotated_file}: {e}", file=sys.stderr)

    def _cleanup_old_logs(self):
        """Remove log files older than retention period."""
        if self.log_retention_hours <= 0:
            return

        log_path = Path(self.baseFilename)
        log_dir = log_path.parent
        cutoff_time = time.time() - (self.log_retention_hours * 3600)

        # Clean up old files in both main log dir and archive dir
        for directory in [log_dir, self.archive_dir]:
            if not directory.exists():
                continue

            for log_file in directory.glob("*.log*"):
                try:
                    if log_file.stat().st_mtime < cutoff_time:
                        log_file.unlink()
                        print(f"Cleaned up old log: {log_file.name}", file=sys.stderr)
                except Exception as e:
                    print(f"Warning: Failed to cleanup {log_file}: {e}", file=sys.stderr)


def get_log_file_path(
    log_dir: Path,
    domain: Optional[str] = None,
    stage: Optional[str] = None,
    instance_id: Optional[str] = None,
) -> Path:
    """
    Build log file path with domain/date subfolder structure.

    New Structure: {log_dir}/{domain}/{YYYY-MM-DD}/{domain}_{stage}_{MMDD}_{HHMM}_{phrase}.log

    Examples:
        logs/xact/2026-01-05/xact_download_0105_1430_happy-tiger.log
        logs/claimx/2026-01-05/claimx_enricher_0105_0930_calm-ocean.log

    When instance_id is provided, it's appended to the filename to prevent
    file locking conflicts when multiple workers of the same type run
    concurrently. If instance_id is not provided, a random coolname phrase
    is generated.

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

    # Generate or use instance ID (coolname phrase)
    if instance_id:
        phrase = instance_id
    else:
        # Generate a random 2-word coolname phrase (e.g., "happy-tiger")
        phrase = coolname.generate_slug(2)

    # Append phrase to filename
    filename = f"{base_name}_{phrase}.log"

    # Build path with subfolders
    if domain:
        log_path = log_dir / domain / date_folder / filename
    else:
        log_path = log_dir / date_folder / filename

    return log_path


def setup_logging(
    name: str = "pipeline",
    stage: Optional[str] = None,
    domain: Optional[str] = None,
    log_dir: Optional[Path] = None,
    json_format: bool = True,
    console_level: int = DEFAULT_CONSOLE_LEVEL,
    file_level: int = DEFAULT_FILE_LEVEL,
    rotation_when: str = DEFAULT_ROTATION_WHEN,
    rotation_interval: int = DEFAULT_ROTATION_INTERVAL,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    suppress_noisy: bool = True,
    worker_id: Optional[str] = None,
    use_instance_id: bool = True,
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
        json_format: Use JSON format for file logs (default: True)
        console_level: Console handler level (default: INFO)
        file_level: File handler level (default: DEBUG)
        rotation_when: When to rotate logs - 'midnight', 'H' (hourly), 'M' (minutes) (default: midnight)
        rotation_interval: Interval for rotation (default: 1)
        backup_count: Number of backup files to keep (default: 7)
        suppress_noisy: Quiet down Azure SDK and HTTP client loggers
        worker_id: Worker identifier for context
        use_instance_id: Generate unique phrase for log filename (default: True).
            Set to False for single-worker deployments or when log aggregation is preferred.

    Returns:
        Configured logger instance
    """
    log_dir = log_dir or DEFAULT_LOG_DIR

    # Set context variables
    if worker_id:
        set_log_context(worker_id=worker_id)
    if stage:
        set_log_context(stage=stage)
    if domain:
        set_log_context(domain=domain)

    # Generate human-readable instance ID for multi-worker isolation
    # Uses coolname to generate phrases like "happy-tiger" or "calm-ocean"
    instance_id = coolname.generate_slug(2) if use_instance_id else None

    # Build log file path with subfolders
    log_file = get_log_file_path(log_dir, domain=domain, stage=stage, instance_id=instance_id)

    # Ensure directory exists
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Create formatters
    if json_format:
        file_formatter = JSONFormatter()
    else:
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
        )
    console_formatter = ConsoleFormatter()

    # File handler with time-based rotation and auto-archiving
    # Calculate centralized archive directory
    # Structure: logs/archive/domain/date
    try:
        relative_path = log_file.relative_to(log_dir)
        archive_dir = log_dir / "archive" / relative_path.parent
    except ValueError:
        # Fallback if relative path calculation fails
        archive_dir = log_file.parent / "archive"

    # Check if OneLake upload is enabled
    upload_enabled = os.getenv("LOG_UPLOAD_ENABLED", "false").lower() == "true"
    max_size_mb = int(os.getenv("LOG_MAX_SIZE_MB", "50"))
    rotation_minutes = int(os.getenv("LOG_ROTATION_MINUTES", "15"))
    retention_hours = int(os.getenv("LOG_RETENTION_HOURS", "2"))

    # Create OneLake client if upload enabled
    onelake_client = None
    if upload_enabled:
        try:
            from kafka_pipeline.common.storage.onelake import OneLakeClient
            from config.pipeline_config import load_pipeline_config

            # Load pipeline config to get OneLake settings
            pipeline_config = load_pipeline_config()
            onelake_client = OneLakeClient(
                base_path=os.getenv("ONELAKE_LOG_PATH", pipeline_config.onelake_base_path),
            )
        except Exception as e:
            print(f"Warning: Failed to initialize OneLake client for log upload: {e}", file=sys.stderr)
            upload_enabled = False

    # Choose handler based on upload configuration
    if upload_enabled and onelake_client:
        file_handler = OneLakeRotatingFileHandler(
            log_file,
            when="M",  # Minute-based rotation
            interval=rotation_minutes,
            backupCount=backup_count,
            encoding="utf-8",
            archive_dir=archive_dir,
            max_bytes=max_size_mb * 1024 * 1024,  # Convert MB to bytes
            onelake_client=onelake_client,
            log_retention_hours=retention_hours,
        )
    else:
        file_handler = ArchivingTimedRotatingFileHandler(
            log_file,
            when=rotation_when,
            interval=rotation_interval,
            backupCount=backup_count,
            encoding="utf-8",
            archive_dir=archive_dir,
        )

    file_handler.setLevel(file_level)
    file_handler.setFormatter(file_formatter)

    # Console handler
    if sys.platform == "win32":
        safe_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        console_handler = logging.StreamHandler(safe_stdout)
    else:
        console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Capture all, handlers filter

    # Remove existing handlers to avoid duplicates on re-init
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Suppress noisy loggers
    if suppress_noisy:
        for logger_name in NOISY_LOGGERS:
            logging.getLogger(logger_name).setLevel(logging.WARNING)

    logger = logging.getLogger(name)
    logger.debug(
        f"Logging initialized: file={log_file}, json={json_format}",
        extra={"stage": stage or "pipeline", "domain": domain or "unknown"},
    )

    return logger


def setup_multi_worker_logging(
    workers: List[str],
    domain: str = "kafka",
    log_dir: Optional[Path] = None,
    json_format: bool = True,
    console_level: int = DEFAULT_CONSOLE_LEVEL,
    file_level: int = DEFAULT_FILE_LEVEL,
    rotation_when: str = DEFAULT_ROTATION_WHEN,
    rotation_interval: int = DEFAULT_ROTATION_INTERVAL,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    suppress_noisy: bool = True,
    use_instance_id: bool = True,
) -> logging.Logger:
    """
    Configure logging with per-worker auto-archiving time-based file handlers.

    Creates one ArchivingTimedRotatingFileHandler per worker type, each filtered
    to only receive logs from that worker's context. Also creates
    a combined log file that receives all logs.

    Log files are organized by domain and date with human-readable names:
        logs/kafka/2026-01-05/kafka_download_0105_1430_happy-tiger.log
        logs/kafka/2026-01-05/kafka_upload_0105_1430_happy-tiger.log
        logs/kafka/2026-01-05/kafka_pipeline_0105_1430_happy-tiger.log  (combined)

    When use_instance_id is True (default), a human-readable phrase is appended to
    log filenames to prevent file locking conflicts when multiple instances
    of the same worker configuration run concurrently.

    Rotated backup files are automatically moved to archive subdirectories.

    Args:
        workers: List of worker stage names (e.g., ["download", "upload"])
        domain: Pipeline domain (default: "kafka")
        log_dir: Directory for log files (default: ./logs)
        json_format: Use JSON format for file logs (default: True)
        console_level: Console handler level (default: INFO)
        file_level: File handler level (default: DEBUG)
        rotation_when: When to rotate logs - 'midnight', 'H' (hourly), 'M' (minutes) (default: midnight)
        rotation_interval: Interval for rotation (default: 1)
        backup_count: Number of backup files to keep (default: 7)
        suppress_noisy: Quiet down Azure SDK and HTTP client loggers
        use_instance_id: Generate unique phrase for log filenames (default: True)

    Returns:
        Configured logger instance
    """
    log_dir = log_dir or DEFAULT_LOG_DIR

    # Generate human-readable instance ID for multi-instance isolation
    instance_id = coolname.generate_slug(2) if use_instance_id else None

    # Create formatters
    if json_format:
        file_formatter = JSONFormatter()
    else:
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
        )
    console_formatter = ConsoleFormatter()

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Capture all, handlers filter
    root_logger.handlers.clear()

    # Add console handler (receives all logs)
    if sys.platform == "win32":
        safe_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        console_handler = logging.StreamHandler(safe_stdout)
    else:
        console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Add per-worker file handlers with auto-archiving
    for worker in workers:
        log_file = get_log_file_path(log_dir, domain=domain, stage=worker, instance_id=instance_id)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            relative_path = log_file.relative_to(log_dir)
            archive_dir = log_dir / "archive" / relative_path.parent
        except ValueError:
            archive_dir = log_file.parent / "archive"

        handler = ArchivingTimedRotatingFileHandler(
            log_file,
            when=rotation_when,
            interval=rotation_interval,
            backupCount=backup_count,
            encoding="utf-8",
            archive_dir=archive_dir,
        )
        handler.setLevel(file_level)
        handler.setFormatter(file_formatter)
        handler.addFilter(StageContextFilter(worker))
        root_logger.addHandler(handler)

    # Add combined file handler with auto-archiving (no filter - receives all logs)
    combined_file = get_log_file_path(
        log_dir, domain=domain, stage="pipeline", instance_id=instance_id
    )
    combined_file.parent.mkdir(parents=True, exist_ok=True)
    # Calculate archive dir for combined log
    try:
        relative_path = combined_file.relative_to(log_dir)
        combined_archive_dir = log_dir / "archive" / relative_path.parent
    except ValueError:
        combined_archive_dir = combined_file.parent / "archive"

    combined_handler = ArchivingTimedRotatingFileHandler(
        combined_file,
        when=rotation_when,
        interval=rotation_interval,
        backupCount=backup_count,
        encoding="utf-8",
        archive_dir=combined_archive_dir,
    )
    combined_handler.setLevel(file_level)
    combined_handler.setFormatter(file_formatter)
    root_logger.addHandler(combined_handler)

    # Suppress noisy loggers
    if suppress_noisy:
        for logger_name in NOISY_LOGGERS:
            logging.getLogger(logger_name).setLevel(logging.WARNING)

    logger = logging.getLogger("kafka_pipeline")
    logger.debug(
        f"Multi-worker logging initialized: workers={workers}, domain={domain}",
        extra={"stage": "pipeline", "domain": domain},
    )

    return logger


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
    kafka_bootstrap_servers: Optional[str] = None,
    input_topic: Optional[str] = None,
    output_topic: Optional[str] = None,
    consumer_group: Optional[str] = None,
    extra_config: Optional[dict] = None,
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
    logger.info(f"Starting {worker_name}")
    logger.info("=" * 70)
    logger.info(f"Kafka bootstrap servers: {kafka_bootstrap_servers}")

    if input_topic:
        logger.info(f"Input topic: {input_topic}")
    if output_topic:
        logger.info(f"Output topic: {output_topic}")
    if consumer_group:
        logger.info(f"Consumer group: {consumer_group}")

    if extra_config:
        for key, value in extra_config.items():
            logger.info(f"{key}: {value}")

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
