"""Logging setup and configuration."""

import io
import logging
import os
import secrets
import shutil
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Optional

import coolname

from core.logging.context import set_log_context
from core.logging.filters import StageContextFilter
from core.logging.formatters import ConsoleFormatter, JSONFormatter

# Default settings
DEFAULT_LOG_DIR = Path("logs")
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10MB
DEFAULT_BACKUP_COUNT = 5
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


class ArchivingRotatingFileHandler(RotatingFileHandler):
    """
    Custom RotatingFileHandler that automatically moves rotated files to an archive folder.

    When a log file is rotated (e.g., file.log -> file.log.1), the backup files
    are automatically moved to an 'archive' subdirectory to keep the main log
    directory clean.

    Example:
        Before rotation:
            logs/xact/2026-01-05/xact_download_0105_1430_happy-tiger.log

        After rotation:
            logs/xact/2026-01-05/xact_download_0105_1430_happy-tiger.log (new file)
            logs/xact/2026-01-05/archive/xact_download_0105_1430_happy-tiger.log.1
            logs/xact/2026-01-05/archive/xact_download_0105_1430_happy-tiger.log.2
            
    If archive_dir is provided, rotated files are moved there instead.
    """

    def __init__(self, filename, mode="a", maxBytes=0, backupCount=0, encoding=None, delay=False, archive_dir=None):
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)
        # Create archive directory
        if archive_dir:
            self.archive_dir = Path(archive_dir)
        else:
            # Fallback to subdirectory behavior
            log_path = Path(self.baseFilename)
            self.archive_dir = log_path.parent / "archive"
        
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def doRollover(self):
        """
        Override doRollover to move rotated files to archive directory.
        """
        # Perform standard rotation first
        super().doRollover()

        # Move rotated files (*.log.1, *.log.2, etc.) to archive
        log_path = Path(self.baseFilename)
        log_dir = log_path.parent
        base_name = log_path.name

        # Find all rotated backup files
        for i in range(1, self.backupCount + 1):
            rotated_file = log_dir / f"{base_name}.{i}"
            if rotated_file.exists():
                archive_file = self.archive_dir / f"{base_name}.{i}"
                try:
                    shutil.move(str(rotated_file), str(archive_file))
                except Exception as e:
                    # Log to stderr if we can't move the file (don't use logger to avoid recursion)
                    print(f"Warning: Failed to archive {rotated_file}: {e}", file=sys.stderr)


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
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    suppress_noisy: bool = True,
    worker_id: Optional[str] = None,
    use_instance_id: bool = True,
) -> logging.Logger:
    """
    Configure logging with console and auto-archiving rotating file handlers.

    Log files are organized by domain and date with human-readable names:
        logs/xact/2026-01-05/xact_download_0105_1430_happy-tiger.log
        logs/claimx/2026-01-05/claimx_enricher_0105_0930_calm-ocean.log

    When use_instance_id is True (default), a human-readable phrase is added to
    the log filename to prevent file locking conflicts when multiple workers of
    the same type run concurrently.

    Rotated backup files (*.log.1, *.log.2, etc.) are automatically moved to
    an 'archive' subdirectory to keep the main log directory clean:
        logs/xact/2026-01-05/archive/xact_download_0105_1430_happy-tiger.log.1

    Args:
        name: Logger name and log file prefix
        stage: Stage name for per-stage log files (ingest/download/retry)
        domain: Pipeline domain (xact, claimx, kafka)
        log_dir: Directory for log files (default: ./logs)
        json_format: Use JSON format for file logs (default: True)
        console_level: Console handler level (default: INFO)
        file_level: File handler level (default: DEBUG)
        max_bytes: Max size per log file before rotation (default: 10MB)
        backup_count: Number of backup files to keep (default: 5)
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
    log_file = get_log_file_path(
        log_dir, domain=domain, stage=stage, instance_id=instance_id
    )

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

    # File handler with rotation and auto-archiving
    # Calculate centralized archive directory
    # Structure: logs/archive/domain/date
    try:
        relative_path = log_file.relative_to(log_dir)
        archive_dir = log_dir / "archive" / relative_path.parent
    except ValueError:
        # Fallback if relative path calculation fails
        archive_dir = log_file.parent / "archive"

    # File handler with rotation and auto-archiving
    file_handler = ArchivingRotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
        archive_dir=archive_dir,
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(file_formatter)

    # Console handler
    if sys.platform == "win32":
        safe_stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace"
        )
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
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    suppress_noisy: bool = True,
    use_instance_id: bool = True,
) -> logging.Logger:
    """
    Configure logging with per-worker auto-archiving file handlers.

    Creates one ArchivingRotatingFileHandler per worker type, each filtered
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
        max_bytes: Max size per log file before rotation (default: 10MB)
        backup_count: Number of backup files to keep (default: 5)
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
        safe_stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace"
        )
        console_handler = logging.StreamHandler(safe_stdout)
    else:
        console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Add per-worker file handlers with auto-archiving
    for worker in workers:
        log_file = get_log_file_path(
            log_dir, domain=domain, stage=worker, instance_id=instance_id
        )
        log_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            relative_path = log_file.relative_to(log_dir)
            archive_dir = log_dir / "archive" / relative_path.parent
        except ValueError:
            archive_dir = log_file.parent / "archive"

        handler = ArchivingRotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
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

    combined_handler = ArchivingRotatingFileHandler(
        combined_file,
        maxBytes=max_bytes,
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
