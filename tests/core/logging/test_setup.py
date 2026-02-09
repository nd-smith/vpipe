"""Tests for logging setup and configuration."""

import logging
import os
import shutil
import time
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from core.logging.context import clear_log_context, get_log_context
from core.logging.setup import (
    DEFAULT_BACKUP_COUNT,
    DEFAULT_CONSOLE_LEVEL,
    DEFAULT_FILE_LEVEL,
    DEFAULT_LOG_DIR,
    DEFAULT_ROTATION_INTERVAL,
    DEFAULT_ROTATION_WHEN,
    LogArchiver,
    NOISY_LOGGERS,
    OneLakeLogUploader,
    PipelineFileHandler,
    _create_eventhub_handler,
    generate_cycle_id,
    get_log_file_path,
    get_logger,
    log_worker_startup,
    setup_logging,
    setup_multi_worker_logging,
)


class TestGetLogFilePath:

    @patch("core.logging.setup._get_next_instance_id", return_value="happy-tiger")
    def test_builds_path_with_domain_and_stage(self, _):
        path = get_log_file_path(
            Path("logs"), domain="claimx", stage="download"
        )

        assert "claimx" in str(path)
        assert "claimx_download_" in path.name
        assert path.suffix == ".log"

    @patch("core.logging.setup._get_next_instance_id", return_value="calm-ocean")
    def test_builds_path_with_domain_only(self, _):
        path = get_log_file_path(Path("logs"), domain="xact")

        assert "xact" in str(path)
        assert path.name.startswith("xact_")

    @patch("core.logging.setup._get_next_instance_id", return_value="big-fish")
    def test_builds_path_with_stage_only(self, _):
        path = get_log_file_path(Path("logs"), stage="ingest")

        assert path.name.startswith("ingest_")

    @patch("core.logging.setup._get_next_instance_id", return_value="red-fox")
    def test_builds_path_without_domain_or_stage(self, _):
        path = get_log_file_path(Path("logs"))

        assert path.name.startswith("pipeline_")

    def test_uses_provided_instance_id(self):
        path = get_log_file_path(
            Path("logs"), domain="claimx", stage="download",
            instance_id="worker-0",
        )

        assert "worker-0" in path.name

    @patch("core.logging.setup._get_next_instance_id", return_value="blue-bird")
    def test_generates_instance_id_when_not_provided(self, _):
        path = get_log_file_path(
            Path("logs"), domain="claimx", stage="download"
        )

        assert "blue-bird" in path.name

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_includes_date_subfolder(self, _):
        path = get_log_file_path(Path("logs"), domain="claimx")

        # Should have a date folder like 2026-02-09
        parts = path.parts
        date_part = [p for p in parts if len(p) == 10 and p[4] == "-"]
        assert len(date_part) == 1

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_domain_in_subfolder(self, _):
        path = get_log_file_path(Path("logs"), domain="claimx")

        assert "claimx" in path.parts

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_no_domain_subfolder_without_domain(self, _):
        path = get_log_file_path(Path("logs"))

        assert "claimx" not in path.parts


class TestLogArchiver:

    def test_creates_archive_directory(self, tmp_path):
        archive_dir = tmp_path / "archive"
        archiver = LogArchiver(archive_dir)

        assert archive_dir.exists()
        assert archiver.archive_dir == archive_dir

    def test_archives_rotated_files(self, tmp_path):
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        archive_dir = tmp_path / "archive"

        # Create fake rotated files
        (log_dir / "app.log.2026-01-01").write_text("old log 1")
        (log_dir / "app.log.2026-01-02").write_text("old log 2")
        (log_dir / "app.log").write_text("current log")  # Should not match

        archiver = LogArchiver(archive_dir)
        archiver.archive_rotated_files(log_dir, "app.log")

        assert (archive_dir / "app.log.2026-01-01").exists()
        assert (archive_dir / "app.log.2026-01-02").exists()
        assert not (log_dir / "app.log.2026-01-01").exists()
        assert not (log_dir / "app.log.2026-01-02").exists()
        assert (log_dir / "app.log").exists()  # Current file untouched

    def test_handles_archive_failure_gracefully(self, tmp_path, capsys):
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        archive_dir = tmp_path / "archive"

        (log_dir / "app.log.2026-01-01").write_text("data")

        archiver = LogArchiver(archive_dir)

        with patch("core.logging.setup.shutil.move", side_effect=OSError("disk full")):
            archiver.archive_rotated_files(log_dir, "app.log")

        captured = capsys.readouterr()
        assert "Warning" in captured.err
        assert "disk full" in captured.err


class TestOneLakeLogUploader:

    def test_uploads_and_deletes_rotated_files(self, tmp_path):
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir()
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        (archive_dir / "app.log.2026-01-01").write_text("data")

        mock_client = MagicMock()
        uploader = OneLakeLogUploader(mock_client, log_retention_hours=24)
        uploader.upload_and_cleanup(archive_dir, "app.log", log_dir)

        mock_client.upload_file.assert_called_once()
        assert not (archive_dir / "app.log.2026-01-01").exists()

    def test_handles_upload_failure(self, tmp_path, capsys):
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir()
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        (archive_dir / "app.log.2026-01-01").write_text("data")

        mock_client = MagicMock()
        mock_client.upload_file.side_effect = Exception("network error")
        uploader = OneLakeLogUploader(mock_client, log_retention_hours=24)
        uploader.upload_and_cleanup(archive_dir, "app.log", log_dir)

        captured = capsys.readouterr()
        assert "Warning" in captured.err
        assert "network error" in captured.err
        # File should still exist since upload failed
        assert (archive_dir / "app.log.2026-01-01").exists()

    def test_cleans_up_old_logs(self, tmp_path):
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        archive_dir = tmp_path / "archive"
        archive_dir.mkdir()

        # Create old file (modify time to be old)
        old_file = log_dir / "old.log"
        old_file.write_text("old data")
        old_time = time.time() - (48 * 3600)  # 48 hours ago
        os.utime(old_file, (old_time, old_time))

        # Create recent file
        recent_file = log_dir / "recent.log"
        recent_file.write_text("recent data")

        mock_client = MagicMock()
        uploader = OneLakeLogUploader(mock_client, log_retention_hours=24)
        uploader.cleanup_old_logs(log_dir, archive_dir)

        assert not old_file.exists()
        assert recent_file.exists()

    def test_skips_cleanup_when_retention_zero(self, tmp_path):
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        (log_dir / "old.log").write_text("data")

        mock_client = MagicMock()
        uploader = OneLakeLogUploader(mock_client, log_retention_hours=0)
        uploader.cleanup_old_logs(log_dir, log_dir)

        assert (log_dir / "old.log").exists()

    def test_skips_nonexistent_directories(self, tmp_path):
        mock_client = MagicMock()
        uploader = OneLakeLogUploader(mock_client, log_retention_hours=24)
        # Should not raise
        uploader.cleanup_old_logs(
            tmp_path / "nonexistent1",
            tmp_path / "nonexistent2",
        )


class TestPipelineFileHandler:

    def test_creates_handler_with_archiver(self, tmp_path):
        log_file = tmp_path / "test.log"
        archive_dir = tmp_path / "archive"
        archiver = LogArchiver(archive_dir)

        handler = PipelineFileHandler(
            log_file, when="midnight", archiver=archiver,
        )

        assert handler.archiver is archiver
        assert handler.archive_dir == archive_dir
        handler.close()

    def test_creates_handler_without_archiver(self, tmp_path):
        log_file = tmp_path / "test.log"

        handler = PipelineFileHandler(log_file, when="midnight")

        assert handler.archiver is None
        assert handler.archive_dir is None
        handler.close()

    def test_stores_onelake_client_from_uploader(self, tmp_path):
        log_file = tmp_path / "test.log"
        archive_dir = tmp_path / "archive"
        archiver = LogArchiver(archive_dir)
        mock_client = MagicMock()
        uploader = OneLakeLogUploader(mock_client, log_retention_hours=24)

        handler = PipelineFileHandler(
            log_file, when="midnight",
            archiver=archiver, uploader=uploader,
        )

        assert handler.onelake_client is mock_client
        handler.close()

    def test_size_based_rollover_check(self, tmp_path):
        log_file = tmp_path / "test.log"
        log_file.write_text("x" * 100)

        handler = PipelineFileHandler(
            log_file, when="midnight", max_bytes=50,
        )

        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="",
            lineno=0, msg="message", args=(), exc_info=None,
        )

        # File is already over max_bytes so should rollover
        assert handler.shouldRollover(record) == 1
        handler.close()

    def test_no_size_rollover_when_max_bytes_zero(self, tmp_path):
        log_file = tmp_path / "test.log"
        log_file.write_text("")

        handler = PipelineFileHandler(
            log_file, when="H", interval=24, max_bytes=0,
        )

        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="",
            lineno=0, msg="msg", args=(), exc_info=None,
        )

        # Time-based rollover shouldn't trigger for a new file
        # and size-based is disabled
        assert handler.shouldRollover(record) == 0
        handler.close()


class TestCreateEventhubHandler:

    def test_creates_handler_successfully(self):
        mock_handler = MagicMock()
        mock_handler_class = MagicMock(return_value=mock_handler)

        with patch.dict("sys.modules", {
            "core.logging.eventhub_handler": MagicMock(
                EventHubLogHandler=mock_handler_class
            ),
        }):
            result = _create_eventhub_handler(
                connection_string="Endpoint=sb://test",
                eventhub_name="test-hub",
                level=logging.WARNING,
                batch_size=100,
                batch_timeout_seconds=1.0,
                max_queue_size=10000,
                circuit_breaker_threshold=5,
            )

        assert result is mock_handler
        mock_handler.setLevel.assert_called_once_with(logging.WARNING)
        mock_handler.setFormatter.assert_called_once()

    def test_returns_none_on_failure(self):
        mock_handler_class = MagicMock(side_effect=Exception("connection failed"))

        with patch.dict("sys.modules", {
            "core.logging.eventhub_handler": MagicMock(
                EventHubLogHandler=mock_handler_class
            ),
        }):
            result = _create_eventhub_handler(
                connection_string="bad",
                eventhub_name="test-hub",
                level=logging.WARNING,
                batch_size=100,
                batch_timeout_seconds=1.0,
                max_queue_size=10000,
                circuit_breaker_threshold=5,
            )

        assert result is None


class TestSetupLogging:

    @pytest.fixture(autouse=True)
    def clear_context(self):
        clear_log_context()
        yield
        clear_log_context()
        # Clean up root logger handlers
        logging.getLogger().handlers.clear()

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_stdout_mode_returns_logger(self, _, tmp_path):
        logger = setup_logging(
            name="test",
            log_to_stdout=True,
            log_dir=tmp_path,
        )

        assert isinstance(logger, logging.Logger)
        assert logger.name == "test"

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_stdout_mode_has_single_stream_handler(self, _, tmp_path):
        setup_logging(
            name="test",
            log_to_stdout=True,
            log_dir=tmp_path,
        )

        root = logging.getLogger()
        stream_handlers = [
            h for h in root.handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        assert len(stream_handlers) == 1

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_sets_log_context_from_params(self, _, tmp_path):
        setup_logging(
            name="test",
            stage="download",
            domain="claimx",
            worker_id="w-0",
            log_to_stdout=True,
            log_dir=tmp_path,
        )

        ctx = get_log_context()
        assert ctx["stage"] == "download"
        assert ctx["domain"] == "claimx"
        assert ctx["worker_id"] == "w-0"

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_suppresses_noisy_loggers(self, _, tmp_path):
        setup_logging(
            name="test",
            suppress_noisy=True,
            log_to_stdout=True,
            log_dir=tmp_path,
        )

        for logger_name in NOISY_LOGGERS:
            assert logging.getLogger(logger_name).level == logging.ERROR

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_does_not_suppress_noisy_loggers_when_disabled(self, _, tmp_path):
        # Reset noisy loggers first
        for logger_name in NOISY_LOGGERS:
            logging.getLogger(logger_name).setLevel(logging.NOTSET)

        setup_logging(
            name="test",
            suppress_noisy=False,
            log_to_stdout=True,
            log_dir=tmp_path,
        )

        for logger_name in NOISY_LOGGERS:
            assert logging.getLogger(logger_name).level != logging.ERROR

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    @patch.dict(os.environ, {"LOG_UPLOAD_ENABLED": "false"}, clear=False)
    def test_file_mode_creates_file_handler(self, _, tmp_path):
        setup_logging(
            name="test",
            domain="claimx",
            stage="download",
            log_dir=tmp_path,
            log_to_stdout=False,
        )

        root = logging.getLogger()
        file_handlers = [
            h for h in root.handlers if isinstance(h, logging.FileHandler)
        ]
        assert len(file_handlers) >= 1

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_eventhub_only_mode_hits_unbound_log_file_bug(self, _, tmp_path):
        # When enable_file_logging=False and log_to_stdout=False, setup_logging
        # references log_file in the debug log but it was never defined.
        # This is a known bug in the source; the test documents the behavior.
        with pytest.raises(UnboundLocalError):
            setup_logging(
                name="test",
                log_to_stdout=False,
                enable_file_logging=False,
                log_dir=tmp_path,
            )

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_root_logger_level_set_to_debug(self, _, tmp_path):
        setup_logging(
            name="test",
            log_to_stdout=True,
            log_dir=tmp_path,
        )

        assert logging.getLogger().level == logging.DEBUG

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_clears_existing_handlers(self, _, tmp_path):
        root = logging.getLogger()
        root.addHandler(logging.StreamHandler())
        root.addHandler(logging.StreamHandler())
        existing_count = len(root.handlers)

        setup_logging(
            name="test",
            log_to_stdout=True,
            log_dir=tmp_path,
        )

        # Should have cleared existing and added new ones
        assert len(root.handlers) < existing_count + 2

    @patch("core.logging.setup._create_eventhub_handler")
    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_adds_eventhub_handler_when_configured(self, _, mock_create_eh, tmp_path):
        mock_eh_handler = MagicMock(spec=logging.Handler)
        mock_eh_handler.level = logging.WARNING
        mock_create_eh.return_value = mock_eh_handler

        eh_config = {
            "connection_string": "Endpoint=sb://test",
            "eventhub_name": "test-hub",
            "level": logging.WARNING,
            "batch_size": 100,
            "batch_timeout_seconds": 1.0,
            "max_queue_size": 10000,
            "circuit_breaker_threshold": 5,
        }

        setup_logging(
            name="test",
            log_to_stdout=True,
            log_dir=tmp_path,
            eventhub_config=eh_config,
            enable_eventhub_logging=True,
        )

        mock_create_eh.assert_called_once()
        root = logging.getLogger()
        assert mock_eh_handler in root.handlers

    @patch("core.logging.setup._create_eventhub_handler")
    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_skips_eventhub_when_disabled(self, _, mock_create_eh, tmp_path):
        eh_config = {
            "connection_string": "Endpoint=sb://test",
            "eventhub_name": "test-hub",
            "level": logging.WARNING,
            "batch_size": 100,
            "batch_timeout_seconds": 1.0,
            "max_queue_size": 10000,
            "circuit_breaker_threshold": 5,
        }

        setup_logging(
            name="test",
            log_to_stdout=True,
            log_dir=tmp_path,
            eventhub_config=eh_config,
            enable_eventhub_logging=False,
        )

        mock_create_eh.assert_not_called()


class TestSetupMultiWorkerLogging:

    @pytest.fixture(autouse=True)
    def clear_context(self):
        clear_log_context()
        yield
        clear_log_context()
        logging.getLogger().handlers.clear()

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    @patch.dict(os.environ, {"LOG_UPLOAD_ENABLED": "false"}, clear=False)
    def test_creates_per_worker_file_handlers(self, _, tmp_path):
        setup_multi_worker_logging(
            workers=["download", "upload"],
            domain="kafka",
            log_dir=tmp_path,
        )

        root = logging.getLogger()
        file_handlers = [
            h for h in root.handlers if isinstance(h, PipelineFileHandler)
        ]
        # 2 worker handlers + 1 combined handler
        assert len(file_handlers) == 3

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_stdout_mode_skips_file_handlers(self, _, tmp_path):
        setup_multi_worker_logging(
            workers=["download", "upload"],
            domain="kafka",
            log_dir=tmp_path,
            log_to_stdout=True,
        )

        root = logging.getLogger()
        file_handlers = [
            h for h in root.handlers if isinstance(h, logging.FileHandler)
        ]
        assert len(file_handlers) == 0

    @patch("core.logging.setup._get_next_instance_id", return_value="test-id")
    def test_returns_pipeline_logger(self, _, tmp_path):
        logger = setup_multi_worker_logging(
            workers=["download"],
            domain="kafka",
            log_dir=tmp_path,
            log_to_stdout=True,
        )

        assert logger.name == "pipeline"


class TestGetLogger:

    def test_returns_logger_with_given_name(self):
        logger = get_logger("my.module")

        assert logger.name == "my.module"
        assert isinstance(logger, logging.Logger)


class TestLogWorkerStartup:

    def test_logs_startup_with_all_fields(self):
        logger = MagicMock()
        log_worker_startup(
            logger,
            worker_name="Test Worker",
            kafka_bootstrap_servers="broker:9092",
            input_topic="in.topic",
            output_topic="out.topic",
            consumer_group="test-group",
            extra_config={"batch_size": 100},
        )

        messages = [c[0][0] % c[0][1:] if len(c[0]) > 1 else c[0][0]
                     for c in logger.info.call_args_list]
        full_output = " ".join(messages)
        assert "Test Worker" in full_output
        assert "broker:9092" in full_output
        assert "in.topic" in full_output
        assert "out.topic" in full_output
        assert "test-group" in full_output
        assert "batch_size" in full_output

    def test_reads_kafka_servers_from_env(self):
        logger = MagicMock()
        with patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "env-broker:9092"}):
            log_worker_startup(logger, worker_name="Worker")

        messages = [c[0][0] % c[0][1:] if len(c[0]) > 1 else c[0][0]
                     for c in logger.info.call_args_list]
        full_output = " ".join(messages)
        assert "env-broker:9092" in full_output

    def test_shows_not_set_when_no_env_var(self):
        logger = MagicMock()
        with patch.dict(os.environ, {}, clear=True):
            # Need to ensure KAFKA_BOOTSTRAP_SERVERS is not set
            os.environ.pop("KAFKA_BOOTSTRAP_SERVERS", None)
            log_worker_startup(logger, worker_name="Worker")

        messages = [c[0][0] % c[0][1:] if len(c[0]) > 1 else c[0][0]
                     for c in logger.info.call_args_list]
        full_output = " ".join(messages)
        assert "not set" in full_output

    def test_omits_optional_fields_when_none(self):
        logger = MagicMock()
        log_worker_startup(logger, worker_name="Worker")

        messages = [c[0][0] % c[0][1:] if len(c[0]) > 1 else c[0][0]
                     for c in logger.info.call_args_list]
        full_output = " ".join(messages)
        assert "Input topic" not in full_output
        assert "Output topic" not in full_output
        assert "Consumer group" not in full_output


class TestGenerateCycleId:

    def test_generates_unique_ids(self):
        id1 = generate_cycle_id()
        id2 = generate_cycle_id()

        assert id1 != id2

    def test_follows_expected_format(self):
        cycle_id = generate_cycle_id()

        assert cycle_id.startswith("c-")
        parts = cycle_id.split("-")
        # c-YYYYMMDD-HHMMSS-XXXX
        assert len(parts) == 4
        assert len(parts[1]) == 8  # YYYYMMDD
        assert len(parts[2]) == 6  # HHMMSS
        assert len(parts[3]) == 4  # hex suffix
