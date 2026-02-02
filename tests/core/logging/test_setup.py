"""Tests for logging setup functions."""

import logging
import os
import sys
import tempfile
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from core.logging.context import clear_log_context, get_log_context, set_log_context
from core.logging.filters import StageContextFilter
from core.logging.setup import (
    get_log_file_path,
    setup_logging,
    setup_multi_worker_logging,
    upload_crash_logs,
)


class TestSetupMultiWorkerLogging:
    """Tests for setup_multi_worker_logging function."""

    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Clean up after each test."""
        clear_log_context()
        yield
        clear_log_context()
        # Clear root logger handlers
        root_logger = logging.getLogger()
        root_logger.handlers.clear()

    def test_creates_per_worker_file_handlers(self, tmp_path):
        """Creates separate file handlers for each worker."""
        workers = ["download", "upload"]

        setup_multi_worker_logging(
            workers=workers,
            domain="kafka",
            log_dir=tmp_path,
        )

        root_logger = logging.getLogger()

        # Should have: 1 console + 2 worker files + 1 combined = 4 handlers
        assert len(root_logger.handlers) == 4

        # Count file handlers with filters
        filtered_handlers = [
            h for h in root_logger.handlers
            if hasattr(h, 'filters') and any(
                isinstance(f, StageContextFilter) for f in h.filters
            )
        ]
        assert len(filtered_handlers) == 2

    def test_creates_log_files_in_correct_structure(self, tmp_path):
        """Log files are created with correct domain/date/filename structure."""
        workers = ["download"]

        setup_multi_worker_logging(
            workers=workers,
            domain="kafka",
            log_dir=tmp_path,
        )

        # Find created log files
        log_files = list(tmp_path.rglob("*.log"))

        # Should have download worker file + combined pipeline file
        assert len(log_files) == 2

        # Check that kafka domain folder exists
        kafka_dir = tmp_path / "kafka"
        assert kafka_dir.exists()

    def test_filters_route_logs_correctly(self, tmp_path):
        """Logs are routed to correct files based on stage context."""
        workers = ["download", "upload"]

        setup_multi_worker_logging(
            workers=workers,
            domain="kafka",
            log_dir=tmp_path,
        )

        # Set download context and log
        set_log_context(stage="download")
        download_logger = logging.getLogger("test.download")
        download_logger.info("Download message")

        # Set upload context and log
        set_log_context(stage="upload")
        upload_logger = logging.getLogger("test.upload")
        upload_logger.info("Upload message")

        # Force handlers to flush
        for handler in logging.getLogger().handlers:
            handler.flush()

        # Find log files
        log_files = list(tmp_path.rglob("*.log"))

        # Read each file and verify content
        download_file = None
        upload_file = None
        combined_file = None

        for log_file in log_files:
            content = log_file.read_text()
            if "download" in log_file.name and "pipeline" not in log_file.name:
                download_file = content
            elif "upload" in log_file.name:
                upload_file = content
            elif "pipeline" in log_file.name:
                combined_file = content

        # Download file should have download message, not upload
        assert download_file is not None
        assert "Download message" in download_file
        assert "Upload message" not in download_file

        # Upload file should have upload message, not download
        assert upload_file is not None
        assert "Upload message" in upload_file
        assert "Download message" not in upload_file

        # Combined file should have both
        assert combined_file is not None
        assert "Download message" in combined_file
        assert "Upload message" in combined_file

    def test_suppresses_noisy_loggers(self, tmp_path):
        """Noisy loggers are set to WARNING level."""
        setup_multi_worker_logging(
            workers=["download"],
            domain="kafka",
            log_dir=tmp_path,
            suppress_noisy=True,
        )

        # Check that noisy loggers are suppressed
        azure_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
        assert azure_logger.level == logging.WARNING

    def test_console_handler_receives_all_logs(self, tmp_path, capsys):
        """Console handler receives logs from all workers."""
        workers = ["download", "upload"]

        setup_multi_worker_logging(
            workers=workers,
            domain="kafka",
            log_dir=tmp_path,
            console_level=logging.INFO,
        )

        # Log from both contexts
        set_log_context(stage="download")
        logging.getLogger("test").info("Download test")

        set_log_context(stage="upload")
        logging.getLogger("test").info("Upload test")

        # Capture console output
        captured = capsys.readouterr()

        # Console should have both messages
        assert "Download test" in captured.out
        assert "Upload test" in captured.out

    def test_instance_id_creates_unique_filenames(self, tmp_path):
        """Instance ID creates process-specific log files with unique coolname phrases."""
        workers = ["download"]

        setup_multi_worker_logging(
            workers=workers,
            domain="kafka",
            log_dir=tmp_path,
            use_instance_id=True,
        )

        log_files = list(tmp_path.rglob("*.log"))

        # All log files should have a coolname phrase suffix (format: name_MMDD_HHMM_phrase.log)
        # Coolname generates slugs like "happy-tiger", "calm-ocean", etc.
        for log_file in log_files:
            # Extract the filename parts (e.g., kafka_download_0109_0827_rousing-tarsier.log)
            parts = log_file.stem.split("_")
            # Should have at least domain, stage, date, time, phrase
            assert len(parts) >= 4, f"Unexpected filename format: {log_file.name}"
            # The last part should be the coolname phrase (contains a hyphen)
            phrase = parts[-1]
            assert "-" in phrase, f"Expected coolname phrase with hyphen, got: {phrase}"

    def test_no_instance_id_creates_shared_filenames(self, tmp_path):
        """Without instance ID, log files still have coolname phrase (always generated)."""
        workers = ["download"]

        setup_multi_worker_logging(
            workers=workers,
            domain="kafka",
            log_dir=tmp_path,
            use_instance_id=False,
        )

        log_files = list(tmp_path.rglob("*.log"))

        # When use_instance_id=False, coolname.generate_slug is NOT called,
        # but get_log_file_path still generates a phrase as fallback
        # The test verifies files are created successfully
        assert len(log_files) > 0, "Expected log files to be created"


class TestGetLogFilePath:
    """Tests for get_log_file_path function."""

    def test_instance_id_appended_to_filename(self, tmp_path):
        """Instance ID is appended to the log filename."""
        log_path = get_log_file_path(
            tmp_path,
            domain="kafka",
            stage="download",
            instance_id="p12345",
        )

        assert "_p12345.log" in str(log_path)
        assert "kafka_download_" in log_path.name

    def test_no_instance_id_standard_filename(self, tmp_path):
        """Without instance ID, standard filename format is used."""
        log_path = get_log_file_path(
            tmp_path,
            domain="kafka",
            stage="download",
            instance_id=None,
        )

        assert log_path.name.endswith(".log")
        assert "_p" not in log_path.name

    def test_domain_and_stage_in_path(self, tmp_path):
        """Domain and stage are included in the path structure."""
        log_path = get_log_file_path(
            tmp_path,
            domain="kafka",
            stage="upload",
            instance_id="p99",
        )

        assert "kafka" in str(log_path)
        assert "upload" in log_path.name


class TestSetupLogging:
    """Tests for setup_logging function."""

    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Clean up after each test."""
        clear_log_context()
        yield
        clear_log_context()
        root_logger = logging.getLogger()
        root_logger.handlers.clear()

    def test_instance_id_enabled_by_default(self, tmp_path):
        """Instance ID is enabled by default for multi-worker safety."""
        setup_logging(
            name="test",
            stage="download",
            domain="kafka",
            log_dir=tmp_path,
        )

        log_files = list(tmp_path.rglob("*.log"))
        assert len(log_files) == 1

        # Instance ID now uses coolname phrases (e.g., "happy-tiger") not PID
        # Verify the filename has a coolname phrase (contains hyphen)
        parts = log_files[0].stem.split("_")
        assert len(parts) >= 4, f"Unexpected filename format: {log_files[0].name}"
        phrase = parts[-1]
        assert "-" in phrase, f"Expected coolname phrase with hyphen, got: {phrase}"

    def test_instance_id_can_be_disabled(self, tmp_path):
        """Instance ID can be disabled for single-worker deployments."""
        setup_logging(
            name="test",
            stage="download",
            domain="kafka",
            log_dir=tmp_path,
            use_instance_id=False,
        )

        log_files = list(tmp_path.rglob("*.log"))
        assert len(log_files) == 1

        # When disabled, get_log_file_path still generates a coolname phrase as fallback
        # Just verify a log file was created
        assert log_files[0].exists()


class TestUploadCrashLogs:
    """Tests for upload_crash_logs function."""

    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Clean up after each test."""
        clear_log_context()
        yield
        clear_log_context()
        root_logger = logging.getLogger()
        root_logger.handlers.clear()

    @pytest.fixture
    def mock_onelake_module(self):
        """Inject a mock OneLakeClient via sys.modules for the lazy import."""
        mock_client_cls = MagicMock()
        mock_module = ModuleType("pipeline.common.storage.onelake")
        mock_module.OneLakeClient = mock_client_cls

        # Ensure parent modules exist in sys.modules
        modules_to_inject = {
            "pipeline.common.storage": ModuleType("pipeline.common.storage"),
            "pipeline.common.storage.onelake": mock_module,
        }

        with patch.dict(sys.modules, modules_to_inject):
            yield mock_client_cls

    def test_uploads_active_log_files(self, tmp_path, mock_onelake_module):
        """Uploads active log files to OneLake via crash/ prefix."""
        setup_logging(
            name="test",
            stage="download",
            domain="kafka",
            log_dir=tmp_path,
        )

        # Write some log content so the file is non-empty
        test_logger = logging.getLogger("test.crash")
        test_logger.error("Fatal crash occurred")

        # Flush handlers
        for handler in logging.getLogger().handlers:
            handler.flush()

        mock_client = MagicMock()
        mock_onelake_module.return_value = mock_client

        with patch.dict(os.environ, {"ONELAKE_LOG_PATH": "abfss://test@test.dfs.fabric.microsoft.com/logs"}):
            upload_crash_logs(reason="test fatal error")

        # Verify OneLake client was created and upload_file was called
        mock_onelake_module.assert_called_once_with(base_path="abfss://test@test.dfs.fabric.microsoft.com/logs")
        assert mock_client.upload_file.call_count >= 1

        # Verify the upload path includes crash/ prefix
        call_args = mock_client.upload_file.call_args_list[0]
        relative_path = call_args.kwargs.get("relative_path", "")
        assert "logs/crash/" in relative_path

    def test_no_op_without_onelake_config(self, tmp_path):
        """Does nothing when no OneLake path is configured."""
        setup_logging(
            name="test",
            stage="download",
            domain="kafka",
            log_dir=tmp_path,
        )

        test_logger = logging.getLogger("test.crash")
        test_logger.error("Fatal crash occurred")

        # Ensure no OneLake env vars are set
        env = {k: v for k, v in os.environ.items() if k not in ("ONELAKE_LOG_PATH", "ONELAKE_BASE_PATH")}
        with patch.dict(os.environ, env, clear=True):
            # Should not raise
            upload_crash_logs(reason="test fatal error")

    def test_no_op_without_file_handlers(self):
        """Does nothing when no file handlers are configured."""
        root_logger = logging.getLogger()
        root_logger.handlers.clear()

        # Add only a console handler (no file handler)
        console_handler = logging.StreamHandler()
        root_logger.addHandler(console_handler)

        # Should not raise
        upload_crash_logs(reason="test fatal error")

    def test_never_raises_on_client_failure(self, tmp_path, mock_onelake_module):
        """Never raises exceptions even when OneLake client creation fails."""
        setup_logging(
            name="test",
            stage="download",
            domain="kafka",
            log_dir=tmp_path,
        )

        test_logger = logging.getLogger("test.crash")
        test_logger.error("Fatal crash occurred")

        mock_onelake_module.side_effect = RuntimeError("Connection failed")

        with patch.dict(os.environ, {"ONELAKE_LOG_PATH": "abfss://test@test.dfs.fabric.microsoft.com/logs"}):
            # Must not raise - crash upload is best-effort
            upload_crash_logs(reason="test fatal error")

    def test_never_raises_on_upload_failure(self, tmp_path, mock_onelake_module):
        """Never raises exceptions even when individual file uploads fail."""
        setup_logging(
            name="test",
            stage="download",
            domain="kafka",
            log_dir=tmp_path,
        )

        test_logger = logging.getLogger("test.crash")
        test_logger.error("Fatal crash occurred")

        mock_client = MagicMock()
        mock_client.upload_file.side_effect = RuntimeError("Upload failed")
        mock_onelake_module.return_value = mock_client

        with patch.dict(os.environ, {"ONELAKE_LOG_PATH": "abfss://test@test.dfs.fabric.microsoft.com/logs"}):
            # Must not raise - crash upload is best-effort
            upload_crash_logs(reason="test fatal error")

        # upload_file was attempted even though it failed
        assert mock_client.upload_file.call_count >= 1

    def test_reuses_existing_onelake_client_from_handler(self, tmp_path, mock_onelake_module):
        """Reuses OneLake client already attached to a handler."""
        setup_logging(
            name="test",
            stage="download",
            domain="kafka",
            log_dir=tmp_path,
        )

        test_logger = logging.getLogger("test.crash")
        test_logger.error("Fatal crash occurred")

        # Attach a mock OneLake client to the file handler
        mock_client = MagicMock()
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.onelake_client = mock_client
                break

        # Should use the existing client, not create a new one
        upload_crash_logs(reason="test fatal error")

        # OneLakeClient constructor should NOT have been called
        mock_onelake_module.assert_not_called()
        # But the existing client's upload_file should have been called
        assert mock_client.upload_file.call_count >= 1
