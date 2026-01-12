"""Tests for KQL Client."""

import asyncio
import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.errors.exceptions import KustoError, KustoQueryError
from kafka_pipeline.common.eventhouse.kql_client import (
    EventhouseConfig,
    KQLClient,
    KQLQueryResult,
)


class TestEventhouseConfig:
    """Tests for EventhouseConfig."""

    def test_from_env_success(self, monkeypatch):
        """Test loading config from environment variables."""
        monkeypatch.setenv("EVENTHOUSE_CLUSTER_URL", "https://test.kusto.windows.net")
        monkeypatch.setenv("EVENTHOUSE_DATABASE", "testdb")
        monkeypatch.setenv("EVENTHOUSE_QUERY_TIMEOUT", "60")
        monkeypatch.setenv("EVENTHOUSE_MAX_RETRIES", "5")

        config = EventhouseConfig.from_env()

        assert config.cluster_url == "https://test.kusto.windows.net"
        assert config.database == "testdb"
        assert config.query_timeout_seconds == 60
        assert config.max_retries == 5

    def test_from_env_missing_cluster_url(self, monkeypatch):
        """Test error when cluster URL is missing."""
        monkeypatch.delenv("EVENTHOUSE_CLUSTER_URL", raising=False)
        monkeypatch.setenv("EVENTHOUSE_DATABASE", "testdb")

        with pytest.raises(ValueError, match="EVENTHOUSE_CLUSTER_URL"):
            EventhouseConfig.from_env()

    def test_from_env_missing_database(self, monkeypatch):
        """Test error when database is missing."""
        monkeypatch.setenv("EVENTHOUSE_CLUSTER_URL", "https://test.kusto.windows.net")
        monkeypatch.delenv("EVENTHOUSE_DATABASE", raising=False)

        with pytest.raises(ValueError, match="EVENTHOUSE_DATABASE"):
            EventhouseConfig.from_env()

    def test_from_env_defaults(self, monkeypatch):
        """Test default values are used when optional env vars are not set."""
        monkeypatch.setenv("EVENTHOUSE_CLUSTER_URL", "https://test.kusto.windows.net")
        monkeypatch.setenv("EVENTHOUSE_DATABASE", "testdb")
        # Clear optional env vars
        monkeypatch.delenv("EVENTHOUSE_QUERY_TIMEOUT", raising=False)
        monkeypatch.delenv("EVENTHOUSE_MAX_RETRIES", raising=False)
        monkeypatch.delenv("EVENTHOUSE_RETRY_BASE_DELAY", raising=False)
        monkeypatch.delenv("EVENTHOUSE_RETRY_MAX_DELAY", raising=False)
        monkeypatch.delenv("EVENTHOUSE_MAX_CONNECTIONS", raising=False)

        config = EventhouseConfig.from_env()

        assert config.query_timeout_seconds == 120
        assert config.max_retries == 3
        assert config.retry_base_delay_seconds == 1.0
        assert config.retry_max_delay_seconds == 30.0
        assert config.max_connections == 10


class TestKQLQueryResult:
    """Tests for KQLQueryResult."""

    def test_empty_result(self):
        """Test empty result detection."""
        result = KQLQueryResult(rows=[], row_count=0)
        assert result.is_empty

    def test_non_empty_result(self):
        """Test non-empty result."""
        result = KQLQueryResult(
            rows=[{"col1": "value1"}],
            row_count=1,
        )
        assert not result.is_empty

    def test_result_with_metadata(self):
        """Test result with metadata."""
        result = KQLQueryResult(
            rows=[{"col1": "value1"}, {"col1": "value2"}],
            row_count=2,
            query_duration_ms=150.5,
            query_text="test query",
        )

        assert result.row_count == 2
        assert result.query_duration_ms == 150.5
        assert result.query_text == "test query"
        assert not result.is_partial


class TestKQLClient:
    """Tests for KQLClient."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return EventhouseConfig(
            cluster_url="https://test.kusto.windows.net",
            database="testdb",
            query_timeout_seconds=30,
            max_retries=3,
            retry_base_delay_seconds=0.01,  # Fast retries for testing
            retry_max_delay_seconds=0.1,
        )

    @pytest.fixture
    def mock_kusto_client(self):
        """Create mock KustoClient."""
        mock = MagicMock()
        return mock

    @pytest.fixture
    def mock_response(self):
        """Create mock Kusto response."""
        # Create mock column
        mock_column = MagicMock()
        mock_column.column_name = "test_col"

        # Create mock row
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, x: "test_value" if x == 0 else None
        mock_row.__iter__ = lambda self: iter(["test_value"])

        # Create mock table
        mock_table = MagicMock()
        mock_table.columns = [mock_column]
        mock_table.__iter__ = lambda self: iter([mock_row])

        # Create mock response
        mock_response = MagicMock()
        mock_response.primary_results = [mock_table]

        return mock_response

    @pytest.mark.asyncio
    async def test_connect_success(self, config):
        """Test successful connection."""
        with (
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.DefaultAzureCredential"
            ) as mock_cred,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder"
            ) as mock_kcsb,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoClient"
            ) as mock_client_class,
        ):
            mock_cred.return_value = MagicMock()
            mock_kcsb.with_azure_token_credential.return_value = MagicMock()
            mock_client_class.return_value = MagicMock()

            client = KQLClient(config)
            await client.connect()

            assert client.is_connected
            mock_cred.assert_called_once()
            mock_kcsb.with_azure_token_credential.assert_called_once_with(
                config.cluster_url,
                mock_cred.return_value,
            )

            await client.close()
            assert not client.is_connected

    @pytest.mark.asyncio
    async def test_context_manager(self, config):
        """Test async context manager."""
        with (
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.DefaultAzureCredential"
            ) as mock_cred,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder"
            ) as mock_kcsb,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoClient"
            ) as mock_client_class,
        ):
            mock_cred.return_value = MagicMock()
            mock_kcsb.with_azure_token_credential.return_value = MagicMock()
            mock_client_class.return_value = MagicMock()

            async with KQLClient(config) as client:
                assert client.is_connected

            assert not client.is_connected

    @pytest.mark.asyncio
    async def test_execute_query_success(self, config, mock_response):
        """Test successful query execution."""
        with (
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.DefaultAzureCredential"
            ) as mock_cred,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder"
            ) as mock_kcsb,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoClient"
            ) as mock_client_class,
        ):
            mock_cred.return_value = MagicMock()
            mock_kcsb.with_azure_token_credential.return_value = MagicMock()

            # Setup mock client to return mock response
            mock_client = MagicMock()
            mock_client.execute.return_value = mock_response
            mock_client_class.return_value = mock_client

            async with KQLClient(config) as client:
                result = await client.execute_query("test query")

            assert result.row_count == 1
            assert len(result.rows) == 1
            assert result.rows[0]["test_col"] == "test_value"
            assert result.query_duration_ms > 0

    @pytest.mark.asyncio
    async def test_execute_query_empty_result(self, config):
        """Test query that returns no results."""
        mock_response = MagicMock()
        mock_response.primary_results = []

        with (
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.DefaultAzureCredential"
            ) as mock_cred,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder"
            ) as mock_kcsb,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoClient"
            ) as mock_client_class,
        ):
            mock_cred.return_value = MagicMock()
            mock_kcsb.with_azure_token_credential.return_value = MagicMock()

            mock_client = MagicMock()
            mock_client.execute.return_value = mock_response
            mock_client_class.return_value = mock_client

            async with KQLClient(config) as client:
                result = await client.execute_query("test query")

            assert result.is_empty
            assert result.row_count == 0
            assert result.rows == []

    @pytest.mark.asyncio
    async def test_execute_query_syntax_error(self, config):
        """Test query with syntax error raises KustoQueryError."""
        from azure.kusto.data.exceptions import KustoServiceError

        with (
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.DefaultAzureCredential"
            ) as mock_cred,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder"
            ) as mock_kcsb,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoClient"
            ) as mock_client_class,
        ):
            mock_cred.return_value = MagicMock()
            mock_kcsb.with_azure_token_credential.return_value = MagicMock()

            mock_client = MagicMock()
            mock_client.execute.side_effect = KustoServiceError(
                "Syntax error in query"
            )
            mock_client_class.return_value = mock_client

            async with KQLClient(config) as client:
                with pytest.raises(KustoQueryError):
                    await client.execute_query("invalid query syntax")

    @pytest.mark.asyncio
    async def test_execute_query_retry_on_transient_error(self, config, mock_response):
        """Test retry logic on transient errors."""
        with (
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.DefaultAzureCredential"
            ) as mock_cred,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder"
            ) as mock_kcsb,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoClient"
            ) as mock_client_class,
        ):
            mock_cred.return_value = MagicMock()
            mock_kcsb.with_azure_token_credential.return_value = MagicMock()

            mock_client = MagicMock()
            # First call fails, second succeeds
            mock_client.execute.side_effect = [
                Exception("503 Service Unavailable"),
                mock_response,
            ]
            mock_client_class.return_value = mock_client

            async with KQLClient(config) as client:
                result = await client.execute_query("test query")

            assert result.row_count == 1
            assert mock_client.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_execute_query_max_retries_exceeded(self, config):
        """Test error raised when max retries exceeded."""
        with (
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.DefaultAzureCredential"
            ) as mock_cred,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder"
            ) as mock_kcsb,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoClient"
            ) as mock_client_class,
        ):
            mock_cred.return_value = MagicMock()
            mock_kcsb.with_azure_token_credential.return_value = MagicMock()

            mock_client = MagicMock()
            # All calls fail
            mock_client.execute.side_effect = Exception("503 Service Unavailable")
            mock_client_class.return_value = mock_client

            async with KQLClient(config) as client:
                with pytest.raises((KustoError, Exception)):
                    await client.execute_query("test query")

            # Should have tried max_retries times
            assert mock_client.execute.call_count == config.max_retries

    @pytest.mark.asyncio
    async def test_health_check_success(self, config):
        """Test successful health check."""
        # Create mock response for "print 1"
        mock_column = MagicMock()
        mock_column.column_name = "print_0"

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, x: 1

        mock_table = MagicMock()
        mock_table.columns = [mock_column]
        mock_table.__iter__ = lambda self: iter([mock_row])

        mock_response = MagicMock()
        mock_response.primary_results = [mock_table]

        with (
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.DefaultAzureCredential"
            ) as mock_cred,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder"
            ) as mock_kcsb,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoClient"
            ) as mock_client_class,
        ):
            mock_cred.return_value = MagicMock()
            mock_kcsb.with_azure_token_credential.return_value = MagicMock()

            mock_client = MagicMock()
            mock_client.execute.return_value = mock_response
            mock_client_class.return_value = mock_client

            async with KQLClient(config) as client:
                is_healthy = await client.health_check()

            assert is_healthy

    @pytest.mark.asyncio
    async def test_health_check_failure(self, config):
        """Test failed health check."""
        with (
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.DefaultAzureCredential"
            ) as mock_cred,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder"
            ) as mock_kcsb,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoClient"
            ) as mock_client_class,
        ):
            mock_cred.return_value = MagicMock()
            mock_kcsb.with_azure_token_credential.return_value = MagicMock()

            mock_client = MagicMock()
            mock_client.execute.side_effect = Exception("Connection refused")
            mock_client_class.return_value = mock_client

            async with KQLClient(config) as client:
                is_healthy = await client.health_check()

            assert not is_healthy

    @pytest.mark.asyncio
    async def test_datetime_serialization(self, config):
        """Test datetime values are serialized to ISO format."""
        test_datetime = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

        # Create mock response with datetime
        mock_column = MagicMock()
        mock_column.column_name = "timestamp"

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, x: test_datetime

        mock_table = MagicMock()
        mock_table.columns = [mock_column]
        mock_table.__iter__ = lambda self: iter([mock_row])

        mock_response = MagicMock()
        mock_response.primary_results = [mock_table]

        with (
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.DefaultAzureCredential"
            ) as mock_cred,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder"
            ) as mock_kcsb,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoClient"
            ) as mock_client_class,
        ):
            mock_cred.return_value = MagicMock()
            mock_kcsb.with_azure_token_credential.return_value = MagicMock()

            mock_client = MagicMock()
            mock_client.execute.return_value = mock_response
            mock_client_class.return_value = mock_client

            async with KQLClient(config) as client:
                result = await client.execute_query("test query")

            assert result.rows[0]["timestamp"] == "2024-01-15T10:30:00+00:00"

    @pytest.mark.asyncio
    async def test_custom_database(self, config, mock_response):
        """Test query with custom database."""
        with (
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.DefaultAzureCredential"
            ) as mock_cred,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder"
            ) as mock_kcsb,
            patch(
                "kafka_pipeline.common.eventhouse.kql_client.KustoClient"
            ) as mock_client_class,
        ):
            mock_cred.return_value = MagicMock()
            mock_kcsb.with_azure_token_credential.return_value = MagicMock()

            mock_client = MagicMock()
            mock_client.execute.return_value = mock_response
            mock_client_class.return_value = mock_client

            async with KQLClient(config) as client:
                await client.execute_query("test query", database="custom_db")

            mock_client.execute.assert_called_once()
            call_args = mock_client.execute.call_args
            assert call_args[0][0] == "custom_db"  # First positional arg is database
