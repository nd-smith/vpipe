"""Tests for KQLClient and related configuration classes."""

import os
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.errors.exceptions import KustoError, KustoQueryError
from pipeline.common.eventhouse.kql_client import (
    EventhouseConfig,
    KQLClient,
    KQLQueryResult,
)

# =============================================================================
# EventhouseConfig tests
# =============================================================================


class TestEventhouseConfigDefaults:
    def test_defaults_applied(self):
        config = EventhouseConfig(
            cluster_url="https://test.kusto.windows.net",
            database="testdb",
        )
        assert config.query_timeout_seconds == 120
        assert config.max_retries == 3
        assert config.retry_base_delay_seconds == 1.0
        assert config.retry_max_delay_seconds == 30.0
        assert config.proxy_url is None


class TestEventhouseConfigLoadConfig:
    def test_load_from_yaml(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "eventhouse:\n"
            "  cluster_url: https://yaml.kusto.windows.net\n"
            "  database: yamldb\n"
            "  query_timeout_seconds: 60\n"
        )
        config = EventhouseConfig.load_config(config_path=config_file)
        assert config.cluster_url == "https://yaml.kusto.windows.net"
        assert config.database == "yamldb"
        assert config.query_timeout_seconds == 60

    def test_env_overrides_yaml(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "eventhouse:\n  cluster_url: https://yaml.kusto.windows.net\n  database: yamldb\n"
        )
        with patch.dict(
            os.environ,
            {
                "EVENTHOUSE_CLUSTER_URL": "https://env.kusto.windows.net",
                "EVENTHOUSE_DATABASE": "envdb",
            },
        ):
            config = EventhouseConfig.load_config(config_path=config_file)
        assert config.cluster_url == "https://env.kusto.windows.net"
        assert config.database == "envdb"

    def test_raises_when_cluster_url_missing(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("eventhouse:\n  database: db\n")
        with pytest.raises(ValueError, match="cluster_url is required"):
            EventhouseConfig.load_config(config_path=config_file)

    def test_raises_when_database_missing(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("eventhouse:\n  cluster_url: https://test.kusto.windows.net\n")
        with pytest.raises(ValueError, match="database is required"):
            EventhouseConfig.load_config(config_path=config_file)

    def test_loads_env_only_when_no_yaml(self, tmp_path):
        missing_file = tmp_path / "nope.yaml"
        with patch.dict(
            os.environ,
            {
                "EVENTHOUSE_CLUSTER_URL": "https://envonly.kusto.windows.net",
                "EVENTHOUSE_DATABASE": "envdb",
            },
        ):
            config = EventhouseConfig.load_config(config_path=missing_file)
        assert config.cluster_url == "https://envonly.kusto.windows.net"
        assert config.database == "envdb"

    def test_proxy_from_eventhouse_proxy_url(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "eventhouse:\n  cluster_url: https://test.kusto.windows.net\n  database: db\n"
        )
        with patch.dict(os.environ, {"EVENTHOUSE_PROXY_URL": "http://proxy:8080"}, clear=False):
            config = EventhouseConfig.load_config(config_path=config_file)
        assert config.proxy_url == "http://proxy:8080"

    def test_proxy_fallback_to_https_proxy(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "eventhouse:\n  cluster_url: https://test.kusto.windows.net\n  database: db\n"
        )
        env = {"HTTPS_PROXY": "http://httpsProxy:8080"}
        # Ensure EVENTHOUSE_PROXY_URL is not set
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("EVENTHOUSE_PROXY_URL", None)
            os.environ.pop("HTTP_PROXY", None)
            config = EventhouseConfig.load_config(config_path=config_file)
        assert config.proxy_url == "http://httpsProxy:8080"

    def test_int_and_float_coercion(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "eventhouse:\n  cluster_url: https://test.kusto.windows.net\n  database: db\n"
        )
        with patch.dict(
            os.environ,
            {
                "EVENTHOUSE_QUERY_TIMEOUT": "90",
                "EVENTHOUSE_MAX_RETRIES": "5",
                "EVENTHOUSE_RETRY_BASE_DELAY": "2.5",
                "EVENTHOUSE_RETRY_MAX_DELAY": "60.0",
            },
        ):
            config = EventhouseConfig.load_config(config_path=config_file)
        assert config.query_timeout_seconds == 90
        assert config.max_retries == 5
        assert config.retry_base_delay_seconds == 2.5
        assert config.retry_max_delay_seconds == 60.0

    def test_empty_yaml_uses_env(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("")
        with patch.dict(
            os.environ,
            {
                "EVENTHOUSE_CLUSTER_URL": "https://empty.kusto.windows.net",
                "EVENTHOUSE_DATABASE": "emptydb",
            },
        ):
            config = EventhouseConfig.load_config(config_path=config_file)
        assert config.cluster_url == "https://empty.kusto.windows.net"


# =============================================================================
# KQLQueryResult tests
# =============================================================================


class TestKQLQueryResult:
    def test_is_empty_when_no_rows(self):
        result = KQLQueryResult(rows=[], row_count=0)
        assert result.is_empty is True

    def test_is_not_empty_with_rows(self):
        result = KQLQueryResult(rows=[{"a": 1}], row_count=1)
        assert result.is_empty is False

    def test_default_values(self):
        result = KQLQueryResult()
        assert result.rows == []
        assert result.query_duration_ms == 0.0
        assert result.row_count == 0
        assert result.is_partial is False
        assert result.query_text == ""


# =============================================================================
# KQLClient tests
# =============================================================================


def _make_config(**overrides):
    defaults = {
        "cluster_url": "https://test.kusto.windows.net",
        "database": "testdb",
        "max_retries": 3,
        "retry_base_delay_seconds": 0.001,
        "retry_max_delay_seconds": 0.01,
    }
    defaults.update(overrides)
    return EventhouseConfig(**defaults)


class TestKQLClientConnect:
    @patch("pipeline.common.eventhouse.kql_client.KustoClient")
    @patch("pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder")
    @patch("pipeline.common.eventhouse.kql_client.DefaultAzureCredential")
    async def test_connect_with_default_credential(
        self, mock_cred_cls, mock_kcsb_cls, mock_kusto_cls
    ):
        mock_kcsb_cls.with_azure_token_credential.return_value = MagicMock()
        config = _make_config()
        client = KQLClient(config)
        await client.connect()
        mock_cred_cls.assert_called_once()
        mock_kcsb_cls.with_azure_token_credential.assert_called_once()
        mock_kusto_cls.assert_called_once()

    @patch("pipeline.common.eventhouse.kql_client.KustoClient")
    @patch("pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder")
    async def test_connect_with_spn_credentials(self, mock_kcsb_cls, mock_kusto_cls):
        env = {
            "AZURE_CLIENT_ID": "client-id",
            "AZURE_CLIENT_SECRET": "secret",
            "AZURE_TENANT_ID": "tenant-id",
        }
        with patch.dict(os.environ, env, clear=False):
            # Also remove AZURE_TOKEN_FILE to ensure SPN path
            os.environ.pop("AZURE_TOKEN_FILE", None)
            config = _make_config()
            client = KQLClient(config)
            await client.connect()
        mock_kcsb_cls.with_aad_application_key_authentication.assert_called_once_with(
            "https://test.kusto.windows.net",
            "client-id",
            "secret",
            "tenant-id",
        )

    @patch("pipeline.common.eventhouse.kql_client.KustoClient")
    @patch("pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder")
    @patch("pipeline.common.eventhouse.kql_client.FileBackedTokenCredential")
    async def test_connect_with_token_file(
        self, mock_token_cred, mock_kcsb_cls, mock_kusto_cls, tmp_path
    ):
        token_file = tmp_path / "token"
        token_file.write_text("token-data")
        env = {"AZURE_TOKEN_FILE": str(token_file)}
        # Clear SPN vars
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("AZURE_CLIENT_ID", None)
            os.environ.pop("AZURE_CLIENT_SECRET", None)
            os.environ.pop("AZURE_TENANT_ID", None)
            config = _make_config()
            client = KQLClient(config)
            await client.connect()
        mock_kcsb_cls.with_azure_token_credential.assert_called_once()

    @patch("pipeline.common.eventhouse.kql_client.KustoClient")
    @patch("pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder")
    @patch("pipeline.common.eventhouse.kql_client.DefaultAzureCredential")
    async def test_connect_skips_if_already_connected(
        self, mock_cred_cls, mock_kcsb_cls, mock_kusto_cls
    ):
        config = _make_config()
        client = KQLClient(config)
        await client.connect()
        await client.connect()  # Second call should be a no-op
        assert mock_kusto_cls.call_count == 1

    @patch("pipeline.common.eventhouse.kql_client.KustoClient")
    @patch("pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder")
    @patch("pipeline.common.eventhouse.kql_client.DefaultAzureCredential")
    async def test_connect_sets_proxy_when_configured(
        self, mock_cred_cls, mock_kcsb_cls, mock_kusto_cls
    ):
        mock_client_instance = MagicMock()
        mock_kusto_cls.return_value = mock_client_instance
        config = _make_config(proxy_url="http://proxy:8080")
        client = KQLClient(config)
        await client.connect()
        mock_client_instance.set_proxy.assert_called_once_with("http://proxy:8080")

    @patch("pipeline.common.eventhouse.kql_client.StorageErrorClassifier")
    @patch("pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder")
    @patch("pipeline.common.eventhouse.kql_client.DefaultAzureCredential")
    async def test_connect_raises_classified_error_on_failure(
        self, mock_cred_cls, mock_kcsb_cls, mock_classifier
    ):
        mock_kcsb_cls.with_azure_token_credential.side_effect = RuntimeError("auth fail")
        classified_error = KustoError("classified", cause=RuntimeError("auth fail"))
        mock_classifier.classify_kusto_error.return_value = classified_error
        config = _make_config()
        client = KQLClient(config)
        with pytest.raises(KustoError, match="classified"):
            await client.connect()

    @patch("pipeline.common.eventhouse.kql_client.KustoClient")
    @patch("pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder")
    @patch("pipeline.common.eventhouse.kql_client.FileBackedTokenCredential")
    async def test_connect_token_file_falls_back_to_default_on_error(
        self, mock_token_cred, mock_kcsb_cls, mock_kusto_cls, tmp_path
    ):
        """If FileBackedTokenCredential raises, fall back to DefaultAzureCredential."""
        token_file = tmp_path / "token"
        token_file.write_text("data")
        mock_token_cred.side_effect = RuntimeError("bad token")
        env = {"AZURE_TOKEN_FILE": str(token_file)}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("AZURE_CLIENT_ID", None)
            os.environ.pop("AZURE_CLIENT_SECRET", None)
            os.environ.pop("AZURE_TENANT_ID", None)
            with patch("pipeline.common.eventhouse.kql_client.DefaultAzureCredential"):
                config = _make_config()
                client = KQLClient(config)
                await client.connect()
        # Should have fallen through to DefaultAzureCredential path
        mock_kcsb_cls.with_azure_token_credential.assert_called_once()

    @patch("pipeline.common.eventhouse.kql_client.KustoClient")
    @patch("pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder")
    @patch("pipeline.common.eventhouse.kql_client.DefaultAzureCredential")
    async def test_connect_falls_back_when_token_file_does_not_exist(
        self, mock_cred_cls, mock_kcsb_cls, mock_kusto_cls
    ):
        """If AZURE_TOKEN_FILE points to a missing file, fall back to default."""
        env = {"AZURE_TOKEN_FILE": "/nonexistent/path/token"}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("AZURE_CLIENT_ID", None)
            os.environ.pop("AZURE_CLIENT_SECRET", None)
            os.environ.pop("AZURE_TENANT_ID", None)
            config = _make_config()
            client = KQLClient(config)
            await client.connect()
        mock_cred_cls.assert_called_once()


class TestKQLClientClose:
    @patch("pipeline.common.eventhouse.kql_client.KustoClient")
    @patch("pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder")
    @patch("pipeline.common.eventhouse.kql_client.DefaultAzureCredential")
    async def test_close_cleans_up(self, mock_cred_cls, mock_kcsb_cls, mock_kusto_cls):
        mock_client_instance = MagicMock()
        mock_kusto_cls.return_value = mock_client_instance
        config = _make_config()
        client = KQLClient(config)
        await client.connect()
        await client.close()
        mock_client_instance.close.assert_called_once()
        assert client._client is None
        assert client._credential is None

    async def test_close_noop_when_not_connected(self):
        config = _make_config()
        client = KQLClient(config)
        # Should not raise
        await client.close()

    @patch("pipeline.common.eventhouse.kql_client.KustoClient")
    @patch("pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder")
    @patch("pipeline.common.eventhouse.kql_client.DefaultAzureCredential")
    async def test_close_handles_exception(self, mock_cred_cls, mock_kcsb_cls, mock_kusto_cls):
        mock_client_instance = MagicMock()
        mock_client_instance.close.side_effect = RuntimeError("close failed")
        mock_kusto_cls.return_value = mock_client_instance
        config = _make_config()
        client = KQLClient(config)
        await client.connect()
        # Should not raise, just logs warning
        await client.close()
        assert client._client is None


class TestKQLClientContextManager:
    @patch("pipeline.common.eventhouse.kql_client.KustoClient")
    @patch("pipeline.common.eventhouse.kql_client.KustoConnectionStringBuilder")
    @patch("pipeline.common.eventhouse.kql_client.DefaultAzureCredential")
    async def test_async_context_manager(self, mock_cred_cls, mock_kcsb_cls, mock_kusto_cls):
        mock_client_instance = MagicMock()
        mock_kusto_cls.return_value = mock_client_instance
        config = _make_config()
        async with KQLClient(config) as client:
            assert client._client is not None
        assert client._client is None


class TestKQLClientExecuteQuery:
    def _make_connected_client(self):
        config = _make_config()
        client = KQLClient(config)
        client._client = MagicMock()
        return client

    def _make_mock_response(self, rows=None, column_names=None):
        """Build a mock KustoClient.execute() response."""
        response = MagicMock()
        if rows is None:
            response.primary_results = []
            return response

        column_names = column_names or ["col1", "col2"]
        table = MagicMock()
        columns = []
        for name in column_names:
            col = MagicMock()
            col.column_name = name
            columns.append(col)
        table.columns = columns
        table.__iter__ = lambda self: iter(rows)
        response.primary_results = [table]
        return response

    async def test_executes_query_returns_result(self):
        client = self._make_connected_client()
        row1 = MagicMock()
        row1.__getitem__ = lambda self, i: ["v1", "v2"][i]
        response = self._make_mock_response(rows=[row1])
        client._client.execute.return_value = response

        result = await client.execute_query("TestTable | take 1")

        assert result.row_count == 1
        assert result.rows == [{"col1": "v1", "col2": "v2"}]
        assert result.query_text == "TestTable | take 1"

    async def test_returns_empty_result_when_no_primary_results(self):
        client = self._make_connected_client()
        response = self._make_mock_response(rows=None)
        client._client.execute.return_value = response

        result = await client.execute_query("TestTable | take 1")

        assert result.is_empty
        assert result.rows == []

    async def test_converts_datetime_to_isoformat(self):
        client = self._make_connected_client()
        dt = datetime(2026, 1, 15, 12, 30, 0)
        row = MagicMock()
        row.__getitem__ = lambda self, i: [dt, "val"][i]
        response = self._make_mock_response(rows=[row], column_names=["timestamp", "data"])
        client._client.execute.return_value = response

        result = await client.execute_query("TestTable | take 1")
        assert result.rows[0]["timestamp"] == dt.isoformat()
        assert result.rows[0]["data"] == "val"

    async def test_auto_connects_when_client_is_none(self):
        config = _make_config()
        client = KQLClient(config)
        assert client._client is None

        with (
            patch.object(client, "connect", new_callable=AsyncMock) as mock_connect,
            patch.object(client, "_execute_query_impl", new_callable=AsyncMock) as mock_impl,
        ):
            mock_impl.return_value = KQLQueryResult()
            await client.execute_query("test")
        mock_connect.assert_awaited_once()

    async def test_uses_custom_database(self):
        client = self._make_connected_client()
        response = self._make_mock_response(rows=None)
        client._client.execute.return_value = response

        await client.execute_query("test", database="customdb")
        client._client.execute.assert_called_once_with("customdb", "test")

    async def test_uses_default_database_when_not_specified(self):
        client = self._make_connected_client()
        response = self._make_mock_response(rows=None)
        client._client.execute.return_value = response

        await client.execute_query("test")
        client._client.execute.assert_called_once_with("testdb", "test")

    async def test_kusto_query_error_not_retried(self):
        """KustoQueryError (syntax/semantic) should not be retried."""
        client = self._make_connected_client()
        from azure.kusto.data.exceptions import KustoServiceError

        error = KustoServiceError("Semantic error: something", MagicMock())
        client._client.execute.side_effect = error

        with pytest.raises(KustoQueryError):
            await client.execute_query("bad query")

        # Only one call, no retries
        assert client._client.execute.call_count == 1

    @patch("pipeline.common.eventhouse.kql_client.StorageErrorClassifier")
    async def test_retries_on_transient_error(self, mock_classifier):
        client = self._make_connected_client()

        # First call fails with transient, second succeeds
        transient_error = RuntimeError("connection timeout")
        response = self._make_mock_response(rows=None)
        client._client.execute.side_effect = [transient_error, response]

        classified = KustoError("transient", cause=transient_error)
        classified._is_retryable = True
        mock_classifier.classify_kusto_error.return_value = classified

        result = await client.execute_query("test")
        assert result.is_empty
        assert client._client.execute.call_count == 2

    @patch("pipeline.common.eventhouse.kql_client._refresh_all_credentials")
    @patch("pipeline.common.eventhouse.kql_client.StorageErrorClassifier")
    async def test_refreshes_credentials_on_auth_error(self, mock_classifier, mock_refresh):
        client = self._make_connected_client()

        auth_error = RuntimeError("auth failed")
        response = self._make_mock_response(rows=None)
        client._client.execute.side_effect = [auth_error, response]

        from core.errors.exceptions import ErrorCategory, PipelineError

        classified = PipelineError("auth", cause=auth_error)
        classified.category = ErrorCategory.AUTH
        mock_classifier.classify_kusto_error.return_value = classified

        await client.execute_query("test")
        mock_refresh.assert_called_once()

    @patch("pipeline.common.eventhouse.kql_client.StorageErrorClassifier")
    async def test_raises_after_max_retries_exhausted(self, mock_classifier):
        config = _make_config(max_retries=2)
        client = KQLClient(config)
        client._client = MagicMock()

        transient_error = RuntimeError("always fails")
        client._client.execute.side_effect = transient_error

        classified = KustoError("transient", cause=transient_error)
        mock_classifier.classify_kusto_error.return_value = classified

        with pytest.raises(KustoError):
            await client.execute_query("test")
        assert client._client.execute.call_count == 2

    @patch("pipeline.common.eventhouse.kql_client.StorageErrorClassifier")
    async def test_non_retryable_error_raises_immediately(self, mock_classifier):
        client = self._make_connected_client()

        perm_error = RuntimeError("permanent")
        client._client.execute.side_effect = perm_error

        from core.errors.exceptions import PermanentError

        classified = PermanentError("permanent", cause=perm_error)
        mock_classifier.classify_kusto_error.return_value = classified

        with pytest.raises(PermanentError):
            await client.execute_query("test")
        assert client._client.execute.call_count == 1

    async def test_kusto_service_error_with_syntax_error_raises_query_error(self):
        client = self._make_connected_client()
        from azure.kusto.data.exceptions import KustoServiceError

        error = KustoServiceError("Syntax error in query", MagicMock())
        client._client.execute.side_effect = error

        with pytest.raises(KustoQueryError, match="KQL query error"):
            await client.execute_query("bad syntax")

    async def test_kusto_service_error_without_syntax_reraises(self):
        """Non-syntax KustoServiceError is re-raised for retry handling."""
        client = self._make_connected_client()

        config = _make_config(max_retries=1)
        client.config = config

        from azure.kusto.data.exceptions import KustoServiceError

        error = KustoServiceError("Server is busy", MagicMock())
        client._client.execute.side_effect = error

        with patch("pipeline.common.eventhouse.kql_client.StorageErrorClassifier") as mock_cls:
            classified = KustoError("busy", cause=error)
            mock_cls.classify_kusto_error.return_value = classified
            with pytest.raises(KustoError):
                await client.execute_query("test")

    async def test_logs_success_after_retry(self):
        """When query succeeds after retries, it logs the retry count."""
        client = self._make_connected_client()

        transient_error = RuntimeError("transient")
        response = self._make_mock_response(rows=None)
        client._client.execute.side_effect = [transient_error, response]

        with patch("pipeline.common.eventhouse.kql_client.StorageErrorClassifier") as mock_cls:
            classified = KustoError("transient", cause=transient_error)
            mock_cls.classify_kusto_error.return_value = classified
            result = await client.execute_query("test")

        assert result.is_empty
