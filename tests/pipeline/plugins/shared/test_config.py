"""Tests for shared plugin configuration loading."""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from pipeline.plugins.shared.config import load_connections, load_yaml_config


@pytest.fixture
def tmp_yaml(tmp_path):
    """Helper to write a YAML file and return its path."""

    def _write(data: dict) -> Path:
        p = tmp_path / "connections.yaml"
        p.write_text(yaml.dump(data))
        return p

    return _write


class TestLoadYamlConfig:
    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_yaml_config(tmp_path / "nonexistent.yaml")

    def test_loads_valid_yaml(self, tmp_yaml):
        path = tmp_yaml({"key": "value"})
        assert load_yaml_config(path) == {"key": "value"}

    def test_empty_file_returns_empty_dict(self, tmp_path):
        p = tmp_path / "empty.yaml"
        p.write_text("")
        assert load_yaml_config(p) == {}


class TestLoadConnections:
    def _minimal_connection(self, **overrides):
        """Return a minimal valid connection config dict."""
        conn = {
            "name": "test_conn",
            "base_url": "https://api.example.com",
        }
        conn.update(overrides)
        return {"connections": {"test_conn": conn}}

    def test_expands_endpoint_env_var(self, tmp_yaml):
        data = self._minimal_connection(endpoint="${TEST_ENDPOINT:-/v1/default}")
        path = tmp_yaml(data)

        with patch.dict("os.environ", {}, clear=False):
            conns = load_connections(path)

        assert len(conns) == 1
        assert conns[0].endpoint == "/v1/default"

    def test_expands_endpoint_env_var_from_env(self, tmp_yaml):
        data = self._minimal_connection(endpoint="${TEST_ENDPOINT:-/v1/default}")
        path = tmp_yaml(data)

        with patch.dict("os.environ", {"TEST_ENDPOINT": "/v2/custom"}, clear=False):
            conns = load_connections(path)

        assert conns[0].endpoint == "/v2/custom"

    def test_expands_method_env_var(self, tmp_yaml):
        data = self._minimal_connection(method="${TEST_METHOD:-POST}")
        path = tmp_yaml(data)

        with patch.dict("os.environ", {}, clear=False):
            conns = load_connections(path)

        assert conns[0].method == "POST"

    def test_expands_method_env_var_from_env(self, tmp_yaml):
        data = self._minimal_connection(method="${TEST_METHOD:-POST}")
        path = tmp_yaml(data)

        with patch.dict("os.environ", {"TEST_METHOD": "PUT"}, clear=False):
            conns = load_connections(path)

        assert conns[0].method == "PUT"

    def test_literal_method_without_env_var(self, tmp_yaml):
        data = self._minimal_connection(method="PATCH")
        path = tmp_yaml(data)

        conns = load_connections(path)
        assert conns[0].method == "PATCH"

    def test_literal_endpoint_without_env_var(self, tmp_yaml):
        data = self._minimal_connection(endpoint="/v1/requests")
        path = tmp_yaml(data)

        conns = load_connections(path)
        assert conns[0].endpoint == "/v1/requests"

    def test_default_method_when_omitted(self, tmp_yaml):
        data = self._minimal_connection()
        path = tmp_yaml(data)

        conns = load_connections(path)
        assert conns[0].method == "POST"

    def test_default_endpoint_when_omitted(self, tmp_yaml):
        data = self._minimal_connection()
        path = tmp_yaml(data)

        conns = load_connections(path)
        assert conns[0].endpoint == ""

    def test_unexpanded_base_url_raises(self, tmp_yaml):
        data = self._minimal_connection(base_url="${MISSING_VAR}")
        path = tmp_yaml(data)

        with patch.dict("os.environ", {}, clear=False):
            with pytest.raises(ValueError, match="not expanded"):
                load_connections(path)

    def test_skips_connection_without_base_url(self, tmp_yaml):
        data = {"connections": {"bad": {"name": "bad"}}}
        path = tmp_yaml(data)

        conns = load_connections(path)
        assert conns == []
