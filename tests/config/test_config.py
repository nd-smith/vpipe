import os
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from config.config import (
    MessageConfig,
    _build_cli_parser,
    _build_merged_config_output,
    _build_validation_output,
    _configure_cli_logging,
    _deep_merge,
    _expand_env_vars,
    _handle_cli_error,
    get_config,
    get_config_value,
    load_config,
    load_yaml,
    reset_config,
    set_config,
)

# =========================================================================
# load_yaml
# =========================================================================


class TestLoadYaml:
    def test_returns_empty_dict_for_nonexistent_file(self):
        result = load_yaml(Path("/nonexistent/path/config.yaml"))
        assert result == {}

    def test_loads_yaml_file(self, tmp_path):
        config_file = tmp_path / "test.yaml"
        config_file.write_text("key: value\nnested:\n  a: 1\n")
        result = load_yaml(config_file)
        assert result == {"key": "value", "nested": {"a": 1}}

    def test_returns_empty_dict_for_empty_file(self, tmp_path):
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")
        result = load_yaml(config_file)
        assert result == {}


# =========================================================================
# get_config_value
# =========================================================================


class TestGetConfigValue:
    def test_prefers_env_var_over_yaml(self):
        with patch.dict(os.environ, {"MY_VAR": "from_env"}):
            result = get_config_value("MY_VAR", "from_yaml", "default")
            assert result == "from_env"

    def test_falls_back_to_yaml_value(self):
        with patch.dict(os.environ, {}, clear=True):
            result = get_config_value("MY_VAR", "from_yaml", "default")
            assert result == "from_yaml"

    def test_falls_back_to_default(self):
        with patch.dict(os.environ, {}, clear=True):
            result = get_config_value("MY_VAR", "", "default")
            assert result == "default"

    def test_returns_empty_string_when_all_empty(self):
        with patch.dict(os.environ, {}, clear=True):
            result = get_config_value("MY_VAR", "")
            assert result == ""

    def test_env_var_empty_string_uses_yaml(self):
        # os.getenv returns "" which is falsy, so yaml_value is used
        with patch.dict(os.environ, {"MY_VAR": ""}):
            result = get_config_value("MY_VAR", "from_yaml")
            assert result == "from_yaml"


# =========================================================================
# _expand_env_vars
# =========================================================================


class TestExpandEnvVars:
    def test_expands_simple_variable(self):
        with patch.dict(os.environ, {"MY_VAR": "hello"}):
            result = _expand_env_vars("prefix-${MY_VAR}-suffix")
            assert result == "prefix-hello-suffix"

    def test_expands_variable_with_default(self):
        with patch.dict(os.environ, {}, clear=True):
            result = _expand_env_vars("${MISSING_VAR:-fallback}")
            assert result == "fallback"

    def test_uses_env_value_over_default(self):
        with patch.dict(os.environ, {"MY_VAR": "real_value"}):
            result = _expand_env_vars("${MY_VAR:-fallback}")
            assert result == "real_value"

    def test_expands_in_dict(self):
        with patch.dict(os.environ, {"DB_HOST": "localhost"}):
            result = _expand_env_vars({"host": "${DB_HOST}", "port": 5432})
            assert result == {"host": "localhost", "port": 5432}

    def test_expands_in_list(self):
        with patch.dict(os.environ, {"ITEM": "expanded"}):
            result = _expand_env_vars(["${ITEM}", "static"])
            assert result == ["expanded", "static"]

    def test_returns_non_string_unchanged(self):
        assert _expand_env_vars(42) == 42
        assert _expand_env_vars(True) is True
        assert _expand_env_vars(None) is None

    def test_nested_dict_expansion(self):
        with patch.dict(os.environ, {"VAL": "x"}):
            data = {"a": {"b": {"c": "${VAL}"}}}
            result = _expand_env_vars(data)
            assert result == {"a": {"b": {"c": "x"}}}

    def test_keeps_literal_when_no_env_var(self):
        with patch.dict(os.environ, {}, clear=True):
            # Without default, the ${VAR} stays as-is
            result = _expand_env_vars("${UNDEFINED_VAR}")
            assert result == "${UNDEFINED_VAR}"

    def test_empty_default_value(self):
        with patch.dict(os.environ, {}, clear=True):
            result = _expand_env_vars("${MISSING:-}")
            assert result == ""


# =========================================================================
# _deep_merge
# =========================================================================


class TestDeepMerge:
    def test_merges_flat_dicts(self):
        base = {"a": 1, "b": 2}
        overlay = {"b": 3, "c": 4}
        result = _deep_merge(base, overlay)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_merges_nested_dicts(self):
        base = {"x": {"a": 1, "b": 2}}
        overlay = {"x": {"b": 3, "c": 4}}
        result = _deep_merge(base, overlay)
        assert result == {"x": {"a": 1, "b": 3, "c": 4}}

    def test_overlay_replaces_non_dict_with_non_dict(self):
        base = {"a": 1}
        overlay = {"a": "new"}
        result = _deep_merge(base, overlay)
        assert result == {"a": "new"}

    def test_overlay_replaces_dict_with_non_dict(self):
        base = {"a": {"nested": True}}
        overlay = {"a": "flat"}
        result = _deep_merge(base, overlay)
        assert result == {"a": "flat"}

    def test_does_not_modify_original(self):
        base = {"a": 1}
        overlay = {"b": 2}
        _deep_merge(base, overlay)
        assert base == {"a": 1}


# =========================================================================
# MessageConfig
# =========================================================================


class TestMessageConfig:
    def test_default_values(self):
        config = MessageConfig()
        assert config.bootstrap_servers == ""
        assert config.request_timeout_ms == 120000
        assert config.verisk == {}
        assert config.claimx == {}
        assert config.claimx_api_url == ""
        assert config.claimx_api_timeout_seconds == 30
        assert config.claimx_api_concurrency == 20

    def test_get_domain_config_verisk(self):
        config = MessageConfig(verisk={"event_ingester": {"consumer": {"group_id": "g1"}}})
        result = config._get_domain_config("verisk")
        assert result == {"event_ingester": {"consumer": {"group_id": "g1"}}}

    def test_get_domain_config_claimx(self):
        config = MessageConfig(claimx={"enrichment_worker": {}})
        result = config._get_domain_config("claimx")
        assert result == {"enrichment_worker": {}}

    def test_get_domain_config_unknown_returns_empty_dict(self):
        config = MessageConfig()
        result = config._get_domain_config("plugins")
        assert result == {}

    def test_get_topic(self):
        config = MessageConfig()
        assert config.get_topic("verisk", "events") == "verisk.events"
        assert config.get_topic("claimx", "downloads") == "claimx.downloads"

    def test_get_consumer_group_from_prefix(self):
        config = MessageConfig(
            verisk={
                "consumer_group_prefix": "myprefix",
                "download_worker": {"consumer": {}},
            }
        )
        result = config.get_consumer_group("verisk", "download_worker")
        assert result == "myprefix-download_worker"

    def test_get_consumer_group_default_prefix(self):
        config = MessageConfig(verisk={"download_worker": {"consumer": {}}})
        result = config.get_consumer_group("verisk", "download_worker")
        assert result == "verisk-download_worker"

    def test_get_retry_delays_from_config(self):
        config = MessageConfig(verisk={"retry_delays": [60, 120, 240]})
        assert config.get_retry_delays("verisk") == [60, 120, 240]

    def test_get_retry_delays_defaults(self):
        config = MessageConfig(verisk={})
        assert config.get_retry_delays("verisk") == [300, 600, 1200, 2400]

    def test_get_max_retries(self):
        config = MessageConfig(verisk={"retry_delays": [60, 120, 240]})
        assert config.get_max_retries("verisk") == 3

    def test_get_storage_config(self):
        config = MessageConfig(
            onelake_base_path="/base",
            onelake_domain_paths={"verisk": "/v"},
            cache_dir="/cache",
            temp_dir="/temp",
        )
        storage = config.get_storage_config()
        assert storage["onelake_base_path"] == "/base"
        assert storage["cache_dir"] == "/cache"
        assert storage["temp_dir"] == "/temp"


# =========================================================================
# MessageConfig.validate
# =========================================================================


class TestMessageConfigValidation:
    def test_validates_empty_config(self):
        config = MessageConfig()
        config.validate()  # Should not raise

    def test_rejects_invalid_claimx_api_url(self):
        config = MessageConfig(claimx_api_url="not-a-url")
        with pytest.raises(ValueError, match="claimx_api_url"):
            config.validate()

    def test_accepts_valid_claimx_api_url(self):
        config = MessageConfig(claimx_api_url="https://api.example.com")
        config.validate()

    def test_accepts_http_claimx_api_url(self):
        config = MessageConfig(claimx_api_url="http://localhost:8080")
        config.validate()

    def test_rejects_url_without_netloc(self):
        config = MessageConfig(claimx_api_url="http://")
        with pytest.raises(ValueError, match="missing hostname"):
            config.validate()


# =========================================================================
# load_config
# =========================================================================


class TestLoadConfig:
    def _write_config(self, tmp_path, data):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(data))
        return config_file

    def test_raises_for_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_config(Path("/nonexistent/config.yaml"))

    def test_raises_for_missing_pipeline_section(self, tmp_path):
        config_file = self._write_config(tmp_path, {"not_pipeline": {}})
        with pytest.raises(ValueError, match="missing 'pipeline:'"):
            load_config(config_file)

    def test_loads_minimal_config(self, tmp_path):
        config_file = self._write_config(tmp_path, {"pipeline": {}})
        config = load_config(config_file)
        assert isinstance(config, MessageConfig)
        assert config.verisk == {}
        assert config.claimx == {}

    def test_loads_verisk_domain_config(self, tmp_path):
        data = {"pipeline": {"verisk": {"download_worker": {"processing": {"concurrency": 3}}}}}
        config_file = self._write_config(tmp_path, data)
        config = load_config(config_file)
        assert config.verisk["download_worker"]["processing"]["concurrency"] == 3

    def test_loads_storage_config(self, tmp_path):
        data = {
            "pipeline": {
                "storage": {
                    "onelake_base_path": "/lake",
                    "cache_dir": "/my/cache",
                    "temp_dir": "/my/temp",
                }
            }
        }
        config_file = self._write_config(tmp_path, data)
        config = load_config(config_file)
        assert config.onelake_base_path == "/lake"
        assert config.cache_dir == "/my/cache"
        assert config.temp_dir == "/my/temp"

    def test_applies_overrides(self, tmp_path):
        data = {"pipeline": {"verisk": {"retry_delays": [100, 200]}}}
        config_file = self._write_config(tmp_path, data)
        overrides = {"verisk": {"retry_delays": [10, 20]}}
        config = load_config(config_file, overrides=overrides)
        assert config.verisk["retry_delays"] == [10, 20]

    def test_loads_claimx_api_config_from_yaml(self, tmp_path):
        data = {
            "pipeline": {},
            "claimx": {
                "api": {
                    "base_url": "https://api.test.com",
                    "token": "yaml-token",
                    "timeout_seconds": 60,
                    "max_concurrent": 10,
                }
            },
        }
        config_file = self._write_config(tmp_path, data)
        with patch.dict(os.environ, {}, clear=True):
            config = load_config(config_file)
        assert config.claimx_api_url == "https://api.test.com"
        assert config.claimx_api_token == "yaml-token"
        assert config.claimx_api_timeout_seconds == 60
        assert config.claimx_api_concurrency == 10

    def test_env_vars_override_claimx_api_config(self, tmp_path):
        data = {
            "pipeline": {},
            "claimx": {
                "api": {
                    "base_url": "https://yaml.com",
                    "token": "yaml-token",
                }
            },
        }
        config_file = self._write_config(tmp_path, data)
        env = {
            "CLAIMX_API_URL": "https://env.com",
            "CLAIMX_API_TOKEN": "env-token",
        }
        with patch.dict(os.environ, env, clear=True):
            config = load_config(config_file)
        assert config.claimx_api_url == "https://env.com"
        assert config.claimx_api_token == "env-token"

    def test_loads_logging_config(self, tmp_path):
        data = {
            "pipeline": {},
            "logging": {
                "level": "DEBUG",
                "log_to_stdout": True,
            },
        }
        config_file = self._write_config(tmp_path, data)
        config = load_config(config_file)
        assert config.logging_config["level"] == "DEBUG"
        assert config.logging_config["log_to_stdout"] is True

    def test_expands_env_vars_in_yaml(self, tmp_path):
        data = {"pipeline": {"storage": {"onelake_base_path": "${TEST_LAKE_PATH}"}}}
        config_file = self._write_config(tmp_path, data)
        with patch.dict(os.environ, {"TEST_LAKE_PATH": "/expanded/path"}, clear=True):
            config = load_config(config_file)
        assert config.onelake_base_path == "/expanded/path"


# =========================================================================
# Singleton: get_config / set_config / reset_config
# =========================================================================


class TestConfigSingleton:
    def setup_method(self):
        reset_config()

    def teardown_method(self):
        reset_config()

    def test_set_config_and_get_config(self):
        custom = MessageConfig(bootstrap_servers="test:9092")
        set_config(custom)
        result = get_config()
        assert result.bootstrap_servers == "test:9092"

    def test_reset_config_clears_singleton(self):
        custom = MessageConfig(bootstrap_servers="test:9092")
        set_config(custom)
        reset_config()
        # After reset, get_config would try to load from default path
        # which may not exist, so we just verify the reset happened
        from config import config as config_module

        assert config_module._message_config is None


# =========================================================================
# CLI Helper Functions
# =========================================================================


class TestBuildCliParser:
    def test_parser_created(self):
        parser = _build_cli_parser()
        assert parser is not None

    def test_list_subcommand(self):
        parser = _build_cli_parser()
        args = parser.parse_args(["--validate"])
        assert args.validate is True
        assert args.show_merged is False

    def test_show_merged_flag(self):
        parser = _build_cli_parser()
        args = parser.parse_args(["--show-merged"])
        assert args.show_merged is True

    def test_json_flag(self):
        parser = _build_cli_parser()
        args = parser.parse_args(["--validate", "--json"])
        assert args.json is True

    def test_verbose_flag(self):
        parser = _build_cli_parser()
        args = parser.parse_args(["--validate", "-v"])
        assert args.verbose is True

    def test_config_path(self):
        parser = _build_cli_parser()
        args = parser.parse_args(["--config", "/path/to/config.yaml", "--validate"])
        assert args.config == Path("/path/to/config.yaml")


class TestConfigureCliLogging:
    def test_verbose(self):
        _configure_cli_logging(verbose=True)  # should not raise

    def test_non_verbose(self):
        _configure_cli_logging(verbose=False)  # should not raise


class TestBuildValidationOutput:
    def test_json_output(self):
        config = MessageConfig(bootstrap_servers="test:9092")
        result = _build_validation_output(config, json_output=True)
        assert result["validation"]["passed"] is True
        assert result["validation"]["errors"] == []

    def test_human_output(self, capsys):
        config = MessageConfig(bootstrap_servers="test:9092")
        result = _build_validation_output(config, json_output=False)
        assert result == {}
        out = capsys.readouterr().out
        assert "Configuration validation passed" in out


class TestBuildMergedConfigOutput:
    def test_json_output(self):
        config_dict = {"key": "value"}
        result = _build_merged_config_output(config_dict, json_output=True)
        assert result == {"merged_config": {"key": "value"}}

    def test_human_output(self, capsys):
        config_dict = {"key": "value"}
        result = _build_merged_config_output(config_dict, json_output=False)
        assert result == {}
        out = capsys.readouterr().out
        assert "Configuration:" in out
        assert "key: value" in out


class TestHandleCliError:
    def test_json_output(self, capsys):
        _handle_cli_error(ValueError("test error"), json_output=True, verbose=False)
        out = capsys.readouterr().out
        assert '"error": "test error"' in out

    def test_human_output(self, capsys):
        _handle_cli_error(ValueError("test error"), json_output=False, verbose=False)
        err = capsys.readouterr().err
        assert "test error" in err

    def test_verbose_unexpected_error(self, capsys):
        _handle_cli_error(
            RuntimeError("boom"),
            json_output=False,
            verbose=True,
            label="Unexpected error",
        )
        err = capsys.readouterr().err
        assert "boom" in err


class TestMessageConfigGetRetryTopic:
    def test_get_retry_topic(self):
        config = MessageConfig(
            bootstrap_servers="test:9092",
            verisk={"topics": {"retry": "verisk.retry"}},
        )
        assert config.get_retry_topic("verisk") == "verisk.retry"


class TestMessageConfigValidateDirectories:
    def test_validate_writable_cache_dir(self, tmp_path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        config = MessageConfig(
            bootstrap_servers="test:9092",
            cache_dir=str(cache_dir),
        )
        config._validate_directories()  # should not raise

    def test_validate_nonexistent_cache_dir_ok(self):
        config = MessageConfig(
            bootstrap_servers="test:9092",
            cache_dir="/tmp/nonexistent_cache_dir_12345",
        )
        config._validate_directories()  # should not raise (doesn't exist)
