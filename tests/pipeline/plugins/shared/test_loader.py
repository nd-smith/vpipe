"""Tests for pipeline.plugins.shared.loader module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from pipeline.plugins.shared.loader import (
    _create_plugin_from_config,
    load_plugins_from_directory,
    load_plugins_from_yaml,
)
from pipeline.plugins.shared.registry import PluginRegistry


class TestLoadPluginsFromDirectory:
    def test_nonexistent_directory(self, tmp_path):
        registry = MagicMock(spec=PluginRegistry)
        result = load_plugins_from_directory(str(tmp_path / "nonexistent"), registry)
        assert result == []

    def test_path_is_file_not_dir(self, tmp_path):
        file_path = tmp_path / "file.txt"
        file_path.write_text("not a dir")
        registry = MagicMock(spec=PluginRegistry)
        result = load_plugins_from_directory(str(file_path), registry)
        assert result == []

    def test_no_config_files_found(self, tmp_path):
        (tmp_path / "subdir").mkdir()
        registry = MagicMock(spec=PluginRegistry)
        result = load_plugins_from_directory(str(tmp_path), registry)
        assert result == []

    def test_empty_config_file_skipped(self, tmp_path):
        plugin_dir = tmp_path / "my_plugin"
        plugin_dir.mkdir()
        (plugin_dir / "config.yaml").write_text("")
        registry = MagicMock(spec=PluginRegistry)
        result = load_plugins_from_directory(str(tmp_path), registry)
        assert result == []

    def test_config_loading_error_continues(self, tmp_path):
        plugin_dir = tmp_path / "bad_plugin"
        plugin_dir.mkdir()
        (plugin_dir / "config.yaml").write_text("invalid: yaml: {{[")
        registry = MagicMock(spec=PluginRegistry)
        result = load_plugins_from_directory(str(tmp_path), registry)
        assert result == []


class TestLoadPluginsFromYaml:
    def test_file_not_found(self, tmp_path):
        registry = MagicMock(spec=PluginRegistry)
        result = load_plugins_from_yaml(str(tmp_path / "nonexistent.yaml"), registry)
        assert result == []

    def test_empty_file(self, tmp_path):
        config_file = tmp_path / "plugins.yaml"
        config_file.write_text("")
        registry = MagicMock(spec=PluginRegistry)
        result = load_plugins_from_yaml(str(config_file), registry)
        assert result == []

    def test_no_plugins_key(self, tmp_path):
        config_file = tmp_path / "plugins.yaml"
        config_file.write_text(yaml.dump({"other": "stuff"}))
        registry = MagicMock(spec=PluginRegistry)
        result = load_plugins_from_yaml(str(config_file), registry)
        assert result == []

    def test_disabled_plugin_skipped(self, tmp_path):
        config_file = tmp_path / "plugins.yaml"
        config_file.write_text(
            yaml.dump(
                {
                    "plugins": [
                        {
                            "name": "disabled_plugin",
                            "module": "some.module",
                            "class": "SomeClass",
                            "enabled": False,
                        }
                    ]
                }
            )
        )
        registry = MagicMock(spec=PluginRegistry)
        result = load_plugins_from_yaml(str(config_file), registry)
        assert result == []


class TestCreatePluginFromConfig:
    def test_disabled_plugin(self):
        result = _create_plugin_from_config({"name": "test", "enabled": False})
        assert result is None

    def test_missing_module(self):
        result = _create_plugin_from_config({"name": "test", "class": "Foo"})
        assert result is None

    def test_missing_class(self):
        result = _create_plugin_from_config({"name": "test", "module": "some.mod"})
        assert result is None

    def test_import_error(self):
        result = _create_plugin_from_config(
            {"name": "test", "module": "nonexistent.module", "class": "Foo"}
        )
        assert result is None

    def test_attribute_error(self):
        # Module exists but class doesn't
        result = _create_plugin_from_config(
            {"name": "test", "module": "os", "class": "NonExistentClass"}
        )
        assert result is None

    def test_creation_error(self):
        # Module/class exists but constructor fails
        result = _create_plugin_from_config(
            {"name": "test", "module": "os.path", "class": "join"}
        )
        assert result is None
