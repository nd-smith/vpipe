"""
Tests for plugin loader.

Ensures plugins are loaded correctly from nested directory structures.
"""

import pytest
import tempfile
import os
from pathlib import Path

from pipeline.plugins.shared.loader import load_plugins_from_directory
from pipeline.plugins.shared.registry import PluginRegistry, reset_plugin_registry


@pytest.fixture
def registry():
    """Fresh registry for each test."""
    reset_plugin_registry()
    return PluginRegistry()


@pytest.fixture
def nested_plugin_dir(tmp_path):
    """
    Create a temporary nested plugin directory structure like:

    plugins/
        domain1/
            plugin_a/
                config.yaml
            plugin_b/
                config.yaml
        domain2/
            plugin_c/
                config.yaml
        shared/
            connections/
                claimx.yaml  (should be skipped)
    """
    plugins_dir = tmp_path / "plugins"

    # Create domain1/plugin_a
    plugin_a_dir = plugins_dir / "domain1" / "plugin_a"
    plugin_a_dir.mkdir(parents=True)
    (plugin_a_dir / "config.yaml").write_text("""
name: plugin_a
module: pipeline.plugins.shared.task_trigger
class: TaskTriggerPlugin
enabled: true
priority: 10
config:
  triggers:
    123:
      name: "Test Trigger A"
      on_assigned:
        publish_to_topic: test-topic-a
""")

    # Create domain1/plugin_b
    plugin_b_dir = plugins_dir / "domain1" / "plugin_b"
    plugin_b_dir.mkdir(parents=True)
    (plugin_b_dir / "config.yaml").write_text("""
name: plugin_b
module: pipeline.plugins.shared.task_trigger
class: TaskTriggerPlugin
enabled: true
priority: 20
config:
  triggers:
    456:
      name: "Test Trigger B"
      on_completed:
        publish_to_topic: test-topic-b
""")

    # Create domain2/plugin_c
    plugin_c_dir = plugins_dir / "domain2" / "plugin_c"
    plugin_c_dir.mkdir(parents=True)
    (plugin_c_dir / "config.yaml").write_text("""
name: plugin_c
module: pipeline.plugins.shared.task_trigger
class: TaskTriggerPlugin
enabled: true
config:
  triggers:
    789:
      name: "Test Trigger C"
      on_any:
        log: "Task triggered"
""")

    # Create shared/connections directory (should be skipped)
    connections_dir = plugins_dir / "shared" / "connections"
    connections_dir.mkdir(parents=True)
    (connections_dir / "config.yaml").write_text("""
# This is NOT a plugin config - it's a connection config
# and should be skipped by the loader
connections:
  test_api:
    name: test_api
    base_url: https://api.example.com
""")

    return plugins_dir


class TestLoadPluginsFromDirectory:
    """Tests for load_plugins_from_directory function."""

    def test_loads_plugins_from_nested_directories(self, nested_plugin_dir, registry):
        """Test that plugins are loaded from nested subdirectories."""
        plugins = load_plugins_from_directory(str(nested_plugin_dir), registry)

        assert len(plugins) == 3
        plugin_names = {p.name for p in plugins}
        assert plugin_names == {"plugin_a", "plugin_b", "plugin_c"}

    def test_skips_connections_directory(self, nested_plugin_dir, registry):
        """Test that shared/connections directory is skipped."""
        plugins = load_plugins_from_directory(str(nested_plugin_dir), registry)

        # Should not have loaded the connections/config.yaml
        plugin_names = {p.name for p in plugins}
        assert "connections" not in plugin_names
        # Verify we still have 3 plugins
        assert len(plugins) == 3

    def test_returns_empty_list_for_nonexistent_directory(self, registry):
        """Test returns empty list when directory doesn't exist."""
        plugins = load_plugins_from_directory("/nonexistent/path", registry)
        assert plugins == []

    def test_returns_empty_list_for_empty_directory(self, tmp_path, registry):
        """Test returns empty list when directory has no plugins."""
        empty_dir = tmp_path / "empty_plugins"
        empty_dir.mkdir()

        plugins = load_plugins_from_directory(str(empty_dir), registry)
        assert plugins == []

    def test_skips_disabled_plugins(self, tmp_path, registry):
        """Test that disabled plugins are skipped."""
        plugins_dir = tmp_path / "plugins"
        plugin_dir = plugins_dir / "disabled_plugin"
        plugin_dir.mkdir(parents=True)

        (plugin_dir / "config.yaml").write_text("""
name: disabled_plugin
module: pipeline.plugins.shared.task_trigger
class: TaskTriggerPlugin
enabled: false
config: {}
""")

        plugins = load_plugins_from_directory(str(plugins_dir), registry)
        assert len(plugins) == 0

    def test_registers_plugins_in_registry(self, nested_plugin_dir, registry):
        """Test that plugins are registered in the provided registry."""
        load_plugins_from_directory(str(nested_plugin_dir), registry)

        registered = registry.list_plugins()
        assert len(registered) == 3

        registered_names = {p.name for p in registered}
        assert registered_names == {"plugin_a", "plugin_b", "plugin_c"}

    def test_handles_empty_config_file(self, tmp_path, registry):
        """Test that empty config files are handled gracefully."""
        plugins_dir = tmp_path / "plugins"
        plugin_dir = plugins_dir / "empty_config"
        plugin_dir.mkdir(parents=True)

        (plugin_dir / "config.yaml").write_text("")

        plugins = load_plugins_from_directory(str(plugins_dir), registry)
        assert plugins == []

    def test_handles_malformed_config_file(self, tmp_path, registry):
        """Test that malformed config files don't crash the loader."""
        plugins_dir = tmp_path / "plugins"

        # Create valid plugin
        valid_dir = plugins_dir / "valid_plugin"
        valid_dir.mkdir(parents=True)
        (valid_dir / "config.yaml").write_text("""
name: valid_plugin
module: pipeline.plugins.shared.task_trigger
class: TaskTriggerPlugin
enabled: true
config:
  triggers: {}
""")

        # Create malformed plugin (missing required fields)
        malformed_dir = plugins_dir / "malformed_plugin"
        malformed_dir.mkdir(parents=True)
        (malformed_dir / "config.yaml").write_text("""
name: malformed_plugin
# Missing module and class
enabled: true
""")

        plugins = load_plugins_from_directory(str(plugins_dir), registry)

        # Should load the valid one and skip the malformed one
        assert len(plugins) == 1
        assert plugins[0].name == "valid_plugin"


class TestActualPluginDirectory:
    """Integration test with actual plugin directory."""

    def test_loads_actual_claimx_plugins(self, registry):
        """Test loading actual ClaimX plugins from config directory."""
        plugins_dir = "src/config/plugins"

        if not os.path.exists(plugins_dir):
            pytest.skip("Plugin directory not found")

        plugins = load_plugins_from_directory(plugins_dir, registry)

        # Should load the ClaimX plugins
        plugin_names = {p.name for p in plugins}

        # At minimum, should find the known plugins
        expected_plugins = {"claimx_mitigation_task", "itel_cabinet_api"}
        loaded_expected = expected_plugins & plugin_names

        assert len(loaded_expected) > 0, f"Expected to load some of {expected_plugins}, but got {plugin_names}"
