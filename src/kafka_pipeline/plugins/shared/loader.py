"""
Plugin configuration loader.

Loads plugin configurations from YAML files and registers them.
"""

import importlib
import logging
from pathlib import Path
from typing import Any

import yaml

from core.logging import log_with_context
from kafka_pipeline.plugins.shared.base import Plugin
from kafka_pipeline.plugins.shared.registry import PluginRegistry

logger = logging.getLogger(__name__)


def load_plugins_from_directory(
    plugins_dir: str,
    registry: PluginRegistry,
    exclude_dirs: set[str] = None,
) -> list[Plugin]:
    """
    Load all plugins from a directory structure.

    Each plugin directory should contain a config.yaml or plugin.yaml file.

    Args:
        plugins_dir: Path to plugins directory
        registry: Registry to register plugins to
        exclude_dirs: Directory names to exclude (default: {"connections"})

    Returns:
        List of loaded plugins
    """
    plugins_path = Path(plugins_dir)
    exclude_dirs = exclude_dirs or {"connections"}

    if not plugins_path.exists():
        log_with_context(
            logger,
            logging.WARNING,
            "Plugins directory not found",
            plugins_dir=plugins_dir,
        )
        return []

    if not plugins_path.is_dir():
        log_with_context(
            logger,
            logging.WARNING,
            "Plugins path is not a directory",
            plugins_dir=plugins_dir,
        )
        return []

    plugins_loaded = []

    config_files = []
    for pattern in ("config.yaml", "plugin.yaml"):
        for config_file in plugins_path.rglob(pattern):
            if not any(excluded in config_file.parts for excluded in exclude_dirs):
                config_files.append(config_file)

    if not config_files:
        log_with_context(
            logger,
            logging.DEBUG,
            "No plugin config files found in directory",
            plugins_dir=plugins_dir,
        )
        return []

    for config_file in config_files:
        plugin_dir = config_file.parent

        # Load plugin from config file
        try:
            with open(config_file) as f:
                plugin_config = yaml.safe_load(f)

            if plugin_config is None:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "Empty plugin config file",
                    config_file=str(config_file),
                )
                continue

            plugin = _create_plugin_from_config(plugin_config)
            if plugin:
                registry.register(plugin)
                plugins_loaded.append(plugin)
                log_with_context(
                    logger,
                    logging.INFO,
                    "Loaded plugin from directory",
                    plugin_name=plugin.name,
                    plugin_dir=plugin_dir.name,
                    config_file=str(config_file),
                )

        except Exception as e:
            log_with_context(
                logger,
                logging.ERROR,
                "Failed to load plugin from directory",
                plugin_dir=str(plugin_dir),
                config_file=str(config_file),
                error=str(e),
            )
            # Continue loading other plugins

    log_with_context(
        logger,
        logging.INFO,
        "Loaded plugins from directory",
        plugins_dir=plugins_dir,
        plugins_loaded=len(plugins_loaded),
        plugin_names=[p.name for p in plugins_loaded],
    )

    return plugins_loaded


def load_plugins_from_yaml(
    config_path: str,
    registry: PluginRegistry,
) -> list[Plugin]:
    """Load plugin configurations from YAML file."""
    path = Path(config_path)

    if not path.exists():
        log_with_context(
            logger,
            logging.WARNING,
            "Plugin config file not found",
            config_path=config_path,
        )
        return []

    with open(path) as f:
        config = yaml.safe_load(f)

    if config is None:
        log_with_context(
            logger,
            logging.WARNING,
            "Empty plugin config file",
            config_path=config_path,
        )
        return []

    plugins_loaded = []
    plugins_config = config.get("plugins", [])
    for plugin_config in plugins_config:
        plugin = _create_plugin_from_config(plugin_config)
        if plugin:
            registry.register(plugin)
            plugins_loaded.append(plugin)

    log_with_context(
        logger,
        logging.INFO,
        "Loaded plugins from config",
        config_path=config_path,
        plugins_loaded=len(plugins_loaded),
        plugin_names=[p.name for p in plugins_loaded],
    )

    return plugins_loaded


def _create_plugin_from_config(config: dict[str, Any]) -> Plugin | None:
    """
    Create plugin instance from generic YAML config.

    Expected format:
        name: my_plugin
        module: kafka_pipeline.plugins.task_trigger
        class: TaskTriggerPlugin
        enabled: true
        priority: 50
        config:
          triggers:
            456:
              name: "Photo Documentation"
              on_completed:
                publish_to_topic: task-456-completed

    Args:
        config: Plugin configuration dict

    Returns:
        Plugin instance or None if disabled or error
    """
    if not config.get("enabled", True):
        log_with_context(
            logger,
            logging.DEBUG,
            "Plugin disabled in config",
            plugin_name=config.get("name", "unknown"),
        )
        return None

    try:
        # Get module and class
        module_name = config.get("module")
        class_name = config.get("class")

        if not module_name or not class_name:
            log_with_context(
                logger,
                logging.ERROR,
                "Plugin config missing module or class",
                plugin_name=config.get("name", "unknown"),
            )
            return None

        # Import module and get class
        module = importlib.import_module(module_name)
        plugin_class: type[Plugin] = getattr(module, class_name)

        # Create plugin instance with config
        plugin_config = config.get("config", {})
        plugin = plugin_class(config=plugin_config)

        # Override metadata if specified
        if "name" in config:
            plugin.name = config["name"]
        if "priority" in config:
            plugin.priority = config["priority"]

        log_with_context(
            logger,
            logging.DEBUG,
            "Created plugin from config",
            plugin_name=plugin.name,
            plugin_class=class_name,
            plugin_module=module_name,
        )

        return plugin

    except (ImportError, AttributeError) as e:
        log_with_context(
            logger,
            logging.ERROR,
            "Failed to import plugin class",
            plugin_name=config.get("name", "unknown"),
            module=config.get("module"),
            class_name=config.get("class"),
            error=str(e),
        )
        return None
    except Exception as e:
        log_with_context(
            logger,
            logging.ERROR,
            "Failed to create plugin instance",
            plugin_name=config.get("name", "unknown"),
            error=str(e),
        )
        return None
