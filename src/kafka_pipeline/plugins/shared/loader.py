"""
Plugin configuration loader.

Loads plugin configurations from YAML files and registers them.
"""

import importlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import yaml

from core.logging import log_with_context

from kafka_pipeline.plugins.shared.base import Plugin, Domain, PipelineStage
from kafka_pipeline.plugins.shared.registry import PluginRegistry, get_plugin_registry
from kafka_pipeline.plugins.shared.task_trigger import TaskTriggerPlugin

logger = logging.getLogger(__name__)


def load_plugins_from_directory(
    plugins_dir: str,
    registry: Optional[PluginRegistry] = None,
) -> List[Plugin]:
    """
    Load all plugins from a directory structure.

    Scans plugins_dir for subdirectories, each representing a plugin.
    Each plugin directory should contain a config.yaml file.

    Directory structure:
        config/plugins/
            shared/
                task_trigger/
                    config.yaml
                    README.md (optional)
            plugin_name/
                config.yaml
                README.md (optional)

    Args:
        plugins_dir: Path to plugins directory (e.g., "config/plugins")
        registry: Registry to register plugins to (uses global if not provided)

    Returns:
        List of loaded plugins
    """
    registry = registry or get_plugin_registry()
    plugins_path = Path(plugins_dir)

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

    # Recursively scan for config.yaml files in all subdirectories
    # This supports nested directory structures like:
    #   plugins/claimx/my_plugin/config.yaml
    #   plugins/xact/status_trigger/config.yaml
    config_files = list(plugins_path.rglob("config.yaml"))
    config_files.extend(plugins_path.rglob("plugin.yaml"))

    # Skip shared/connections directory (contains connection configs, not plugins)
    config_files = [
        f for f in config_files
        if "connections" not in f.parts
    ]

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
    registry: Optional[PluginRegistry] = None,
) -> List[Plugin]:
    """
    Load plugin configurations from YAML file.

    Supports two formats:
    1. New generic format with 'plugins' list (module/class specification)
    2. Legacy 'task_triggers' format

    Args:
        config_path: Path to YAML configuration file
        registry: Registry to register plugins to (uses global if not provided)

    Returns:
        List of loaded plugins
    """
    registry = registry or get_plugin_registry()
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

    # Load plugins from new generic format
    plugins_config = config.get("plugins", [])
    for plugin_config in plugins_config:
        plugin = _create_plugin_from_config(plugin_config)
        if plugin:
            registry.register(plugin)
            plugins_loaded.append(plugin)

    # Load legacy task triggers (backwards compatibility)
    task_triggers = config.get("task_triggers", [])
    for trigger_config in task_triggers:
        plugin = _create_task_trigger_from_config(trigger_config)
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


def _create_plugin_from_config(config: Dict[str, Any]) -> Optional[Plugin]:
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
        plugin_class: Type[Plugin] = getattr(module, class_name)

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


def _create_task_trigger_from_config(config: Dict[str, Any]) -> Optional[TaskTriggerPlugin]:
    """
    Create TaskTriggerPlugin from YAML config.

    Expected format:
        name: my_task_trigger
        description: "Triggers on specific tasks"
        enabled: true
        triggers:
          - task_id: 456
            name: "Photo Documentation"
            on_assigned:
              publish_to_topic: task-456-assigned
            on_completed:
              publish_to_topic: task-456-completed
              webhook: https://api.example.com/notify
    """
    if not config.get("enabled", True):
        return None

    name = config.get("name", "task_trigger")
    description = config.get("description", "")

    # Convert triggers list to dict keyed by task_id
    triggers_dict = {}
    for trigger in config.get("triggers", []):
        task_id = trigger.get("task_id")
        if task_id is not None:
            triggers_dict[task_id] = {
                "name": trigger.get("name", f"Task {task_id}"),
                "on_assigned": trigger.get("on_assigned"),
                "on_completed": trigger.get("on_completed"),
                "on_any": trigger.get("on_any"),
            }

    plugin = TaskTriggerPlugin(config={
        "triggers": triggers_dict,
        "include_task_data": config.get("include_task_data", True),
        "include_project_data": config.get("include_project_data", False),
    })

    # Override metadata
    plugin.name = name
    plugin.description = description

    return plugin


def load_plugins_from_config(
    main_config: Dict[str, Any],
    registry: Optional[PluginRegistry] = None,
) -> List[Plugin]:
    """
    Load plugins from main application config dict.

    Looks for 'plugins' section in the config.

    Args:
        main_config: Main application configuration dict
        registry: Registry to use (global if not provided)

    Returns:
        List of loaded plugins
    """
    registry = registry or get_plugin_registry()
    plugins_config = main_config.get("plugins", {})

    if not plugins_config.get("enabled", True):
        log_with_context(
            logger,
            logging.INFO,
            "Plugins disabled in config",
        )
        return []

    plugins_loaded = []

    # Load from external YAML if specified
    yaml_path = plugins_config.get("config_path")
    if yaml_path:
        plugins_loaded.extend(load_plugins_from_yaml(yaml_path, registry))

    # Load inline task triggers
    task_triggers = plugins_config.get("task_triggers", [])
    for trigger_config in task_triggers:
        plugin = _create_task_trigger_from_config(trigger_config)
        if plugin:
            registry.register(plugin)
            plugins_loaded.append(plugin)

    return plugins_loaded
