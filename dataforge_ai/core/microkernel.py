from dataforge_ai.core.plugin_interface import PluginInterface
from typing import Dict, Any
import logging

class Microkernel:
    def __init__(self):
        self.plugins: Dict[str, PluginInterface] = {}
        self.logger = logging.getLogger(__name__)

    def register_plugin(self, name: str, plugin: PluginInterface):
        """
        Register a plugin with the Microkernel.

        :param name: The name of the plugin.
        :param plugin: An instance of the plugin.
        """
        if not isinstance(plugin, PluginInterface):
            raise TypeError(f"Plugin {name} must implement PluginInterface")
        self.plugins[name] = plugin
        self.logger.info(f"Plugin '{name}' registered successfully.")

    def execute_pipeline(self, pipeline_config: Dict[str, Any]) -> Any:
        """
        Execute the pipeline based on the provided configuration.

        :param pipeline_config: A dictionary containing the pipeline configuration.
        :return: The result of the pipeline execution.
        """
        self.logger.info("Starting pipeline execution.")
        steps = pipeline_config.get('steps', [])
        context = {}

        for step in steps:
            plugin_name = step.get('plugin')
            plugin_input = step.get('input')

            if plugin_name not in self.plugins:
                raise ValueError(f"Plugin '{plugin_name}' not registered.")

            plugin = self.plugins[plugin_name]
            self.logger.info(f"Executing plugin '{plugin_name}'.")

            try:
                # Replace any references to previous step outputs
                if isinstance(plugin_input, str) and plugin_input.startswith('$'):
                    key = plugin_input[1:]
                    if key in context:
                        plugin_input = context[key]
                    else:
                        raise ValueError(f"Referenced output '{key}' not found in context.")

                result = plugin.execute(plugin_input)
                context[plugin_name] = result
                self.logger.info(f"Plugin '{plugin_name}' executed successfully.")
            except Exception as e:
                self.logger.error(f"Error executing plugin '{plugin_name}': {str(e)}")
                raise

        self.logger.info("Pipeline execution completed.")
        return context

    def get_plugin(self, name: str) -> PluginInterface:
        """
        Get a registered plugin by name.

        :param name: The name of the plugin.
        :return: The plugin instance.
        """
        if name not in self.plugins:
            raise ValueError(f"Plugin '{name}' not registered.")
        return self.plugins[name]

    def list_plugins(self) -> Dict[str, PluginInterface]:
        """
        Get a dictionary of all registered plugins.

        :return: A dictionary of plugin names and their instances.
        """
        return self.plugins
